using System.Text.Json;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Aggregator.Api.Services;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Aggregator.Api.BackgroundServices;

/// <summary>
/// Фоновый сервис для потребления тиков из Kafka и сохранения в БД.
/// </summary>
public class KafkaToDbConsumer : BackgroundService
{
    private readonly IMetricsService _metrics;
    private readonly IConsumer<Null, string> _consumer;
    private readonly ITickRepository _repository;
    private readonly TickRetryQueue _retryQueue;
    private readonly ILogger<KafkaToDbConsumer> _logger;

    public KafkaToDbConsumer(
        IMetricsService metrics,
        IConfiguration configuration,
        ITickRepository repository,
        TickRetryQueue retryQueue,
        ILogger<KafkaToDbConsumer> logger)
    {
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _retryQueue = retryQueue ?? throw new ArgumentNullException(nameof(retryQueue));
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var bootstrapServers = configuration.GetValue<string>("Kafka:BootstrapServers");

        var config = new ConsumerConfig
        {
            GroupId = "aggregator-db-writer",
            BootstrapServers = bootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        _consumer = new ConsumerBuilder<Null, string>(config).Build();

        _consumer.Subscribe("ticks");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(stoppingToken);
                var tick = JsonSerializer.Deserialize<NormalizedTick>(result.Message.Value!);

                if (tick == null) continue;

                await _repository.SaveAsync(tick, stoppingToken);

                _metrics.AddSavedToDb(1);
                _consumer.Commit();
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[DB Error] Failed to save tick. Adding to retry queue.");

                // Пытаемся извлечь tick из сообщения и добавить в очередь повторных попыток
                try
                {
                    var result = _consumer.Consume(TimeSpan.Zero);
                    if (result != null && result.Message.Value != null)
                    {
                        var tick = JsonSerializer.Deserialize<NormalizedTick>(result.Message.Value);
                        if (tick != null)
                        {
                            _retryQueue.Enqueue(tick);
                            _consumer.Commit();
                        }
                    }
                }
                catch (Exception retryEx)
                {
                    _logger.LogError(retryEx, "[DB Error] Could not extract tick for retry queue.");
                }
            }
        }
    }
}

