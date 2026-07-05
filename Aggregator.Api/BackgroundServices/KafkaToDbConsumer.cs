using System.Text.Json;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Confluent.Kafka;

namespace Aggregator.Api.BackgroundServices;

/// <summary>
/// Фоновый сервис для потребления тиков из Kafka и сохранения в БД с батчингом.
/// </summary>
public class KafkaToDbConsumer : BackgroundService
{
    private readonly IMetricsService _metrics;
    private readonly IConsumer<Null, string> _consumer;
    private readonly ITickRepository _repository;
    private readonly TickRetryQueue _retryQueue;
    private readonly ILogger<KafkaToDbConsumer> _logger;
    private readonly int _minBatchSize;
    private readonly int _maxBatchSize;
    private readonly TimeSpan _batchInterval;

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

        _minBatchSize = configuration.GetValue<int>("Kafka:MinBatchSize", 100);
        _maxBatchSize = configuration.GetValue<int>("Kafka:MaxBatchSize", 5000);
        _batchInterval = configuration.GetValue<TimeSpan>("Kafka:BatchInterval", TimeSpan.FromSeconds(5));

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
        var batch = new List<NormalizedTick>(_maxBatchSize);
        var lastSaveTime = DateTimeOffset.UtcNow;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(stoppingToken);
                var tick = JsonSerializer.Deserialize<NormalizedTick>(result.Message.Value!);

                if (tick == null) continue;

                batch.Add(tick);
                _consumer.Commit(result);

                // Вычисляем динамический размер батча на основе глубины очереди
                var currentQueueDepth = _metrics.QueueDepth;
                var dynamicBatchSize = CalculateDynamicBatchSize(currentQueueDepth);

                var shouldSaveBySize = batch.Count >= dynamicBatchSize;
                var shouldSaveByTime = DateTimeOffset.UtcNow - lastSaveTime >= _batchInterval;

                if (shouldSaveBySize || shouldSaveByTime)
                {
                    await SaveBatchAsync(batch, stoppingToken);
                    batch.Clear();
                    lastSaveTime = DateTimeOffset.UtcNow;
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[DB Error] Failed to process tick. Adding remaining batch to retry queue.");

                // Добавляем весь текущий батч в очередь повторных попыток
                foreach (var tick in batch)
                {
                    try
                    {
                        _retryQueue.Enqueue(tick);
                    }
                    catch (Exception enqueueEx)
                    {
                        _logger.LogError(enqueueEx, "[DB Error] Could not enqueue tick for retry.");
                    }
                }
                batch.Clear();
            }
        }

        // Сохраняем оставшиеся тики при остановке сервиса
        if (batch.Count > 0)
        {
            try
            {
                await SaveBatchAsync(batch, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[DB Error] Failed to save final batch. Adding to retry queue.");
                foreach (var tick in batch)
                {
                    try
                    {
                        _retryQueue.Enqueue(tick);
                    }
                    catch (Exception enqueueEx)
                    {
                        _logger.LogError(enqueueEx, "[DB Error] Could not enqueue tick for retry.");
                    }
                }
            }
        }
    }

    /// <summary>
    /// Вычисляет динамический размер батча на основе глубины очереди.
    /// </summary>
    private int CalculateDynamicBatchSize(int queueDepth)
    {
        if (queueDepth <= 0)
            return _minBatchSize;

        // Линейная интерполяция между минимальным и максимальным размером батча
        // Чем больше глубина очереди, тем больше размер батча
        var ratio = (double)queueDepth / _maxBatchSize;
        ratio = Math.Min(1.0, Math.Max(0.0, ratio)); // Ограничиваем от 0 до 1

        return (int)(_minBatchSize + (_maxBatchSize - _minBatchSize) * ratio);
    }

    private async Task SaveBatchAsync(List<NormalizedTick> batch, CancellationToken cancellationToken)
    {
        if (batch.Count == 0) return;

        await _repository.SaveRangeAsync(batch, cancellationToken);
        _metrics.AddSavedToDb(batch.Count);
        _logger.LogDebug("Saved batch of {BatchSize} ticks to database.", batch.Count);
    }
}









