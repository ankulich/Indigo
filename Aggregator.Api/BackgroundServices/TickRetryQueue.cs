using System.Text.Json;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

namespace Aggregator.Api.BackgroundServices;

/// <summary>
/// Очередь для повторных попыток записи ticks в БД через Kafka.
/// </summary>
public class TickRetryQueue : BackgroundService, ITickRetryQueue
{
    private readonly string _retryTopic = "retry";
    private readonly string _errorTopic = "errors";
    private readonly string _bootstrapServers;
    private readonly ITickRepository _repository;
    private readonly IMetricsService _metrics;
    private readonly ILogger<TickRetryQueue> _logger;
    private readonly IProducer<Null, string> _producer;
    private readonly IConsumer<Null, string> _consumer;
    private int _queueCount;

    public TickRetryQueue(
        IMetricsService metrics,
        ITickRepository repository,
        IConfiguration configuration,
        ILogger<TickRetryQueue> logger)
    {
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _bootstrapServers = configuration.GetValue<string>("Kafka:BootstrapServers") ?? "localhost:9092";

        var producerConfig = new ProducerConfig { BootstrapServers = _bootstrapServers };
        _producer = new ProducerBuilder<Null, string>(producerConfig).Build();

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "retry-queue-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };
        _consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
    }

    public void Enqueue(NormalizedTick tick)
    {
        if (tick == null) throw new ArgumentNullException(nameof(tick));

        var json = JsonSerializer.Serialize(tick);
        _producer.Produce(_retryTopic, new Message<Null, string> { Value = json });
        Interlocked.Increment(ref _queueCount);
        _logger.LogInformation("[RetryQueue] Tick enqueued for retry. Queue depth: {Count}", _queueCount);
    }

    public int Count => _queueCount;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Создаём топик retry если не существует
        await CreateTopicIfNotExists(_retryTopic, stoppingToken);
        await CreateTopicIfNotExists(_errorTopic, stoppingToken);

        _consumer.Subscribe(_retryTopic);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(TimeSpan.FromSeconds(10));
                if (result.Message == null) continue;

                var tick = JsonSerializer.Deserialize<NormalizedTick>(result.Message.Value!);
                if (tick == null) continue;

                try
                {
                    await _repository.SaveAsync(tick, stoppingToken);
                    Interlocked.Decrement(ref _queueCount);
                    _metrics.AddSavedToDb(1);
                    _logger.LogInformation("[RetryQueue] Successfully re-saved tick: {Ticker}", tick.Ticker);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[RetryQueue] Failed to re-save tick: {Message}", ex.Message);
                    await SendToErrorTopicAsync(tick);
                    Interlocked.Decrement(ref _queueCount);
                }
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "[RetryQueue] Kafka consume error: {Message}", ex.Message);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[RetryQueue] Unexpected error: {Message}", ex.Message);
            }
        }
    }

    private async Task CreateTopicIfNotExists(string topicName, CancellationToken stoppingToken)
    {
        try
        {
            var adminConfig = new AdminClientConfig { BootstrapServers = _bootstrapServers };
            using var adminClient = new AdminClientBuilder(adminConfig).Build();

            var metadata = adminClient.GetMetadata(timeout: TimeSpan.FromSeconds(10));
            var existingTopics = metadata.Topics.Select(t => t.Topic).ToHashSet();
            
            if (!existingTopics.Contains(topicName))
            {
                var topicSpecification = new TopicSpecification 
                { 
                    Name = topicName, 
                    NumPartitions = 1, 
                    ReplicationFactor = 1 
                };
                var options = new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) };
                options.OperationTimeout = TimeSpan.FromSeconds(30);

                await adminClient.CreateTopicsAsync(new[] { topicSpecification }, options);
                _logger.LogInformation("[RetryQueue] Created topic: {TopicName}", topicName);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[RetryQueue] Failed to create topic {TopicName}: {Message}", topicName, ex.Message);
        }
    }

    private async Task SendToErrorTopicAsync(NormalizedTick tick)
    {
        try
        {
            var json = JsonSerializer.Serialize(tick);
            await _producer.ProduceAsync(_errorTopic, new Message<Null, string> { Value = json });
            _logger.LogWarning("[RetryQueue] Tick sent to error topic: {Ticker}", tick.Ticker);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[RetryQueue] Failed to send tick to error topic: {Message}", ex.Message);
        }
    }

    public override void Dispose()
    {
        _producer?.Dispose();
        _consumer?.Close();
        _consumer?.Dispose();
        base.Dispose();
    }
}
