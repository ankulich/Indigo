using System.Text.Json;
using System.Threading.Channels;
using Aggregator.Core.Models;
using Aggregator.Api.Services;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;

namespace Aggregator.Api.BackgroundServices;

public class KafkaProducerService : BackgroundService
{
    private readonly Channel<NormalizedTick> _channel;
    private readonly MetricsService _metrics;
    private readonly IProducer<Null, string> _producer;
    private readonly string[] _exchangeNames;
    private readonly string _bootstrapServers;

    public KafkaProducerService(Channel<NormalizedTick> channel, MetricsService metrics, IConfiguration configuration)
    {
        _channel = channel;
        _metrics = metrics;
        _bootstrapServers = configuration.GetValue<string>("Kafka:BootstrapServers") ?? "localhost:9092";
        var config = new ProducerConfig { BootstrapServers = _bootstrapServers };
        _producer = new ProducerBuilder<Null, string>(config).Build();

        // Получаем список exchange-серверов из конфигурации
        _exchangeNames = configuration.GetSection("ExchangeServers").GetChildren()
            .Select(s => s.Key)
            .ToArray();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Создаём топик для exchange
        var topicName = $"ticks";
        await CreateTopicIfNotExists(topicName, stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            var batch = new List<NormalizedTick>();
            if (await _channel.Reader.WaitToReadAsync(stoppingToken))
            {
                while (_channel.Reader.TryRead(out var tick))
                {
                    batch.Add(tick);
                    if (batch.Count >= 500) break;
                }
            }

            if (batch.Count > 0)
            {
                _metrics.SetQueueDepth(_channel.Reader.Count);

                foreach (var tick in batch)
                {
                    var json = JsonSerializer.Serialize(tick);
                    await _producer.ProduceAsync(topicName, new Message<Null, string> { Value = json }, stoppingToken);
                }
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
                var topicSpecification = new TopicSpecification { Name = topicName, NumPartitions = 1, ReplicationFactor = 1 };
                var options = new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) };
                options.OperationTimeout = TimeSpan.FromSeconds(30);

                // Отменяем CancellationToken, так как CreateTopicsAsync не принимает его напрямую
                await adminClient.CreateTopicsAsync(new[] { topicSpecification }, options);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ошибка при создании топика {topicName}: {ex.Message}");
        }
    }
}

