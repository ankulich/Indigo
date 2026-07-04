using System.Threading.Channels;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Aggregator.Api.Services;
using Aggregator.Api.Models;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Aggregator.Api.BackgroundServices;

/// <summary>
/// Фоновый сервис для подключения к биржам через WebSocket.
/// </summary>
public class ExchangeConnector : BackgroundService
{
    private readonly IEnumerable<ExchangeServerSettings> _exchangeSettings;
    private readonly IDeduplicator _deduplicator;
    private readonly TickNormalizerFactory _normalizerFactory;
    private readonly IMetricsService _metrics;
    private readonly IConfiguration _configuration;
    private readonly ILoggerFactory _loggerFactory;

    public ExchangeConnector(
        IEnumerable<ExchangeServerSettings> exchangeSettings,
        IDeduplicator deduplicator,
        TickNormalizerFactory normalizerFactory,
        IMetricsService metrics,
        IConfiguration configuration,
        ILoggerFactory loggerFactory)
    {
        _exchangeSettings = exchangeSettings ?? throw new ArgumentNullException(nameof(exchangeSettings));
        _deduplicator = deduplicator ?? throw new ArgumentNullException(nameof(deduplicator));
        _normalizerFactory = normalizerFactory ?? throw new ArgumentNullException(nameof(normalizerFactory));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var connectionTasks = new List<Task>();

        foreach (var exchange in _exchangeSettings)
        {
            var task = ConnectToExchange(exchange, stoppingToken);
            connectionTasks.Add(task);
        }

        await Task.WhenAll(connectionTasks);
    }

    private async Task ConnectToExchange(ExchangeServerSettings exchangeSettings, CancellationToken stoppingToken)
    {
        using var exchangeCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var exchangeToken = exchangeCts.Token;

        var connectionTasks = new List<Task>();
        foreach (var wsUrl in exchangeSettings.WebSocketUrls)
        {
            var task = ConnectToWebSocket(exchangeSettings, wsUrl, exchangeToken);
            connectionTasks.Add(task);
        }

        await Task.WhenAll(connectionTasks);
    }

    private async Task ConnectToWebSocket(ExchangeServerSettings exchangeSettings, string wsUrl, CancellationToken stoppingToken)
    {
        var bootstrapServers = _configuration.GetValue<string>("Kafka:BootstrapServers") ?? "localhost:9092";
        var exchangeProducerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
        var exchangeProducer = new ProducerBuilder<Null, string>(exchangeProducerConfig).Build();
        var exchangeTopic = "ticks";

        // Создаём топик, если он не существует
        await CreateTopicIfNotExists(exchangeTopic, bootstrapServers, stoppingToken);

        // Получаем нормализатор для биржи
        var normalizer = _normalizerFactory.GetNormalizer(exchangeSettings.Name);

        var logger = _loggerFactory.CreateLogger<WebSocketConnection>();

        var connection = new WebSocketConnection(
            exchangeSettings,
            wsUrl,
            _deduplicator,
            normalizer,
            exchangeProducer,
            exchangeTopic,
            _metrics,
            logger);

        try
        {
            await connection.ConnectAndReceiveAsync(stoppingToken);
        }
        finally
        {
            connection.Dispose();
        }
    }

    private async Task CreateTopicIfNotExists(string topicName, string bootstrapServers, CancellationToken stoppingToken)
    {
        var logger = _loggerFactory.CreateLogger<ExchangeConnector>();

        try
        {
            var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
            using var adminClient = new AdminClientBuilder(adminConfig).Build();

            var metadata = adminClient.GetMetadata(timeout: TimeSpan.FromSeconds(10));
            var existingTopics = metadata.Topics.Select(t => t.Topic).ToHashSet();
            if (!existingTopics.Contains(topicName))
            {
                var topicSpecification = new TopicSpecification { Name = topicName, NumPartitions = 1, ReplicationFactor = 1 };
                var options = new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) };
                options.OperationTimeout = TimeSpan.FromSeconds(30);

                await adminClient.CreateTopicsAsync(new[] { topicSpecification }, options);
                logger.LogInformation("[Kafka] Topic '{Topic}' created.", topicName);
            }
            else
            {
                logger.LogInformation("[Kafka] Topic '{Topic}' already exists.", topicName);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[Kafka] Error creating topic {Topic}", topicName);
        }
    }
}

