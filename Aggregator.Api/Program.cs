using System.Diagnostics;
using System.Threading.Channels;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Aggregator.Api.BackgroundServices;
using Aggregator.Api.Services;
using Aggregator.Api.Models;

var builder = WebApplication.CreateBuilder(args);

// Services
builder.Services.AddSingleton<IDeduplicator, Deduplicator>();
builder.Services.AddSingleton<IMetricsService, MetricsService>();
builder.Services.AddSingleton<TickRetryQueue>();
builder.Services.AddSingleton<ITickRepository, PostgresTickRepository>();

// Register TickNormalizerFactory and normalizers
builder.Services.AddSingleton<TickNormalizerFactory>(sp =>
{
    var factory = new TickNormalizerFactory();

    // Register all normalizers
    factory.Register(new BinanceTickNormalizer());
    factory.Register(new CoinbaseTickNormalizer());
    factory.Register(new ForexTickNormalizer());

    return factory;
});

// Channel for Backpressure handling
builder.Services.AddSingleton(Channel.CreateBounded<NormalizedTick>(new BoundedChannelOptions(100000)
{
    FullMode = BoundedChannelFullMode.DropOldest
}));

// Load exchange settings from configuration
var exchangeSettings = builder.Configuration.GetSection("ExchangeServers").GetChildren()
    .Select(section => new ExchangeServerSettings
    {
        Name = section.Key,
        Port = section.GetValue<int>("Port"),
        WebSocketPath = section.GetValue<string>("WebSocketPath") ?? "/ws",
        IdleTimeoutSeconds = section.GetValue<int>("IdleTimeoutSeconds"),
        MaxReconnectAttempts = section.GetValue<int>("MaxReconnectAttempts"),
        InitialBackoffMs = section.GetValue<int>("InitialBackoffMs"),
        MaxBackoffMs = section.GetValue<int>("MaxBackoffMs")
    })
    .ToList();

// Create WebSocket URLs for each exchange
var exchangeServerSettings = exchangeSettings.Select(es => new ExchangeServerSettings
{
    Name = es.Name,
    Port = es.Port,
    WebSocketPath = es.WebSocketPath,
    IdleTimeoutSeconds = es.IdleTimeoutSeconds,
    MaxReconnectAttempts = es.MaxReconnectAttempts,
    InitialBackoffMs = es.InitialBackoffMs,
    MaxBackoffMs = es.MaxBackoffMs,
    WebSocketUrls = new[] { $"ws://aggregator-admin:{es.Port}{es.WebSocketPath}" }
}).ToList();

// Register exchange settings as a singleton
builder.Services.AddSingleton<IEnumerable<ExchangeServerSettings>>(exchangeServerSettings);

// Background Services
builder.Services.AddHostedService<ExchangeConnector>();
builder.Services.AddHostedService<KafkaToDbConsumer>();
builder.Services.AddHostedService<TickRetryQueue>();

var app = builder.Build();

// Web Admin API
app.MapGet("/metrics", (IMetricsService metrics, TickRetryQueue retryQueue) => new
{
    metrics.TicksProcessed,
    metrics.TicksDuplicated,
    metrics.TicksSavedToDb,
    metrics.QueueDepth,
    RetryQueueDepth = retryQueue.Count
});

app.MapPost("/admin/simulate/drop", () => Results.Ok("Simulated drop triggered (mock)"));

app.Run();
