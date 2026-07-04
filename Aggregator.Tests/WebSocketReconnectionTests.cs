using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Aggregator.Api.BackgroundServices;
using Aggregator.Api.Models;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Aggregator.Tests;

/// <summary>
/// Тесты на переподключение WebSocket после обрыва соединения.
/// </summary>
public class WebSocketReconnectionTests : IDisposable
{
    private readonly List<TestWebSocketServer> _servers = [];

    public void Dispose()
    {
        foreach (var server in _servers)
        {
            server.Dispose();
        }
        GC.SuppressFinalize(this);
    }

    [Fact(Timeout = 30000)]
    public async Task WebSocketConnection_ShouldReconnectAfterDisconnect_OtherSourcesContinueWorking()
    {
        // Arrange — создаём два "биржевых" сервера
        var server1 = new TestWebSocketServer(8181, delayBeforeDisconnect: 500);
        var server2 = new TestWebSocketServer(8182, delayBeforeDisconnect: null); // не отключается
        _servers.Add(server1);
        _servers.Add(server2);

        await server1.StartAsync();
        await server2.StartAsync();

        var dedup = new Deduplicator(TimeSpan.FromSeconds(60));
        var normalizer = new TestNormalizer("TestExchange");
        var producer = new TestKafkaProducer();
        var metrics = new TestMetricsService();
        var logger = new TestLogger<WebSocketConnection>();

        var exchangeSettings = new ExchangeServerSettings
        {
            Name = "TestExchange",
            Port = 8181,
            WebSocketUrls = [$"ws://localhost:8181/ws", $"ws://localhost:8182/ws"],
            InitialBackoffMs = 100,
            MaxBackoffMs = 500,
            IdleTimeoutSeconds = 5
        };

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        // Act — запускаем подключения к обоим серверам
        var connection1 = new WebSocketConnection(
            exchangeSettings,
            $"ws://localhost:8181/ws",
            dedup, normalizer, producer, "ticks", metrics, logger);

        var connection2 = new WebSocketConnection(
            exchangeSettings,
            $"ws://localhost:8182/ws",
            dedup, normalizer, producer, "ticks", metrics, logger);

        var task1 = connection1.ConnectAndReceiveAsync(cts.Token);
        var task2 = connection2.ConnectAndReceiveAsync(cts.Token);

        // Ждём, пока первый сервер отключится и переподключится
        await Task.Delay(2000);

        // Проверяем, что второй сервер продолжает работать
        Assert.True(server2.ClientConnectedCount >= 1, "Server 2 should have had at least one connection");

        // Проверяем, что первый сервер переподключился
        Assert.True(server1.ClientConnectedCount >= 2, $"Server 1 should have reconnected (had {server1.ClientConnectedCount} connections)");

        // Останавливаем
        cts.Cancel();
        connection1.Dispose();
        connection2.Dispose();

        await Task.WhenAny(task1, task2);
    }

    [Fact(Timeout = 30000)]
    public async Task WebSocketConnection_ShouldSurviveMultipleDisconnects_AndContinueProcessing()
    {
        // Arrange — сервер, который несколько раз отключается
        var server = new TestWebSocketServer(8183, delayBeforeDisconnect: 300, maxDisconnects: 3);
        _servers.Add(server);

        await server.StartAsync();

        var dedup = new Deduplicator(TimeSpan.FromSeconds(60));
        var normalizer = new TestNormalizer("TestExchange");
        var producer = new TestKafkaProducer();
        var metrics = new TestMetricsService();
        var logger = new TestLogger<WebSocketConnection>();

        var exchangeSettings = new ExchangeServerSettings
        {
            Name = "TestExchange",
            Port = 8183,
            WebSocketUrls = [$"ws://localhost:8183/ws"],
            InitialBackoffMs = 100,
            MaxBackoffMs = 500,
            IdleTimeoutSeconds = 5
        };

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        var connection = new WebSocketConnection(
            exchangeSettings,
            $"ws://localhost:8183/ws",
            dedup, normalizer, producer, "ticks", metrics, logger);

        var task = connection.ConnectAndReceiveAsync(cts.Token);

        // Ждём, пока произойдут все отключения и переподключения
        await Task.Delay(3000);

        // Assert — соединение должно было переподключиться несколько раз
        Assert.True(server.ClientConnectedCount >= 3, $"Should have reconnected multiple times (had {server.ClientConnectedCount} connections)");

        // И должно быть обработано хотя бы несколько тиков
        Assert.True(metrics.TicksProcessed > 0, $"Should have processed some ticks (processed {metrics.TicksProcessed})");

        cts.Cancel();
        connection.Dispose();

        await Task.WhenAny(task, Task.Delay(5000));
    }

    [Fact(Timeout = 30000)]
    public async Task WebSocketConnection_ShouldNotLoseTicksFromOtherSource_WhenOneSourceDisconnects()
    {
        // Arrange — два сервера, один отключается, второй работает стабильно
        var unstableServer = new TestWebSocketServer(8184, delayBeforeDisconnect: 400);
        var stableServer = new TestWebSocketServer(8185, delayBeforeDisconnect: null);
        _servers.Add(unstableServer);
        _servers.Add(stableServer);

        await unstableServer.StartAsync();
        await stableServer.StartAsync();

        var dedup = new Deduplicator(TimeSpan.FromSeconds(60));
        var normalizer = new TestNormalizer("TestExchange");
        var producer = new TestKafkaProducer();
        var metrics = new TestMetricsService();
        var logger = new TestLogger<WebSocketConnection>();

        var exchangeSettings = new ExchangeServerSettings
        {
            Name = "TestExchange",
            Port = 8184,
            WebSocketUrls = [$"ws://localhost:8184/ws", $"ws://localhost:8185/ws"],
            InitialBackoffMs = 100,
            MaxBackoffMs = 500,
            IdleTimeoutSeconds = 5
        };

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        var connection1 = new WebSocketConnection(
            exchangeSettings,
            $"ws://localhost:8184/ws",
            dedup, normalizer, producer, "ticks", metrics, logger);

        var connection2 = new WebSocketConnection(
            exchangeSettings,
            $"ws://localhost:8185/ws",
            dedup, normalizer, producer, "ticks", metrics, logger);

        var task1 = connection1.ConnectAndReceiveAsync(cts.Token);
        var task2 = connection2.ConnectAndReceiveAsync(cts.Token);

        // Ждём, пока нестабильный сервер отключится и переподключится
        await Task.Delay(2000);

        // Считаем, сколько тиков обработал стабильный сервер
        int ticksFromStableBefore = stableServer.TicksSent;

        // Ждём ещё, чтобы стабильный сервер отправил больше тиков
        await Task.Delay(1500);

        int ticksFromStableAfter = stableServer.TicksSent;

        // Assert — стабильный сервер продолжал отправлять тики, пока нестабильный восстанавливался
        Assert.True(ticksFromStableAfter > ticksFromStableBefore,
            $"Stable server should have continued sending ticks (sent {ticksFromStableAfter - ticksFromStableBefore} more ticks)");

        cts.Cancel();
        connection1.Dispose();
        connection2.Dispose();

        await Task.WhenAny(task1, task2);
    }
}

/// <summary>
/// Простой WebSocket-сервер для тестирования.
/// </summary>
public class TestWebSocketServer : IDisposable
{
    private System.Net.HttpListener? _listener;
    private readonly int _port;
    private readonly int? _delayBeforeDisconnect;
    private readonly int _maxDisconnects;
    private int _disconnectCount = 0;
    private int _ticksSent = 0;
    private int _clientConnectedCount = 0;
    private readonly CancellationTokenSource _cts = new();

    public int ClientConnectedCount => _clientConnectedCount;
    public int TicksSent => _ticksSent;

    public TestWebSocketServer(int port, int? delayBeforeDisconnect = null, int maxDisconnects = int.MaxValue)
    {
        _port = port;
        _delayBeforeDisconnect = delayBeforeDisconnect;
        _maxDisconnects = maxDisconnects;
    }

    public async Task StartAsync()
    {
        _listener = new System.Net.HttpListener();
        _listener.Prefixes.Add($"http://localhost:{_port}/");
        _listener.Start();

        Task.Run(async () =>
        {
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    var context = await _listener!.GetContextAsync();
                    _ = Task.Run(() => HandleClientAsync(context));
                }
                catch when (_cts.IsCancellationRequested)
                {
                    break;
                }
            }
        });

        await Task.Delay(100); // Даем время на запуск
    }

    private async Task HandleClientAsync(System.Net.HttpListenerContext context)
    {
        if (!context.Request.IsWebSocketRequest) return;

        var wsContext = await context.AcceptWebSocketAsync(null);
        var ws = wsContext.WebSocket;
        Interlocked.Increment(ref _clientConnectedCount);

        if (_delayBeforeDisconnect.HasValue && _disconnectCount < _maxDisconnects)
        {
            _ = Task.Delay(_delayBeforeDisconnect.Value).ContinueWith(_ =>
            {
                if (_disconnectCount < _maxDisconnects)
                {
                    Interlocked.Increment(ref _disconnectCount);
                    try { _ = ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None); } catch { }
                }
            });
        }

        int tickCounter = 0;
        try
        {
            while (ws.State == WebSocketState.Open && !_cts.IsCancellationRequested)
            {
                var tick = new NormalizedTick
                {
                    Source = "TestServer",
                    Ticker = "BTC",
                    Price = 50000 + tickCounter,
                    Volume = 1,
                    Timestamp = DateTime.UtcNow
                };

                var json = JsonSerializer.Serialize(tick);
                var buffer = Encoding.UTF8.GetBytes(json);
                await ws.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, CancellationToken.None);
                Interlocked.Increment(ref _ticksSent);
                tickCounter++;

                await Task.Delay(200); // Отправляем тик каждые 200 мс
            }
        }
        catch (ObjectDisposedException)
        {
            // Клиент отключился
        }
        catch when (_cts.IsCancellationRequested)
        {
            // Сервер останавливается
        }
        finally
        {
            try { _ = ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None); } catch { }
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _listener?.Stop();
        _listener?.Close();
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Тестовый нормализатор, который принимает любой JSON и возвращает тик.
/// </summary>
public class TestNormalizer(string exchangeName) : ITickNormalizer
{
    public string ExchangeName { get; } = exchangeName;

    public NormalizedTick Normalize(string json)
    {
        try
        {
            return JsonSerializer.Deserialize<NormalizedTick>(json) ?? new NormalizedTick();
        }
        catch
        {
            return new NormalizedTick
            {
                Source = ExchangeName,
                Ticker = "BTC",
                Price = 50000,
                Volume = 1,
                Timestamp = DateTime.UtcNow
            };
        }
    }
}

/// <summary>
/// Тестовый Kafka-продюсер, который просто игнорирует сообщения.
/// </summary>
public class TestKafkaProducer : IProducer<Null, string>, IDisposable
{
    private bool _disposed;

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            GC.SuppressFinalize(this);
        }
    }

    public int Flush(TimeSpan timeout) => 0;
    public void Flush(CancellationToken cancellationToken = default) { }
    public void InitTransactions(TimeSpan timeout) { }
    public void InitTransactions(CancellationToken cancellationToken = default) { }
    public void BeginTransaction() { }
    public void CommitTransaction(TimeSpan timeout) { }
    public void CommitTransaction(CancellationToken cancellationToken = default) { }
    public void AbortTransaction(TimeSpan timeout) { }
    public void AbortTransaction(CancellationToken cancellationToken = default) { }
    public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata metadata, TimeSpan timeout) { }
    public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata metadata, CancellationToken cancellationToken = default) { }
    public int Poll(TimeSpan timeout) => 0;
    public int Poll(CancellationToken cancellationToken = default) => 0;
    public int AddBrokers(string brokers) => 0;
    public void SetSaslCredentials(string username, string password) { }
    public Handle Handle { get; } = null!;
    public string Name => "TestProducer";

    // Additional overloads that may be required by the interface
    public void CommitTransaction() { }
    public void AbortTransaction() { }

    public void Produce(string topic, Message<Null, string> message, Action<DeliveryReport<Null, string>>? deliveryHandler = null)
    {
        // Test producer — delivery handler не вызывается
    }

    public void Produce(TopicPartition topicPartition, Message<Null, string> message, Action<DeliveryReport<Null, string>>? deliveryHandler = null)
    {
        // Test producer — delivery handler не вызывается
    }

    public Task<DeliveryResult<Null, string>> ProduceAsync(string topic, Message<Null, string> message, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new DeliveryResult<Null, string>
        {
            Status = PersistenceStatus.Persisted,
            Message = message
        });
    }

    public Task<DeliveryResult<Null, string>> ProduceAsync(TopicPartition topicPartition, Message<Null, string> message, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new DeliveryResult<Null, string>
        {
            Status = PersistenceStatus.Persisted,
            Message = message
        });
    }
}

/// <summary>
/// Тестовый сервис метрик.
/// </summary>
public class TestMetricsService : IMetricsService
{
    private int _ticksProcessed;
    private int _ticksDuplicated;
    private int _ticksSavedToDb;
    private int _queueDepth;

    public int TicksProcessed => _ticksProcessed;
    public int TicksDuplicated => _ticksDuplicated;
    public int TicksSavedToDb => _ticksSavedToDb;
    public int QueueDepth => _queueDepth;

    public void IncrementProcessed() => Interlocked.Increment(ref _ticksProcessed);
    public void IncrementDuplicated() => Interlocked.Increment(ref _ticksDuplicated);
    public void AddSavedToDb(int count) => Interlocked.Add(ref _ticksSavedToDb, count);
    public void SetQueueDepth(int depth) => Interlocked.Exchange(ref _queueDepth, depth);
}

/// <summary>
/// Тестовый логгер.
/// </summary>
public class TestLogger<T> : ILogger<T>
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => true;
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
}