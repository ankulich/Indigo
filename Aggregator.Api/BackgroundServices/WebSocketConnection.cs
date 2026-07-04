using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Aggregator.Api.Models;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;

namespace Aggregator.Api.BackgroundServices;

/// <summary>
/// Управляет одним WebSocket-соединением с биржей.
/// </summary>
public class WebSocketConnection : IDisposable
{
    private readonly ExchangeServerSettings _exchangeSettings;
    private readonly string _wsUrl;
    private readonly IDeduplicator _deduplicator;
    private readonly ITickNormalizer _normalizer;
    private readonly IProducer<Null, string> _producer;
    private readonly string _topic;
    private readonly IMetricsService _metrics;
    private readonly ILogger<WebSocketConnection> _logger;

    public WebSocketConnection(
        ExchangeServerSettings exchangeSettings,
        string wsUrl,
        IDeduplicator deduplicator,
        ITickNormalizer normalizer,
        IProducer<Null, string> producer,
        string topic,
        IMetricsService metrics,
        ILogger<WebSocketConnection> logger)
    {
        _exchangeSettings = exchangeSettings;
        _wsUrl = wsUrl;
        _deduplicator = deduplicator;
        _normalizer = normalizer;
        _producer = producer;
        _topic = topic;
        _metrics = metrics;
        _logger = logger;
    }

    public async Task ConnectAndReceiveAsync(CancellationToken stoppingToken)
    {
        int backoffMs = _exchangeSettings.InitialBackoffMs;

        while (!stoppingToken.IsCancellationRequested)
        {
            ClientWebSocket? ws = null;
            try
            {
                ws = new ClientWebSocket();
                await ws.ConnectAsync(new Uri(_wsUrl), stoppingToken);
                backoffMs = _exchangeSettings.InitialBackoffMs;
                _logger.LogInformation("[{Exchange}] Connected to {Url}.", _exchangeSettings.Name, _wsUrl);

                await ReceiveLoop(ws, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("[{Exchange}] Cancellation requested for {Url}.", _exchangeSettings.Name, _wsUrl);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[{Exchange}] Disconnected from {Url}. Reconnecting in {Backoff}ms..",
                    _exchangeSettings.Name, _wsUrl, backoffMs);
                await Task.Delay(backoffMs, stoppingToken);
                backoffMs = Math.Min(backoffMs * 2, _exchangeSettings.MaxBackoffMs);
            }
            finally
            {
                try
                {
                    ws?.Dispose();
                }
                catch
                {
                    // Ignore disposal errors
                }
            }
        }
    }

    private async Task ReceiveLoop(ClientWebSocket ws, CancellationToken stoppingToken)
    {
        var buffer = new byte[8192];
        var idleCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        try
        {
            while (ws.State == WebSocketState.Open && !stoppingToken.IsCancellationRequested)
            {
                idleCts.CancelAfter(TimeSpan.FromSeconds(_exchangeSettings.IdleTimeoutSeconds));

                try
                {
                    var result = await ws.ReceiveAsync(buffer, idleCts.Token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        _logger.LogInformation("[{Exchange}] Server initiated close for {Url}.",
                            _exchangeSettings.Name, _wsUrl);
                        break;
                    }

                    var json = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    var tick = _normalizer.Normalize(json);

                    if (!_deduplicator.IsDuplicate(tick))
                    {
                        var jsonTick = JsonSerializer.Serialize(tick);
                        await _producer.ProduceAsync(_topic, new Message<Null, string> { Value = jsonTick }, stoppingToken);
                        _metrics.IncrementProcessed();
                    }
                    else
                    {
                        _metrics.IncrementDuplicated();
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogWarning("[{Exchange}] Idle timeout ({Timeout}s) detected for {Url}. Reconnecting..",
                        _exchangeSettings.Name, _exchangeSettings.IdleTimeoutSeconds, _wsUrl);
                    break;
                }
            }
        }
        finally
        {
            idleCts.Cancel();
            idleCts.Dispose();
        }
    }

    public void Dispose()
    {
        _producer?.Dispose();
    }
}