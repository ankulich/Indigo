using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Globalization;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Aggregator.Admin.ExchangeSimulator;

/// <summary>
/// Симулятор сервера биржи для тестирования.
/// </summary>
public class ExchangeServer(string name, int port, bool enabled, int frequency, ILogger<ExchangeServer> logger)
    : IHostedService, IDisposable
{
    public string Name { get; private set; } = name ?? throw new ArgumentNullException(nameof(name));
    public int Port { get; private set; } = port;
    public bool Enabled { get; private set; } = enabled;
    public int Frequency { get; private set; } = frequency;

    private HttpListener? _listener;
    private readonly Random _rnd = new();
    private readonly ILogger<ExchangeServer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private CancellationTokenSource? _cancellationTokenSource;
    private Task? _serverTask;
    private bool _isDisposed;

    // Потокобезопасные поля для управления состоянием
    private volatile bool _shouldDisconnect;
    private volatile bool _shouldResend;
    private int _resendCount;
    private int _maxResendCount = 3;
    private readonly object _resendLock = new();

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_listener is { IsListening: true }) return;
        
        _listener = CreateServer();

        try
        {
            _listener.Start();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start HttpListener on port {Port}", Port);
            _listener.Close();
            return;
        }

        Enabled = true;
        _logger.LogInformation("Exchange Simulator started for {Name} on ws://localhost:{Port}", Name, Port);

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _serverTask = Task.Run(() => ServerLoop(_cancellationTokenSource.Token), _cancellationTokenSource.Token);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            Enabled = false;
            _cancellationTokenSource?.Cancel();

            if (_listener != null)
            {
                _listener.Stop();
                _listener.Close();
            }

            if (_serverTask != null && !_serverTask.IsCompleted)
            {
                await Task.WhenAny(_serverTask, Task.Delay(5000, cancellationToken)); // Таймаут на случай зависания
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error stopping listener for {Name}", Name);
        }

        _logger.LogInformation("Exchange Simulator stopped for {Name}", Name);
    }

    public void Dispose()
    {
        if (_isDisposed) return;

        try
        {
            StopAsync(CancellationToken.None).GetAwaiter().GetResult();
            _cancellationTokenSource?.Dispose();
        }
        catch
        {
            // Игнорируем ошибки при DISPOSAL
        }

        _isDisposed = true;
    }

    public async Task UpdateFrequency(int frequency)
    {
        if (frequency > 100 && frequency < 10000)
        {
            Frequency = frequency;
            _logger.LogInformation($"Frequency updated to {frequency} for {Name}");
        }
        else
        {
            _logger.LogError($"Unable to set frequency because of limit for {Name}. Frequency must be between 100 and 10000.");
        }
    }

    public void DisconnectClient()
    {
        _shouldDisconnect = true;
    }

    public void ResendLastMessage(int count)
    {
        lock (_resendLock)
        {
            _shouldResend = true;
            _maxResendCount = count;
            _resendCount = 0;
        }
    }

    private async Task ServerLoop(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested && Enabled)
            {
                try
                {
                    var context = await _listener.GetContextAsync().ConfigureAwait(false);
                    if (context.Request.IsWebSocketRequest)
                    {
                        var wsContext = await context.AcceptWebSocketAsync(null).ConfigureAwait(false);
                        _ = HandleClient(wsContext.WebSocket, cancellationToken);
                    }
                    else
                    {
                        context.Response.StatusCode = 400;
                        context.Response.Close();
                    }
                }
                catch (ObjectDisposedException)
                {
                    // Ожидаемое поведение при остановке
                    break;
                }
                catch (Exception ex)
                {
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        _logger.LogError($"Error in {Name} server: {ex.Message}");
                    }
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Ожидаемое поведение при отмене
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error in {Name} server task: {ex.Message}");
        }
    }

    private async Task HandleClient(WebSocket ws, CancellationToken cancellationToken)
    {
        try
        {
            string? lastJson = null;

            while (ws.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
            {
                if (_shouldDisconnect)
                {
                    _logger.LogInformation("Disconnecting client for {Name}", Name);

                    await ws.CloseAsync(
                        WebSocketCloseStatus.NormalClosure,
                        "Manual disconnect",
                        cancellationToken);

                    _shouldDisconnect = false;
                    break;
                }

                bool shouldResend;
                int resendCount;
                int maxResendCount;

                lock (_resendLock)
                {
                    shouldResend = _shouldResend;
                    resendCount = _resendCount;
                    maxResendCount = _maxResendCount;
                }

                if (shouldResend && resendCount < maxResendCount && lastJson != null)
                {
                    await SendAsync(ws, lastJson, cancellationToken);

                    _logger.LogInformation("Resending message for {Name}: {Message}", Name, lastJson);

                    lock (_resendLock)
                    {
                        _resendCount++;
                    }

                    await Task.Delay(Frequency, cancellationToken);
                    continue;
                }

                if (shouldResend)
                {
                    lock (_resendLock)
                    {
                        _shouldResend = false;
                        _resendCount = 0;
                    }
                }

                var tick = CreateTick();
                lastJson = JsonSerializer.Serialize(tick);

                await SendAsync(ws, lastJson, cancellationToken);

                _logger.LogInformation("ExchangeName: {Name}, Message: {Message}", Name, lastJson);

                await Task.Delay(Frequency, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Игнорируем отмену
        }
        catch (Exception ex)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation($"Client disconnected for {Name}: {ex.Message}");
            }
        }
    }

    private async Task SendAsync(WebSocket ws, string json, CancellationToken ct)
    {
        var bytes = Encoding.UTF8.GetBytes(json);

        await ws.SendAsync(
            bytes,
            WebSocketMessageType.Text,
            true,
            ct);
    }

    private object CreateTick()
    {
        var price = Math.Round(50000 + _rnd.NextDouble() * 100, 2);
        var name = "BTCUSD";
        var quantity = Math.Round(_rnd.NextDouble() * 10, 0);
        var now = DateTimeOffset.UtcNow;

        return Name.ToLower() switch
        {
            "binance" => new
            {
                s = name,
                p = price.ToString(CultureInfo.InvariantCulture),
                q = quantity,
                T = now.ToUnixTimeMilliseconds()
            },

            "coinbase" => new
            {
                product_id = name,
                price,
                count = quantity.ToString(),
                time = now.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture)
            },

            "forex" => new
            {
                pair = name,
                price = (decimal)price,
                quantity,
                timestamp = now.ToUnixTimeSeconds()
            },

            _ => new
            {
                symbol = name,
                price,
                quantity,
                timestamp = now.ToUnixTimeSeconds()
            }
        };
    }

    private HttpListener CreateServer()
    {
        var listener = new HttpListener();
        listener.Prefixes.Add($"http://+:{Port}/");
        return listener;
    }
}

