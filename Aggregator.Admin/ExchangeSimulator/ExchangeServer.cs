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
public class ExchangeServer : IHostedService, IDisposable
{
    public string Name { get; private set; }
    public int Port { get; private set; }
    public bool Enabled { get; private set; }
    public int Frequency { get; private set; }

    private readonly HttpListener _listener;
    private readonly Random _rnd;
    private readonly ILogger<ExchangeServer> _logger;
    private CancellationTokenSource? _cancellationTokenSource;
    private Task? _serverTask;
    private bool _isDisposed;

    // Потокобезопасные поля для управления состоянием
    private volatile bool _shouldDisconnect;
    private volatile bool _shouldResend;
    private int _resendCount;
    private int _maxResendCount = 3;
    private readonly object _resendLock = new();

    public ExchangeServer(string name, int port, bool enabled, int frequency, ILogger<ExchangeServer> logger)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Port = port;
        Enabled = enabled;
            Frequency = frequency;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _listener = new HttpListener();
        _rnd = new Random();
        _listener.Prefixes.Add($"http://+:{Port}/");
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_listener.IsListening) return;

        try
        {
            _listener.Start();
        }
    catch (Exception ex)
    {
            _logger.LogError($"Failed to start HttpListener on port {Port}: {ex.Message}");
            return;
            }
        Enabled = true;

        _logger.LogInformation($"Exchange Simulator started for {Name} on ws://localhost:{Port}");

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        _serverTask = Task.Run(() => ServerLoop(_cancellationTokenSource.Token), _cancellationTokenSource.Token);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
{
        try
{
            Enabled = false;
            _cancellationTokenSource?.Cancel();
            _listener?.Stop();

            if (_serverTask != null && !_serverTask.IsCompleted)
    {
                await _serverTask.WaitAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error stopping listener for {Name}: {ex.Message}");
        }

        _logger.LogInformation($"Exchange Simulator stopped for {Name}");
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

    public void Dispose()
    {
        if (_isDisposed) return;

        try
        {
            StopAsync(CancellationToken.None).GetAwaiter().GetResult();
            _cancellationTokenSource?.Dispose();
            _listener?.Close();
        }
        catch
        {
            // Игнорируем ошибки при DISPOSAL
        }

        _isDisposed = true;
    }
}

