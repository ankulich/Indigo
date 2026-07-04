using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

namespace Aggregator.Admin.ExchangeSimulator;

/// <summary>
/// Менеджер для управления серверами бирж.
/// </summary>
public class ExchangeServerManager : IDisposable
{
    private readonly Dictionary<string, ExchangeServer> _servers;
    private readonly ILogger<ExchangeServerManager> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IHostApplicationLifetime _applicationLifetime;
    private bool _isDisposed;

    public ExchangeServerManager(
        ILogger<ExchangeServerManager> logger,
        IConfiguration configuration,
        IHostApplicationLifetime applicationLifetime,
        ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        _logger = _loggerFactory.CreateLogger<ExchangeServerManager>();
        _applicationLifetime = applicationLifetime ?? throw new ArgumentNullException(nameof(applicationLifetime));
        _servers = new Dictionary<string, ExchangeServer>(StringComparer.OrdinalIgnoreCase);

        InitializeServers(configuration);
    }

    private void InitializeServers(IConfiguration configuration)
    {
        var exchangeServersSection = configuration.GetSection("ExchangeServers");

        foreach (var serverSection in exchangeServersSection.GetChildren())
        {
            try
            {
                var name = serverSection.Key;
                var port = serverSection.GetValue<int>("Port");
                var enabled = serverSection.GetValue<bool>("Enabled");
                var frequency = serverSection.GetValue<int>("Frequency");
                var exchangeServerlogger = _loggerFactory.CreateLogger<ExchangeServer>();

                var server = new ExchangeServer(name, port, enabled, frequency, exchangeServerlogger);
                AddServer(server);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize server {Name}", serverSection.Key);
            }
        }
    }

    public void AddServer(ExchangeServer server)
    {
        if (server == null) throw new ArgumentNullException(nameof(server));

        if (_servers.TryAdd(server.Name, server))
        {
            _logger.LogInformation($"Added server {server.Name} on port {server.Port}");
        }
        else
        {
            _logger.LogWarning($"Server {server.Name} already exists");
        }
    }

    public async Task StartAllAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting all exchange servers..");

        var tasks = new List<Task>();

        foreach (var server in _servers.Values.Where(s => s.Enabled))
        {
            var task = server.StartAsync(_applicationLifetime.ApplicationStopping);
            tasks.Add(task);
        }

        if (tasks.Count > 0)
        {
            await Task.WhenAll(tasks);
        }

        _logger.LogInformation("All exchange servers started.");
    }

    public async Task StopAllAsync()
    {
        _logger.LogInformation("Stopping all exchange servers..");

        var tasks = new List<Task>();

        foreach (var server in _servers.Values)
        {
            var task = server.StopAsync(_applicationLifetime.ApplicationStopping);
            tasks.Add(task);
        }

        if (tasks.Count > 0)
        {
            await Task.WhenAll(tasks);
        }

        _logger.LogInformation("All exchange servers stopped.");
    }

    public async Task StartServerAsync(string name)
    {
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Server name cannot be null or empty.", nameof(name));

        if (TryGetServer(name, out var server) && !server.Enabled)
        {
            _logger.LogInformation($"Starting server {name}");
            await server.StartAsync(_applicationLifetime.ApplicationStopping);
        }
        else if (!TryGetServer(name, out _))
        {
            _logger.LogWarning($"Server {name} not found");
        }
    }

    public async Task StopServerAsync(string name)
    {
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Server name cannot be null or empty.", nameof(name));

        if (TryGetServer(name, out var server) && server.Enabled)
        {
            _logger.LogInformation($"Stopping server {name}");
            await server.StopAsync(_applicationLifetime.ApplicationStopping);
        }
        else if (!TryGetServer(name, out _))
        {
            _logger.LogWarning($"Server {name} not found");
        }
    }

    public ExchangeServer? GetServer(string name)
    {
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Server name cannot be null or empty.", nameof(name));

        return TryGetServer(name, out var server) ? server : null;
    }

    public bool TryGetServer(string name, out ExchangeServer? server)
    {
        return _servers.TryGetValue(name, out server);
    }

    public IEnumerable<ExchangeServer> GetAllServers()
    {
        return _servers.Values.ToList();
    }

    public void Dispose()
    {
        if (_isDisposed) return;

        try
        {
            StopAllAsync().Wait();

            foreach (var server in _servers.Values)
            {
                server.Dispose();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during disposal of ExchangeServerManager");
        }

        _isDisposed = true;
    }
}
