using Aggregator.Admin.Models;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Microsoft.Extensions.Logging;

namespace Aggregator.Admin.Services;

/// <summary>
/// Сервис для работы с тиками.
/// </summary>
public class TickService
{
    private readonly ITickRepository _repository;
    private readonly ILogger<TickService> _logger;

    public TickService(ITickRepository repository, ILogger<TickService> logger)
    {
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<List<Tick>> GetTicksAsync(int limit = 100, CancellationToken cancellationToken = default)
    {
        try
        {
            var normalizedTicks = await _repository.GetTicksAsync(limit, cancellationToken);

            return normalizedTicks.Select(nt => new Tick
            {
                Source = nt.Source,
                Ticker = nt.Ticker,
                Price = nt.Price,
                Volume = nt.Volume,
                Timestamp = nt.Timestamp
            }).ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get ticks");
            throw;
        }
    }
}