using System.Collections.Concurrent;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

namespace Aggregator.Api.BackgroundServices;

/// <summary>
/// Очередь для повторных попыток записи ticks в БД.
/// </summary>
public class TickRetryQueue : BackgroundService, ITickRetryQueue
{
    private readonly ConcurrentQueue<NormalizedTick> _retryQueue = new();
    private readonly ITickRepository _repository;
    private readonly IMetricsService _metrics;
    private readonly ILogger<TickRetryQueue> _logger;

    public TickRetryQueue(
        IMetricsService metrics,
        ITickRepository repository,
        ILogger<TickRetryQueue> logger)
    {
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public void Enqueue(NormalizedTick tick)
    {
        if (tick == null) throw new ArgumentNullException(nameof(tick));

        _retryQueue.Enqueue(tick);
        _logger.LogInformation("[RetryQueue] Tick enqueued for retry. Queue depth: {Count}", _retryQueue.Count);
    }

    public int Count => _retryQueue.Count;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);

            if (_retryQueue.IsEmpty)
                continue;

            var ticksToRetry = new List<NormalizedTick>();
            while (_retryQueue.TryDequeue(out var tick))
            {
                ticksToRetry.Add(tick);
            }

            if (ticksToRetry.Count == 0)
                continue;

            _logger.LogInformation("[RetryQueue] Attempting to re-save {Count} ticks from retry queue.", ticksToRetry.Count);

            var saved = 0;
            var failed = new List<NormalizedTick>();

            try
            {
                foreach (var tick in ticksToRetry)
                {
                    try
                    {
                        await _repository.SaveAsync(tick, stoppingToken);
                        saved++;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "[RetryQueue] Failed to re-save tick: {Message}", ex.Message);
                        failed.Add(tick);
                    }
                }

                _metrics.AddSavedToDb(saved);
                _logger.LogInformation("[RetryQueue] Successfully re-saved {Saved}/{Total} ticks.", saved, ticksToRetry.Count);

                // Возвращаем неудачные ticks обратно в очередь
                foreach (var failedTick in failed)
                {
                    Enqueue(failedTick);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[RetryQueue] Connection failed. Re-enqueueing all ticks.");
                foreach (var tick in ticksToRetry)
                {
                    Enqueue(tick);
                }
            }
        }
    }
}
