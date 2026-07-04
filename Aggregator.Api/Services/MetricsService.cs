using System.Threading;
using Aggregator.Core.Services;

namespace Aggregator.Api.Services;

public class MetricsService : IMetricsService
{
    public int TicksProcessed => _ticksProcessed;
    public int TicksDuplicated => _ticksDuplicated;
    public int TicksSavedToDb => _ticksSavedToDb;
    public int QueueDepth => _queueDepth;

    private int _ticksProcessed;
    private int _ticksDuplicated;
    private int _ticksSavedToDb;
    private int _queueDepth;

    public void IncrementProcessed() => Interlocked.Increment(ref _ticksProcessed);
    public void IncrementDuplicated() => Interlocked.Increment(ref _ticksDuplicated);
    public void AddSavedToDb(int count) => Interlocked.Add(ref _ticksSavedToDb, count);
    public void SetQueueDepth(int depth) => Interlocked.Exchange(ref _queueDepth, depth);
}