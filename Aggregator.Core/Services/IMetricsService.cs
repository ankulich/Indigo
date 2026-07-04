namespace Aggregator.Core.Services;

/// <summary>
/// Интерфейс для сбора метрик системы.
/// </summary>
public interface IMetricsService
{
    int TicksProcessed { get; }
    int TicksDuplicated { get; }
    int TicksSavedToDb { get; }
    int QueueDepth { get; }

    void IncrementProcessed();
    void IncrementDuplicated();
    void AddSavedToDb(int count);
    void SetQueueDepth(int depth);
}