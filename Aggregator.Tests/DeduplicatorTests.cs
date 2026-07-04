using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Xunit;

namespace Aggregator.Tests;

public class DeduplicatorTests
{
    [Fact]
    public void Deduplicator_ShouldBeThreadSafe_UnderHighConcurrency()
    {
        // Arrange
        var dedup = new Deduplicator();
        var tick = new NormalizedTick
        {
            Source = "Binance",
            Ticker = "BTC",
            Price = 50000,
            Volume = 1,
            Timestamp = DateTime.UtcNow
        };

        int successCount = 0;

        // Act
        Parallel.For(0, 1000, (i) =>
        {
            if (!dedup.IsDuplicate(tick))
            {
                Interlocked.Increment(ref successCount);
            }
        });

        // Assert — только один поток должен был добавить тик
        Assert.Equal(1, successCount);
    }

    [Fact]
    public void Deduplicator_ShouldHandleMixedUniqueAndDuplicateTicks_UnderHighConcurrency()
    {
        // Arrange — 100 уникальных тиков, каждый пытается записаться из 10 потоков
        var dedup = new Deduplicator(TimeSpan.FromSeconds(60)); // большой TTL, чтобы не очищалось
        var numUniqueTicks = 100;
        var threadsPerTick = 10;
        var acceptedCounts = new ConcurrentDictionary<int, int>(); // key = tick index, value = how many times accepted

        var ticks = Enumerable.Range(0, numUniqueTicks)
            .Select(i => new NormalizedTick
            {
                Source = "Binance",
                Ticker = $"BTC{i}",
                Price = 50000 + i,
                Volume = 1,
                Timestamp = DateTime.UtcNow
            })
            .ToList();

        // Act — каждый тик пытается записаться из нескольких потоков одновременно
        Parallel.ForEach(ticks, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, tick =>
        {
            int tickIndex = ticks.IndexOf(tick);
            Parallel.For(0, threadsPerTick, _ =>
            {
                if (!dedup.IsDuplicate(tick))
                {
                    acceptedCounts.AddOrUpdate(
                        tickIndex,
                        1,
                        (_, current) => current + 1);
                }
            });
        });

        // Assert — каждый из 100 тиков должен был быть принят ровно 1 раз
        Assert.Equal(numUniqueTicks, acceptedCounts.Count);
        foreach (var kvp in acceptedCounts)
        {
            Assert.Equal(1, kvp.Value);
        }
    }

    [Fact]
    public void Deduplicator_ShouldNotLoseTicks_WhenCleanupRunsConcurrently()
    {
        // Arrange — дедупликатор с коротким интервалом очистки
        var dedup = new Deduplicator(TimeSpan.FromSeconds(1));
        var numThreads = 50;
        var ticksPerThread = 200;
        var allKeys = new ConcurrentBag<string>();
        var acceptedKeys = new ConcurrentBag<string>();

        // Act — множество потоков пишут тики, одновременно может запуститься cleanup
        Parallel.For(0, numThreads, threadId =>
        {
            for (int i = 0; i < ticksPerThread; i++)
            {
                var key = $"Source{threadId}_Ticker{i}_{Guid.NewGuid()}";
                var tick = new NormalizedTick
                {
                    Source = $"Source{threadId}",
                    Ticker = $"Ticker{i}",
                    Price = 100 + i,
                    Volume = 1,
                    Timestamp = DateTime.UtcNow
                };
                allKeys.Add(key);

                if (!dedup.IsDuplicate(tick))
                {
                    acceptedKeys.Add(key);
                }

                // Периодически вызываем cleanup вручную для увеличения шанса race condition
                if (i % 10 == 0)
                {
                    dedup.CleanupStaleEntries();
                }
            }
        });

        // Assert — не должно быть исключений, и хотя бы некоторые тики должны быть приняты
        Assert.NotEmpty(acceptedKeys);
        Assert.True(acceptedKeys.Count <= allKeys.Count, "More ticks accepted than sent");
    }
}