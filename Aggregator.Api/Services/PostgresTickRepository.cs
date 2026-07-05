using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Aggregator.Api.Services;

/// <summary>
/// Реализация репозитория тиков для PostgreSQL.
/// </summary>
public class PostgresTickRepository(IConfiguration configuration, ILogger<PostgresTickRepository> logger) : ITickRepository
{
    private readonly string _connectionString = configuration.GetConnectionString("DefaultConnection")
        ?? throw new ArgumentNullException("Connection string 'DefaultConnection' is not configured.");
    private readonly ILogger<PostgresTickRepository> _logger = logger;

    public async Task SaveAsync(NormalizedTick tick, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tick);

        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(
                "INSERT INTO ticks (source, ticker, price, volume, timestamp) VALUES (@s, @t, @p, @v, @ts)", conn);

            cmd.Parameters.AddWithValue("@s", tick.Source);
            cmd.Parameters.AddWithValue("@t", tick.Ticker);
            cmd.Parameters.AddWithValue("@p", tick.Price);
            cmd.Parameters.AddWithValue("@v", tick.Volume);
            cmd.Parameters.AddWithValue("@ts", tick.Timestamp);

            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save tick: {Ticker} from {Source}", tick.Ticker, tick.Source);
            throw;
        }
    }

    public async Task SaveRangeAsync(IEnumerable<NormalizedTick> ticks, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(ticks);

        var ticksList = ticks.ToList();
        if (ticksList.Count == 0) return;

        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var transaction = await conn.BeginTransactionAsync(cancellationToken);

            await using var writer = await conn.BeginBinaryImportAsync(
                "COPY ticks (source, ticker, price, volume, timestamp) FROM STDIN (FORMAT BINARY)", cancellationToken);

            foreach (var tick in ticksList)
            {
                await writer.StartRowAsync(cancellationToken);
                await writer.WriteAsync(tick.Source ?? string.Empty, cancellationToken);
                await writer.WriteAsync(tick.Ticker ?? string.Empty, cancellationToken);
                await writer.WriteAsync(tick.Price, cancellationToken);
                await writer.WriteAsync(tick.Volume, cancellationToken);
                await writer.WriteAsync(tick.Timestamp, cancellationToken);
            }

            await writer.CompleteAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save range of {Count} ticks", ticksList.Count);
            throw;
        }
    }

    public async Task<IEnumerable<NormalizedTick>> GetTicksAsync(int limit = 100, CancellationToken cancellationToken = default)
    {
        var ticks = new List<NormalizedTick>();

        try
        {
            var sql = "SELECT source, ticker, price, volume, timestamp FROM ticks ORDER BY timestamp DESC LIMIT @limit";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@limit", limit);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                ticks.Add(new NormalizedTick
                {
                    Source = reader.GetString(0),
                    Ticker = reader.GetString(1),
                    Price = reader.GetDecimal(2),
                    Volume = reader.GetDecimal(3),
                    Timestamp = reader.GetDateTime(4)
                });
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get ticks");
            throw;
        }

        return ticks;
    }
}