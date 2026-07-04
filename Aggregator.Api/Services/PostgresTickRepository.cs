using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Aggregator.Api.Services;

/// <summary>
/// Реализация репозитория тиков для PostgreSQL.
/// </summary>
public class PostgresTickRepository : ITickRepository
{
    private readonly string _connectionString;
    private readonly ILogger<PostgresTickRepository> _logger;

    public PostgresTickRepository(IConfiguration configuration, ILogger<PostgresTickRepository> logger)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection");
        _logger = logger;
    }

    public async Task SaveAsync(NormalizedTick tick, CancellationToken cancellationToken = default)
    {
        if (tick == null) throw new ArgumentNullException(nameof(tick));

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
        if (ticks == null) throw new ArgumentNullException(nameof(ticks));

        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(
                "INSERT INTO ticks (source, ticker, price, volume, timestamp) VALUES (@s, @t, @p, @v, @ts)", conn);

            foreach (var tick in ticks)
            {
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@s", tick.Source);
                cmd.Parameters.AddWithValue("@t", tick.Ticker);
                cmd.Parameters.AddWithValue("@p", tick.Price);
                cmd.Parameters.AddWithValue("@v", tick.Volume);
                cmd.Parameters.AddWithValue("@ts", tick.Timestamp);

                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save range of ticks");
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