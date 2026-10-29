using Dapper;
using GeocodeQueue.Worker.Options;
using Microsoft.Extensions.Options;

namespace GeocodeQueue.Worker.Data;

/// <summary>Creates the queue table and seeds it with the bundled addresses.</summary>
public sealed class DatabaseInitializer(
    SqliteConnectionFactory connections,
    IOptions<DatabaseOptions> options,
    TimeProvider time,
    ILogger<DatabaseInitializer> logger)
{
    private const string Schema = """
        PRAGMA journal_mode = WAL;

        CREATE TABLE IF NOT EXISTS Addresses (
            Id               INTEGER PRIMARY KEY AUTOINCREMENT,
            Line1            TEXT    NOT NULL,
            City             TEXT    NOT NULL,
            Region           TEXT    NOT NULL,
            PostalCode       TEXT    NULL,
            Country          TEXT    NOT NULL DEFAULT 'US',

            Status           TEXT    NOT NULL DEFAULT 'Pending'
                             CHECK (Status IN ('Pending', 'Leased', 'Completed', 'Failed')),

            -- Lease: who owns the row, the token that proves it, and when that ownership ends.
            LeaseOwner       TEXT    NULL,
            LeaseToken       TEXT    NULL,
            LeaseExpiresAt   INTEGER NULL,   -- unix ms

            Attempts         INTEGER NOT NULL DEFAULT 0,
            NextAttemptAt    INTEGER NOT NULL DEFAULT 0,   -- unix ms

            Latitude         REAL    NULL,
            Longitude        REAL    NULL,
            FormattedAddress TEXT    NULL,
            PlaceId          TEXT    NULL,
            LastError        TEXT    NULL,

            CreatedAt        INTEGER NOT NULL,
            UpdatedAt        INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS IX_Addresses_Claim
            ON Addresses (Status, NextAttemptAt, LeaseExpiresAt);
        """;

    public async Task InitializeAsync(CancellationToken ct)
    {
        await using var connection = await connections.OpenAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition(Schema, cancellationToken: ct));

        if (!options.Value.SeedOnStartup)
        {
            return;
        }

        var seedPath = Path.IsPathRooted(options.Value.SeedFile)
            ? options.Value.SeedFile
            : Path.Combine(AppContext.BaseDirectory, options.Value.SeedFile);
        var addresses = await SeedReader.ReadAsync(seedPath, ct);

        // Check and insert under one write lock so workers starting together don't both seed.
        await using var tx = connection.BeginTransaction(deferred: false);
        var existing = await connection.ExecuteScalarAsync<long>(
            new CommandDefinition("SELECT COUNT(*) FROM Addresses", transaction: tx, cancellationToken: ct));
        if (existing > 0)
        {
            logger.LogInformation("Queue already holds {Count} addresses; skipping seed", existing);
            return;
        }

        var now = time.GetUtcNow().ToUnixTimeMilliseconds();
        await connection.ExecuteAsync(new CommandDefinition(
            """
            INSERT INTO Addresses (Line1, City, Region, PostalCode, Country, CreatedAt, UpdatedAt)
            VALUES (@Line1, @City, @Region, @PostalCode, @Country, @Now, @Now)
            """,
            addresses.Select(a => new { a.Line1, a.City, a.Region, a.PostalCode, a.Country, Now = now }),
            tx,
            cancellationToken: ct));
        await tx.CommitAsync(ct);

        logger.LogInformation("Seeded {Count} addresses from {SeedFile}", addresses.Count, seedPath);
    }
}

public static class SeedReader
{
    /// <summary>Reads "Line1|City|Region|PostalCode|Country" lines; '#' starts a comment.</summary>
    public static async Task<IReadOnlyList<SeedAddress>> ReadAsync(string path, CancellationToken ct)
    {
        var result = new List<SeedAddress>();
        foreach (var raw in await File.ReadAllLinesAsync(path, ct))
        {
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith('#'))
            {
                continue;
            }

            var parts = line.Split('|');
            if (parts.Length != 5)
            {
                throw new FormatException($"Seed line must have 5 '|'-separated fields: {line}");
            }

            result.Add(new SeedAddress(
                parts[0].Trim(),
                parts[1].Trim(),
                parts[2].Trim(),
                string.IsNullOrWhiteSpace(parts[3]) ? null : parts[3].Trim(),
                parts[4].Trim()));
        }

        return result;
    }
}
