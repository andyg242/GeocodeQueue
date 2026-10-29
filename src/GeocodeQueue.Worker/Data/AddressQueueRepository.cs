using Dapper;
using GeocodeQueue.Worker.Options;
using Microsoft.Extensions.Options;

namespace GeocodeQueue.Worker.Data;

/// <summary>
/// The lease pattern applied to queue rows in SQLite.
///
/// The tutorial's Redis operations map onto row updates:
///   SET key token NX PX ttl        -> ClaimAsync: take a row that is free or whose lease has expired
///   compare-token-then-extend Lua  -> RenewAsync: extend only if our token is still on the row
///   compare-token-then-delete Lua  -> ReleaseAsync: hand the row back only if we still own it
///
/// Every write that finishes a row (complete / retry / fail) also checks the token, so a worker that
/// lost its lease (GC pause, network stall, laptop lid closed) cannot overwrite the result of the
/// worker that took over. "If you cannot renew, you are not the owner."
/// </summary>
public sealed class AddressQueueRepository(
    SqliteConnectionFactory connections,
    TimeProvider time,
    IOptions<LeaseOptions> leaseOptions,
    IOptions<ProcessingOptions> processingOptions)
{
    private long Now => time.GetUtcNow().ToUnixTimeMilliseconds();

    /// <summary>
    /// Atomically leases up to <paramref name="count"/> rows to <paramref name="owner"/>. Each row gets a fresh
    /// random token. SQLite allows one writer at a time and the statement runs inside an IMMEDIATE
    /// transaction, so two workers can never claim the same row.
    /// </summary>
    public async Task<IReadOnlyList<AddressLease>> ClaimAsync(string owner, int count, CancellationToken ct)
    {
        var now = Now;
        await using var connection = await connections.OpenAsync(ct);
        // BEGIN IMMEDIATE: take the write lock up front so the SELECT and UPDATE see the same snapshot.
        await using var tx = connection.BeginTransaction(deferred: false);

        var leases = await connection.QueryAsync<AddressLease>(new CommandDefinition(
            """
            UPDATE Addresses
               SET Status         = 'Leased',
                   LeaseOwner     = @Owner,
                   LeaseToken     = lower(hex(randomblob(16))),
                   LeaseExpiresAt = @ExpiresAt,
                   Attempts       = Attempts + 1,
                   UpdatedAt      = @Now
             WHERE Id IN (
                   SELECT Id FROM Addresses
                    WHERE Attempts < @MaxAttempts
                      AND (   (Status = 'Pending' AND NextAttemptAt  <= @Now)
                           OR (Status = 'Leased'  AND LeaseExpiresAt <= @Now))
                    ORDER BY NextAttemptAt, Id
                    LIMIT @Count)
            RETURNING Id, Line1, City, Region, PostalCode, Country, LeaseToken, LeaseExpiresAt, Attempts
            """,
            new
            {
                Owner = owner,
                Now = now,
                ExpiresAt = now + (long)leaseOptions.Value.Duration.TotalMilliseconds,
                processingOptions.Value.MaxAttempts,
                Count = count,
            },
            tx,
            cancellationToken: ct));

        await tx.CommitAsync(ct);
        return leases.AsList();
    }

    /// <summary>Extends the lease if, and only if, we still hold it and it has not already expired.</summary>
    public async Task<long?> RenewAsync(AddressLease lease, CancellationToken ct)
    {
        var now = Now;
        var expiresAt = now + (long)leaseOptions.Value.Duration.TotalMilliseconds;
        await using var connection = await connections.OpenAsync(ct);
        var rows = await connection.ExecuteAsync(new CommandDefinition(
            """
            UPDATE Addresses
               SET LeaseExpiresAt = @ExpiresAt, UpdatedAt = @Now
             WHERE Id = @Id AND LeaseToken = @Token AND Status = 'Leased' AND LeaseExpiresAt > @Now
            """,
            new { lease.Id, Token = lease.LeaseToken, ExpiresAt = expiresAt, Now = now },
            cancellationToken: ct));
        return rows == 1 ? expiresAt : null;
    }

    public Task<bool> CompleteAsync(
        AddressLease lease, double latitude, double longitude, string? formattedAddress, string? placeId,
        CancellationToken ct) =>
        ExecuteFencedAsync(
            """
            UPDATE Addresses
               SET Status = 'Completed', Latitude = @Latitude, Longitude = @Longitude,
                   FormattedAddress = @FormattedAddress, PlaceId = @PlaceId, LastError = NULL,
                   LeaseOwner = NULL, LeaseToken = NULL, LeaseExpiresAt = NULL, UpdatedAt = @Now
             WHERE Id = @Id AND LeaseToken = @Token AND Status = 'Leased'
            """,
            new { lease.Id, Token = lease.LeaseToken, Latitude = latitude, Longitude = longitude, FormattedAddress = formattedAddress, PlaceId = placeId, Now },
            ct);

    /// <summary>Gives the row back for another attempt after <paramref name="delay"/>.</summary>
    public Task<bool> ScheduleRetryAsync(AddressLease lease, string error, TimeSpan delay, CancellationToken ct) =>
        ExecuteFencedAsync(
            """
            UPDATE Addresses
               SET Status = CASE WHEN Attempts >= @MaxAttempts THEN 'Failed' ELSE 'Pending' END,
                   NextAttemptAt = @NextAttemptAt, LastError = @Error,
                   LeaseOwner = NULL, LeaseToken = NULL, LeaseExpiresAt = NULL, UpdatedAt = @Now
             WHERE Id = @Id AND LeaseToken = @Token AND Status = 'Leased'
            """,
            new
            {
                lease.Id, Token = lease.LeaseToken, Error = error, Now,
                NextAttemptAt = Now + (long)delay.TotalMilliseconds,
                processingOptions.Value.MaxAttempts,
            },
            ct);

    public Task<bool> FailAsync(AddressLease lease, string error, CancellationToken ct) =>
        ExecuteFencedAsync(
            """
            UPDATE Addresses
               SET Status = 'Failed', LastError = @Error,
                   LeaseOwner = NULL, LeaseToken = NULL, LeaseExpiresAt = NULL, UpdatedAt = @Now
             WHERE Id = @Id AND LeaseToken = @Token AND Status = 'Leased'
            """,
            new { lease.Id, Token = lease.LeaseToken, Error = error, Now },
            ct);

    /// <summary>
    /// Hands the row straight back (graceful shutdown or a configuration problem) without counting the attempt,
    /// so another worker can pick it up immediately instead of waiting for the lease to expire.
    /// </summary>
    public Task<bool> ReleaseAsync(AddressLease lease, CancellationToken ct) =>
        ExecuteFencedAsync(
            """
            UPDATE Addresses
               SET Status = 'Pending', Attempts = max(Attempts - 1, 0),
                   LeaseOwner = NULL, LeaseToken = NULL, LeaseExpiresAt = NULL, UpdatedAt = @Now
             WHERE Id = @Id AND LeaseToken = @Token AND Status = 'Leased'
            """,
            new { lease.Id, Token = lease.LeaseToken, Now },
            ct);

    /// <summary>Rows whose lease expired on their final attempt would never be claimed again; mark them failed.</summary>
    public async Task<int> FailAbandonedAsync(CancellationToken ct)
    {
        await using var connection = await connections.OpenAsync(ct);
        return await connection.ExecuteAsync(new CommandDefinition(
            """
            UPDATE Addresses
               SET Status = 'Failed', LastError = coalesce(LastError, 'Lease expired on final attempt'),
                   LeaseOwner = NULL, LeaseToken = NULL, LeaseExpiresAt = NULL, UpdatedAt = @Now
             WHERE Status = 'Leased' AND LeaseExpiresAt <= @Now AND Attempts >= @MaxAttempts
            """,
            new { Now, processingOptions.Value.MaxAttempts },
            cancellationToken: ct));
    }

    public async Task<QueueStats> GetStatsAsync(CancellationToken ct)
    {
        await using var connection = await connections.OpenAsync(ct);
        return await connection.QuerySingleAsync<QueueStats>(new CommandDefinition(
            """
            SELECT coalesce(sum(Status = 'Pending'), 0)   AS Pending,
                   coalesce(sum(Status = 'Leased'), 0)    AS Leased,
                   coalesce(sum(Status = 'Completed'), 0) AS Completed,
                   coalesce(sum(Status = 'Failed'), 0)    AS Failed
              FROM Addresses
            """,
            cancellationToken: ct));
    }

    private async Task<bool> ExecuteFencedAsync(string sql, object args, CancellationToken ct)
    {
        await using var connection = await connections.OpenAsync(ct);
        return await connection.ExecuteAsync(new CommandDefinition(sql, args, cancellationToken: ct)) == 1;
    }
}
