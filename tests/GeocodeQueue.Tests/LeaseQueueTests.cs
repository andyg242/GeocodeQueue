using GeocodeQueue.Worker.Data;
using GeocodeQueue.Worker.Options;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace GeocodeQueue.Tests;

public sealed class LeaseQueueTests : IAsyncLifetime
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"geocode-tests-{Guid.NewGuid():N}.db");
    private readonly FakeTimeProvider _time = new(DateTimeOffset.Parse("2026-01-01T00:00:00Z"));
    private readonly LeaseOptions _lease = new() { Duration = TimeSpan.FromSeconds(30) };
    private readonly ProcessingOptions _processing = new() { MaxAttempts = 3 };
    private AddressQueueRepository _queue = null!;

    public async Task InitializeAsync()
    {
        var db = MsOptions.Create(new DatabaseOptions
        {
            ConnectionString = $"Data Source={_dbPath}",
            SeedFile = Path.Combine(AppContext.BaseDirectory, "Seed", "addresses.txt"),
        });
        var connections = new SqliteConnectionFactory(db);
        await new DatabaseInitializer(connections, db, _time, NullLogger<DatabaseInitializer>.Instance)
            .InitializeAsync(CancellationToken.None);
        _queue = new AddressQueueRepository(connections, _time, MsOptions.Create(_lease), MsOptions.Create(_processing));
    }

    public Task DisposeAsync()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { "", "-wal", "-shm" })
        {
            File.Delete(_dbPath + suffix);
        }

        return Task.CompletedTask;
    }

    [Fact]
    public async Task Seeds_200_addresses()
    {
        var stats = await _queue.GetStatsAsync(default);
        Assert.Equal(new QueueStats(200, 0, 0, 0), stats);
    }

    [Fact]
    public async Task Concurrent_workers_never_lease_the_same_row()
    {
        var claimed = new System.Collections.Concurrent.ConcurrentBag<long>();
        var workers = Enumerable.Range(0, 8).Select(w => Task.Run(async () =>
        {
            while (true)
            {
                var batch = await _queue.ClaimAsync($"worker-{w}", 3, default);
                if (batch.Count == 0)
                {
                    return;
                }

                foreach (var lease in batch)
                {
                    claimed.Add(lease.Id);
                }
            }
        }));
        await Task.WhenAll(workers);

        Assert.Equal(200, claimed.Count);
        Assert.Equal(200, claimed.Distinct().Count());
    }

    [Fact]
    public async Task Renewal_keeps_the_lease_alive_past_its_original_expiry()
    {
        var lease = (await _queue.ClaimAsync("a", 1, default)).Single();

        _time.Advance(TimeSpan.FromSeconds(20));
        Assert.NotNull(await _queue.RenewAsync(lease, default));
        _time.Advance(TimeSpan.FromSeconds(20)); // 40s after claim, but only 20s after renewal

        Assert.DoesNotContain(await _queue.ClaimAsync("b", 200, default), l => l.Id == lease.Id);
        Assert.True(await _queue.CompleteAsync(lease, 1, 2, "x", null, default));
    }

    [Fact]
    public async Task Expired_lease_is_taken_over_and_the_old_owner_is_fenced_out()
    {
        var stale = (await _queue.ClaimAsync("slow-worker", 1, default)).Single();

        _time.Advance(TimeSpan.FromSeconds(31)); // slow-worker stalls past its lease
        var takeover = (await _queue.ClaimAsync("fast-worker", 1, default)).Single();
        Assert.Equal(stale.Id, takeover.Id);
        Assert.NotEqual(stale.LeaseToken, takeover.LeaseToken);
        Assert.Equal(2, takeover.Attempts);

        // "If you cannot renew, you are not the owner" and none of its writes land.
        Assert.Null(await _queue.RenewAsync(stale, default));
        Assert.False(await _queue.CompleteAsync(stale, 0, 0, "stale", null, default));
        Assert.False(await _queue.ReleaseAsync(stale, default));

        Assert.True(await _queue.CompleteAsync(takeover, 40.7484, -73.9857, "fresh", null, default));
        Assert.Equal(1, (await _queue.GetStatsAsync(default)).Completed);
    }

    [Fact]
    public async Task Release_hands_the_row_back_immediately_without_using_an_attempt()
    {
        var lease = (await _queue.ClaimAsync("a", 1, default)).Single();
        Assert.True(await _queue.ReleaseAsync(lease, default));

        var again = (await _queue.ClaimAsync("b", 1, default)).Single();
        Assert.Equal(lease.Id, again.Id);
        Assert.Equal(1, again.Attempts);
    }

    [Fact]
    public async Task Retries_wait_for_their_delay_then_fail_after_max_attempts()
    {
        var first = (await _queue.ClaimAsync("a", 1, default)).Single();
        await _queue.ScheduleRetryAsync(first, "OVER_QUERY_LIMIT", TimeSpan.FromMinutes(1), default);

        // Not eligible until the delay passes; other rows are picked instead.
        Assert.DoesNotContain(await _queue.ClaimAsync("a", 199, default), l => l.Id == first.Id);

        for (var attempt = 2; attempt <= _processing.MaxAttempts; attempt++)
        {
            _time.Advance(TimeSpan.FromMinutes(2));
            _time.Advance(_lease.Duration); // let the 199 leases from above lapse; irrelevant here
            var lease = (await _queue.ClaimAsync("a", 200, default)).Single(l => l.Id == first.Id);
            Assert.Equal(attempt, lease.Attempts);
            await _queue.ScheduleRetryAsync(lease, "OVER_QUERY_LIMIT", TimeSpan.FromMinutes(1), default);
        }

        _time.Advance(TimeSpan.FromMinutes(2));
        Assert.DoesNotContain(await _queue.ClaimAsync("a", 200, default), l => l.Id == first.Id);
        Assert.Equal(1, (await _queue.GetStatsAsync(default)).Failed);
    }

    [Fact]
    public async Task Lease_that_expires_on_the_final_attempt_is_marked_failed()
    {
        var lease = (await _queue.ClaimAsync("a", 1, default)).Single();
        for (var i = 1; i < _processing.MaxAttempts; i++)
        {
            _time.Advance(TimeSpan.FromSeconds(31));
            lease = (await _queue.ClaimAsync("a", 1, default)).Single();
        }

        _time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal(1, await _queue.FailAbandonedAsync(default));
        Assert.Equal(1, (await _queue.GetStatsAsync(default)).Failed);
    }
}
