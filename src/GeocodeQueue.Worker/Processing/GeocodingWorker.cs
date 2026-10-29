using GeocodeQueue.Worker.Data;
using GeocodeQueue.Worker.Geocoding;
using GeocodeQueue.Worker.Options;
using Microsoft.Extensions.Options;

namespace GeocodeQueue.Worker.Processing;

/// <summary>
/// Drains the address queue. Runs <see cref="ProcessingOptions.Concurrency"/> lanes; each lane leases one row,
/// keeps the lease alive with a heartbeat while Google is called, and writes the result back with a fenced update.
/// Run as many copies of this process as you like against the same database: leases keep them from colliding.
/// </summary>
public sealed class GeocodingWorker(
    AddressQueueRepository queue,
    IGeocoder geocoder,
    IOptions<LeaseOptions> leaseOptions,
    IOptions<ProcessingOptions> processingOptions,
    IHostApplicationLifetime lifetime,
    ILogger<GeocodingWorker> logger) : BackgroundService
{
    // Unique per process so leases can be traced back to the worker that holds them.
    private readonly string _owner = $"{Environment.MachineName}:{Environment.ProcessId}:{Guid.NewGuid().ToString("N")[..8]}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = processingOptions.Value;
        logger.LogInformation(
            "Worker {Owner} starting {Lanes} lanes, lease {Lease}, renew every ~{Renew}",
            _owner, settings.Concurrency, leaseOptions.Value.Duration, leaseOptions.Value.RenewalInterval);

        var lanes = Enumerable.Range(1, settings.Concurrency).Select(lane => RunLaneAsync(lane, stoppingToken));
        await Task.WhenAll(lanes);

        var stats = await queue.GetStatsAsync(CancellationToken.None);
        logger.LogInformation(
            "Worker {Owner} stopped. Pending {Pending}, leased {Leased}, completed {Completed}, failed {Failed}",
            _owner, stats.Pending, stats.Leased, stats.Completed, stats.Failed);
    }

    private async Task RunLaneAsync(int lane, CancellationToken stoppingToken)
    {
        var settings = processingOptions.Value;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var leases = await queue.ClaimAsync(_owner, 1, stoppingToken);
                if (leases.Count == 0)
                {
                    if (await IsQueueDrainedAsync(stoppingToken))
                    {
                        return;
                    }

                    await Task.Delay(settings.IdlePollInterval, stoppingToken);
                    continue;
                }

                await ProcessAsync(lane, leases[0], stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // A database hiccup should not kill the lane; leases we failed to finish simply expire.
                logger.LogError(ex, "Lane {Lane} hit an unexpected error; backing off", lane);
                await Task.Delay(settings.IdlePollInterval, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
        }
    }

    private async Task<bool> IsQueueDrainedAsync(CancellationToken ct)
    {
        var abandoned = await queue.FailAbandonedAsync(ct);
        if (abandoned > 0)
        {
            logger.LogWarning("Marked {Count} rows failed after their final lease expired", abandoned);
        }

        if (!processingOptions.Value.ExitWhenQueueEmpty)
        {
            return false;
        }

        var stats = await queue.GetStatsAsync(ct);
        if (stats.Outstanding > 0)
        {
            return false;
        }

        logger.LogInformation("Queue is empty; stopping because Processing:ExitWhenQueueEmpty is set");
        lifetime.StopApplication();
        return true;
    }

    private async Task ProcessAsync(int lane, AddressLease lease, CancellationToken stoppingToken)
    {
        // Cancelled when the host stops OR when we discover we no longer own the row.
        using var work = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        using var heartbeatStop = new CancellationTokenSource();
        var heartbeat = KeepLeaseAliveAsync(lease, work, heartbeatStop.Token);

        try
        {
            logger.LogDebug("Lane {Lane} leased #{Id} (attempt {Attempt})", lane, lease.Id, lease.Attempts);
            var result = await geocoder.GeocodeAsync(lease.FullAddress, work.Token);

            // Stop renewing before the final write so the heartbeat cannot race it.
            await heartbeatStop.CancelAsync();
            await heartbeat;

            await RecordAsync(lane, lease, result);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Graceful shutdown: hand the row back now rather than making others wait out the lease.
            await heartbeatStop.CancelAsync();
            await heartbeat;
            await queue.ReleaseAsync(lease, CancellationToken.None);
            logger.LogInformation("Lane {Lane} released #{Id} during shutdown", lane, lease.Id);
        }
        catch (OperationCanceledException) when (work.IsCancellationRequested)
        {
            // Lease lost mid-flight. Another worker may own the row now, so we must not write anything.
            await heartbeat;
            logger.LogWarning("Lane {Lane} abandoned #{Id}: lease lost before Google responded", lane, lease.Id);
        }
        catch (Exception ex)
        {
            await heartbeatStop.CancelAsync();
            await heartbeat;
            await queue.ScheduleRetryAsync(lease, ex.Message, RetryDelay(lease), CancellationToken.None);
            logger.LogError(ex, "Lane {Lane} failed on #{Id}; scheduled retry", lane, lease.Id);
        }
    }

    private async Task RecordAsync(int lane, AddressLease lease, GeocodeResult result)
    {
        var ct = CancellationToken.None; // finish the write even if shutdown has begun
        bool stillOwner;
        switch (result.Outcome)
        {
            case GeocodeOutcome.Success:
                stillOwner = await queue.CompleteAsync(
                    lease, result.Latitude, result.Longitude, result.FormattedAddress, result.PlaceId, ct);
                if (stillOwner)
                {
                    logger.LogInformation(
                        "Lane {Lane} geocoded #{Id} {Address} -> {Lat}, {Lng}",
                        lane, lease.Id, lease.FullAddress, result.Latitude, result.Longitude);
                }

                break;

            case GeocodeOutcome.NotFound:
                stillOwner = await queue.FailAsync(lease, result.Error!, ct);
                logger.LogWarning("Lane {Lane} could not geocode #{Id} {Address}: {Error}", lane, lease.Id, lease.FullAddress, result.Error);
                break;

            case GeocodeOutcome.Transient:
                var delay = RetryDelay(lease);
                stillOwner = await queue.ScheduleRetryAsync(lease, result.Error!, delay, ct);
                logger.LogWarning("Lane {Lane} will retry #{Id} in {Delay}: {Error}", lane, lease.Id, delay, result.Error);
                break;

            case GeocodeOutcome.Fatal:
                // Every call will fail the same way (e.g. bad API key). Return the row untouched and stop the host
                // instead of burning through the attempts of all 200 rows.
                stillOwner = await queue.ReleaseAsync(lease, ct);
                logger.LogCritical("Google rejected the request ({Error}). Check Geocoding:ApiKey. Stopping.", result.Error);
                lifetime.StopApplication();
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(result), result.Outcome, null);
        }

        if (!stillOwner)
        {
            logger.LogWarning("Lane {Lane} discarded result for #{Id}: lease was taken over by another worker", lane, lease.Id);
        }
    }

    /// <summary>
    /// The tutorial's renewal loop, scoped to one row: renew at ~1/3 of the lease plus jitter. If renewal fails we are
    /// no longer the owner, so cancel the in-flight Google call.
    /// </summary>
    private async Task KeepLeaseAliveAsync(AddressLease lease, CancellationTokenSource work, CancellationToken stop)
    {
        var options = leaseOptions.Value;
        try
        {
            while (true)
            {
                var jitter = TimeSpan.FromMilliseconds(Random.Shared.NextDouble() * options.RenewalJitter.TotalMilliseconds);
                await Task.Delay(options.RenewalInterval + jitter, stop);

                if (await queue.RenewAsync(lease, stop) is null)
                {
                    logger.LogWarning("Lease on #{Id} could not be renewed; this worker is no longer the owner", lease.Id);
                    await work.CancelAsync();
                    return;
                }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            // Couldn't reach the database to renew: assume the worst.
            logger.LogWarning(ex, "Lease renewal for #{Id} failed; treating the lease as lost", lease.Id);
            await work.CancelAsync();
        }
    }

    private TimeSpan RetryDelay(AddressLease lease)
    {
        var baseDelay = processingOptions.Value.RetryBaseDelay;
        var exponential = baseDelay * Math.Pow(2, Math.Max(0, lease.Attempts - 1));
        var jitter = TimeSpan.FromMilliseconds(Random.Shared.NextDouble() * baseDelay.TotalMilliseconds / 2);
        return exponential + jitter;
    }
}
