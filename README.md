# GeocodeQueue

A .NET 10 worker service that drains a SQLite queue of addresses, calls the Google Maps Geocoding API, and
stores latitude/longitude. Rows are claimed with the **lease pattern** from Chris Woodruff's
[Lease Pattern in .NET](https://woodruff.dev/blog/lease-pattern-in-net-a-lock-with-an-expiration-date-that-saves-your-data/),
so you can run any number of workers against the same database without double-processing a row or losing one
when a worker dies.

The database is created and seeded with **200 real US addresses** (civic buildings, museums, stadiums, campuses)
on first run.

## Run it

```bash
# With Google (key from env var, user secrets, or Geocoding__ApiKey)
export GOOGLE_MAPS_API_KEY=your-key
dotnet run --project src/GeocodeQueue.Worker

# Or keep the key out of your shell history
cd src/GeocodeQueue.Worker && dotnet user-secrets set Geocoding:ApiKey your-key && dotnet run

# Offline, without a key: deterministic fake coordinates
dotnet run --project src/GeocodeQueue.Worker -- --Geocoding:Provider=Fake --Processing:ExitWhenQueueEmpty=true
```

The database file (`geocode-queue.db`) lands in the working directory. Inspect results with:

```bash
sqlite3 geocode-queue.db "SELECT Status, COUNT(*) FROM Addresses GROUP BY Status;"
sqlite3 geocode-queue.db "SELECT Line1, City, Latitude, Longitude, FormattedAddress FROM Addresses WHERE Status='Completed' LIMIT 10;"
```

Requeue failed rows: `UPDATE Addresses SET Status='Pending', Attempts=0, NextAttemptAt=0 WHERE Status='Failed';`

Run tests: `dotnet test`

## How the lease pattern maps onto SQLite

The tutorial leases a single Redis key for leader election. Here every queue row carries its own lease, which
gives the same guarantees per unit of work:

| Tutorial (Redis)                                 | Here (SQLite, `AddressQueueRepository`)                                                                 |
|--------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `SET key token NX PX ttl`                        | `ClaimAsync`: `UPDATE … RETURNING` a row that is `Pending`, or `Leased` with an expired lease. Fresh random `LeaseToken` per claim, inside a `BEGIN IMMEDIATE` transaction |
| Lua: renew only if token matches                 | `RenewAsync`: extend `LeaseExpiresAt` only `WHERE LeaseToken = @Token AND LeaseExpiresAt > now`          |
| Lua: delete only if token matches                | `ReleaseAsync`: on graceful shutdown, hand the row back instantly (attempt not counted)                 |
| Renew every ~TTL/3 plus 0–250 ms jitter          | `GeocodingWorker.KeepLeaseAliveAsync` heartbeat while Google is being called                            |
| "If you cannot renew, you are not the owner"     | A failed renewal cancels the in-flight Google call and the worker writes nothing                        |
| Fencing at the write boundary (tutorial's follow-up) | `CompleteAsync` / `ScheduleRetryAsync` / `FailAsync` all require the caller's token, so a stalled worker that lost its lease can't overwrite the new owner's result |

Processing flow per lane (`Processing:Concurrency` lanes, default 4):

1. Claim one row (lease 30 s, attempt counter +1).
2. Start a heartbeat that renews the lease every ~10 s.
3. Call Google. Then, by outcome:
   - `OK` → `Completed` with lat/lng, formatted address, place id.
   - `ZERO_RESULTS` / `INVALID_REQUEST` → `Failed` (won't ever succeed).
   - `OVER_QUERY_LIMIT`, `UNKNOWN_ERROR`, 429/5xx, timeouts → back to `Pending` with exponential backoff; `Failed` after `MaxAttempts`.
   - `REQUEST_DENIED` (bad/restricted key) → row released untouched and the host stops, so a bad key doesn't burn the attempts of all 200 rows.
4. If the process crashes, its leases simply expire and another worker reclaims the rows.

## Configuration (`appsettings.json`, env vars with `__`, or command line)

| Setting | Default | Notes |
|---|---|---|
| `Database:ConnectionString` | `Data Source=geocode-queue.db` | WAL mode, 30 s busy timeout |
| `Database:SeedOnStartup` | `true` | Seeds `Seed/addresses.txt` only when the table is empty |
| `Lease:Duration` | `00:00:30` | Renewal at a third of this |
| `Lease:RenewalJitter` | `00:00:00.250` | |
| `Processing:Concurrency` | `4` | Lanes per process (Google's default limit is 50 QPS) |
| `Processing:MaxAttempts` | `5` | |
| `Processing:RetryBaseDelay` | `00:00:10` | Doubles each attempt |
| `Processing:ExitWhenQueueEmpty` | `false` | `true` for a one-shot batch run |
| `Geocoding:Provider` | `Google` | `Fake` for offline runs |
| `Geocoding:ApiKey` | — | Required for Google. Also read from `GOOGLE_MAPS_API_KEY` |

## Layout

```
src/GeocodeQueue.Worker
  Program.cs                      DI, options validation, DB init
  Data/AddressQueueRepository.cs  claim / renew / complete / retry / fail / release (the lease SQL)
  Data/DatabaseInitializer.cs     schema + seed
  Processing/GeocodingWorker.cs   BackgroundService: lanes, heartbeat, outcome handling
  Geocoding/GoogleGeocoder.cs     Google Maps Geocoding API client
  Geocoding/FakeGeocoder.cs       offline stand-in
  Seed/addresses.txt              200 addresses
tests/GeocodeQueue.Tests          lease behaviour tests (concurrency, takeover, fencing, retries)
```
