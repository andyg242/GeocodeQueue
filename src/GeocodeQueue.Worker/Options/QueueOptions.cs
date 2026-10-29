using System.ComponentModel.DataAnnotations;

namespace GeocodeQueue.Worker.Options;

public sealed class DatabaseOptions
{
    public const string SectionName = "Database";

    [Required]
    public string ConnectionString { get; set; } = "Data Source=geocode-queue.db";

    /// <summary>Seed the bundled addresses when the queue table is empty.</summary>
    public bool SeedOnStartup { get; set; } = true;

    public string SeedFile { get; set; } = "Seed/addresses.txt";
}

public sealed class LeaseOptions
{
    public const string SectionName = "Lease";

    /// <summary>How long a claimed row belongs to this worker before another worker may take it.</summary>
    [Range(typeof(TimeSpan), "00:00:01", "01:00:00")]
    public TimeSpan Duration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Random jitter added to each renewal so workers don't renew in lockstep.</summary>
    [Range(typeof(TimeSpan), "00:00:00", "00:00:10")]
    public TimeSpan RenewalJitter { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Renew at roughly a third of the lease duration, as the tutorial recommends.</summary>
    public TimeSpan RenewalInterval => Duration / 3;
}

public sealed class ProcessingOptions
{
    public const string SectionName = "Processing";

    /// <summary>Number of rows processed concurrently. Each lane claims and leases one row at a time.</summary>
    [Range(1, 64)]
    public int Concurrency { get; set; } = 4;

    [Range(1, 50)]
    public int MaxAttempts { get; set; } = 5;

    [Range(typeof(TimeSpan), "00:00:00.100", "00:10:00")]
    public TimeSpan IdlePollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Base delay for retries; doubles with every attempt.</summary>
    [Range(typeof(TimeSpan), "00:00:00", "01:00:00")]
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Stop the host once nothing is left to process (handy for one-off runs).</summary>
    public bool ExitWhenQueueEmpty { get; set; }
}

public sealed class GeocodingOptions
{
    public const string SectionName = "Geocoding";

    /// <summary>"Google" calls the Google Maps Geocoding API; "Fake" returns deterministic coordinates offline.</summary>
    [Required]
    [RegularExpression("^(Google|Fake)$")]
    public string Provider { get; set; } = "Google";

    public string? ApiKey { get; set; }

    [Required]
    public string BaseUrl { get; set; } = "https://maps.googleapis.com/maps/api/geocode/json";

    /// <summary>Fake provider only: share of calls that fail transiently, to exercise retries.</summary>
    [Range(0.0, 1.0)]
    public double FakeTransientFailureRate { get; set; }
}
