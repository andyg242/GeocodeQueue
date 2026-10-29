namespace GeocodeQueue.Worker.Data;

public static class AddressStatus
{
    public const string Pending = "Pending";
    public const string Leased = "Leased";
    public const string Completed = "Completed";
    public const string Failed = "Failed";
}

/// <summary>A queue row this worker currently holds a lease on.</summary>
public sealed record AddressLease(
    long Id,
    string Line1,
    string City,
    string Region,
    string? PostalCode,
    string Country,
    string LeaseToken,
    long LeaseExpiresAt,
    long Attempts)
{
    public string FullAddress =>
        string.Join(", ", new[] { Line1, City, $"{Region} {PostalCode}".Trim(), Country }
            .Where(p => !string.IsNullOrWhiteSpace(p)));
}

public sealed record SeedAddress(string Line1, string City, string Region, string? PostalCode, string Country);

public sealed record QueueStats(long Pending, long Leased, long Completed, long Failed)
{
    public long Outstanding => Pending + Leased;
}
