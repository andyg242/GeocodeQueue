namespace GeocodeQueue.Worker.Geocoding;

public interface IGeocoder
{
    Task<GeocodeResult> GeocodeAsync(string address, CancellationToken ct);
}

public enum GeocodeOutcome
{
    /// <summary>Coordinates found.</summary>
    Success,

    /// <summary>The address will never resolve (ZERO_RESULTS, INVALID_REQUEST). Mark the row failed.</summary>
    NotFound,

    /// <summary>Worth trying again later (OVER_QUERY_LIMIT, UNKNOWN_ERROR, 5xx, timeouts).</summary>
    Transient,

    /// <summary>Nothing will succeed until someone fixes configuration (REQUEST_DENIED: bad or restricted key).</summary>
    Fatal,
}

public sealed record GeocodeResult(
    GeocodeOutcome Outcome,
    double Latitude = 0,
    double Longitude = 0,
    string? FormattedAddress = null,
    string? PlaceId = null,
    string? Error = null)
{
    public static GeocodeResult Found(double lat, double lng, string? formatted, string? placeId) =>
        new(GeocodeOutcome.Success, lat, lng, formatted, placeId);

    public static GeocodeResult Problem(GeocodeOutcome outcome, string error) => new(outcome, Error: error);
}
