using System.Security.Cryptography;
using System.Text;
using GeocodeQueue.Worker.Options;
using Microsoft.Extensions.Options;

namespace GeocodeQueue.Worker.Geocoding;

/// <summary>
/// Offline stand-in for Google: returns stable coordinates inside the continental US derived from a hash of the
/// address. Use it to exercise the queue without an API key or quota.
/// </summary>
public sealed class FakeGeocoder(IOptions<GeocodingOptions> options) : IGeocoder
{
    public async Task<GeocodeResult> GeocodeAsync(string address, CancellationToken ct)
    {
        await Task.Delay(Random.Shared.Next(50, 250), ct);

        if (Random.Shared.NextDouble() < options.Value.FakeTransientFailureRate)
        {
            return GeocodeResult.Problem(GeocodeOutcome.Transient, "Simulated OVER_QUERY_LIMIT");
        }

        var hash = BitConverter.ToUInt64(SHA256.HashData(Encoding.UTF8.GetBytes(address)));
        var lat = 25 + (hash & 0xFFFFFFFF) / (double)uint.MaxValue * 24;      // 25..49 N
        var lng = -124 + (hash >> 32) / (double)uint.MaxValue * 57;           // -124..-67 W
        return GeocodeResult.Found(Math.Round(lat, 6), Math.Round(lng, 6), $"{address} (fake)", null);
    }
}
