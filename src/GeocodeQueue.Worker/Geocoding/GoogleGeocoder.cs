using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using GeocodeQueue.Worker.Options;
using Microsoft.Extensions.Options;

namespace GeocodeQueue.Worker.Geocoding;

/// <summary>Calls the Google Maps Geocoding API: https://developers.google.com/maps/documentation/geocoding</summary>
public sealed class GoogleGeocoder(HttpClient http, IOptions<GeocodingOptions> options, ILogger<GoogleGeocoder> logger)
    : IGeocoder
{
    public async Task<GeocodeResult> GeocodeAsync(string address, CancellationToken ct)
    {
        var settings = options.Value;
        var url = $"{settings.BaseUrl}?address={Uri.EscapeDataString(address)}&key={Uri.EscapeDataString(settings.ApiKey!)}";

        HttpResponseMessage response;
        try
        {
            response = await http.GetAsync(url, ct);
        }
        catch (HttpRequestException ex)
        {
            return GeocodeResult.Problem(GeocodeOutcome.Transient, $"HTTP error: {ex.Message}");
        }
        catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
        {
            return GeocodeResult.Problem(GeocodeOutcome.Transient, $"Timed out: {ex.Message}");
        }

        using (response)
        {
            if (response.StatusCode is HttpStatusCode.TooManyRequests || (int)response.StatusCode >= 500)
            {
                return GeocodeResult.Problem(GeocodeOutcome.Transient, $"HTTP {(int)response.StatusCode}");
            }

            if (!response.IsSuccessStatusCode)
            {
                return GeocodeResult.Problem(GeocodeOutcome.Fatal, $"HTTP {(int)response.StatusCode}");
            }

            var body = await response.Content.ReadFromJsonAsync<GeocodeResponse>(ct);
            if (body is null)
            {
                return GeocodeResult.Problem(GeocodeOutcome.Transient, "Empty response body");
            }

            var detail = string.IsNullOrEmpty(body.ErrorMessage) ? body.Status : $"{body.Status}: {body.ErrorMessage}";
            switch (body.Status)
            {
                case "OK" when body.Results is [var first, ..]:
                    if (body.Results.Count > 1)
                    {
                        logger.LogDebug("{Count} matches for {Address}; using the first", body.Results.Count, address);
                    }

                    return GeocodeResult.Found(
                        first.Geometry.Location.Lat, first.Geometry.Location.Lng, first.FormattedAddress, first.PlaceId);
                case "OK":
                case "ZERO_RESULTS":
                    return GeocodeResult.Problem(GeocodeOutcome.NotFound, "ZERO_RESULTS");
                case "INVALID_REQUEST":
                    return GeocodeResult.Problem(GeocodeOutcome.NotFound, detail);
                case "REQUEST_DENIED":
                    return GeocodeResult.Problem(GeocodeOutcome.Fatal, detail);
                default: // OVER_DAILY_LIMIT, OVER_QUERY_LIMIT, UNKNOWN_ERROR
                    return GeocodeResult.Problem(GeocodeOutcome.Transient, detail);
            }
        }
    }

    private sealed record GeocodeResponse(
        [property: JsonPropertyName("status")] string Status,
        [property: JsonPropertyName("error_message")] string? ErrorMessage,
        [property: JsonPropertyName("results")] List<GeocodeResponseResult>? Results);

    private sealed record GeocodeResponseResult(
        [property: JsonPropertyName("formatted_address")] string? FormattedAddress,
        [property: JsonPropertyName("place_id")] string? PlaceId,
        [property: JsonPropertyName("geometry")] Geometry Geometry);

    private sealed record Geometry([property: JsonPropertyName("location")] LatLng Location);

    private sealed record LatLng(
        [property: JsonPropertyName("lat")] double Lat,
        [property: JsonPropertyName("lng")] double Lng);
}
