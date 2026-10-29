using GeocodeQueue.Worker.Data;
using GeocodeQueue.Worker.Geocoding;
using GeocodeQueue.Worker.Options;
using GeocodeQueue.Worker.Processing;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

// Accept the conventional GOOGLE_MAPS_API_KEY env var as well as Geocoding__ApiKey / user secrets.
if (string.IsNullOrWhiteSpace(builder.Configuration["Geocoding:ApiKey"])
    && Environment.GetEnvironmentVariable("GOOGLE_MAPS_API_KEY") is { Length: > 0 } envKey)
{
    builder.Configuration["Geocoding:ApiKey"] = envKey;
}

builder.Services.AddOptions<DatabaseOptions>().BindConfiguration(DatabaseOptions.SectionName).ValidateDataAnnotations().ValidateOnStart();
builder.Services.AddOptions<LeaseOptions>().BindConfiguration(LeaseOptions.SectionName).ValidateDataAnnotations().ValidateOnStart();
builder.Services.AddOptions<ProcessingOptions>().BindConfiguration(ProcessingOptions.SectionName).ValidateDataAnnotations().ValidateOnStart();
builder.Services.AddOptions<GeocodingOptions>()
    .BindConfiguration(GeocodingOptions.SectionName)
    .ValidateDataAnnotations()
    .Validate(
        o => o.Provider != "Google" || !string.IsNullOrWhiteSpace(o.ApiKey),
        "Geocoding:ApiKey is required for the Google provider. Set the GOOGLE_MAPS_API_KEY environment variable, " +
        "Geocoding__ApiKey, or `dotnet user-secrets set Geocoding:ApiKey <key>`. Use Geocoding:Provider=Fake to run offline.")
    .ValidateOnStart();

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<SqliteConnectionFactory>();
builder.Services.AddSingleton<DatabaseInitializer>();
builder.Services.AddSingleton<AddressQueueRepository>();

// The geocoder lives for the life of the worker, so recycle pooled connections ourselves to pick up DNS changes.
builder.Services.AddHttpClient<GoogleGeocoder>(http => http.Timeout = TimeSpan.FromSeconds(10))
    .UseSocketsHttpHandler((handler, _) => handler.PooledConnectionLifetime = TimeSpan.FromMinutes(5))
    .SetHandlerLifetime(Timeout.InfiniteTimeSpan);
builder.Services.AddSingleton<FakeGeocoder>();
builder.Services.AddSingleton<IGeocoder>(sp =>
    sp.GetRequiredService<IOptions<GeocodingOptions>>().Value.Provider == "Fake"
        ? sp.GetRequiredService<FakeGeocoder>()
        : sp.GetRequiredService<GoogleGeocoder>());

builder.Services.AddHostedService<GeocodingWorker>();

var host = builder.Build();

await host.Services.GetRequiredService<DatabaseInitializer>().InitializeAsync(CancellationToken.None);

host.Run();
