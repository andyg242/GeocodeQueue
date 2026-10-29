using GeocodeQueue.Worker.Options;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace GeocodeQueue.Worker.Data;

public sealed class SqliteConnectionFactory(IOptions<DatabaseOptions> options)
{
    private readonly string _connectionString = new SqliteConnectionStringBuilder(options.Value.ConnectionString)
    {
        // Wait (rather than fail) when another worker holds the write lock.
        DefaultTimeout = 30,
        Pooling = true,
    }.ToString();

    public async Task<SqliteConnection> OpenAsync(CancellationToken ct)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(ct);
        return connection;
    }
}
