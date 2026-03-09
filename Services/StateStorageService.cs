using Microsoft.Data.SqlClient;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// DB-backed state storage using Monitor_State table.
/// Replaces file-based persistence for multi-instance scaling.
/// </summary>
public class StateStorageService
{
    private readonly string _connStr;
    private readonly ILogger<StateStorageService> _logger;

    public StateStorageService(IConfiguration config, ILogger<StateStorageService> logger)
    {
        _connStr = config.GetConnectionString("SqlServer")
            ?? throw new InvalidOperationException("Missing SqlServer connection string");
        _logger = logger;
    }

    public async Task SaveStateAsync<T>(string serviceName, string stateKey, T state)
    {
        try
        {
            var json = JsonSerializer.Serialize(state, new JsonSerializerOptions { WriteIndented = false });
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(@"
                MERGE Monitor_State AS target
                USING (SELECT @ServiceName AS ServiceName, @StateKey AS StateKey) AS source
                ON target.ServiceName = source.ServiceName AND target.StateKey = source.StateKey
                WHEN MATCHED THEN
                    UPDATE SET StateJson = @StateJson, LastUpdated = GETUTCDATE()
                WHEN NOT MATCHED THEN
                    INSERT (ServiceName, StateKey, StateJson, LastUpdated)
                    VALUES (@ServiceName, @StateKey, @StateJson, GETUTCDATE());", conn);
            cmd.Parameters.AddWithValue("@ServiceName", serviceName);
            cmd.Parameters.AddWithValue("@StateKey", stateKey);
            cmd.Parameters.AddWithValue("@StateJson", json);
            cmd.CommandTimeout = 10;
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Failed to save state {Service}/{Key}: {Err}", serviceName, stateKey, ex.Message);
        }
    }

    public async Task<T?> LoadStateAsync<T>(string serviceName, string stateKey)
    {
        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(
                "SELECT StateJson FROM Monitor_State WHERE ServiceName = @ServiceName AND StateKey = @StateKey", conn);
            cmd.Parameters.AddWithValue("@ServiceName", serviceName);
            cmd.Parameters.AddWithValue("@StateKey", stateKey);
            cmd.CommandTimeout = 10;
            var result = await cmd.ExecuteScalarAsync();
            if (result == null || result == DBNull.Value) return default;
            return JsonSerializer.Deserialize<T>(result.ToString()!);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Failed to load state {Service}/{Key}: {Err}", serviceName, stateKey, ex.Message);
            return default;
        }
    }
}
