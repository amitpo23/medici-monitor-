using Microsoft.Data.SqlClient;

namespace MediciMonitor.Services;

/// <summary>
/// Database health monitoring — connection pools, active connections,
/// long-running queries, deadlocks, DB size, and index fragmentation.
/// </summary>
public class DatabaseHealthService
{
    private readonly string _connStr;
    private readonly ILogger<DatabaseHealthService> _logger;

    public DatabaseHealthService(IConfiguration config, ILogger<DatabaseHealthService> logger)
    {
        _connStr = config.GetConnectionString("SqlServer") ?? "";
        _logger = logger;
    }

    public async Task<DbHealthReport> GetHealthReport()
    {
        var report = new DbHealthReport { Timestamp = DateTime.UtcNow };

        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            report.IsConnected = true;

            await Safe(() => LoadConnectionStats(conn, report));
            await Safe(() => LoadDatabaseSize(conn, report));
            await Safe(() => LoadLongRunningQueries(conn, report));
            await Safe(() => LoadDeadlockInfo(conn, report));
            await Safe(() => LoadIndexFragmentation(conn, report));
            await Safe(() => LoadWaitStats(conn, report));
            await Safe(() => LoadPerformanceCounters(conn, report));
        }
        catch (Exception ex)
        {
            report.IsConnected = false;
            report.Error = ex.Message;
            _logger.LogError(ex, "DatabaseHealthService failed: {Msg}", ex.Message);
        }

        return report;
    }

    private async Task LoadConnectionStats(SqlConnection conn, DbHealthReport r)
    {
        const string sql = @"
            SELECT
                (SELECT COUNT(*) FROM sys.dm_exec_connections) as ActiveConnections,
                (SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE is_user_process = 1) as UserSessions,
                (SELECT COUNT(*) FROM sys.dm_exec_requests WHERE status = 'running') as RunningRequests,
                (SELECT COUNT(*) FROM sys.dm_exec_requests WHERE status = 'suspended') as BlockedRequests";

        using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 10 };
        using var rdr = await cmd.ExecuteReaderAsync();
        if (await rdr.ReadAsync())
        {
            r.ActiveConnections = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0);
            r.UserSessions = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
            r.RunningRequests = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2);
            r.BlockedRequests = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3);
        }
    }

    private async Task LoadDatabaseSize(SqlConnection conn, DbHealthReport r)
    {
        const string sql = @"
            SELECT
                DB_NAME() as DbName,
                SUM(CASE WHEN type_desc = 'ROWS' THEN size * 8.0 / 1024 ELSE 0 END) as DataSizeMB,
                SUM(CASE WHEN type_desc = 'LOG' THEN size * 8.0 / 1024 ELSE 0 END) as LogSizeMB,
                SUM(size * 8.0 / 1024) as TotalSizeMB
            FROM sys.database_files";

        using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 10 };
        using var rdr = await cmd.ExecuteReaderAsync();
        if (await rdr.ReadAsync())
        {
            r.DatabaseName = rdr.IsDBNull(0) ? "" : rdr.GetString(0);
            r.DataSizeMB = rdr.IsDBNull(1) ? 0 : Math.Round(rdr.GetDouble(1), 2);
            r.LogSizeMB = rdr.IsDBNull(2) ? 0 : Math.Round(rdr.GetDouble(2), 2);
            r.TotalSizeMB = rdr.IsDBNull(3) ? 0 : Math.Round(rdr.GetDouble(3), 2);
        }
    }

    private async Task LoadLongRunningQueries(SqlConnection conn, DbHealthReport r)
    {
        const string sql = @"
            SELECT TOP 10
                r.session_id,
                r.status,
                r.command,
                DATEDIFF(SECOND, r.start_time, GETDATE()) as DurationSeconds,
                r.cpu_time as CpuTimeMs,
                r.reads as LogicalReads,
                r.writes as LogicalWrites,
                r.wait_type,
                SUBSTRING(t.text, (r.statement_start_offset/2)+1,
                    ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text)
                      ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1) as SqlText
            FROM sys.dm_exec_requests r
            CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
            WHERE r.session_id != @@SPID
            ORDER BY r.start_time ASC";

        using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 10 };
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            r.LongRunningQueries.Add(new LongRunningQuery
            {
                SessionId = rdr.IsDBNull(0) ? 0 : rdr.GetInt16(0),
                Status = rdr.IsDBNull(1) ? "" : rdr.GetString(1),
                Command = rdr.IsDBNull(2) ? "" : rdr.GetString(2),
                DurationSeconds = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3),
                CpuTimeMs = rdr.IsDBNull(4) ? 0 : rdr.GetInt32(4),
                LogicalReads = rdr.IsDBNull(5) ? 0 : rdr.GetInt64(5),
                LogicalWrites = rdr.IsDBNull(6) ? 0 : rdr.GetInt64(6),
                WaitType = rdr.IsDBNull(7) ? null : rdr.GetString(7),
                SqlText = rdr.IsDBNull(8) ? null : rdr.GetString(8)?.Substring(0, Math.Min(rdr.GetString(8)?.Length ?? 0, 200))
            });
        }
    }

    private async Task LoadDeadlockInfo(SqlConnection conn, DbHealthReport r)
    {
        const string sql = @"
            SELECT
                cntr_value
            FROM sys.dm_os_performance_counters
            WHERE counter_name = 'Number of Deadlocks/sec' AND instance_name = '_Total'";

        using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 10 };
        var result = await cmd.ExecuteScalarAsync();
        r.TotalDeadlocks = result != null ? Convert.ToInt64(result) : 0;
    }

    private async Task LoadIndexFragmentation(SqlConnection conn, DbHealthReport r)
    {
        const string sql = @"
            SELECT TOP 15
                OBJECT_NAME(ips.object_id) as TableName,
                i.name as IndexName,
                ips.avg_fragmentation_in_percent as FragPercent,
                ips.page_count as PageCount
            FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
            INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
            WHERE ips.avg_fragmentation_in_percent > 10 AND ips.page_count > 100 AND i.name IS NOT NULL
            ORDER BY ips.avg_fragmentation_in_percent DESC";

        using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 30 };
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            r.FragmentedIndexes.Add(new FragmentedIndex
            {
                TableName = rdr.IsDBNull(0) ? "" : rdr.GetString(0),
                IndexName = rdr.IsDBNull(1) ? "" : rdr.GetString(1),
                FragmentationPercent = rdr.IsDBNull(2) ? 0 : Math.Round(rdr.GetDouble(2), 1),
                PageCount = rdr.IsDBNull(3) ? 0 : rdr.GetInt64(3)
            });
        }
    }

    private async Task LoadWaitStats(SqlConnection conn, DbHealthReport r)
    {
        const string sql = @"
            SELECT TOP 10
                wait_type,
                wait_time_ms,
                waiting_tasks_count,
                signal_wait_time_ms
            FROM sys.dm_os_wait_stats
            WHERE wait_type NOT IN (
                'SLEEP_TASK','BROKER_IO_FLUSH','BROKER_EVENTHANDLER','BROKER_RECEIVE_WAITFOR',
                'BROKER_TASK_STOP','BROKER_TO_FLUSH','CHECKPOINT_QUEUE','CLR_AUTO_EVENT',
                'CLR_MANUAL_EVENT','DISPATCHER_QUEUE_SEMAPHORE','FT_IFTS_SCHEDULER_IDLE_WAIT',
                'HADR_FILESTREAM_IOMGR_IOCOMPLETION','HADR_WORK_QUEUE','LAZYWRITER_SLEEP',
                'LOGMGR_QUEUE','ONDEMAND_TASK_QUEUE','REQUEST_FOR_DEADLOCK_SEARCH',
                'RESOURCE_QUEUE','SERVER_IDLE_CHECK','SLEEP_BPOOL_FLUSH','SLEEP_DBSTARTUP',
                'SLEEP_DCOMSTARTUP','SLEEP_MASTERDBREADY','SLEEP_MASTERMDREADY',
                'SLEEP_MASTERUPGRADED','SLEEP_MSDBSTARTUP','SLEEP_SYSTEMTASK',
                'SLEEP_TEMPDBSTARTUP','SNI_HTTP_ACCEPT','SP_SERVER_DIAGNOSTICS_SLEEP',
                'SQLTRACE_BUFFER_FLUSH','SQLTRACE_INCREMENTAL_FLUSH_SLEEP','WAITFOR',
                'XE_DISPATCHER_WAIT','XE_TIMER_EVENT','DIRTY_PAGE_POLL','HADR_TIMER_TASK'
            )
            AND waiting_tasks_count > 0
            ORDER BY wait_time_ms DESC";

        using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 10 };
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            r.TopWaits.Add(new WaitStat
            {
                WaitType = rdr.IsDBNull(0) ? "" : rdr.GetString(0),
                WaitTimeMs = rdr.IsDBNull(1) ? 0 : rdr.GetInt64(1),
                WaitingTasksCount = rdr.IsDBNull(2) ? 0 : rdr.GetInt64(2),
                SignalWaitTimeMs = rdr.IsDBNull(3) ? 0 : rdr.GetInt64(3)
            });
        }
    }

    private async Task LoadPerformanceCounters(SqlConnection conn, DbHealthReport r)
    {
        const string sql = @"
            SELECT counter_name, cntr_value
            FROM sys.dm_os_performance_counters
            WHERE counter_name IN (
                'Batch Requests/sec', 'SQL Compilations/sec', 'SQL Re-Compilations/sec',
                'Page life expectancy', 'Buffer cache hit ratio',
                'Transactions/sec', 'Lock Waits/sec'
            ) AND instance_name IN ('', '_Total')";

        using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 10 };
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            var name = rdr.IsDBNull(0) ? "" : rdr.GetString(0).Trim();
            var value = rdr.IsDBNull(1) ? 0 : rdr.GetInt64(1);
            r.PerformanceCounters[name] = value;
        }
    }

    private async Task Safe(Func<Task> action)
    {
        try { await action(); }
        catch (Exception ex) { _logger.LogWarning("DbHealth query failed: {Err}", ex.Message); }
    }
}

// ── Models ──

public class DbHealthReport
{
    public DateTime Timestamp { get; set; }
    public bool IsConnected { get; set; }
    public string? Error { get; set; }
    public string DatabaseName { get; set; } = "";

    // Connections
    public int ActiveConnections { get; set; }
    public int UserSessions { get; set; }
    public int RunningRequests { get; set; }
    public int BlockedRequests { get; set; }

    // Size
    public double DataSizeMB { get; set; }
    public double LogSizeMB { get; set; }
    public double TotalSizeMB { get; set; }

    // Deadlocks
    public long TotalDeadlocks { get; set; }

    // Details
    public List<LongRunningQuery> LongRunningQueries { get; set; } = new();
    public List<FragmentedIndex> FragmentedIndexes { get; set; } = new();
    public List<WaitStat> TopWaits { get; set; } = new();
    public Dictionary<string, long> PerformanceCounters { get; set; } = new();
}

public class LongRunningQuery
{
    public int SessionId { get; set; }
    public string Status { get; set; } = "";
    public string Command { get; set; } = "";
    public int DurationSeconds { get; set; }
    public int CpuTimeMs { get; set; }
    public long LogicalReads { get; set; }
    public long LogicalWrites { get; set; }
    public string? WaitType { get; set; }
    public string? SqlText { get; set; }
}

public class FragmentedIndex
{
    public string TableName { get; set; } = "";
    public string IndexName { get; set; } = "";
    public double FragmentationPercent { get; set; }
    public long PageCount { get; set; }
}

public class WaitStat
{
    public string WaitType { get; set; } = "";
    public long WaitTimeMs { get; set; }
    public long WaitingTasksCount { get; set; }
    public long SignalWaitTimeMs { get; set; }
}
