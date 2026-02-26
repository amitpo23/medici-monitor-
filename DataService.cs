using Microsoft.Data.SqlClient;

namespace MediciMonitor;

/// <summary>
/// All SQL query logic — reads from Azure SQL medici-db.
/// Every method is self-contained and uses raw ADO.NET for speed.
/// </summary>
public class DataService
{
    private readonly string _connStr;
    private readonly ILogger<DataService> _logger;

    public DataService(IConfiguration config, ILogger<DataService> logger)
    {
        _connStr = config.GetConnectionString("SqlServer")
            ?? throw new InvalidOperationException("Missing SqlServer connection string");
        _logger = logger;
    }

    public async Task<SystemStatus> GetFullStatus()
    {
        var s = new SystemStatus { Timestamp = DateTime.UtcNow };

        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            s.DbConnected = true;

            // Run all queries — each wrapped in try/catch so one failure doesn't kill everything
            await Safe(() => LoadBookingSummary(conn, s));
            await Safe(() => LoadStuckCancellations(conn, s));
            await Safe(() => LoadCancelStats(conn, s));
            await Safe(() => LoadRecentCancelErrors(conn, s));
            await Safe(() => LoadBookingErrors(conn, s));
            await Safe(() => LoadPushStatus(conn, s));
            await Safe(() => LoadQueueStatus(conn, s));
            await Safe(() => LoadBackOfficeErrors(conn, s));
            await Safe(() => LoadActiveBookingsByHotel(conn, s));
            await Safe(() => LoadSalesOfficeStatus(conn, s));
            await Safe(() => LoadSalesOfficeDetails(conn, s));
            await Safe(() => LoadOpportunitiesAndRooms(conn, s));

            // NEW features
            await Safe(() => LoadReservations(conn, s));
            await Safe(() => LoadRoomWaste(conn, s));
            await Safe(() => LoadConversionRevenue(conn, s));
            await Safe(() => LoadPriceDrift(conn, s));
            await Safe(() => LoadBuyRoomsHeartbeat(conn, s));
        }
        catch (Exception ex)
        {
            s.DbConnected = false;
            s.Error = ex.Message;
        }

        return s;
    }

    private async Task Safe(Func<Task> action, [System.Runtime.CompilerServices.CallerArgumentExpression("action")] string? expr = null)
    {
        try { await action(); }
        catch (Exception ex)
        {
            var method = "Unknown";
            if (expr != null)
            {
                var i = expr.IndexOf("Load");
                if (i >= 0) { var e = expr.IndexOf('(', i); method = e > i ? expr[i..e] : expr[i..]; }
            }
            _logger.LogError(ex, "DataService.{Method} failed: {Message}", method, ex.Message);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXISTING: Booking Summary
    // ═══════════════════════════════════════════════════════════════

    private async Task LoadBookingSummary(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            SELECT 
                COUNT(*) as Total,
                SUM(CASE WHEN CancellationTo < GETDATE() THEN 1 ELSE 0 END) as Stuck,
                SUM(CASE WHEN CancellationTo >= GETDATE() AND CancellationTo <= DATEADD(DAY, 2, GETDATE()) THEN 1 ELSE 0 END) as Upcoming,
                SUM(CASE WHEN CancellationTo > DATEADD(DAY, 2, GETDATE()) OR CancellationTo IS NULL THEN 1 ELSE 0 END) as Future
            FROM MED_Book WHERE IsActive = 1";

        using var cmd = new SqlCommand(sql, conn);
        using var rdr = await cmd.ExecuteReaderAsync();
        if (await rdr.ReadAsync())
        {
            s.TotalActiveBookings = rdr.GetInt32(0);
            s.StuckCancellations = rdr.GetInt32(1);
            s.UpcomingCancellations = rdr.GetInt32(2);
            s.FutureBookings = rdr.GetInt32(3);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXISTING: Stuck Cancellations Detail
    // ═══════════════════════════════════════════════════════════════

    private async Task LoadStuckCancellations(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            SELECT TOP 50
                b.PreBookId, b.contentBookingID, b.CancellationTo, b.source, b.HotelId, b.price,
                DATEDIFF(DAY, b.CancellationTo, GETDATE()) as DaysStuck,
                (SELECT COUNT(*) FROM MED_CancelBookError e WHERE e.PreBookId = b.PreBookId) as ErrorCount,
                (SELECT TOP 1 e.Error FROM MED_CancelBookError e WHERE e.PreBookId = b.PreBookId ORDER BY e.DateInsert DESC) as LastError
            FROM MED_Book b
            WHERE b.IsActive = 1 AND b.CancellationTo < GETDATE()
            ORDER BY b.CancellationTo ASC";

        using var cmd = new SqlCommand(sql, conn);
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            s.StuckBookings.Add(new StuckBookingInfo
            {
                PreBookId = rdr.GetInt32(0),
                ContentBookingId = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                CancellationTo = rdr.IsDBNull(2) ? null : rdr.GetDateTime(2),
                Source = rdr.IsDBNull(3) ? null : rdr.GetInt32(3),
                SourceName = rdr.IsDBNull(3) ? "Unknown" : (rdr.GetInt32(3) == 1 ? "Innstant" : rdr.GetInt32(3) == 2 ? "GoGlobal" : $"Source-{rdr.GetInt32(3)}"),
                HotelId = rdr.IsDBNull(4) ? null : rdr.GetInt32(4),
                Price = rdr.IsDBNull(5) ? null : rdr.GetDouble(5),
                DaysStuck = rdr.GetInt32(6),
                ErrorCount = rdr.GetInt32(7),
                LastError = rdr.IsDBNull(8) ? null : rdr.GetString(8)
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXISTING: Cancel Stats (24h)
    // ═══════════════════════════════════════════════════════════════

    private async Task LoadCancelStats(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            SELECT 
                (SELECT COUNT(*) FROM MED_CancelBook WHERE DateInsert >= DATEADD(HOUR, -24, GETDATE())) as SuccessCount,
                (SELECT COUNT(*) FROM MED_CancelBookError WHERE DateInsert >= DATEADD(HOUR, -24, GETDATE())) as ErrorCount";

        using var cmd = new SqlCommand(sql, conn);
        using var rdr = await cmd.ExecuteReaderAsync();
        if (await rdr.ReadAsync())
        {
            s.CancelSuccessLast24h = rdr.GetInt32(0);
            s.CancelErrorsLast24h = rdr.GetInt32(1);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXISTING: Recent Cancel Errors
    // ═══════════════════════════════════════════════════════════════

    private async Task LoadRecentCancelErrors(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            SELECT TOP 25 PreBookId, contentBookingID, Error, DateInsert 
            FROM MED_CancelBookError ORDER BY DateInsert DESC";

        using var cmd = new SqlCommand(sql, conn);
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            s.RecentCancelErrors.Add(new CancelErrorInfo
            {
                PreBookId = rdr.IsDBNull(0) ? null : rdr.GetInt32(0),
                ContentBookingId = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                Error = rdr.IsDBNull(2) ? null : rdr.GetString(2),
                DateInsert = rdr.IsDBNull(3) ? null : rdr.GetDateTime(3)
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXISTING: Booking Errors
    // ═══════════════════════════════════════════════════════════════

    private async Task LoadBookingErrors(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            SELECT COUNT(*) FROM MED_BookError WHERE DateInsert >= DATEADD(HOUR, -24, GETDATE());
            SELECT TOP 25 PreBookId, Error, code, DateInsert FROM MED_BookError ORDER BY DateInsert DESC";

        using var cmd = new SqlCommand(sql, conn);
        using var rdr = await cmd.ExecuteReaderAsync();
        if (await rdr.ReadAsync()) s.BookingErrorsLast24h = rdr.GetInt32(0);
        await rdr.NextResultAsync();
        while (await rdr.ReadAsync())
        {
            s.RecentBookingErrors.Add(new BookingErrorInfo
            {
                PreBookId = rdr.IsDBNull(0) ? null : rdr.GetInt32(0),
                Error = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                Code = rdr.IsDBNull(2) ? null : rdr.GetString(2),
                DateInsert = rdr.IsDBNull(3) ? null : rdr.GetDateTime(3)
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXISTING: Push Status
    // ═══════════════════════════════════════════════════════════════

    private async Task LoadPushStatus(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            SELECT 
                SUM(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END) as ActivePush,
                SUM(CASE WHEN IsActive = 0 AND Error IS NOT NULL AND Error != 'CancelBook' THEN 1 ELSE 0 END) as FailedPush
            FROM Med_HotelsToPush;
            SELECT TOP 25 Id, BookId, OpportunityId, Error, DateInsert
            FROM Med_HotelsToPush 
            WHERE IsActive = 0 AND Error IS NOT NULL AND Error != 'CancelBook'
            ORDER BY DateInsert DESC";

        using var cmd = new SqlCommand(sql, conn);
        using var rdr = await cmd.ExecuteReaderAsync();
        if (await rdr.ReadAsync())
        {
            s.ActivePushOperations = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0);
            s.FailedPushOperations = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
        }
        await rdr.NextResultAsync();
        while (await rdr.ReadAsync())
        {
            s.FailedPushItems.Add(new PushFailureInfo
            {
                Id = rdr.GetInt32(0),
                BookId = rdr.IsDBNull(1) ? null : rdr.GetInt32(1),
                OpportunityId = rdr.IsDBNull(2) ? null : rdr.GetInt32(2),
                Error = rdr.IsDBNull(3) ? null : rdr.GetString(3),
                DateInsert = rdr.IsDBNull(4) ? null : rdr.GetDateTime(4)
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXISTING: Queue Status
    // ═══════════════════════════════════════════════════════════════

    private async Task LoadQueueStatus(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            SELECT 
                SUM(CASE WHEN Status = 'AddedToQueue' THEN 1 ELSE 0 END) as Pending,
                SUM(CASE WHEN Status = 'Processing' THEN 1 ELSE 0 END) as Processing,
                SUM(CASE WHEN Status = 'Finished' THEN 1 ELSE 0 END) as Finished,
                SUM(CASE WHEN Status = 'Error' THEN 1 ELSE 0 END) as [Error]
            FROM [Queue];
            SELECT TOP 20 Id, Status, Message, HotelName, CreatedOn
            FROM [Queue] WHERE Status = 'Error' ORDER BY CreatedOn DESC";

        using var cmd = new SqlCommand(sql, conn);
        using var rdr = await cmd.ExecuteReaderAsync();
        if (await rdr.ReadAsync())
        {
            s.QueuePending = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0);
            s.QueueErrors = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3);
        }
        await rdr.NextResultAsync();
        while (await rdr.ReadAsync())
        {
            s.QueueErrorItems.Add(new QueueErrorInfo
            {
                Id = rdr.GetInt32(0),
                Status = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                Message = rdr.IsDBNull(2) ? null : rdr.GetString(2),
                HotelName = rdr.IsDBNull(3) ? null : rdr.GetString(3),
                CreatedOn = rdr.IsDBNull(4) ? null : rdr.GetDateTime(4)
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXISTING: BackOffice Errors
    // ═══════════════════════════════════════════════════════════════

    private async Task LoadBackOfficeErrors(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            SELECT COUNT(*) FROM BackOfficeOptLog WHERE DateCreate >= DATEADD(HOUR, -24, GETDATE());
            SELECT TOP 25 BackOfficeOptID, ErrorLog, DateCreate FROM BackOfficeOptLog ORDER BY DateCreate DESC";

        using var cmd = new SqlCommand(sql, conn);
        using var rdr = await cmd.ExecuteReaderAsync();
        if (await rdr.ReadAsync()) s.BackOfficeErrorsLast24h = rdr.GetInt32(0);
        await rdr.NextResultAsync();
        while (await rdr.ReadAsync())
        {
            s.RecentBackOfficeErrors.Add(new BackOfficeErrorInfo
            {
                BackOfficeOptId = rdr.IsDBNull(0) ? null : rdr.GetInt32(0),
                ErrorLog = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                DateCreate = rdr.IsDBNull(2) ? null : rdr.GetDateTime(2)
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXISTING: Active Bookings by Hotel
    // ═══════════════════════════════════════════════════════════════

    private async Task LoadActiveBookingsByHotel(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            SELECT 
                b.HotelId,
                ISNULL(h.Name, 'Hotel ' + CAST(b.HotelId AS VARCHAR)) as HotelName,
                COUNT(*) as ActiveCount,
                SUM(CASE WHEN b.CancellationTo < GETDATE() THEN 1 ELSE 0 END) as StuckCount,
                SUM(CASE WHEN b.IsSold = 1 THEN 1 ELSE 0 END) as SoldCount
            FROM MED_Book b
            LEFT JOIN Med_Hotels h ON h.HotelId = b.HotelId
            WHERE b.IsActive = 1
            GROUP BY b.HotelId, h.Name
            ORDER BY StuckCount DESC, ActiveCount DESC";

        using var cmd = new SqlCommand(sql, conn);
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            s.ActiveBookingsByHotel.Add(new ActiveBookingSummary
            {
                HotelId = rdr.IsDBNull(0) ? null : rdr.GetInt32(0),
                HotelName = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                ActiveCount = rdr.GetInt32(2),
                StuckCount = rdr.GetInt32(3),
                SoldCount = rdr.GetInt32(4)
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXISTING: SalesOffice Status
    // ═══════════════════════════════════════════════════════════════

    private async Task LoadSalesOfficeStatus(SqlConnection conn, SystemStatus s)
    {
        string[] tablePatterns = ["SalesOfficeOrders", "[SalesOffice.Orders]", "SalesOffice_Orders"];

        foreach (var tableName in tablePatterns)
        {
            try
            {
                var sql = $@"
                    SELECT 
                        SUM(CASE WHEN WebJobStatus IS NULL THEN 1 ELSE 0 END) as Pending,
                        SUM(CASE WHEN WebJobStatus = 'In Progress' THEN 1 ELSE 0 END) as InProgress,
                        SUM(CASE WHEN WebJobStatus LIKE 'Completed%' THEN 1 ELSE 0 END) as Completed,
                        SUM(CASE WHEN WebJobStatus LIKE 'Failed%' OR WebJobStatus = 'DateRangeError' THEN 1 ELSE 0 END) as Failed
                    FROM {tableName} WHERE IsActive = 1;
                    SELECT TOP 15 Id, WebJobStatus, DateFrom, DateTo, IsActive
                    FROM {tableName}
                    WHERE IsActive = 1 AND (WebJobStatus IS NULL OR WebJobStatus = 'In Progress' OR WebJobStatus LIKE 'Failed%')
                    ORDER BY Id DESC";

                using var cmd = new SqlCommand(sql, conn);
                cmd.CommandTimeout = 5;
                using var rdr = await cmd.ExecuteReaderAsync();
                if (await rdr.ReadAsync())
                {
                    s.SalesOfficePending = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0);
                    s.SalesOfficeInProgress = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
                    s.SalesOfficeCompleted = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2);
                    s.SalesOfficeFailed = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3);
                }
                await rdr.NextResultAsync();
                while (await rdr.ReadAsync())
                {
                    s.SalesOfficeStuckOrders.Add(new SalesOfficeOrderInfo
                    {
                        Id = rdr.GetInt32(0),
                        WebJobStatus = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                        DateFrom = rdr.IsDBNull(2) ? null : rdr.GetDateTime(2),
                        DateTo = rdr.IsDBNull(3) ? null : rdr.GetDateTime(3),
                        IsActive = !rdr.IsDBNull(4) && rdr.GetBoolean(4)
                    });
                }
                return;
            }
            catch { /* try next table name pattern */ }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  SalesOffice Details/Callback Monitoring
    // ═══════════════════════════════════════════════════════════════

    private async Task LoadSalesOfficeDetails(SqlConnection conn, SystemStatus s)
    {
        string[] detailsTablePatterns = ["[SalesOffice.Details]", "SalesOfficeDetails", "SalesOffice_Details"];
        string[] ordersTablePatterns = ["SalesOfficeOrders", "[SalesOffice.Orders]", "SalesOffice_Orders"];

        string? detailsTable = null;
        string? ordersTable = null;

        // Resolve Details table name
        foreach (var name in detailsTablePatterns)
        {
            try
            {
                using var probe = new SqlCommand($"SELECT TOP 1 1 FROM {name}", conn);
                probe.CommandTimeout = 3;
                await probe.ExecuteScalarAsync();
                detailsTable = name;
                break;
            }
            catch { /* try next */ }
        }

        // Resolve Orders table name
        foreach (var name in ordersTablePatterns)
        {
            try
            {
                using var probe = new SqlCommand($"SELECT TOP 1 1 FROM {name}", conn);
                probe.CommandTimeout = 3;
                await probe.ExecuteScalarAsync();
                ordersTable = name;
                break;
            }
            catch { /* try next */ }
        }

        if (detailsTable == null) return; // Table not found, skip silently

        // Query 1: Callback KPIs
        var sql1 = $@"
            SELECT
                COUNT(*) as TotalDetails,
                SUM(CASE WHEN IsProcessedCallback = 0 THEN 1 ELSE 0 END) as Unprocessed,
                SUM(CASE WHEN IsProcessedCallback = 1 THEN 1 ELSE 0 END) as Processed
            FROM {detailsTable};

            SELECT
                (SELECT COUNT(*) FROM {detailsTable}
                 WHERE IsProcessedCallback = 1
                 AND DateInsert >= DATEADD(HOUR, -24, GETDATE())) as ProcessedLast24h,
                (SELECT COUNT(*) FROM {detailsTable}
                 WHERE IsProcessedCallback = 1
                 AND DateInsert >= DATEADD(HOUR, -1, GETDATE())) as ProcessedLastHour";

        using var cmd1 = new SqlCommand(sql1, conn) { CommandTimeout = 10 };
        using var rdr1 = await cmd1.ExecuteReaderAsync();

        if (await rdr1.ReadAsync())
        {
            s.SalesOfficeTotalDetails = rdr1.IsDBNull(0) ? 0 : rdr1.GetInt32(0);
            s.SalesOfficeUnprocessedCallbacks = rdr1.IsDBNull(1) ? 0 : rdr1.GetInt32(1);
            s.SalesOfficeProcessedCallbacks = rdr1.IsDBNull(2) ? 0 : rdr1.GetInt32(2);
        }

        await rdr1.NextResultAsync();
        if (await rdr1.ReadAsync())
        {
            s.SalesOfficeCallbacksProcessedLast24h = rdr1.IsDBNull(0) ? 0 : rdr1.GetInt32(0);
            s.SalesOfficeCallbacksProcessedLastHour = rdr1.IsDBNull(1) ? 0 : rdr1.GetInt32(1);
            s.SalesOfficeCallbackProcessingRate = s.SalesOfficeCallbacksProcessedLast24h / 24.0;
        }

        // Query 2: Zero-mapping orders (completed orders with no details)
        if (ordersTable != null)
        {
            var sql2 = $@"
                SELECT
                    o.Id as OrderId,
                    o.WebJobStatus,
                    o.DateFrom,
                    o.DateTo,
                    ISNULL(d.Cnt, 0) as DetailCount,
                    ISNULL(d.Cnt, 0) as MappingCount
                FROM {ordersTable} o
                LEFT JOIN (SELECT OrderId, COUNT(*) as Cnt FROM {detailsTable} GROUP BY OrderId) d ON d.OrderId = o.Id
                WHERE o.IsActive = 1 AND o.WebJobStatus LIKE 'Completed%' AND (d.Cnt IS NULL OR d.Cnt = 0)
                ORDER BY o.Id DESC;

                SELECT
                    (SELECT COUNT(*) FROM {ordersTable}
                     WHERE IsActive = 1 AND WebJobStatus LIKE 'Completed%') as TotalCompleted,
                    (SELECT COUNT(*) FROM {ordersTable} o
                     LEFT JOIN (SELECT OrderId, COUNT(*) as Cnt FROM {detailsTable} GROUP BY OrderId) d ON d.OrderId = o.Id
                     WHERE o.IsActive = 1 AND o.WebJobStatus LIKE 'Completed%' AND (d.Cnt IS NULL OR d.Cnt = 0)) as ZeroMapping";

            using var cmd2 = new SqlCommand(sql2, conn) { CommandTimeout = 15 };
            using var rdr2 = await cmd2.ExecuteReaderAsync();

            while (await rdr2.ReadAsync())
            {
                s.SalesOfficeZeroMappingItems.Add(new SalesOfficeZeroMappingInfo
                {
                    OrderId = rdr2.GetInt32(0),
                    WebJobStatus = rdr2.IsDBNull(1) ? null : rdr2.GetString(1),
                    DateFrom = rdr2.IsDBNull(2) ? null : rdr2.GetDateTime(2),
                    DateTo = rdr2.IsDBNull(3) ? null : rdr2.GetDateTime(3),
                    DetailCount = rdr2.GetInt32(4),
                    MappingCount = rdr2.GetInt32(5)
                });
            }

            await rdr2.NextResultAsync();
            if (await rdr2.ReadAsync())
            {
                s.SalesOfficeTotalCompletedOrders = rdr2.IsDBNull(0) ? 0 : rdr2.GetInt32(0);
                s.SalesOfficeZeroMappingOrders = rdr2.IsDBNull(1) ? 0 : rdr2.GetInt32(1);
            }

            // Query 3a: Mapping analytics — avg/max details per order + partial mapping
            try
            {
                var sql3a = $@"
                    ;WITH DetailCounts AS (
                        SELECT OrderId, COUNT(*) as Cnt
                        FROM {detailsTable} GROUP BY OrderId
                    ),
                    CompletedWithDetails AS (
                        SELECT o.Id, d.Cnt
                        FROM {ordersTable} o
                        INNER JOIN DetailCounts d ON d.OrderId = o.Id
                        WHERE o.IsActive = 1 AND o.WebJobStatus LIKE 'Completed%'
                    )
                    SELECT
                        AVG(CAST(Cnt AS FLOAT)) as AvgDetails,
                        MAX(Cnt) as MaxDetails,
                        (SELECT COUNT(*) FROM CompletedWithDetails
                         WHERE Cnt < (SELECT AVG(CAST(Cnt AS FLOAT)) / 2.0 FROM CompletedWithDetails)) as PartialCount
                    FROM CompletedWithDetails";

                using var cmd3a = new SqlCommand(sql3a, conn) { CommandTimeout = 15 };
                using var rdr3a = await cmd3a.ExecuteReaderAsync();
                if (await rdr3a.ReadAsync())
                {
                    s.SalesOfficeAvgDetailsPerOrder = rdr3a.IsDBNull(0) ? 0 : Math.Round(rdr3a.GetDouble(0), 1);
                    s.SalesOfficeMaxDetailsPerOrder = rdr3a.IsDBNull(1) ? 0 : rdr3a.GetInt32(1);
                    s.SalesOfficePartialMappingOrders = rdr3a.IsDBNull(2) ? 0 : rdr3a.GetInt32(2);
                }
            }
            catch { /* columns might not exist */ }

            // Query 3b: Time-to-map (tries DateInsert on both tables)
            try
            {
                var sql3b = $@"
                    SELECT
                        AVG(CAST(DATEDIFF(MINUTE, o.DateInsert, d.FirstDetail) AS FLOAT)) as AvgTimeMin,
                        MAX(DATEDIFF(MINUTE, o.DateInsert, d.FirstDetail)) as MaxTimeMin
                    FROM {ordersTable} o
                    INNER JOIN (SELECT OrderId, MIN(DateInsert) as FirstDetail FROM {detailsTable} GROUP BY OrderId) d ON d.OrderId = o.Id
                    WHERE o.IsActive = 1 AND o.WebJobStatus LIKE 'Completed%'
                      AND DATEDIFF(MINUTE, o.DateInsert, d.FirstDetail) >= 0";

                using var cmd3b = new SqlCommand(sql3b, conn) { CommandTimeout = 15 };
                using var rdr3b = await cmd3b.ExecuteReaderAsync();
                if (await rdr3b.ReadAsync())
                {
                    s.SalesOfficeAvgTimeToMapMinutes = rdr3b.IsDBNull(0) ? 0 : Math.Round(rdr3b.GetDouble(0), 1);
                    s.SalesOfficeMaxTimeToMapMinutes = rdr3b.IsDBNull(1) ? 0 : rdr3b.GetInt32(1);
                }
            }
            catch { /* DateInsert column might not exist on orders */ }

            // Query 3c: Retry detection (multiple approaches)
            try
            {
                // Try RetryCount column first
                var sql3c = $"SELECT COUNT(*) FROM {ordersTable} WHERE IsActive = 1 AND RetryCount > 0";
                using var cmd3c = new SqlCommand(sql3c, conn) { CommandTimeout = 5 };
                s.SalesOfficeRetriedOrders = Convert.ToInt32(await cmd3c.ExecuteScalarAsync() ?? 0);
            }
            catch
            {
                // Fallback: count orders with 'Retry' in status
                try
                {
                    var sql3c2 = $"SELECT COUNT(*) FROM {ordersTable} WHERE IsActive = 1 AND WebJobStatus LIKE '%Retry%'";
                    using var cmd3c2 = new SqlCommand(sql3c2, conn) { CommandTimeout = 5 };
                    s.SalesOfficeRetriedOrders = Convert.ToInt32(await cmd3c2.ExecuteScalarAsync() ?? 0);
                }
                catch { /* no retry info available */ }
            }

            // Query 3d: Callback errors (tries Error column on Details)
            try
            {
                var sql3d = $"SELECT COUNT(*) FROM {detailsTable} WHERE Error IS NOT NULL AND Error != ''";
                using var cmd3d = new SqlCommand(sql3d, conn) { CommandTimeout = 5 };
                s.SalesOfficeCallbackErrors = Convert.ToInt32(await cmd3d.ExecuteScalarAsync() ?? 0);
            }
            catch
            {
                // Try ErrorMessage column
                try
                {
                    var sql3d2 = $"SELECT COUNT(*) FROM {detailsTable} WHERE ErrorMessage IS NOT NULL AND ErrorMessage != ''";
                    using var cmd3d2 = new SqlCommand(sql3d2, conn) { CommandTimeout = 5 };
                    s.SalesOfficeCallbackErrors = Convert.ToInt32(await cmd3d2.ExecuteScalarAsync() ?? 0);
                }
                catch { /* no error column available */ }
            }

            // Query 3e: Build mapping detail items (partial/slow orders for table display)
            try
            {
                var sql3e = $@"
                    ;WITH DetailCounts AS (
                        SELECT OrderId, COUNT(*) as Cnt, MIN(DateInsert) as FirstDetail
                        FROM {detailsTable} GROUP BY OrderId
                    ),
                    AvgCnt AS (
                        SELECT AVG(CAST(d.Cnt AS FLOAT)) as Avg
                        FROM {ordersTable} o
                        INNER JOIN DetailCounts d ON d.OrderId = o.Id
                        WHERE o.IsActive = 1 AND o.WebJobStatus LIKE 'Completed%'
                    )
                    SELECT TOP 20
                        o.Id, o.WebJobStatus, d.Cnt as DetailCount,
                        DATEDIFF(MINUTE, o.DateInsert, d.FirstDetail) as TimeToMapMin,
                        CASE
                            WHEN d.Cnt < (SELECT Avg / 2.0 FROM AvgCnt) THEN 'Partial'
                            WHEN DATEDIFF(MINUTE, o.DateInsert, d.FirstDetail) > 120 THEN 'Slow'
                            ELSE 'OK'
                        END as Issue
                    FROM {ordersTable} o
                    INNER JOIN DetailCounts d ON d.OrderId = o.Id
                    WHERE o.IsActive = 1 AND o.WebJobStatus LIKE 'Completed%'
                      AND (d.Cnt < (SELECT Avg / 2.0 FROM AvgCnt) OR DATEDIFF(MINUTE, o.DateInsert, d.FirstDetail) > 120)
                    ORDER BY
                        CASE WHEN d.Cnt < (SELECT Avg / 2.0 FROM AvgCnt) THEN 0 ELSE 1 END,
                        d.Cnt ASC";

                using var cmd3e = new SqlCommand(sql3e, conn) { CommandTimeout = 15 };
                using var rdr3e = await cmd3e.ExecuteReaderAsync();
                while (await rdr3e.ReadAsync())
                {
                    s.SalesOfficeMappingDetails.Add(new SalesOfficeMappingDetailInfo
                    {
                        OrderId = rdr3e.GetInt32(0),
                        WebJobStatus = rdr3e.IsDBNull(1) ? null : rdr3e.GetString(1),
                        DetailCount = rdr3e.GetInt32(2),
                        TimeToMapMinutes = rdr3e.IsDBNull(3) ? null : rdr3e.GetInt32(3),
                        Issue = rdr3e.IsDBNull(4) ? null : rdr3e.GetString(4)
                    });
                }
            }
            catch { /* DateInsert might not exist — skip detail items */ }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  EXISTING: Opportunities & Today Rooms
    // ═══════════════════════════════════════════════════════════════

    private async Task LoadOpportunitiesAndRooms(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            SELECT COUNT(*) FROM BackOfficeOPT WHERE Status = 1;
            SELECT COUNT(*) FROM MED_Book WHERE DateInsert >= CAST(GETDATE() AS DATE)";

        using var cmd = new SqlCommand(sql, conn);
        using var rdr = await cmd.ExecuteReaderAsync();
        if (await rdr.ReadAsync()) s.ActiveOpportunities = rdr.GetInt32(0);
        await rdr.NextResultAsync();
        if (await rdr.ReadAsync()) s.RoomsBoughtToday = rdr.GetInt32(0);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  NEW FEATURE 1: Reservations (Zenith cockpit — incoming sales)
    // ═══════════════════════════════════════════════════════════════════

    private async Task LoadReservations(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            -- KPIs
            SELECT 
                (SELECT COUNT(*) FROM Med_Reservation WHERE DateInsert >= CAST(GETDATE() AS DATE)) as Today,
                (SELECT COUNT(*) FROM Med_Reservation WHERE DateInsert >= DATEADD(DAY, -7, GETDATE())) as ThisWeek,
                (SELECT COUNT(*) FROM Med_ReservationCancel WHERE DateInsert >= DATEADD(DAY, -7, GETDATE())) as CancelsWeek,
                (SELECT COUNT(*) FROM Med_ReservationModify WHERE DateInsert >= DATEADD(DAY, -7, GETDATE())) as ModifiesWeek,
                (SELECT ISNULL(SUM(ISNULL(AmountAfterTax, 0)), 0) FROM Med_Reservation WHERE DateInsert >= DATEADD(DAY, -7, GETDATE())) as RevenueWeek;

            -- Recent reservations (combined view of new + cancel + modify)
            SELECT TOP 30 * FROM (
                SELECT Id, HotelCode, UniqueId, ResStatus, Datefrom, Dateto, AmountAfterTax, CurrencyCode, 
                       RoomTypeCode, RatePlanCode, IsApproved, DateInsert, 'New' as Type
                FROM Med_Reservation WHERE DateInsert >= DATEADD(DAY, -7, GETDATE())
                UNION ALL
                SELECT Id, HotelCode, UniqueId, ResStatus, Datefrom, Dateto, AmountAfterTax, CurrencyCode, 
                       RoomTypeCode, RatePlanCode, IsApproved, DateInsert, 'Cancel' as Type
                FROM Med_ReservationCancel WHERE DateInsert >= DATEADD(DAY, -7, GETDATE())
                UNION ALL
                SELECT Id, HotelCode, UniqueId, ResStatus, Datefrom, Dateto, AmountAfterTax, CurrencyCode, 
                       RoomTypeCode, RatePlanCode, IsApproved, DateInsert, 'Modify' as Type
                FROM Med_ReservationModify WHERE DateInsert >= DATEADD(DAY, -7, GETDATE())
            ) combined ORDER BY DateInsert DESC";

        using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = 15;
        using var rdr = await cmd.ExecuteReaderAsync();

        // KPIs
        if (await rdr.ReadAsync())
        {
            s.ReservationsToday = rdr.GetInt32(0);
            s.ReservationsThisWeek = rdr.GetInt32(1);
            s.ReservationCancelsThisWeek = rdr.GetInt32(2);
            s.ReservationModifiesThisWeek = rdr.GetInt32(3);
            s.ReservationRevenueThisWeek = rdr.GetDouble(4);
        }

        // Detail rows
        await rdr.NextResultAsync();
        while (await rdr.ReadAsync())
        {
            s.RecentReservations.Add(new ReservationInfo
            {
                Id = rdr.GetInt32(0),
                HotelCode = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                UniqueId = rdr.IsDBNull(2) ? null : rdr.GetString(2),
                ResStatus = rdr.IsDBNull(3) ? null : rdr.GetString(3),
                DateFrom = rdr.IsDBNull(4) ? null : rdr.GetDateTime(4),
                DateTo = rdr.IsDBNull(5) ? null : rdr.GetDateTime(5),
                AmountAfterTax = rdr.IsDBNull(6) ? null : rdr.GetDouble(6),
                CurrencyCode = rdr.IsDBNull(7) ? null : rdr.GetString(7),
                RoomTypeCode = rdr.IsDBNull(8) ? null : rdr.GetString(8),
                RatePlanCode = rdr.IsDBNull(9) ? null : rdr.GetString(9),
                IsApproved = rdr.IsDBNull(10) ? null : rdr.GetBoolean(10),
                DateInsert = rdr.IsDBNull(11) ? null : rdr.GetDateTime(11),
                Type = rdr.IsDBNull(12) ? null : rdr.GetString(12)
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  NEW FEATURE 2: Room Waste (unsold rooms burning money)
    // ═══════════════════════════════════════════════════════════════════

    private async Task LoadRoomWaste(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            -- KPIs
            SELECT 
                COUNT(*) as Total,
                ISNULL(SUM(ISNULL(b.Price, 0)), 0) as TotalValue,
                SUM(CASE WHEN b.CancellationTo <= DATEADD(HOUR, 24, GETDATE()) THEN 1 ELSE 0 END) as Expiring24h,
                SUM(CASE WHEN b.CancellationTo <= DATEADD(HOUR, 48, GETDATE()) AND b.CancellationTo > DATEADD(HOUR, 24, GETDATE()) THEN 1 ELSE 0 END) as Expiring48h
            FROM MED_Book b
            WHERE b.IsActive = 1 AND (b.IsSold = 0 OR b.IsSold IS NULL) AND b.CancellationTo >= GETDATE();

            -- Detail: rooms closest to expiring
            SELECT TOP 30
                b.PreBookId, b.contentBookingID, b.HotelId,
                ISNULL(h.Name, 'Hotel ' + CAST(b.HotelId AS VARCHAR)) as HotelName,
                b.Price, b.CancellationTo, b.StartDate, b.EndDate,
                DATEDIFF(HOUR, GETDATE(), b.CancellationTo) as HoursLeft,
                CASE WHEN b.Source = 1 THEN 'Innstant' WHEN b.Source = 2 THEN 'GoGlobal' ELSE 'Source-' + CAST(ISNULL(b.Source,0) AS VARCHAR) END as SourceName
            FROM MED_Book b
            LEFT JOIN Med_Hotels h ON h.HotelId = b.HotelId
            WHERE b.IsActive = 1 AND (b.IsSold = 0 OR b.IsSold IS NULL) AND b.CancellationTo >= GETDATE()
            ORDER BY b.CancellationTo ASC";

        using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = 15;
        using var rdr = await cmd.ExecuteReaderAsync();

        if (await rdr.ReadAsync())
        {
            s.WasteRoomsTotal = rdr.GetInt32(0);
            s.WasteTotalValue = rdr.GetDouble(1);
            s.WasteExpiring24h = rdr.GetInt32(2);
            s.WasteExpiring48h = rdr.GetInt32(3);
        }

        await rdr.NextResultAsync();
        while (await rdr.ReadAsync())
        {
            s.WasteRooms.Add(new RoomWasteInfo
            {
                PreBookId = rdr.GetInt32(0),
                ContentBookingId = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                HotelId = rdr.IsDBNull(2) ? null : rdr.GetInt32(2),
                HotelName = rdr.IsDBNull(3) ? null : rdr.GetString(3),
                Price = rdr.IsDBNull(4) ? null : rdr.GetDouble(4),
                CancellationTo = rdr.IsDBNull(5) ? null : rdr.GetDateTime(5),
                StartDate = rdr.IsDBNull(6) ? null : rdr.GetDateTime(6),
                EndDate = rdr.IsDBNull(7) ? null : rdr.GetDateTime(7),
                HoursUntilExpiry = rdr.IsDBNull(8) ? 0 : rdr.GetInt32(8),
                Source = rdr.IsDBNull(9) ? null : rdr.GetString(9)
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  NEW FEATURE 3: Conversion & Revenue (P&L)
    // ═══════════════════════════════════════════════════════════════════

    private async Task LoadConversionRevenue(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            -- Overall conversion
            SELECT 
                COUNT(*) as TotalBought,
                SUM(CASE WHEN IsSold = 1 THEN 1 ELSE 0 END) as TotalSold,
                ISNULL(SUM(ISNULL(Price, 0)), 0) as TotalBoughtValue,
                ISNULL(SUM(CASE WHEN IsSold = 1 THEN ISNULL(Price, 0) ELSE 0 END), 0) as TotalSoldValue
            FROM MED_Book;

            -- Today's numbers
            SELECT 
                COUNT(*) as BoughtToday,
                SUM(CASE WHEN IsSold = 1 THEN 1 ELSE 0 END) as SoldToday,
                ISNULL(SUM(ISNULL(Price, 0)), 0) as BoughtTodayValue,
                ISNULL(SUM(CASE WHEN IsSold = 1 THEN ISNULL(Price, 0) ELSE 0 END), 0) as SoldTodayValue
            FROM MED_Book WHERE DateInsert >= CAST(GETDATE() AS DATE);

            -- Per-hotel conversion
            SELECT TOP 20
                b.HotelId,
                ISNULL(h.Name, 'Hotel ' + CAST(b.HotelId AS VARCHAR)) as HotelName,
                COUNT(*) as Bought,
                SUM(CASE WHEN b.IsSold = 1 THEN 1 ELSE 0 END) as Sold,
                ISNULL(SUM(ISNULL(b.Price, 0)), 0) as BoughtValue,
                ISNULL(SUM(CASE WHEN b.IsSold = 1 THEN ISNULL(b.Price, 0) ELSE 0 END), 0) as SoldValue,
                CASE WHEN COUNT(*) > 0 
                     THEN CAST(SUM(CASE WHEN b.IsSold = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100 
                     ELSE 0 END as Conversion
            FROM MED_Book b
            LEFT JOIN Med_Hotels h ON h.HotelId = b.HotelId
            GROUP BY b.HotelId, h.Name
            ORDER BY Bought DESC";

        using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = 30;
        using var rdr = await cmd.ExecuteReaderAsync();

        // Overall
        if (await rdr.ReadAsync())
        {
            s.TotalBought = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0);
            s.TotalSold = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
            s.TotalBoughtValue = rdr.IsDBNull(2) ? 0 : rdr.GetDouble(2);
            s.TotalSoldValue = rdr.IsDBNull(3) ? 0 : rdr.GetDouble(3);
            s.ConversionRate = s.TotalBought > 0 ? Math.Round((double)s.TotalSold / s.TotalBought * 100, 1) : 0;
            s.ProfitLoss = s.TotalSoldValue - s.TotalBoughtValue;
        }

        // Today
        await rdr.NextResultAsync();
        if (await rdr.ReadAsync())
        {
            s.BoughtToday = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0);
            s.SoldToday = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
            s.BoughtTodayValue = rdr.IsDBNull(2) ? 0 : rdr.GetDouble(2);
            s.SoldTodayValue = rdr.IsDBNull(3) ? 0 : rdr.GetDouble(3);
        }

        // Per hotel
        await rdr.NextResultAsync();
        while (await rdr.ReadAsync())
        {
            s.ConversionByHotel.Add(new ConversionByHotelInfo
            {
                HotelId = rdr.IsDBNull(0) ? null : rdr.GetInt32(0),
                HotelName = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                Bought = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2),
                Sold = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3),
                BoughtValue = rdr.IsDBNull(4) ? 0 : rdr.GetDouble(4),
                SoldValue = rdr.IsDBNull(5) ? 0 : rdr.GetDouble(5),
                Conversion = rdr.IsDBNull(6) ? 0 : Math.Round(rdr.GetDouble(6), 1)
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  NEW FEATURE 4: Price Drift (supplier price changes)
    // ═══════════════════════════════════════════════════════════════════

    private async Task LoadPriceDrift(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            -- KPIs
            SELECT 
                COUNT(*) as DriftCount,
                ISNULL(SUM(ABS(ISNULL(LastPrice, 0) - ISNULL(Price, 0))), 0) as TotalImpact
            FROM MED_Book
            WHERE IsActive = 1 AND LastPrice IS NOT NULL AND Price IS NOT NULL AND LastPrice != Price;

            -- Detail: biggest drifts
            SELECT TOP 25
                b.PreBookId, b.HotelId,
                ISNULL(h.Name, 'Hotel ' + CAST(b.HotelId AS VARCHAR)) as HotelName,
                b.Price, b.LastPrice,
                (b.LastPrice - b.Price) as DriftAmount,
                CASE WHEN b.Price != 0 THEN ROUND((b.LastPrice - b.Price) / b.Price * 100, 1) ELSE 0 END as DriftPercent,
                b.DateLastPrice
            FROM MED_Book b
            LEFT JOIN Med_Hotels h ON h.HotelId = b.HotelId
            WHERE b.IsActive = 1 AND b.LastPrice IS NOT NULL AND b.Price IS NOT NULL AND b.LastPrice != b.Price
            ORDER BY ABS(b.LastPrice - b.Price) DESC";

        using var cmd = new SqlCommand(sql, conn);
        cmd.CommandTimeout = 15;
        using var rdr = await cmd.ExecuteReaderAsync();

        if (await rdr.ReadAsync())
        {
            s.PriceDriftCount = rdr.GetInt32(0);
            s.PriceDriftTotalImpact = rdr.GetDouble(1);
        }

        await rdr.NextResultAsync();
        while (await rdr.ReadAsync())
        {
            s.PriceDrifts.Add(new PriceDriftInfo
            {
                PreBookId = rdr.GetInt32(0),
                HotelId = rdr.IsDBNull(1) ? null : rdr.GetInt32(1),
                HotelName = rdr.IsDBNull(2) ? null : rdr.GetString(2),
                OriginalPrice = rdr.IsDBNull(3) ? null : rdr.GetDouble(3),
                LastPrice = rdr.IsDBNull(4) ? null : rdr.GetDouble(4),
                DriftAmount = rdr.IsDBNull(5) ? 0 : rdr.GetDouble(5),
                DriftPercent = rdr.IsDBNull(6) ? 0 : rdr.GetDouble(6),
                DateLastPrice = rdr.IsDBNull(7) ? null : rdr.GetDateTime(7)
            });
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  NEW FEATURE 5: BuyRooms Heartbeat
    // ═══════════════════════════════════════════════════════════════════

    private async Task LoadBuyRoomsHeartbeat(SqlConnection conn, SystemStatus s)
    {
        const string sql = @"
            SELECT 
                (SELECT MAX(DateInsert) FROM MED_Book) as LastBook,
                (SELECT MAX(DateInsert) FROM MED_PreBook) as LastPreBook";

        using var cmd = new SqlCommand(sql, conn);
        using var rdr = await cmd.ExecuteReaderAsync();
        if (await rdr.ReadAsync())
        {
            s.LastBookPurchaseTime = rdr.IsDBNull(0) ? null : rdr.GetDateTime(0);
            s.LastPreBookTime = rdr.IsDBNull(1) ? null : rdr.GetDateTime(1);

            if (s.LastBookPurchaseTime.HasValue)
            {
                s.MinutesSinceLastPurchase = (int)(DateTime.UtcNow - s.LastBookPurchaseTime.Value).TotalMinutes;
                // Healthy if last purchase was within 30 minutes (BuyRooms has 200s sleep cycle)
                s.BuyRoomsHealthy = s.MinutesSinceLastPurchase <= 30;
            }
            else
            {
                s.MinutesSinceLastPurchase = -1;
                s.BuyRoomsHealthy = false;
            }
        }
    }
}
