using Microsoft.Data.SqlClient;

namespace MediciMonitor;

/// <summary>
/// All SQL query logic — reads from Azure SQL medici-db.
/// Every method is self-contained and uses raw ADO.NET for speed.
/// </summary>
public class DataService
{
    private readonly string _connStr;
    private readonly int _soRunningNoResultHours;
    private readonly int _soScanStuckMinutes;
    private readonly ILogger<DataService> _logger;

    public DataService(IConfiguration config, ILogger<DataService> logger)
    {
        var cs = config.GetConnectionString("SqlServer")
            ?? throw new InvalidOperationException("Missing SqlServer connection string");
        // Ensure MARS is enabled for multiple queries on same connection
        if (!cs.Contains("MultipleActiveResultSets", StringComparison.OrdinalIgnoreCase))
            cs += ";MultipleActiveResultSets=true";
        _connStr = cs;
        _soRunningNoResultHours = config.GetValue<int?>("AlertThresholds:SalesOfficeRunningNoResultHours") ?? 2;
        _soScanStuckMinutes = config.GetValue<int?>("AlertThresholds:SalesOfficeScanStuckMinutes") ?? 30;
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

            // Run all queries in parallel (MARS enabled) — each wrapped in try/catch
            await Task.WhenAll(
                Safe(() => LoadBookingSummary(conn, s)),
                Safe(() => LoadStuckCancellations(conn, s)),
                Safe(() => LoadCancelStats(conn, s)),
                Safe(() => LoadRecentCancelErrors(conn, s)),
                Safe(() => LoadBookingErrors(conn, s)),
                Safe(() => LoadPushStatus(conn, s)),
                Safe(() => LoadQueueStatus(conn, s)),
                Safe(() => LoadBackOfficeErrors(conn, s)),
                Safe(() => LoadActiveBookingsByHotel(conn, s)),
                Safe(() => LoadSalesOfficeStatus(conn, s)),
                Safe(() => LoadSalesOfficeDetails(conn, s)),
                Safe(() => LoadOpportunitiesAndRooms(conn, s)),
                Safe(() => LoadReservations(conn, s)),
                Safe(() => LoadRoomWaste(conn, s)),
                Safe(() => LoadConversionRevenue(conn, s)),
                Safe(() => LoadPriceDrift(conn, s)),
                Safe(() => LoadBuyRoomsHeartbeat(conn, s)),
                Safe(() => LoadBuyRoomsFunnel(conn, s))
            );
        }
        catch (Exception ex)
        {
            s.DbConnected = false;
            s.Error = ex.Message;
        }

        return s;
    }

    public async Task<SalesOrderDiagnostics> GetSalesOrderDiagnostics()
    {
        var r = new SalesOrderDiagnostics
        {
            Timestamp = DateTime.UtcNow,
            StaleRunningThresholdHours = _soRunningNoResultHours,
            ScanStuckThresholdMinutes = _soScanStuckMinutes
        };

        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            r.DbConnected = true;

            var ordersTable = await ResolveExistingTable(conn, ["SalesOfficeOrders", "[SalesOffice.Orders]", "SalesOffice_Orders"]);
            var detailsTable = await ResolveExistingTable(conn, ["[SalesOffice.Details]", "SalesOfficeDetails", "SalesOffice_Details"]);

            r.OrdersTable = ordersTable;
            r.DetailsTable = detailsTable;

            if (ordersTable == null)
            {
                r.Error = "SalesOffice orders table not found";
                return r;
            }

            var hasOrderDateInsert = await ColumnExists(conn, ordersTable, "DateInsert");
            var hasDetailsDateInsert = detailsTable != null && await ColumnExists(conn, detailsTable, "DateInsert");
            var hasProcessedCallback = detailsTable != null && await ColumnExists(conn, detailsTable, "IsProcessedCallback");
            var hasErrCol = detailsTable != null && await ColumnExists(conn, detailsTable, "Error");
            var hasErrMsgCol = detailsTable != null && await ColumnExists(conn, detailsTable, "ErrorMessage");
            var hasDetailId = detailsTable != null && await ColumnExists(conn, detailsTable, "Id");
            var hasHotelId = detailsTable != null && await ColumnExists(conn, detailsTable, "HotelId");
            var orderHotelIdColumn = await ResolveFirstExistingColumn(conn, ordersTable, ["HotelId", "HotelCode", "PropertyId", "PropertyCode"]);
            var orderHotelNameColumn = await ResolveFirstExistingColumn(conn, ordersTable, ["HotelName", "PropertyName", "Hotel", "Property"]);
            var orderSupplierColumn = await ResolveFirstExistingColumn(conn, ordersTable, ["SupplierName", "Supplier", "SourceName", "ProviderName", "Provider", "Source"]);
            var orderRoomColumn = await ResolveFirstExistingColumn(conn, ordersTable, ["RoomTypeCode", "RoomType", "RoomName", "Room", "Category"]);
            var supplierColumn = detailsTable != null
                ? await ResolveFirstExistingColumn(conn, detailsTable, ["SupplierName", "Supplier", "SourceName", "ProviderName", "Provider", "Source"])
                : null;
            var roomColumn = detailsTable != null
                ? await ResolveFirstExistingColumn(conn, detailsTable, ["RoomTypeCode", "RoomType", "RoomName", "Room", "RoomId"])
                : null;
            var detailOrderFk = detailsTable != null
                ? await ResolveFirstExistingColumn(conn, detailsTable, ["OrderId", "SalesOfficeOrderId", "Order_Id", "SalesOrderId", "FK_OrderId"])
                : null;

            string detailCte;
            if (detailsTable != null && detailOrderFk != null)
            {
                var unprocessedExpr = hasProcessedCallback
                    ? "SUM(CASE WHEN ISNULL(IsProcessedCallback, 0) = 0 THEN 1 ELSE 0 END)"
                    : "0";
                var errorExpr = hasErrCol
                    ? "SUM(CASE WHEN [Error] IS NOT NULL AND [Error] <> '' THEN 1 ELSE 0 END)"
                    : hasErrMsgCol
                        ? "SUM(CASE WHEN [ErrorMessage] IS NOT NULL AND [ErrorMessage] <> '' THEN 1 ELSE 0 END)"
                        : "0";
                detailCte = $@";WITH d AS (
                    SELECT
                        [{detailOrderFk}] AS OrderId,
                        COUNT(*) AS DetailCount,
                        {unprocessedExpr} AS UnprocessedCount,
                        {errorExpr} AS ErrorCount
                    FROM {detailsTable}
                    GROUP BY [{detailOrderFk}]
                )";
            }
            else
            {
                detailCte = ";WITH d AS (SELECT CAST(NULL AS INT) AS OrderId, 0 AS DetailCount, 0 AS UnprocessedCount, 0 AS ErrorCount WHERE 1 = 0)";
            }

            var stalePredicate = hasOrderDateInsert
                ? $"AND o.DateInsert <= DATEADD(HOUR, -{r.StaleRunningThresholdHours}, GETDATE())"
                : "";
            var minutesExpr = hasOrderDateInsert
                ? "DATEDIFF(MINUTE, o.DateInsert, GETDATE())"
                : "NULL";
            var orderDateSelect = hasOrderDateInsert ? "o.DateInsert" : "NULL";
            var orderHotelExpr = orderHotelNameColumn != null && orderHotelIdColumn != null
                ? $"COALESCE(NULLIF(LTRIM(RTRIM(CAST(o.[{orderHotelNameColumn}] AS NVARCHAR(200)))), ''), N'Hotel ' + CAST(o.[{orderHotelIdColumn}] AS NVARCHAR(50)))"
                : orderHotelNameColumn != null
                    ? $"NULLIF(LTRIM(RTRIM(CAST(o.[{orderHotelNameColumn}] AS NVARCHAR(200)))), '')"
                    : orderHotelIdColumn != null
                        ? $"N'Hotel ' + CAST(o.[{orderHotelIdColumn}] AS NVARCHAR(50))"
                        : "NULL";
            var detailHotelExpr = detailsTable != null && detailOrderFk != null && hasHotelId
                ? $"(SELECT TOP 1 ISNULL(h.Name, N'Hotel ' + CAST(x.HotelId AS NVARCHAR(20))) FROM {detailsTable} x LEFT JOIN Med_Hotels h ON h.HotelId = x.HotelId WHERE x.[{detailOrderFk}] = o.Id AND x.HotelId IS NOT NULL GROUP BY x.HotelId, h.Name ORDER BY COUNT(*) DESC, ISNULL(h.Name, N'Hotel ' + CAST(x.HotelId AS NVARCHAR(20))))"
                : "NULL";
            var hotelExpr = $"COALESCE({detailHotelExpr}, {orderHotelExpr})";
            var orderSupplierExpr = orderSupplierColumn != null
                ? $"NULLIF(LTRIM(RTRIM(CAST(o.[{orderSupplierColumn}] AS NVARCHAR(200)))), '')"
                : "NULL";
            var supplierExpr = detailsTable != null && supplierColumn != null && detailOrderFk != null
                ? $"COALESCE((SELECT TOP 1 CAST(x.[{supplierColumn}] AS NVARCHAR(200)) FROM {detailsTable} x WHERE x.[{detailOrderFk}] = o.Id AND x.[{supplierColumn}] IS NOT NULL AND LTRIM(RTRIM(CAST(x.[{supplierColumn}] AS NVARCHAR(200)))) <> '' GROUP BY CAST(x.[{supplierColumn}] AS NVARCHAR(200)) ORDER BY COUNT(*) DESC, CAST(x.[{supplierColumn}] AS NVARCHAR(200))), {orderSupplierExpr})"
                : orderSupplierExpr;
            var orderRoomExpr = orderRoomColumn != null
                ? $"NULLIF(LTRIM(RTRIM(CAST(o.[{orderRoomColumn}] AS NVARCHAR(200)))), '')"
                : "NULL";
            var roomExpr = detailsTable != null && roomColumn != null && detailOrderFk != null
                ? $"COALESCE((SELECT TOP 1 CAST(x.[{roomColumn}] AS NVARCHAR(200)) FROM {detailsTable} x WHERE x.[{detailOrderFk}] = o.Id AND x.[{roomColumn}] IS NOT NULL AND LTRIM(RTRIM(CAST(x.[{roomColumn}] AS NVARCHAR(200)))) <> '' GROUP BY CAST(x.[{roomColumn}] AS NVARCHAR(200)) ORDER BY COUNT(*) DESC, CAST(x.[{roomColumn}] AS NVARCHAR(200))), {orderRoomExpr})"
                : orderRoomExpr;

            var summarySql = $@"
                {detailCte}
                SELECT
                    COUNT(*) AS TotalActive,
                    SUM(CASE WHEN o.WebJobStatus IS NULL THEN 1 ELSE 0 END) AS PendingCnt,
                    SUM(CASE WHEN o.WebJobStatus = 'In Progress' THEN 1 ELSE 0 END) AS RunningCnt,
                    SUM(CASE WHEN o.WebJobStatus LIKE 'Completed%' THEN 1 ELSE 0 END) AS CompletedCnt,
                    SUM(CASE WHEN o.WebJobStatus LIKE 'Failed%' OR o.WebJobStatus = 'DateRangeError' THEN 1 ELSE 0 END) AS FailedCnt,
                    ISNULL(SUM(CASE WHEN o.WebJobStatus LIKE 'Completed%' AND ISNULL(d.DetailCount, 0) = 0 THEN 1 ELSE 0 END), 0) AS CompletedNoMap,
                    ISNULL(SUM(CASE WHEN (o.WebJobStatus IS NULL OR o.WebJobStatus = 'In Progress') AND ISNULL(d.DetailCount, 0) = 0 THEN 1 ELSE 0 END), 0) AS RunningNoResult,
                    ISNULL(SUM(CASE WHEN (o.WebJobStatus IS NULL OR o.WebJobStatus = 'In Progress') AND ISNULL(d.DetailCount, 0) = 0 {stalePredicate} THEN 1 ELSE 0 END), 0) AS RunningStale,
                    ISNULL(AVG(CASE WHEN o.WebJobStatus LIKE 'Completed%' THEN CAST(ISNULL(d.DetailCount, 0) AS FLOAT) END), 0) AS AvgDetailsPerCompleted
                FROM {ordersTable} o
                LEFT JOIN d ON d.OrderId = o.Id
                WHERE o.IsActive = 1";

            using (var cmd = new SqlCommand(summarySql, conn) { CommandTimeout = 20 })
            using (var rdr = await cmd.ExecuteReaderAsync())
            {
                if (await rdr.ReadAsync())
                {
                    r.TotalActiveOrders = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0);
                    r.PendingOrders = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
                    r.RunningOrders = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2);
                    r.CompletedOrders = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3);
                    r.FailedOrders = rdr.IsDBNull(4) ? 0 : rdr.GetInt32(4);
                    r.CompletedWithoutMapping = rdr.IsDBNull(5) ? 0 : rdr.GetInt32(5);
                    r.RunningWithoutResult = rdr.IsDBNull(6) ? 0 : rdr.GetInt32(6);
                    r.RunningStaleWithoutResult = rdr.IsDBNull(7) ? 0 : rdr.GetInt32(7);
                    r.AvgDetailsPerCompletedOrder = rdr.IsDBNull(8) ? 0 : Math.Round(rdr.GetDouble(8), 2);
                }
            }

            if (detailsTable != null)
            {
                var detailsSql = $@"
                    SELECT
                        COUNT(*) AS TotalRows,
                        {(hasProcessedCallback ? "SUM(CASE WHEN ISNULL(IsProcessedCallback, 0) = 0 THEN 1 ELSE 0 END)" : "0")} AS UnprocessedRows,
                        {(hasDetailsDateInsert ? "SUM(CASE WHEN DateInsert >= DATEADD(HOUR, -1, GETDATE()) THEN 1 ELSE 0 END)" : "0")} AS LastHourRows,
                        {(hasErrCol ? "SUM(CASE WHEN [Error] IS NOT NULL AND [Error] <> '' THEN 1 ELSE 0 END)" : hasErrMsgCol ? "SUM(CASE WHEN [ErrorMessage] IS NOT NULL AND [ErrorMessage] <> '' THEN 1 ELSE 0 END)" : "0")} AS CallbackErrRows
                    FROM {detailsTable}";

                using var dcmd = new SqlCommand(detailsSql, conn) { CommandTimeout = 10 };
                using var dr = await dcmd.ExecuteReaderAsync();
                if (await dr.ReadAsync())
                {
                    r.TotalDetailsRows = dr.IsDBNull(0) ? 0 : dr.GetInt32(0);
                    r.UnprocessedDetailsRows = dr.IsDBNull(1) ? 0 : dr.GetInt32(1);
                    r.DetailsLastHour = dr.IsDBNull(2) ? 0 : dr.GetInt32(2);
                    r.CallbackErrors = dr.IsDBNull(3) ? 0 : dr.GetInt32(3);
                }

                if (hasDetailsDateInsert)
                {
                    using var lastAnyCmd = new SqlCommand($"SELECT MAX(DateInsert) FROM {detailsTable}", conn) { CommandTimeout = 10 };
                    var lastAny = await lastAnyCmd.ExecuteScalarAsync();
                    if (lastAny != null && lastAny != DBNull.Value) r.LastDetailAt = Convert.ToDateTime(lastAny);
                }

                if (hasProcessedCallback && hasDetailsDateInsert)
                {
                    using var lastProcCmd = new SqlCommand($"SELECT MAX(DateInsert) FROM {detailsTable} WHERE ISNULL(IsProcessedCallback,0)=1", conn) { CommandTimeout = 10 };
                    var lastProc = await lastProcCmd.ExecuteScalarAsync();
                    if (lastProc != null && lastProc != DBNull.Value) r.LastProcessedCallbackAt = Convert.ToDateTime(lastProc);
                }

                var orderByFallback = hasDetailsDateInsert ? "DateInsert DESC" : hasDetailId ? "Id DESC" : detailOrderFk != null ? $"[{detailOrderFk}] DESC" : "1";
                var scanLogSql = $@"
                    SELECT TOP 120
                        {(hasDetailId ? "Id" : "NULL")} AS DetailId,
                        {(detailOrderFk != null ? $"[{detailOrderFk}]" : "NULL")} AS OrderId,
                        {(hasDetailsDateInsert ? "DateInsert" : "NULL")} AS DateInsert,
                        {(hasProcessedCallback ? "IsProcessedCallback" : "NULL")} AS IsProcessedCallback,
                        {(hasErrCol ? "[Error]" : "NULL")} AS [Error],
                        {(hasErrMsgCol ? "[ErrorMessage]" : "NULL")} AS [ErrorMessage]
                    FROM {detailsTable}
                    ORDER BY {orderByFallback}";

                using (var slcmd = new SqlCommand(scanLogSql, conn) { CommandTimeout = 15 })
                using (var slr = await slcmd.ExecuteReaderAsync())
                {
                    while (await slr.ReadAsync())
                    {
                        var err = slr.IsDBNull(4) ? null : slr.GetString(4);
                        var errMsg = slr.IsDBNull(5) ? null : slr.GetString(5);
                        var hasErr = !string.IsNullOrWhiteSpace(err) || !string.IsNullOrWhiteSpace(errMsg);
                        var processed = slr.IsDBNull(3) ? (bool?)null : slr.GetBoolean(3);

                        var level = hasErr ? "Error" : processed == false ? "Warning" : "Info";
                        var msg = hasErr
                            ? (errMsg ?? err ?? "Callback error")
                            : processed == false
                                ? "Callback עדיין לא עובד"
                                : "Callback עובד בהצלחה";

                        r.ScanLog.Add(new SalesOrderScanLogEntry
                        {
                            DetailId = slr.IsDBNull(0) ? null : slr.GetInt32(0),
                            OrderId = slr.IsDBNull(1) ? null : slr.GetInt32(1),
                            DateInsert = slr.IsDBNull(2) ? null : slr.GetDateTime(2),
                            IsProcessedCallback = processed,
                            Level = level,
                            Message = msg
                        });
                    }
                }

                if (hasHotelId)
                {
                    try
                    {
                        var hotelCovSql = $@"
                            SELECT TOP 60
                                d.HotelId,
                                ISNULL(h.Name, N'Hotel ' + CAST(d.HotelId AS NVARCHAR(20))) AS HotelName,
                                COUNT(DISTINCT d.[{detailOrderFk}]) AS OrdersCount,
                                COUNT(*) AS DetailsCount,
                                {(hasProcessedCallback ? "SUM(CASE WHEN ISNULL(d.IsProcessedCallback,0)=0 THEN 1 ELSE 0 END)" : "0")} AS UnprocessedCount,
                                {(hasErrCol ? "SUM(CASE WHEN d.[Error] IS NOT NULL AND d.[Error] <> '' THEN 1 ELSE 0 END)" : hasErrMsgCol ? "SUM(CASE WHEN d.[ErrorMessage] IS NOT NULL AND d.[ErrorMessage] <> '' THEN 1 ELSE 0 END)" : "0")} AS ErrorCount,
                                {(hasDetailsDateInsert ? "MAX(d.DateInsert)" : "NULL")} AS LastSeen
                            FROM {detailsTable} d
                            LEFT JOIN Med_Hotels h ON h.HotelId = d.HotelId
                            WHERE d.HotelId IS NOT NULL
                            GROUP BY d.HotelId, h.Name
                            ORDER BY ErrorCount DESC, UnprocessedCount DESC, DetailsCount DESC";

                        using var hcmd = new SqlCommand(hotelCovSql, conn) { CommandTimeout = 20 };
                        using var hrdr = await hcmd.ExecuteReaderAsync();
                        while (await hrdr.ReadAsync())
                        {
                            r.HotelCoverage.Add(new SalesOrderHotelCoverageInfo
                            {
                                HotelId = hrdr.IsDBNull(0) ? null : hrdr.GetInt32(0),
                                HotelName = hrdr.IsDBNull(1) ? null : hrdr.GetString(1),
                                OrdersCount = hrdr.IsDBNull(2) ? 0 : hrdr.GetInt32(2),
                                DetailsCount = hrdr.IsDBNull(3) ? 0 : hrdr.GetInt32(3),
                                UnprocessedCount = hrdr.IsDBNull(4) ? 0 : hrdr.GetInt32(4),
                                ErrorCount = hrdr.IsDBNull(5) ? 0 : hrdr.GetInt32(5),
                                LastSeen = hrdr.IsDBNull(6) ? null : hrdr.GetDateTime(6)
                            });
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug("Hotel coverage query skipped: {Err}", ex.Message);
                    }
                }
            }

            var runningSql = $@"
                {detailCte}
                SELECT TOP 100
                    o.Id,
                    o.WebJobStatus,
                    {orderDateSelect} AS DateInsert,
                    o.DateFrom,
                    o.DateTo,
                    ISNULL(d.DetailCount, 0) AS DetailCount,
                    ISNULL(d.UnprocessedCount, 0) AS UnprocessedCount,
                    ISNULL(d.ErrorCount, 0) AS ErrorCount,
                    {supplierExpr} AS SupplierInfo,
                    {roomExpr} AS RoomInfo,
                    {minutesExpr} AS MinutesSinceStart,
                    CASE
                        WHEN ISNULL(d.DetailCount, 0) = 0 {(hasOrderDateInsert ? $"AND o.DateInsert <= DATEADD(HOUR, -{r.StaleRunningThresholdHours}, GETDATE())" : "")} THEN N'Running בלי תוצאה מעבר לסף זמן'
                        WHEN ISNULL(d.DetailCount, 0) = 0 THEN N'Running בלי תוצאה עדיין'
                        WHEN ISNULL(d.UnprocessedCount, 0) > 0 THEN N'יש תוצאות אך callback לא עובד במלואו'
                        WHEN ISNULL(d.ErrorCount, 0) > 0 THEN N'נוצרו שגיאות callback/מיפוי'
                        ELSE N'במעקב'
                    END AS IssueReason
                FROM {ordersTable} o
                LEFT JOIN d ON d.OrderId = o.Id
                WHERE o.IsActive = 1 AND (o.WebJobStatus IS NULL OR o.WebJobStatus = 'In Progress')
                ORDER BY {(hasOrderDateInsert ? "o.DateInsert ASC" : "o.Id DESC")}";

            using (var cmd = new SqlCommand(runningSql, conn) { CommandTimeout = 20 })
            using (var rdr = await cmd.ExecuteReaderAsync())
            {
                while (await rdr.ReadAsync())
                {
                    r.RunningOrderItems.Add(new SalesOrderRuntimeInfo
                    {
                        OrderId = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0),
                        WebJobStatus = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                        DateInsert = rdr.IsDBNull(2) ? null : rdr.GetDateTime(2),
                        DateFrom = rdr.IsDBNull(3) ? null : rdr.GetDateTime(3),
                        DateTo = rdr.IsDBNull(4) ? null : rdr.GetDateTime(4),
                        DetailCount = rdr.IsDBNull(5) ? 0 : rdr.GetInt32(5),
                        UnprocessedDetails = rdr.IsDBNull(6) ? 0 : rdr.GetInt32(6),
                        ErrorCount = rdr.IsDBNull(7) ? 0 : rdr.GetInt32(7),
                        SupplierInfo = rdr.IsDBNull(8) ? null : rdr.GetString(8),
                        RoomInfo = rdr.IsDBNull(9) ? null : rdr.GetString(9),
                        MinutesSinceStart = rdr.IsDBNull(10) ? null : rdr.GetInt32(10),
                        IssueReason = rdr.IsDBNull(11) ? null : rdr.GetString(11)
                    });
                }
            }

            var zeroMapSql = $@"
                {detailCte}
                SELECT TOP 100
                    o.Id,
                    o.WebJobStatus,
                    {orderDateSelect} AS DateInsert,
                    o.DateFrom,
                    o.DateTo,
                    ISNULL(d.DetailCount, 0) AS DetailCount,
                    ISNULL(d.UnprocessedCount, 0) AS UnprocessedCount,
                    ISNULL(d.ErrorCount, 0) AS ErrorCount,
                    {hotelExpr} AS HotelInfo,
                    {supplierExpr} AS SupplierInfo,
                    {roomExpr} AS RoomInfo,
                    {minutesExpr} AS MinutesSinceStart,
                    N'Completed ללא שום תוצאת mapping' AS IssueReason,
                    CONCAT(
                        N'חסר Detail/Mapping',
                        CASE WHEN {hotelExpr} IS NULL THEN N' · חסר מלון/יעד' ELSE N'' END,
                        CASE WHEN {supplierExpr} IS NULL THEN N' · חסר ספק' ELSE N'' END,
                        CASE WHEN {roomExpr} IS NULL THEN N' · חסר חדר' ELSE N'' END
                    ) AS MissingWhat
                FROM {ordersTable} o
                LEFT JOIN d ON d.OrderId = o.Id
                WHERE o.IsActive = 1 AND o.WebJobStatus LIKE 'Completed%' AND ISNULL(d.DetailCount, 0) = 0
                ORDER BY {(hasOrderDateInsert ? "o.DateInsert DESC" : "o.Id DESC")}";

            using (var cmd = new SqlCommand(zeroMapSql, conn) { CommandTimeout = 20 })
            using (var rdr = await cmd.ExecuteReaderAsync())
            {
                while (await rdr.ReadAsync())
                {
                    r.CompletedWithoutMappingItems.Add(new SalesOrderRuntimeInfo
                    {
                        OrderId = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0),
                        WebJobStatus = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                        DateInsert = rdr.IsDBNull(2) ? null : rdr.GetDateTime(2),
                        DateFrom = rdr.IsDBNull(3) ? null : rdr.GetDateTime(3),
                        DateTo = rdr.IsDBNull(4) ? null : rdr.GetDateTime(4),
                        DetailCount = rdr.IsDBNull(5) ? 0 : rdr.GetInt32(5),
                        UnprocessedDetails = rdr.IsDBNull(6) ? 0 : rdr.GetInt32(6),
                        ErrorCount = rdr.IsDBNull(7) ? 0 : rdr.GetInt32(7),
                        HotelInfo = rdr.IsDBNull(8) ? null : rdr.GetString(8),
                        SupplierInfo = rdr.IsDBNull(9) ? null : rdr.GetString(9),
                        RoomInfo = rdr.IsDBNull(10) ? null : rdr.GetString(10),
                        MinutesSinceStart = rdr.IsDBNull(11) ? null : rdr.GetInt32(11),
                        IssueReason = rdr.IsDBNull(12) ? null : rdr.GetString(12),
                        MissingWhat = rdr.IsDBNull(13) ? null : rdr.GetString(13)
                    });
                }
            }

            if (r.CompletedWithoutMappingItems.Count > 0)
            {
                var ranges = ToOrderRanges(r.CompletedWithoutMappingItems.Select(x => x.OrderId));
                r.ScanLog.InsertRange(0, ranges.Take(6).Select(rr => new SalesOrderScanLogEntry
                {
                    DateInsert = DateTime.UtcNow,
                    Level = "Warning",
                    Message = $"Completed ללא Mapping בטווח הזמנות {rr}"
                }));
            }

            var failedSql = $@"
                {detailCte}
                SELECT TOP 100
                    o.Id,
                    o.WebJobStatus,
                    {orderDateSelect} AS DateInsert,
                    o.DateFrom,
                    o.DateTo,
                    ISNULL(d.DetailCount, 0) AS DetailCount,
                    ISNULL(d.UnprocessedCount, 0) AS UnprocessedCount,
                    ISNULL(d.ErrorCount, 0) AS ErrorCount,
                    {supplierExpr} AS SupplierInfo,
                    {roomExpr} AS RoomInfo,
                    {minutesExpr} AS MinutesSinceStart,
                    CASE
                        WHEN o.WebJobStatus = 'DateRangeError' THEN N'טווח תאריכים לא חוקי להזמנה'
                        WHEN o.WebJobStatus LIKE 'Failed%' THEN N'סטטוס WebJob נכשל'
                        WHEN ISNULL(d.ErrorCount, 0) > 0 THEN N'נמצאו שגיאות callback/מיפוי'
                        ELSE N'כשל לא מסווג'
                    END AS IssueReason
                FROM {ordersTable} o
                LEFT JOIN d ON d.OrderId = o.Id
                WHERE o.IsActive = 1 AND (o.WebJobStatus LIKE 'Failed%' OR o.WebJobStatus = 'DateRangeError')
                ORDER BY {(hasOrderDateInsert ? "o.DateInsert DESC" : "o.Id DESC")}";

            using (var cmd = new SqlCommand(failedSql, conn) { CommandTimeout = 20 })
            using (var rdr = await cmd.ExecuteReaderAsync())
            {
                while (await rdr.ReadAsync())
                {
                    r.FailedOrderItems.Add(new SalesOrderRuntimeInfo
                    {
                        OrderId = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0),
                        WebJobStatus = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                        DateInsert = rdr.IsDBNull(2) ? null : rdr.GetDateTime(2),
                        DateFrom = rdr.IsDBNull(3) ? null : rdr.GetDateTime(3),
                        DateTo = rdr.IsDBNull(4) ? null : rdr.GetDateTime(4),
                        DetailCount = rdr.IsDBNull(5) ? 0 : rdr.GetInt32(5),
                        UnprocessedDetails = rdr.IsDBNull(6) ? 0 : rdr.GetInt32(6),
                        ErrorCount = rdr.IsDBNull(7) ? 0 : rdr.GetInt32(7),
                        SupplierInfo = rdr.IsDBNull(8) ? null : rdr.GetString(8),
                        RoomInfo = rdr.IsDBNull(9) ? null : rdr.GetString(9),
                        MinutesSinceStart = rdr.IsDBNull(10) ? null : rdr.GetInt32(10),
                        IssueReason = rdr.IsDBNull(11) ? null : rdr.GetString(11)
                    });
                }
            }

            r.ProcessMetrics = new List<SalesOrderProcessMetric>
            {
                new() { Key = "active", Label = "סה\"כ Active", Count = r.TotalActiveOrders, Severity = "info" },
                new() { Key = "pending", Label = "Pending", Count = r.PendingOrders, Severity = r.PendingOrders > 0 ? "warning" : "ok" },
                new() { Key = "running", Label = "In Progress", Count = r.RunningOrders, Severity = r.RunningOrders > 0 ? "info" : "ok" },
                new() { Key = "running_no_result", Label = "Running/Pending בלי תוצאה", Count = r.RunningWithoutResult, Severity = r.RunningWithoutResult > 0 ? "warning" : "ok" },
                new() { Key = "running_stale", Label = "Running בלי תוצאה מעל סף", Count = r.RunningStaleWithoutResult, Severity = r.RunningStaleWithoutResult > 0 ? "danger" : "ok", Notes = $"> {r.StaleRunningThresholdHours}h" },
                new() { Key = "completed", Label = "Completed", Count = r.CompletedOrders, Severity = "ok" },
                new() { Key = "completed_no_map", Label = "Completed ללא Mapping", Count = r.CompletedWithoutMapping, Severity = r.CompletedWithoutMapping > 0 ? "danger" : "ok" },
                new() { Key = "failed", Label = "Failed/DateRangeError", Count = r.FailedOrders, Severity = r.FailedOrders > 0 ? "danger" : "ok" },
                new() { Key = "callback_errors", Label = "Callback Errors", Count = r.CallbackErrors, Severity = r.CallbackErrors > 0 ? "warning" : "ok" },
                new() { Key = "callback_unprocessed", Label = "Callback Unprocessed", Count = r.UnprocessedDetailsRows, Severity = r.UnprocessedDetailsRows > 0 ? "warning" : "ok" }
            };

            var issueSql = $@"
                {detailCte}
                SELECT Issue, COUNT(*) AS Cnt
                FROM (
                    SELECT CASE
                        WHEN o.WebJobStatus LIKE 'Completed%' AND ISNULL(d.DetailCount, 0) = 0 THEN N'Completed ללא Mapping'
                        WHEN (o.WebJobStatus IS NULL OR o.WebJobStatus = 'In Progress') AND ISNULL(d.DetailCount, 0) = 0 {(hasOrderDateInsert ? $"AND o.DateInsert <= DATEADD(HOUR, -{r.StaleRunningThresholdHours}, GETDATE())" : "")} THEN N'Running/Pending ללא תוצאה (Stale)'
                        WHEN (o.WebJobStatus IS NULL OR o.WebJobStatus = 'In Progress') AND ISNULL(d.DetailCount, 0) = 0 THEN N'Running/Pending ללא תוצאה'
                        WHEN (o.WebJobStatus IS NULL OR o.WebJobStatus = 'In Progress') AND ISNULL(d.UnprocessedCount, 0) > 0 THEN N'Callback לא מעובד'
                        WHEN (o.WebJobStatus IS NULL OR o.WebJobStatus = 'In Progress') AND ISNULL(d.ErrorCount, 0) > 0 THEN N'Callback עם שגיאה'
                        WHEN o.WebJobStatus = 'DateRangeError' THEN N'DateRangeError'
                        WHEN o.WebJobStatus LIKE 'Failed%' THEN N'Failed Status'
                        WHEN o.WebJobStatus LIKE 'Completed%' THEN N'Completed תקין'
                        ELSE N'אחר/לא מסווג'
                    END AS Issue
                    FROM {ordersTable} o
                    LEFT JOIN d ON d.OrderId = o.Id
                    WHERE o.IsActive = 1
                ) x
                GROUP BY Issue
                ORDER BY Cnt DESC";

            using (var cmd = new SqlCommand(issueSql, conn) { CommandTimeout = 20 })
            using (var rdr = await cmd.ExecuteReaderAsync())
            {
                while (await rdr.ReadAsync())
                {
                    r.IssueBreakdown.Add(new SalesOrderIssueSummary
                    {
                        Issue = rdr.IsDBNull(0) ? "Unknown" : rdr.GetString(0),
                        Count = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1)
                    });
                }
            }

            if (hasOrderDateInsert)
            {
                var detailsHourlyCte = detailsTable != null && hasDetailsDateInsert
                    ? $@", d AS (
                        SELECT
                            DATEADD(HOUR, DATEDIFF(HOUR, 0, DateInsert), 0) AS Hr,
                            COUNT(*) AS DetailRows,
                            {(hasErrCol ? "SUM(CASE WHEN [Error] IS NOT NULL AND [Error] <> '' THEN 1 ELSE 0 END)" : hasErrMsgCol ? "SUM(CASE WHEN [ErrorMessage] IS NOT NULL AND [ErrorMessage] <> '' THEN 1 ELSE 0 END)" : "0")} AS CallbackErrors
                        FROM {detailsTable}
                        WHERE DateInsert >= DATEADD(HOUR, -24, GETDATE())
                        GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, DateInsert), 0)
                    )"
                    : ", d AS (SELECT CAST(NULL AS DATETIME) AS Hr, 0 AS DetailRows, 0 AS CallbackErrors WHERE 1 = 0)";

                var throughputSql = $@"
                    ;WITH h AS (
                        SELECT DATEADD(HOUR, DATEDIFF(HOUR, 0, DATEADD(HOUR, -23, GETDATE())), 0) AS Hr
                        UNION ALL
                        SELECT DATEADD(HOUR, 1, Hr)
                        FROM h
                        WHERE Hr < DATEADD(HOUR, DATEDIFF(HOUR, 0, GETDATE()), 0)
                    ),
                    o AS (
                        SELECT
                            DATEADD(HOUR, DATEDIFF(HOUR, 0, o.DateInsert), 0) AS Hr,
                            COUNT(*) AS CreatedOrders,
                            SUM(CASE WHEN o.WebJobStatus LIKE 'Completed%' THEN 1 ELSE 0 END) AS CompletedOrders,
                            SUM(CASE WHEN o.WebJobStatus LIKE 'Failed%' OR o.WebJobStatus = 'DateRangeError' THEN 1 ELSE 0 END) AS FailedOrders
                        FROM {ordersTable} o
                        WHERE o.IsActive = 1
                          AND o.DateInsert >= DATEADD(HOUR, -24, GETDATE())
                        GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, o.DateInsert), 0)
                    )
                    {detailsHourlyCte}
                    SELECT
                        h.Hr,
                        ISNULL(o.CreatedOrders, 0) AS CreatedOrders,
                        ISNULL(o.CompletedOrders, 0) AS CompletedOrders,
                        ISNULL(o.FailedOrders, 0) AS FailedOrders,
                        ISNULL(d.DetailRows, 0) AS DetailRows,
                        ISNULL(d.CallbackErrors, 0) AS CallbackErrors
                    FROM h
                    LEFT JOIN o ON o.Hr = h.Hr
                    LEFT JOIN d ON d.Hr = h.Hr
                    ORDER BY h.Hr ASC
                    OPTION (MAXRECURSION 25)";

                using var tcmd = new SqlCommand(throughputSql, conn) { CommandTimeout = 20 };
                using var trdr = await tcmd.ExecuteReaderAsync();
                while (await trdr.ReadAsync())
                {
                    r.Throughput24h.Add(new SalesOrderHourlyPoint
                    {
                        Hour = trdr.IsDBNull(0) ? DateTime.UtcNow : trdr.GetDateTime(0),
                        CreatedOrders = trdr.IsDBNull(1) ? 0 : trdr.GetInt32(1),
                        CompletedOrders = trdr.IsDBNull(2) ? 0 : trdr.GetInt32(2),
                        FailedOrders = trdr.IsDBNull(3) ? 0 : trdr.GetInt32(3),
                        DetailRows = trdr.IsDBNull(4) ? 0 : trdr.GetInt32(4),
                        CallbackErrors = trdr.IsDBNull(5) ? 0 : trdr.GetInt32(5)
                    });
                }
            }

            if ((r.RunningOrders + r.PendingOrders) > 0)
            {
                var now = DateTime.UtcNow;
                if (r.LastProcessedCallbackAt.HasValue && (now - r.LastProcessedCallbackAt.Value).TotalMinutes > r.ScanStuckThresholdMinutes)
                {
                    r.IsScanStuck = true;
                    r.ScanStuckReason = $"אין callback מעובד מעל {r.ScanStuckThresholdMinutes} דקות";
                }
                else if (!r.LastProcessedCallbackAt.HasValue && !r.LastDetailAt.HasValue)
                {
                    r.IsScanStuck = true;
                    r.ScanStuckReason = "לא נמצאו אירועי סריקה בטבלת Details";
                }
                else if (r.DetailsLastHour == 0 && r.RunningWithoutResult > 0)
                {
                    r.IsScanStuck = true;
                    r.ScanStuckReason = "אין תוצאות סריקה בשעה האחרונה למרות הזמנות פעילות";
                }
            }

            if (r.IsScanStuck && !string.IsNullOrWhiteSpace(r.ScanStuckReason))
            {
                r.ScanLog.Insert(0, new SalesOrderScanLogEntry
                {
                    DateInsert = DateTime.UtcNow,
                    Level = "Error",
                    Message = $"SCAN STUCK: {r.ScanStuckReason}"
                });
            }
        }
        catch (Exception ex)
        {
            if (!r.DbConnected) r.DbConnected = false;
            r.Error = ex.Message;
        }

        return r;
    }

    public async Task<SalesOrderOrderTraceResponse> GetSalesOrderTrace(int orderId)
    {
        var r = new SalesOrderOrderTraceResponse { Timestamp = DateTime.UtcNow };

        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            r.DbConnected = true;

            var ordersTable = await ResolveExistingTable(conn, ["SalesOfficeOrders", "[SalesOffice.Orders]", "SalesOffice_Orders"]);
            var detailsTable = await ResolveExistingTable(conn, ["[SalesOffice.Details]", "SalesOfficeDetails", "SalesOffice_Details"]);

            r.OrdersTable = ordersTable;
            r.DetailsTable = detailsTable;

            if (ordersTable == null)
            {
                r.Error = "SalesOffice orders table not found";
                return r;
            }

            var hasOrderDateInsert = await ColumnExists(conn, ordersTable, "DateInsert");
            var hasDetailsDateInsert = detailsTable != null && await ColumnExists(conn, detailsTable, "DateInsert");
            var hasProcessedCallback = detailsTable != null && await ColumnExists(conn, detailsTable, "IsProcessedCallback");
            var hasErrCol = detailsTable != null && await ColumnExists(conn, detailsTable, "Error");
            var hasErrMsgCol = detailsTable != null && await ColumnExists(conn, detailsTable, "ErrorMessage");
            var hasDetailId = detailsTable != null && await ColumnExists(conn, detailsTable, "Id");
            var supplierColumn = detailsTable != null
                ? await ResolveFirstExistingColumn(conn, detailsTable, ["SupplierName", "Supplier", "SourceName", "ProviderName", "Provider", "Source"])
                : null;
            var roomColumn = detailsTable != null
                ? await ResolveFirstExistingColumn(conn, detailsTable, ["RoomTypeCode", "RoomType", "RoomName", "Room", "RoomId"])
                : null;
            var detailOrderFk = detailsTable != null
                ? await ResolveFirstExistingColumn(conn, detailsTable, ["OrderId", "SalesOfficeOrderId", "Order_Id", "SalesOrderId", "FK_OrderId"])
                : null;

            var orderSql = $@"
                SELECT TOP 1
                    o.Id,
                    o.WebJobStatus,
                    o.IsActive,
                    {(hasOrderDateInsert ? "o.DateInsert" : "NULL")} AS DateInsert,
                    o.DateFrom,
                    o.DateTo
                FROM {ordersTable} o
                WHERE o.Id = @id";

            using (var cmd = new SqlCommand(orderSql, conn))
            {
                cmd.Parameters.AddWithValue("@id", orderId);
                using var rdr = await cmd.ExecuteReaderAsync();
                if (!await rdr.ReadAsync())
                {
                    r.Error = $"Order {orderId} not found";
                    return r;
                }

                r.Order = new SalesOrderOrderTrace
                {
                    OrderId = rdr.IsDBNull(0) ? orderId : rdr.GetInt32(0),
                    WebJobStatus = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                    IsActive = !rdr.IsDBNull(2) && rdr.GetBoolean(2),
                    DateInsert = rdr.IsDBNull(3) ? null : rdr.GetDateTime(3),
                    DateFrom = rdr.IsDBNull(4) ? null : rdr.GetDateTime(4),
                    DateTo = rdr.IsDBNull(5) ? null : rdr.GetDateTime(5)
                };
            }

            if (detailsTable != null && r.Order != null)
            {
                var aggSql = $@"
                    SELECT
                        COUNT(*) AS DetailCount,
                        {(hasProcessedCallback ? "SUM(CASE WHEN ISNULL(IsProcessedCallback,0)=0 THEN 1 ELSE 0 END)" : "0")} AS UnprocessedCount,
                        {(hasErrCol ? "SUM(CASE WHEN [Error] IS NOT NULL AND [Error] <> '' THEN 1 ELSE 0 END)" : hasErrMsgCol ? "SUM(CASE WHEN [ErrorMessage] IS NOT NULL AND [ErrorMessage] <> '' THEN 1 ELSE 0 END)" : "0")} AS ErrorCount
                    FROM {detailsTable}
                    WHERE {(detailOrderFk != null ? $"[{detailOrderFk}]" : "OrderId")} = @id";

                using (var acmd = new SqlCommand(aggSql, conn))
                {
                    acmd.Parameters.AddWithValue("@id", orderId);
                    using var ar = await acmd.ExecuteReaderAsync();
                    if (await ar.ReadAsync())
                    {
                        r.Order.DetailCount = ar.IsDBNull(0) ? 0 : ar.GetInt32(0);
                        r.Order.UnprocessedDetails = ar.IsDBNull(1) ? 0 : ar.GetInt32(1);
                        r.Order.ErrorCount = ar.IsDBNull(2) ? 0 : ar.GetInt32(2);
                    }
                }

                var detailSql = $@"
                    SELECT TOP 100
                        {(hasDetailId ? "d.Id" : "NULL")} AS DetailId,
                        {(detailOrderFk != null ? $"d.[{detailOrderFk}]" : "NULL")} AS OrderId,
                        {(hasDetailsDateInsert ? "d.DateInsert" : "NULL")} AS DateInsert,
                        {(hasProcessedCallback ? "d.IsProcessedCallback" : "NULL")} AS IsProcessedCallback,
                        {(supplierColumn != null ? $"CAST(d.[{supplierColumn}] AS NVARCHAR(200))" : "NULL")} AS SupplierInfo,
                        {(roomColumn != null ? $"CAST(d.[{roomColumn}] AS NVARCHAR(200))" : "NULL")} AS RoomInfo,
                        {(hasErrCol ? "d.[Error]" : "NULL")} AS [Error],
                        {(hasErrMsgCol ? "d.[ErrorMessage]" : "NULL")} AS [ErrorMessage]
                    FROM {detailsTable} d
                    WHERE d.{(detailOrderFk != null ? $"[{detailOrderFk}]" : "OrderId")} = @id
                    ORDER BY {(hasDetailsDateInsert ? "d.DateInsert DESC" : hasDetailId ? "d.Id DESC" : detailOrderFk != null ? $"d.[{detailOrderFk}] DESC" : "1")}";

                using var dcmd = new SqlCommand(detailSql, conn);
                dcmd.Parameters.AddWithValue("@id", orderId);
                using var dr = await dcmd.ExecuteReaderAsync();
                while (await dr.ReadAsync())
                {
                    r.RecentDetails.Add(new SalesOrderTraceDetail
                    {
                        DetailId = dr.IsDBNull(0) ? null : dr.GetInt32(0),
                        OrderId = dr.IsDBNull(1) ? orderId : dr.GetInt32(1),
                        DateInsert = dr.IsDBNull(2) ? null : dr.GetDateTime(2),
                        IsProcessedCallback = dr.IsDBNull(3) ? null : dr.GetBoolean(3),
                        SupplierInfo = dr.IsDBNull(4) ? null : dr.GetString(4),
                        RoomInfo = dr.IsDBNull(5) ? null : dr.GetString(5),
                        Error = dr.IsDBNull(6) ? null : dr.GetString(6),
                        ErrorMessage = dr.IsDBNull(7) ? null : dr.GetString(7)
                    });
                }
            }
        }
        catch (Exception ex)
        {
            if (!r.DbConnected) r.DbConnected = false;
            r.Error = ex.Message;
        }

        return r;
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

    private async Task<string?> ResolveExistingTable(SqlConnection conn, string[] tablePatterns)
    {
        foreach (var name in tablePatterns)
        {
            if (!IsAllowedTableName(name)) continue;
            try
            {
                using var probe = new SqlCommand($"SELECT TOP 1 1 FROM {name}", conn);
                probe.CommandTimeout = 3;
                await probe.ExecuteScalarAsync();
                return name;
            }
            catch { }
        }
        return null;
    }

    private static async Task<bool> ColumnExists(SqlConnection conn, string tableName, string columnName)
    {
        var safeTableName = tableName.Replace("'", "''");
        var safeColumn = columnName.Replace("'", "''");
        using var cmd = new SqlCommand($"SELECT CASE WHEN COL_LENGTH('{safeTableName}', '{safeColumn}') IS NULL THEN 0 ELSE 1 END", conn);
        cmd.CommandTimeout = 5;
        return Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0) == 1;
    }

    private static async Task<string?> ResolveFirstExistingColumn(SqlConnection conn, string tableName, string[] candidates)
    {
        foreach (var c in candidates)
        {
            if (await ColumnExists(conn, tableName, c)) return c;
        }

        return null;
    }

    private static List<string> ToOrderRanges(IEnumerable<int> ids)
    {
        var list = ids.Where(x => x > 0).Distinct().OrderBy(x => x).ToList();
        var ranges = new List<string>();
        if (list.Count == 0) return ranges;

        var start = list[0];
        var prev = list[0];
        for (var i = 1; i < list.Count; i++)
        {
            var current = list[i];
            if (current == prev + 1)
            {
                prev = current;
                continue;
            }

            ranges.Add(start == prev ? $"{start}" : $"{start}-{prev}");
            start = current;
            prev = current;
        }

        ranges.Add(start == prev ? $"{start}" : $"{start}-{prev}");
        return ranges;
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
            if (!IsAllowedTableName(tableName)) continue;
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

    // Whitelist of allowed dynamic table names to prevent SQL injection
    private static readonly HashSet<string> AllowedTableNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "[SalesOffice.Details]", "SalesOfficeDetails", "SalesOffice_Details",
        "SalesOfficeOrders", "[SalesOffice.Orders]", "SalesOffice_Orders"
    };

    private static bool IsAllowedTableName(string name) => AllowedTableNames.Contains(name);

    private async Task LoadSalesOfficeDetails(SqlConnection conn, SystemStatus s)
    {
        string[] detailsTablePatterns = ["[SalesOffice.Details]", "SalesOfficeDetails", "SalesOffice_Details"];
        string[] ordersTablePatterns = ["SalesOfficeOrders", "[SalesOffice.Orders]", "SalesOffice_Orders"];

        string? detailsTable = null;
        string? ordersTable = null;

        // Resolve Details table name (validated against whitelist)
        foreach (var name in detailsTablePatterns)
        {
            if (!IsAllowedTableName(name)) continue;
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

        // Resolve Orders table name (validated against whitelist)
        foreach (var name in ordersTablePatterns)
        {
            if (!IsAllowedTableName(name)) continue;
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
            catch (Exception ex) { _logger.LogDebug("Mapping analytics query skipped: {Err}", ex.Message); }

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
            catch (Exception ex) { _logger.LogDebug("Time-to-map query skipped: {Err}", ex.Message); }

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
                catch (Exception ex) { _logger.LogDebug("Retry fallback query skipped: {Err}", ex.Message); }
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
                catch (Exception ex) { _logger.LogDebug("Error column fallback skipped: {Err}", ex.Message); }
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
            catch (Exception ex) { _logger.LogDebug("Mapping detail items query skipped: {Err}", ex.Message); }
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

    private async Task LoadBuyRoomsFunnel(SqlConnection conn, SystemStatus s)
    {
        // PreBook creation rate (last hour)
        using var cmd1 = new SqlCommand(
            "SELECT COUNT(*) FROM MED_PreBook WHERE DateInsert >= DATEADD(HOUR, -1, GETDATE())", conn);
        cmd1.CommandTimeout = 10;
        s.PreBooksLastHour = Convert.ToInt32(await cmd1.ExecuteScalarAsync() ?? 0);

        // Book creation rate (last hour)
        using var cmd2 = new SqlCommand(
            "SELECT COUNT(*) FROM MED_Book WHERE DateInsert >= DATEADD(HOUR, -1, GETDATE())", conn);
        cmd2.CommandTimeout = 10;
        s.BooksLastHour = Convert.ToInt32(await cmd2.ExecuteScalarAsync() ?? 0);

        // Funnel conversion
        s.BuyRoomsFunnelRate = s.PreBooksLastHour > 0
            ? Math.Round((double)s.BooksLastHour / s.PreBooksLastHour * 100, 1)
            : 0;

        // PreBooks without matching Book (started but not completed)
        using var cmd3 = new SqlCommand(@"
            SELECT COUNT(*) FROM MED_PreBook p
            WHERE p.DateInsert >= DATEADD(HOUR, -2, GETDATE())
              AND NOT EXISTS (SELECT 1 FROM MED_Book b WHERE b.PreBookId = p.PreBookId)", conn);
        cmd3.CommandTimeout = 10;
        s.OrphanedPreBooks = Convert.ToInt32(await cmd3.ExecuteScalarAsync() ?? 0);
    }

    // ═══════════════════════════════════════════════════════════════
    //  Monitor Tables (owned by MediciMonitor — safe to write)
    // ═══════════════════════════════════════════════════════════════

    public async Task EnsureMonitorTablesExist()
    {
        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Monitor_CircuitBreakers')
                CREATE TABLE Monitor_CircuitBreakers (
                    BreakerName NVARCHAR(50) PRIMARY KEY,
                    IsOpen BIT NOT NULL DEFAULT 0,
                    Label NVARCHAR(200),
                    Reason NVARCHAR(500),
                    TriggeredBy NVARCHAR(100),
                    OpenedAt DATETIME2,
                    ClosedAt DATETIME2,
                    LastUpdated DATETIME2 DEFAULT GETUTCDATE()
                )", conn);
            cmd.CommandTimeout = 15;
            await cmd.ExecuteNonQueryAsync();

            using var cmd2 = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Monitor_State')
                CREATE TABLE Monitor_State (
                    ServiceName NVARCHAR(100),
                    StateKey NVARCHAR(100),
                    StateJson NVARCHAR(MAX),
                    LastUpdated DATETIME2 DEFAULT GETUTCDATE(),
                    PRIMARY KEY (ServiceName, StateKey)
                )", conn);
            cmd2.CommandTimeout = 15;
            await cmd2.ExecuteNonQueryAsync();

            _logger.LogInformation("Monitor tables ensured (CircuitBreakers, State)");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to ensure Monitor tables: {Err}", ex.Message);
        }
    }

    public async Task SyncBreakerToDb(string name, bool isOpen, string? label, string? reason, string? triggeredBy)
    {
        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(@"
                MERGE Monitor_CircuitBreakers AS target
                USING (SELECT @Name AS BreakerName) AS source
                ON target.BreakerName = source.BreakerName
                WHEN MATCHED THEN
                    UPDATE SET IsOpen = @IsOpen, Label = @Label, Reason = @Reason,
                               TriggeredBy = @TriggeredBy,
                               OpenedAt = CASE WHEN @IsOpen = 1 AND target.IsOpen = 0 THEN GETUTCDATE() ELSE target.OpenedAt END,
                               ClosedAt = CASE WHEN @IsOpen = 0 AND target.IsOpen = 1 THEN GETUTCDATE() ELSE target.ClosedAt END,
                               LastUpdated = GETUTCDATE()
                WHEN NOT MATCHED THEN
                    INSERT (BreakerName, IsOpen, Label, Reason, TriggeredBy, OpenedAt, LastUpdated)
                    VALUES (@Name, @IsOpen, @Label, @Reason, @TriggeredBy,
                            CASE WHEN @IsOpen = 1 THEN GETUTCDATE() ELSE NULL END, GETUTCDATE());", conn);
            cmd.Parameters.AddWithValue("@Name", name);
            cmd.Parameters.AddWithValue("@IsOpen", isOpen);
            cmd.Parameters.AddWithValue("@Label", (object?)label ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Reason", (object?)reason ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@TriggeredBy", (object?)triggeredBy ?? DBNull.Value);
            cmd.CommandTimeout = 10;
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to sync breaker {Name} to DB: {Err}", name, ex.Message);
        }
    }

    public async Task<Dictionary<string, bool>> GetBreakersFromDb()
    {
        var result = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand("SELECT BreakerName, IsOpen FROM Monitor_CircuitBreakers", conn);
            cmd.CommandTimeout = 10;
            using var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
            {
                result[rdr.GetString(0)] = rdr.GetBoolean(1);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to read breakers from DB: {Err}", ex.Message);
        }
        return result;
    }
}
