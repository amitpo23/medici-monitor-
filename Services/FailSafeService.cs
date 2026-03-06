using Microsoft.Data.SqlClient;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MediciMonitor.Services;

/// <summary>
/// Kill-Switch & Fail-Safe Engine.
/// Monitors business conditions and can freeze/block operations until manual approval.
/// 
/// Rules:
///   1. HIGH_VALUE_BOOKING   — booking > $X executed → hold
///   2. SELL_BELOW_COST      — room sold via Zenith at price lower than buy cost → hold
///   3. MASSIVE_SPEND_SPIKE  — total buy spend in last N hours exceeds threshold → freeze buying
///   4. REVENUE_LOSS_STREAK  — selling at a loss repeatedly → freeze sells
///   5. PRICE_DRIFT_DANGER   — supplier price jumped by > X% → hold booking
///   6. RUNAWAY_QUEUE        — queue growing uncontrollably → freeze queue
///   7. CANCEL_STORM         — mass cancellations in short window → freeze cancels
///   8. DUPLICATE_BOOKING    — same hotel+dates purchased multiple times → hold
/// </summary>
public class FailSafeService
{
    private readonly string _connStr;
    private readonly ILogger<FailSafeService> _logger;
    private readonly NotificationService? _notifications;
    private readonly object _lock = new();

    // ── Circuit Breaker State ──
    private readonly Dictionary<string, CircuitBreaker> _breakers = new()
    {
        ["BUYING"]  = new() { Name = "BUYING",  Label = "רכישת חדרים (BuyRooms)",     IsOpen = false },
        ["SELLING"] = new() { Name = "SELLING", Label = "מכירות (Zenith Reservations)", IsOpen = false },
        ["QUEUE"]   = new() { Name = "QUEUE",   Label = "תור עיבוד (Queue Processing)", IsOpen = false },
        ["PUSH"]    = new() { Name = "PUSH",    Label = "Push לזניט (CRM Push)",       IsOpen = false },
        ["CANCELS"] = new() { Name = "CANCELS", Label = "ביטולים (Cancel Operations)",  IsOpen = false }
    };

    // ── Fail-Safe Rules Configuration ──
    public FailSafeConfig Config { get; set; } = new();

    // ── Flagged Items (pending approval) ──
    private readonly List<FailSafeFlaggedItem> _flaggedItems = new();
    private int _nextFlagId = 1;
    private const int MaxFlaggedItems = 500;

    // ── Trigger History ──
    private readonly List<FailSafeTriggerEvent> _triggerHistory = new();
    private const int MaxTriggerHistory = 1000;

    // ── Notification Cooldown ──
    private readonly Dictionary<string, DateTime> _lastNotifiedPerRule = new();

    // ── Daily Summary Counters ──
    private int _dailyScans;
    private int _dailyCritical;
    private int _dailyWarning;
    private int _dailyTrips;
    private int _dailyFlagged;
    private int _dailyApproved;
    private int _dailyRejected;
    private double _dailyAmountFlagged;
    private readonly Dictionary<string, int> _dailyByRule = new();
    private DateTime _dailyResetDate = DateTime.UtcNow.Date;
    private DateTime _lastDailySummarySent = DateTime.MinValue;

    // ── State Persistence ──
    private readonly string _stateFilePath;
    private DateTime _lastStateSave = DateTime.MinValue;
    private static readonly TimeSpan StateSaveInterval = TimeSpan.FromMinutes(1);

    // ── Last scan cache ──
    private FailSafeScanResult? _lastScan;
    private DateTime _lastScanTime = DateTime.MinValue;

    // ── Scan counter for background ──
    public int ScanCount { get; private set; }
    public DateTime? LastBackgroundScanTime { get; set; }

    public FailSafeService(IConfiguration config, ILogger<FailSafeService> logger, NotificationService? notifications = null)
    {
        var cs = config.GetConnectionString("SqlServer")
            ?? throw new InvalidOperationException("Missing SqlServer connection string");
        if (!cs.Contains("MultipleActiveResultSets", StringComparison.OrdinalIgnoreCase))
            cs += ";MultipleActiveResultSets=true";
        _connStr = cs;
        _logger = logger;
        _notifications = notifications;

        // Load config from appsettings
        var section = config.GetSection("FailSafe");
        if (section.Exists())
            section.Bind(Config);

        // State persistence file
        _stateFilePath = Path.Combine(AppContext.BaseDirectory, "failsafe-state.json");
        LoadPersistedState();
    }

    // ── State Persistence Methods ──

    private void LoadPersistedState()
    {
        try
        {
            if (!File.Exists(_stateFilePath)) return;
            var json = File.ReadAllText(_stateFilePath);
            var state = JsonSerializer.Deserialize<FailSafePersistedState>(json);
            if (state == null) return;

            lock (_lock)
            {
                // Restore breakers
                foreach (var (name, saved) in state.Breakers)
                {
                    if (_breakers.TryGetValue(name, out var breaker))
                    {
                        breaker.IsOpen = saved.IsOpen;
                        breaker.OpenedAt = saved.OpenedAt;
                        breaker.ClosedAt = saved.ClosedAt;
                        breaker.Reason = saved.Reason;
                        breaker.TriggeredBy = saved.TriggeredBy;
                        breaker.ApprovedBy = saved.ApprovedBy;
                    }
                }

                // Restore flagged items
                _flaggedItems.Clear();
                _flaggedItems.AddRange(state.FlaggedItems);
                _nextFlagId = state.NextFlagId > 0 ? state.NextFlagId : _flaggedItems.Count + 1;

                // Restore history
                _triggerHistory.Clear();
                _triggerHistory.AddRange(state.TriggerHistory);
            }

            _logger.LogInformation("Fail-safe state restored: {Breakers} breakers, {Flags} flagged items, {History} history events",
                state.Breakers.Count(b => b.Value.IsOpen), state.FlaggedItems.Count, state.TriggerHistory.Count);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not load fail-safe state: {Err}", ex.Message);
        }
    }

    public void SaveState(bool force = false)
    {
        if (!force && DateTime.UtcNow - _lastStateSave < StateSaveInterval) return;
        try
        {
            FailSafePersistedState state;
            lock (_lock)
            {
                state = new FailSafePersistedState
                {
                    Breakers = _breakers.ToDictionary(kv => kv.Key, kv => kv.Value.Clone()),
                    FlaggedItems = _flaggedItems.ToList(),
                    TriggerHistory = _triggerHistory.TakeLast(200).ToList(),
                    NextFlagId = _nextFlagId,
                    LastSaved = DateTime.UtcNow
                };
            }
            var json = JsonSerializer.Serialize(state, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(_stateFilePath, json);
            _lastStateSave = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not save fail-safe state: {Err}", ex.Message);
        }
    }

    // ── PIN Validation ──

    public bool ValidatePin(string? pin) => !string.IsNullOrEmpty(pin) && pin == Config.OperatorPin;

    // ════════════════════════════════════════════════════════════════
    //  MAIN: Scan all fail-safe rules
    // ════════════════════════════════════════════════════════════════

    public async Task<FailSafeScanResult> ScanAsync()
    {
        var result = new FailSafeScanResult
        {
            Timestamp = DateTime.UtcNow,
            Breakers = GetBreakers(),
            Config = Config
        };

        if (!Config.Enabled)
        {
            result.Status = "DISABLED";
            result.Message = "מערכת Fail-Safe מושבתת";
            result.FlaggedItems = GetFlaggedItems();
            _lastScan = result;
            _lastScanTime = DateTime.UtcNow;
            return result;
        }

        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            result.DbConnected = true;

            // ── Rule 1: HIGH_VALUE_BOOKING ──
            if (Config.HighValueBookingThreshold > 0)
                await Safe(() => CheckHighValueBookings(conn, result));

            // ── Rule 2: SELL_BELOW_COST ──
            if (Config.SellBelowCostEnabled)
                await Safe(() => CheckSellBelowCost(conn, result));

            // ── Rule 3: MASSIVE_SPEND_SPIKE ──
            if (Config.SpendSpikeThreshold > 0)
                await Safe(() => CheckSpendSpike(conn, result));

            // ── Rule 4: REVENUE_LOSS_STREAK ──
            if (Config.RevenueLossStreakEnabled)
                await Safe(() => CheckRevenueLossStreak(conn, result));

            // ── Rule 5: PRICE_DRIFT_DANGER ──
            if (Config.PriceDriftDangerPercent > 0)
                await Safe(() => CheckPriceDriftDanger(conn, result));

            // ── Rule 6: RUNAWAY_QUEUE ──
            if (Config.RunawayQueueThreshold > 0)
                await Safe(() => CheckRunawayQueue(conn, result));

            // ── Rule 7: CANCEL_STORM ──
            if (Config.CancelStormThreshold > 0)
                await Safe(() => CheckCancelStorm(conn, result));

            // ── Rule 8: DUPLICATE_BOOKING ──
            if (Config.DuplicateBookingEnabled)
                await Safe(() => CheckDuplicateBookings(conn, result));

            // Auto-trigger breakers if configured
            if (Config.AutoTriggerBreakers)
                AutoTriggerBreakers(result);

            // Summary
            var criticalCount = result.Violations.Count(v => v.Severity == "Critical");
            var warningCount = result.Violations.Count(v => v.Severity == "Warning");
            var openBreakers = result.Breakers.Count(b => b.IsOpen);

            result.Status = criticalCount > 0 ? "CRITICAL"
                : warningCount > 0 ? "WARNING"
                : openBreakers > 0 ? "BREAKER_OPEN"
                : "OK";

            result.Message = criticalCount > 0
                ? $"🚨 {criticalCount} כללי fail-safe קריטיים הופעלו! {openBreakers} circuit breakers פתוחים"
                : warningCount > 0
                    ? $"⚠️ {warningCount} אזהרות fail-safe. {openBreakers} breakers פתוחים"
                    : openBreakers > 0
                        ? $"🔒 {openBreakers} circuit breakers פתוחים ידנית"
                        : "✅ המערכת פועלת בטווח בטוח";

            // Send notifications for new critical violations (with cooldown)
            if (criticalCount > 0 && _notifications != null)
            {
                var cooldown = TimeSpan.FromMinutes(Config.NotificationCooldownMinutes);
                var critViolations = result.Violations.Where(v => v.Severity == "Critical").ToList();
                var newViolations = new List<FailSafeViolation>();

                foreach (var v in critViolations)
                {
                    if (_lastNotifiedPerRule.TryGetValue(v.RuleId, out var lastNotif) && DateTime.UtcNow - lastNotif < cooldown)
                        continue;  // Still in cooldown
                    newViolations.Add(v);
                    _lastNotifiedPerRule[v.RuleId] = DateTime.UtcNow;
                }

                if (newViolations.Any())
                {
                    var msg = string.Join("\n", newViolations.Select(v => $"• {v.RuleName}: {v.Description}"));
                    _ = _notifications.SendAsync("🚨 Fail-Safe CRITICAL", msg, "Critical", "FailSafe");
                }
            }

            // Update daily counters
            ResetDailyIfNeeded();
            _dailyScans++;
            _dailyCritical += criticalCount;
            _dailyWarning += warningCount;
            foreach (var v in result.Violations)
            {
                if (!_dailyByRule.ContainsKey(v.RuleId)) _dailyByRule[v.RuleId] = 0;
                _dailyByRule[v.RuleId] += v.Count;
            }
        }
        catch (Exception ex)
        {
            result.Status = "ERROR";
            result.Message = $"שגיאת סריקה: {ex.Message}";
            result.Error = ex.Message;
        }

        result.FlaggedItems = GetFlaggedItems();
        _lastScan = result;
        _lastScanTime = DateTime.UtcNow;
        ScanCount++;
        SaveState();
        return result;
    }

    // ════════════════════════════════════════════════════════════════
    //  RULE IMPLEMENTATIONS
    // ════════════════════════════════════════════════════════════════

    /// Rule 1: High-value bookings above threshold
    private async Task CheckHighValueBookings(SqlConnection conn, FailSafeScanResult result)
    {
        var threshold = Config.HighValueBookingThreshold;
        var sql = $@"
            SELECT TOP 20 b.PreBookId, b.Price, b.HotelId, h.Name AS HotelName,
                   b.DateInsert, b.StartDate, b.EndDate, b.IsSold,
                   ISNULL(b.ContentBookingId,'') AS ContentBookingId
            FROM MED_Book b
            LEFT JOIN Med_Hotels h ON h.HotelId = b.HotelId
            WHERE b.IsActive = 1 AND b.Price > {threshold}
              AND b.DateInsert >= DATEADD(DAY, -7, GETDATE())
            ORDER BY b.Price DESC";

        using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 15 };
        using var rdr = await cmd.ExecuteReaderAsync();
        var items = new List<FailSafeViolationDetail>();
        while (await rdr.ReadAsync())
        {
            var preBookId = rdr.GetInt32(0);
            var price = rdr.IsDBNull(1) ? 0.0 : rdr.GetDouble(1);
            var hotelId = rdr.IsDBNull(2) ? (int?)null : rdr.GetInt32(2);
            var hotelName = rdr.IsDBNull(3) ? $"Hotel {hotelId}" : rdr.GetString(3);
            var dateInsert = rdr.IsDBNull(4) ? (DateTime?)null : rdr.GetDateTime(4);
            var isSold = !rdr.IsDBNull(7) && rdr.GetBoolean(7);

            items.Add(new FailSafeViolationDetail
            {
                PreBookId = preBookId,
                HotelId = hotelId,
                HotelName = hotelName,
                BuyPrice = price,
                DateInsert = dateInsert,
                IsSold = isSold,
                Detail = $"הזמנה #{preBookId} — ${price:N0} ({hotelName}){(isSold ? " [נמכר]" : "")}"
            });

            // Auto-flag for approval
            if (Config.AutoFlagHighValue)
                FlagItem("HIGH_VALUE_BOOKING", preBookId, $"הזמנה ${price:N0} מעל סף ${threshold:N0}", price, hotelName);
        }

        if (items.Any())
        {
            result.Violations.Add(new FailSafeViolation
            {
                RuleId = "HIGH_VALUE_BOOKING",
                RuleName = "הזמנה בסכום גבוה",
                Severity = items.Any(i => i.BuyPrice >= threshold * 2) ? "Critical" : "Warning",
                Count = items.Count,
                Description = $"{items.Count} הזמנות מעל ${threshold:N0} (סה\"כ ${items.Sum(i => i.BuyPrice):N0})",
                Details = items,
                SuggestedAction = "PAUSE_BUYING",
                SuggestedBreakerName = "BUYING"
            });
        }
    }

    /// Rule 2: Rooms sold below cost (Zenith sell price < buy price)
    private async Task CheckSellBelowCost(SqlConnection conn, FailSafeScanResult result)
    {
        var minLoss = Config.SellBelowCostMinLoss;
        var sql = $@"
            SELECT TOP 30
                b.PreBookId, b.Price AS BuyPrice, b.HotelId, h.Name AS HotelName,
                r.AmountAfterTax AS SellPrice, r.CurrencyCode,
                r.DateInsert AS SellDate, b.DateInsert AS BuyDate,
                b.StartDate, b.EndDate,
                (b.Price - ISNULL(r.AmountAfterTax, 0)) AS Loss
            FROM MED_Book b
            INNER JOIN Med_Reservation r ON r.HotelCode = CAST(b.HotelId AS NVARCHAR(20))
                AND r.Datefrom = b.StartDate AND r.Dateto = b.EndDate
                AND r.ResStatus = 'Committed'
            LEFT JOIN Med_Hotels h ON h.HotelId = b.HotelId
            WHERE b.IsActive = 1 AND b.IsSold = 1 AND b.Price IS NOT NULL
              AND r.AmountAfterTax IS NOT NULL AND r.AmountAfterTax > 0
              AND b.Price > r.AmountAfterTax + {minLoss}
              AND r.DateInsert >= DATEADD(DAY, -30, GETDATE())
            ORDER BY (b.Price - r.AmountAfterTax) DESC";

        try
        {
            using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 20 };
            using var rdr = await cmd.ExecuteReaderAsync();
            var items = new List<FailSafeViolationDetail>();
            while (await rdr.ReadAsync())
            {
                var preBookId = rdr.GetInt32(0);
                var buyPrice = rdr.IsDBNull(1) ? 0.0 : rdr.GetDouble(1);
                var hotelId = rdr.IsDBNull(2) ? (int?)null : rdr.GetInt32(2);
                var hotelName = rdr.IsDBNull(3) ? $"Hotel {hotelId}" : rdr.GetString(3);
                var sellPrice = rdr.IsDBNull(4) ? 0.0 : rdr.GetDouble(4);
                var loss = rdr.IsDBNull(10) ? 0.0 : rdr.GetDouble(10);

                items.Add(new FailSafeViolationDetail
                {
                    PreBookId = preBookId,
                    HotelId = hotelId,
                    HotelName = hotelName,
                    BuyPrice = buyPrice,
                    SellPrice = sellPrice,
                    Loss = loss,
                    IsSold = true,
                    Detail = $"#{preBookId} — קניה: ${buyPrice:N0}, מכירה: ${sellPrice:N0}, הפסד: ${loss:N0} ({hotelName})"
                });

                if (Config.AutoFlagSellBelowCost)
                    FlagItem("SELL_BELOW_COST", preBookId, $"הפסד ${loss:N0} — קניה ${buyPrice:N0} > מכירה ${sellPrice:N0}", loss, hotelName);
            }

            if (items.Any())
            {
                var totalLoss = items.Sum(i => i.Loss);
                result.Violations.Add(new FailSafeViolation
                {
                    RuleId = "SELL_BELOW_COST",
                    RuleName = "מכירה מתחת למחיר עלות",
                    Severity = totalLoss > Config.SellBelowCostCriticalLoss ? "Critical" : "Warning",
                    Count = items.Count,
                    Description = $"{items.Count} חדרים נמכרים בהפסד! הפסד כולל: ${totalLoss:N0}",
                    Details = items,
                    SuggestedAction = "PAUSE_SELLING",
                    SuggestedBreakerName = "SELLING"
                });
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug("SellBelowCost check skipped: {Err}", ex.Message);
        }
    }

    /// Rule 3: Massive spending spike
    private async Task CheckSpendSpike(SqlConnection conn, FailSafeScanResult result)
    {
        var hours = Config.SpendSpikeWindowHours;
        var threshold = Config.SpendSpikeThreshold;

        var recentSpend = await ScalarDouble(conn,
            $"SELECT ISNULL(SUM(Price), 0) FROM MED_Book WHERE IsActive = 1 AND DateInsert >= DATEADD(HOUR, -{hours}, GETDATE()) AND Price IS NOT NULL");

        var prevSpend = await ScalarDouble(conn,
            $"SELECT ISNULL(SUM(Price), 0) FROM MED_Book WHERE IsActive = 1 AND DateInsert >= DATEADD(HOUR, -{hours * 2}, GETDATE()) AND DateInsert < DATEADD(HOUR, -{hours}, GETDATE()) AND Price IS NOT NULL");

        if (recentSpend > threshold)
        {
            var ratio = prevSpend > 0 ? recentSpend / prevSpend : 99;
            result.Violations.Add(new FailSafeViolation
            {
                RuleId = "SPEND_SPIKE",
                RuleName = "זינוק הוצאות פתאומי",
                Severity = recentSpend > threshold * 2 ? "Critical" : "Warning",
                Count = 1,
                Description = $"הוצאות ב-{hours} שעות אחרונות: ${recentSpend:N0} (סף: ${threshold:N0}). יחס לתקופה קודמת: x{ratio:F1}",
                Details = new List<FailSafeViolationDetail>
                {
                    new() { Detail = $"הוצאות אחרונות: ${recentSpend:N0}", BuyPrice = recentSpend },
                    new() { Detail = $"הוצאות תקופה קודמת: ${prevSpend:N0}", BuyPrice = prevSpend }
                },
                SuggestedAction = "PAUSE_BUYING",
                SuggestedBreakerName = "BUYING"
            });
        }
    }

    /// Rule 4: Revenue loss streak — multiple sells below cost
    private async Task CheckRevenueLossStreak(SqlConnection conn, FailSafeScanResult result)
    {
        var days = Config.RevenueLossStreakDays;
        try
        {
            var lossBookings = await ScalarInt(conn,
                $@"SELECT COUNT(*)
                   FROM MED_Book b
                   INNER JOIN Med_Reservation r ON r.HotelCode = CAST(b.HotelId AS NVARCHAR(20))
                       AND r.Datefrom = b.StartDate AND r.Dateto = b.EndDate
                       AND r.ResStatus = 'Committed'
                   WHERE b.IsActive = 1 AND b.IsSold = 1
                     AND b.Price IS NOT NULL AND r.AmountAfterTax IS NOT NULL
                     AND b.Price > r.AmountAfterTax
                     AND r.DateInsert >= DATEADD(DAY, -{days}, GETDATE())");

            var totalSells = await ScalarInt(conn,
                $@"SELECT COUNT(*)
                   FROM MED_Book b
                   INNER JOIN Med_Reservation r ON r.HotelCode = CAST(b.HotelId AS NVARCHAR(20))
                       AND r.Datefrom = b.StartDate AND r.Dateto = b.EndDate
                       AND r.ResStatus = 'Committed'
                   WHERE b.IsActive = 1 AND b.IsSold = 1
                     AND r.DateInsert >= DATEADD(DAY, -{days}, GETDATE())");

            var lossRate = totalSells > 0 ? (double)lossBookings / totalSells * 100 : 0;

            if (lossBookings >= Config.RevenueLossStreakMinCount && lossRate >= Config.RevenueLossStreakMinPercent)
            {
                result.Violations.Add(new FailSafeViolation
                {
                    RuleId = "REVENUE_LOSS_STREAK",
                    RuleName = "רצף הפסדים במכירות",
                    Severity = lossRate > 50 ? "Critical" : "Warning",
                    Count = lossBookings,
                    Description = $"{lossBookings} מכירות בהפסד מתוך {totalSells} סה\"כ ({lossRate:F1}%) ב-{days} ימים אחרונים",
                    Details = new List<FailSafeViolationDetail>
                    {
                        new() { Detail = $"מכירות בהפסד: {lossBookings}", BuyPrice = lossBookings },
                        new() { Detail = $"סה\"כ מכירות: {totalSells}", BuyPrice = totalSells },
                        new() { Detail = $"אחוז הפסד: {lossRate:F1}%", BuyPrice = lossRate }
                    },
                    SuggestedAction = "PAUSE_SELLING",
                    SuggestedBreakerName = "SELLING"
                });
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug("RevenueLossStreak check skipped: {Err}", ex.Message);
        }
    }

    /// Rule 5: Dangerous price drift (massive supplier price jump)
    private async Task CheckPriceDriftDanger(SqlConnection conn, FailSafeScanResult result)
    {
        var pctThreshold = Config.PriceDriftDangerPercent;
        var sql = $@"
            SELECT TOP 20 PreBookId, HotelId, Price, LastPrice,
                   ABS(LastPrice - Price) AS Drift,
                   CASE WHEN Price > 0 THEN ABS(LastPrice - Price) / Price * 100 ELSE 0 END AS DriftPct,
                   DateLastPrice
            FROM MED_Book
            WHERE IsActive = 1 AND Price IS NOT NULL AND LastPrice IS NOT NULL
              AND Price > 0
              AND ABS(LastPrice - Price) / Price * 100 > {pctThreshold}
            ORDER BY ABS(LastPrice - Price) DESC";

        using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 15 };
        using var rdr = await cmd.ExecuteReaderAsync();
        var items = new List<FailSafeViolationDetail>();
        while (await rdr.ReadAsync())
        {
            var id = rdr.GetInt32(0);
            var hotelId = rdr.IsDBNull(1) ? (int?)null : rdr.GetInt32(1);
            var price = rdr.IsDBNull(2) ? 0.0 : rdr.GetDouble(2);
            var lastPrice = rdr.IsDBNull(3) ? 0.0 : rdr.GetDouble(3);
            var drift = rdr.IsDBNull(4) ? 0.0 : rdr.GetDouble(4);
            var driftPct = rdr.IsDBNull(5) ? 0.0 : rdr.GetDouble(5);

            items.Add(new FailSafeViolationDetail
            {
                PreBookId = id,
                HotelId = hotelId,
                BuyPrice = price,
                SellPrice = lastPrice,
                Loss = drift,
                Detail = $"#{id} — מחיר: ${price:N0} → ${lastPrice:N0} (שינוי {driftPct:F1}%)"
            });
        }

        if (items.Any())
        {
            result.Violations.Add(new FailSafeViolation
            {
                RuleId = "PRICE_DRIFT_DANGER",
                RuleName = "סחיפת מחיר מסוכנת",
                Severity = items.Any(i => i.Loss > Config.PriceDriftDangerAbsolute) ? "Critical" : "Warning",
                Count = items.Count,
                Description = $"{items.Count} חדרים עם שינוי מחיר > {pctThreshold}%",
                Details = items,
                SuggestedAction = "REVIEW",
                SuggestedBreakerName = "BUYING"
            });
        }
    }

    /// Rule 6: Runaway queue (growing uncontrollably)
    private async Task CheckRunawayQueue(SqlConnection conn, FailSafeScanResult result)
    {
        var threshold = Config.RunawayQueueThreshold;
        var pending = await ScalarInt(conn,
            "SELECT COUNT(*) FROM Queue WHERE Status IN ('AddedToQueue', 'Processing')");
        var errorsLastHour = await ScalarInt(conn,
            "SELECT COUNT(*) FROM Queue WHERE Status = 'Error' AND CreatedOn >= DATEADD(HOUR, -1, GETDATE())");

        if (pending > threshold || errorsLastHour > threshold / 2)
        {
            result.Violations.Add(new FailSafeViolation
            {
                RuleId = "RUNAWAY_QUEUE",
                RuleName = "תור חריג",
                Severity = pending > threshold * 2 ? "Critical" : "Warning",
                Count = pending,
                Description = $"{pending} פריטים בתור ({errorsLastHour} שגיאות בשעה האחרונה). סף: {threshold}",
                Details = new List<FailSafeViolationDetail>
                {
                    new() { Detail = $"ממתינים: {pending}" },
                    new() { Detail = $"שגיאות (שעה): {errorsLastHour}" }
                },
                SuggestedAction = "FREEZE_QUEUE",
                SuggestedBreakerName = "QUEUE"
            });
        }
    }

    /// Rule 7: Cancel storm — mass cancellations in short window
    private async Task CheckCancelStorm(SqlConnection conn, FailSafeScanResult result)
    {
        var threshold = Config.CancelStormThreshold;
        var windowHours = Config.CancelStormWindowHours;

        var cancelCount = await ScalarInt(conn,
            $"SELECT COUNT(*) FROM MED_CancelBook WHERE DateInsert >= DATEADD(HOUR, -{windowHours}, GETDATE())");
        var cancelErrors = await ScalarInt(conn,
            $"SELECT COUNT(*) FROM MED_CancelBookError WHERE DateInsert >= DATEADD(HOUR, -{windowHours}, GETDATE())");

        if (cancelCount + cancelErrors > threshold)
        {
            result.Violations.Add(new FailSafeViolation
            {
                RuleId = "CANCEL_STORM",
                RuleName = "סופת ביטולים",
                Severity = cancelCount + cancelErrors > threshold * 2 ? "Critical" : "Warning",
                Count = cancelCount + cancelErrors,
                Description = $"{cancelCount} ביטולים + {cancelErrors} שגיאות ביטול ב-{windowHours} שעות ({cancelCount + cancelErrors} סה\"כ, סף: {threshold})",
                Details = new List<FailSafeViolationDetail>
                {
                    new() { Detail = $"ביטולים מוצלחים: {cancelCount}" },
                    new() { Detail = $"שגיאות ביטול: {cancelErrors}" }
                },
                SuggestedAction = "FREEZE_CANCELS",
                SuggestedBreakerName = "CANCELS"
            });
        }
    }

    /// Rule 8: Duplicate bookings (same hotel+dates)
    private async Task CheckDuplicateBookings(SqlConnection conn, FailSafeScanResult result)
    {
        var sql = @"
            SELECT TOP 10 HotelId, StartDate, EndDate, COUNT(*) AS Cnt, SUM(Price) AS TotalPrice
            FROM MED_Book
            WHERE IsActive = 1 AND Price IS NOT NULL
              AND DateInsert >= DATEADD(DAY, -7, GETDATE())
            GROUP BY HotelId, StartDate, EndDate
            HAVING COUNT(*) >= 2
            ORDER BY SUM(Price) DESC";

        using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 15 };
        using var rdr = await cmd.ExecuteReaderAsync();
        var items = new List<FailSafeViolationDetail>();
        while (await rdr.ReadAsync())
        {
            var hotelId = rdr.IsDBNull(0) ? (int?)null : rdr.GetInt32(0);
            var start = rdr.IsDBNull(1) ? (DateTime?)null : rdr.GetDateTime(1);
            var end = rdr.IsDBNull(2) ? (DateTime?)null : rdr.GetDateTime(2);
            var cnt = rdr.GetInt32(3);
            var total = rdr.IsDBNull(4) ? 0.0 : rdr.GetDouble(4);

            items.Add(new FailSafeViolationDetail
            {
                HotelId = hotelId,
                BuyPrice = total,
                Detail = $"Hotel {hotelId}: {cnt} הזמנות כפולות ({start:dd/MM}–{end:dd/MM}) — ${total:N0}"
            });
        }

        if (items.Any())
        {
            result.Violations.Add(new FailSafeViolation
            {
                RuleId = "DUPLICATE_BOOKING",
                RuleName = "הזמנות כפולות",
                Severity = items.Sum(i => i.BuyPrice) > Config.HighValueBookingThreshold ? "Critical" : "Warning",
                Count = items.Count,
                Description = $"{items.Count} קבוצות הזמנות כפולות (אותו מלון+תאריכים)",
                Details = items,
                SuggestedAction = "REVIEW",
                SuggestedBreakerName = null
            });
        }
    }

    // ════════════════════════════════════════════════════════════════
    //  CIRCUIT BREAKER MANAGEMENT
    // ════════════════════════════════════════════════════════════════

    public List<CircuitBreaker> GetBreakers()
    {
        lock (_lock) { return _breakers.Values.Select(b => b.Clone()).ToList(); }
    }

    public CircuitBreaker? TripBreaker(string name, string reason, string triggeredBy = "Manual")
    {
        lock (_lock)
        {
            if (!_breakers.TryGetValue(name.ToUpper(), out var breaker)) return null;
            breaker.IsOpen = true;
            breaker.OpenedAt = DateTime.UtcNow;
            breaker.Reason = reason;
            breaker.TriggeredBy = triggeredBy;

            RecordTrigger("BREAKER_TRIPPED", name, reason, triggeredBy);
            _logger.LogWarning("Circuit breaker {Name} TRIPPED: {Reason} (by {By})", name, reason, triggeredBy);

            if (_notifications != null)
                _ = _notifications.SendAsync($"🔴 Circuit Breaker: {breaker.Label}", $"Breaker {name} הופעל: {reason}", "Critical", "FailSafe");

            SaveState(true);
            _dailyTrips++;
            return breaker.Clone();
        }
    }

    public CircuitBreaker? ResetBreaker(string name, string approvedBy = "Manual")
    {
        lock (_lock)
        {
            if (!_breakers.TryGetValue(name.ToUpper(), out var breaker)) return null;
            breaker.IsOpen = false;
            breaker.ClosedAt = DateTime.UtcNow;
            breaker.ApprovedBy = approvedBy;

            RecordTrigger("BREAKER_RESET", name, $"Reset by {approvedBy}", approvedBy);
            _logger.LogInformation("Circuit breaker {Name} RESET by {By}", name, approvedBy);

            if (_notifications != null)
                _ = _notifications.SendAsync($"🟢 Circuit Breaker: {breaker.Label}", $"Breaker {name} שוחרר ע\"י {approvedBy}", "Info", "FailSafe");

            SaveState(true);
            return breaker.Clone();
        }
    }

    public bool TripAll(string reason, string triggeredBy = "Manual")
    {
        lock (_lock)
        {
            foreach (var b in _breakers.Values)
            {
                b.IsOpen = true;
                b.OpenedAt = DateTime.UtcNow;
                b.Reason = reason;
                b.TriggeredBy = triggeredBy;
            }
            RecordTrigger("KILL_SWITCH_ALL", "ALL", reason, triggeredBy);
            _logger.LogCritical("🚨 KILL SWITCH — ALL breakers tripped: {Reason}", reason);

            if (_notifications != null)
                _ = _notifications.SendAsync("🚨 KILL SWITCH — כל הפעולות הוקפאו", reason, "Critical", "FailSafe");

            SaveState(true);
            _dailyTrips += _breakers.Count;
            return true;
        }
    }

    public bool ResetAll(string approvedBy = "Manual")
    {
        lock (_lock)
        {
            foreach (var b in _breakers.Values)
            {
                b.IsOpen = false;
                b.ClosedAt = DateTime.UtcNow;
                b.ApprovedBy = approvedBy;
            }
            RecordTrigger("RESET_ALL", "ALL", $"All breakers reset by {approvedBy}", approvedBy);
            _logger.LogInformation("All circuit breakers RESET by {By}", approvedBy);
            SaveState(true);
            return true;
        }
    }

    // ════════════════════════════════════════════════════════════════
    //  FLAGGED ITEMS (pending approval)
    // ════════════════════════════════════════════════════════════════

    public void FlagItem(string ruleId, int? preBookId, string reason, double? amount = null, string? hotelName = null)
    {
        lock (_lock)
        {
            // Don't duplicate
            if (_flaggedItems.Any(f => f.RuleId == ruleId && f.PreBookId == preBookId && f.Status == "Pending"))
                return;

            _flaggedItems.Add(new FailSafeFlaggedItem
            {
                Id = _nextFlagId++,
                RuleId = ruleId,
                PreBookId = preBookId,
                Reason = reason,
                Amount = amount,
                HotelName = hotelName,
                FlaggedAt = DateTime.UtcNow,
                Status = "Pending"
            });

            while (_flaggedItems.Count > MaxFlaggedItems) _flaggedItems.RemoveAt(0);
            _dailyFlagged++;
            _dailyAmountFlagged += amount ?? 0;
        }
    }

    public FailSafeFlaggedItem? ApproveFlag(int flagId, string approvedBy)
    {
        lock (_lock)
        {
            var item = _flaggedItems.FirstOrDefault(f => f.Id == flagId);
            if (item == null) return null;
            item.Status = "Approved";
            item.ReviewedBy = approvedBy;
            item.ReviewedAt = DateTime.UtcNow;
            RecordTrigger("ITEM_APPROVED", item.RuleId, $"Flag #{flagId} approved: {item.Reason}", approvedBy);
            SaveState(true);
            _dailyApproved++;
            return item;
        }
    }

    public FailSafeFlaggedItem? RejectFlag(int flagId, string rejectedBy, string? note = null)
    {
        lock (_lock)
        {
            var item = _flaggedItems.FirstOrDefault(f => f.Id == flagId);
            if (item == null) return null;
            item.Status = "Rejected";
            item.ReviewedBy = rejectedBy;
            item.ReviewedAt = DateTime.UtcNow;
            item.ReviewNote = note;
            RecordTrigger("ITEM_REJECTED", item.RuleId, $"Flag #{flagId} rejected: {item.Reason} — {note}", rejectedBy);
            SaveState(true);
            _dailyRejected++;
            return item;
        }
    }

    public List<FailSafeFlaggedItem> GetFlaggedItems(string? status = null)
    {
        lock (_lock)
        {
            var q = _flaggedItems.AsEnumerable();
            if (!string.IsNullOrEmpty(status))
                q = q.Where(f => f.Status.Equals(status, StringComparison.OrdinalIgnoreCase));
            return q.OrderByDescending(f => f.FlaggedAt).ToList();
        }
    }

    public int GetPendingCount()
    {
        lock (_lock) { return _flaggedItems.Count(f => f.Status == "Pending"); }
    }

    // ════════════════════════════════════════════════════════════════
    //  AUTO-TRIGGER LOGIC
    // ════════════════════════════════════════════════════════════════

    private void AutoTriggerBreakers(FailSafeScanResult result)
    {
        foreach (var v in result.Violations.Where(v => v.Severity == "Critical" && v.SuggestedBreakerName != null))
        {
            lock (_lock)
            {
                if (_breakers.TryGetValue(v.SuggestedBreakerName!, out var breaker) && !breaker.IsOpen)
                {
                    breaker.IsOpen = true;
                    breaker.OpenedAt = DateTime.UtcNow;
                    breaker.Reason = $"Auto-trigger: {v.RuleName} — {v.Description}";
                    breaker.TriggeredBy = "AutoTrigger";
                    RecordTrigger("AUTO_BREAKER_TRIP", v.SuggestedBreakerName!, breaker.Reason, "AutoTrigger");
                    _logger.LogWarning("Auto-triggered breaker {Name}: {Reason}", v.SuggestedBreakerName, breaker.Reason);
                }
            }
        }

        // Update result breaker state
        result.Breakers = GetBreakers();
    }

    // ════════════════════════════════════════════════════════════════
    //  HISTORY & HELPERS
    // ════════════════════════════════════════════════════════════════

    public List<FailSafeTriggerEvent> GetTriggerHistory(int last = 100)
    {
        lock (_lock)
        {
            return _triggerHistory.OrderByDescending(t => t.Timestamp).Take(last).ToList();
        }
    }

    public FailSafeScanResult? GetLastScan() => _lastScan;

    private void RecordTrigger(string action, string target, string detail, string actor)
    {
        lock (_lock)
        {
            _triggerHistory.Add(new FailSafeTriggerEvent
            {
                Timestamp = DateTime.UtcNow,
                Action = action,
                Target = target,
                Detail = detail,
                Actor = actor
            });
            while (_triggerHistory.Count > MaxTriggerHistory) _triggerHistory.RemoveAt(0);
        }
    }

    private static async Task Safe(Func<Task> action)
    {
        try { await action(); } catch { /* swallow — individual rule failure shouldn't kill scan */ }
    }

    private static async Task<int> ScalarInt(SqlConnection conn, string sql)
    {
        using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 15 };
        return Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
    }

    private static async Task<double> ScalarDouble(SqlConnection conn, string sql)
    {
        using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 15 };
        return Convert.ToDouble(await cmd.ExecuteScalarAsync() ?? 0.0);
    }

    // ════════════════════════════════════════════════════════════════
    //  DAILY SUMMARY
    // ════════════════════════════════════════════════════════════════

    private void ResetDailyIfNeeded()
    {
        var today = DateTime.UtcNow.Date;
        if (_dailyResetDate < today)
        {
            _dailyScans = 0; _dailyCritical = 0; _dailyWarning = 0;
            _dailyTrips = 0; _dailyFlagged = 0; _dailyApproved = 0;
            _dailyRejected = 0; _dailyAmountFlagged = 0;
            _dailyByRule.Clear();
            _dailyResetDate = today;
        }
    }

    public FailSafeDailySummary GetDailySummary()
    {
        ResetDailyIfNeeded();
        return new FailSafeDailySummary
        {
            Date = _dailyResetDate,
            TotalScans = _dailyScans,
            CriticalViolations = _dailyCritical,
            WarningViolations = _dailyWarning,
            BreakerTrips = _dailyTrips,
            ItemsFlagged = _dailyFlagged,
            ItemsApproved = _dailyApproved,
            ItemsRejected = _dailyRejected,
            TotalAmountFlagged = _dailyAmountFlagged,
            ViolationsByRule = new Dictionary<string, int>(_dailyByRule)
        };
    }

    public async Task SendDailySummaryIfDueAsync()
    {
        if (!Config.DailySummaryEnabled || _notifications == null) return;
        var now = DateTime.UtcNow;
        if (now.Hour != Config.DailySummaryHourUtc) return;
        if (_lastDailySummarySent.Date == now.Date) return;  // Already sent today

        var summary = GetDailySummary();
        var sb = new StringBuilder();
        sb.AppendLine($"📊 סיכום יומי Fail-Safe — {summary.Date:dd/MM/yyyy}");
        sb.AppendLine($"סריקות: {summary.TotalScans}");
        sb.AppendLine($"הפרות קריטיות: {summary.CriticalViolations} | אזהרות: {summary.WarningViolations}");
        sb.AppendLine($"Breaker trips: {summary.BreakerTrips}");
        sb.AppendLine($"פריטים מסומנים: {summary.ItemsFlagged} (${summary.TotalAmountFlagged:N0})");
        sb.AppendLine($"אושרו: {summary.ItemsApproved} | נדחו: {summary.ItemsRejected}");
        if (summary.ViolationsByRule.Any())
        {
            sb.AppendLine("── הפרות לפי כלל ──");
            foreach (var kv in summary.ViolationsByRule.OrderByDescending(x => x.Value))
                sb.AppendLine($"  {kv.Key}: {kv.Value}");
        }

        await _notifications.SendAsync("📊 Fail-Safe Daily Summary", sb.ToString(), "Info", "FailSafe");
        _lastDailySummarySent = now;
        _logger.LogInformation("Daily fail-safe summary sent");
    }
}

// ════════════════════════════════════════════════════════════════════════
//  MODELS
// ════════════════════════════════════════════════════════════════════════

public class FailSafeConfig
{
    public bool Enabled { get; set; } = true;
    public bool AutoTriggerBreakers { get; set; } = false; // Manual by default for safety

    // Background scan settings
    public int ScanIntervalSeconds { get; set; } = 300;  // 5 minutes
    public bool ScanOnStartup { get; set; } = true;

    // Notification cooldown (prevent spam)
    public int NotificationCooldownMinutes { get; set; } = 60;  // 1 hour cooldown per rule
    public bool DailySummaryEnabled { get; set; } = true;
    public int DailySummaryHourUtc { get; set; } = 6;  // 6 AM UTC = ~9 AM Israel

    // PIN protection for critical operations
    [JsonIgnore]
    public string OperatorPin { get; set; } = "7743";  // Default PIN, change in config

    // Rule 1: High value booking
    public double HighValueBookingThreshold { get; set; } = 3000;
    public bool AutoFlagHighValue { get; set; } = true;

    // Rule 2: Sell below cost
    public bool SellBelowCostEnabled { get; set; } = true;
    public double SellBelowCostMinLoss { get; set; } = 50;        // Min $ loss to flag
    public double SellBelowCostCriticalLoss { get; set; } = 5000;  // Total loss for Critical

    // Rule 3: Spend spike
    public double SpendSpikeThreshold { get; set; } = 50000;  // $ in window
    public int SpendSpikeWindowHours { get; set; } = 6;
    public bool AutoFlagSellBelowCost { get; set; } = true;

    // Rule 4: Revenue loss streak
    public bool RevenueLossStreakEnabled { get; set; } = true;
    public int RevenueLossStreakDays { get; set; } = 7;
    public int RevenueLossStreakMinCount { get; set; } = 5;
    public double RevenueLossStreakMinPercent { get; set; } = 30;

    // Rule 5: Price drift danger
    public double PriceDriftDangerPercent { get; set; } = 30;
    public double PriceDriftDangerAbsolute { get; set; } = 500;

    // Rule 6: Runaway queue
    public int RunawayQueueThreshold { get; set; } = 200;

    // Rule 7: Cancel storm
    public int CancelStormThreshold { get; set; } = 50;
    public int CancelStormWindowHours { get; set; } = 2;

    // Rule 8: Duplicate booking
    public bool DuplicateBookingEnabled { get; set; } = true;
}

public class FailSafeScanResult
{
    public DateTime Timestamp { get; set; }
    public bool DbConnected { get; set; }
    public string Status { get; set; } = "OK";  // OK, WARNING, CRITICAL, BREAKER_OPEN, ERROR, DISABLED
    public string? Message { get; set; }
    public string? Error { get; set; }
    public FailSafeConfig Config { get; set; } = new();
    public List<CircuitBreaker> Breakers { get; set; } = new();
    public List<FailSafeViolation> Violations { get; set; } = new();
    public List<FailSafeFlaggedItem> FlaggedItems { get; set; } = new();
}

public class CircuitBreaker
{
    public string Name { get; set; } = "";
    public string Label { get; set; } = "";
    public bool IsOpen { get; set; }
    public DateTime? OpenedAt { get; set; }
    public DateTime? ClosedAt { get; set; }
    public string? Reason { get; set; }
    public string? TriggeredBy { get; set; }
    public string? ApprovedBy { get; set; }

    public CircuitBreaker Clone() => new()
    {
        Name = Name, Label = Label, IsOpen = IsOpen, OpenedAt = OpenedAt,
        ClosedAt = ClosedAt, Reason = Reason, TriggeredBy = TriggeredBy, ApprovedBy = ApprovedBy
    };
}

public class FailSafeViolation
{
    public string RuleId { get; set; } = "";
    public string RuleName { get; set; } = "";
    public string Severity { get; set; } = "Warning";
    public int Count { get; set; }
    public string Description { get; set; } = "";
    public List<FailSafeViolationDetail> Details { get; set; } = new();
    public string? SuggestedAction { get; set; }
    public string? SuggestedBreakerName { get; set; }
}

public class FailSafeViolationDetail
{
    public int? PreBookId { get; set; }
    public int? HotelId { get; set; }
    public string? HotelName { get; set; }
    public double BuyPrice { get; set; }
    public double SellPrice { get; set; }
    public double Loss { get; set; }
    public bool IsSold { get; set; }
    public DateTime? DateInsert { get; set; }
    public string Detail { get; set; } = "";
}

public class FailSafeFlaggedItem
{
    public int Id { get; set; }
    public string RuleId { get; set; } = "";
    public int? PreBookId { get; set; }
    public string Reason { get; set; } = "";
    public double? Amount { get; set; }
    public string? HotelName { get; set; }
    public DateTime FlaggedAt { get; set; }
    public string Status { get; set; } = "Pending";  // Pending, Approved, Rejected
    public string? ReviewedBy { get; set; }
    public DateTime? ReviewedAt { get; set; }
    public string? ReviewNote { get; set; }
}

public class FailSafeTriggerEvent
{
    public DateTime Timestamp { get; set; }
    public string Action { get; set; } = "";
    public string Target { get; set; } = "";
    public string Detail { get; set; } = "";
    public string Actor { get; set; } = "";
}

public class FailSafeDailySummary
{
    public DateTime Date { get; set; }
    public int TotalScans { get; set; }
    public int CriticalViolations { get; set; }
    public int WarningViolations { get; set; }
    public int BreakerTrips { get; set; }
    public int ItemsFlagged { get; set; }
    public int ItemsApproved { get; set; }
    public int ItemsRejected { get; set; }
    public double TotalAmountFlagged { get; set; }
    public Dictionary<string, int> ViolationsByRule { get; set; } = new();
}

public class FailSafePersistedState
{
    public Dictionary<string, CircuitBreaker> Breakers { get; set; } = new();
    public List<FailSafeFlaggedItem> FlaggedItems { get; set; } = new();
    public List<FailSafeTriggerEvent> TriggerHistory { get; set; } = new();
    public int NextFlagId { get; set; } = 1;
    public DateTime LastSaved { get; set; }
}
