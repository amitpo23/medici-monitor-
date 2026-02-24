using Microsoft.Data.SqlClient;

namespace MediciMonitor.Services;

/// <summary>
/// Business Intelligence analytics by period.
/// Ported from Medici-Control-Panel AdvancedBusinessIntelligenceService — adapted to use SQL auth.
/// </summary>
public class BusinessIntelligenceService
{
    private readonly string _connStr;
    private readonly ILogger<BusinessIntelligenceService> _logger;

    public BusinessIntelligenceService(IConfiguration config, ILogger<BusinessIntelligenceService> logger)
    {
        _connStr = config.GetConnectionString("SqlServer")
            ?? throw new InvalidOperationException("Missing SqlServer connection string");
        _logger = logger;
    }

    // ── Main entry point ─────────────────────────────────────────

    public async Task<object> GetBusinessIntelligence(string period = "today")
    {
        try
        {
            _logger.LogInformation("BI analysis for period: {Period}", period);
            var (start, end) = CalcPeriod(period);
            var metrics = await GatherMetrics(start, end);
            var predictions = GeneratePredictions(metrics);

            return new
            {
                Success = true, Period = period,
                TimeRange = $"{start:yyyy-MM-dd} to {end:yyyy-MM-dd}",
                Metrics = metrics, PredictiveAlerts = predictions,
                Insights = GenerateInsights(metrics),
                Recommendations = GenerateRecommendations(metrics),
                LastAnalyzed = DateTime.Now
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "BI error");
            return new { Success = false, Error = ex.Message };
        }
    }

    // ── Gather Metrics ───────────────────────────────────────────

    private async Task<BIMetrics> GatherMetrics(DateTime start, DateTime end)
    {
        var m = new BIMetrics { Period = DateTime.Now };
        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();

            // Total bookings
            m.TotalBookings = await ScalarInt(conn, @"
                SELECT COUNT(*) FROM MED_Book WHERE DateInsert >= @S AND DateInsert <= @E", start, end);

            // Successful (IsActive=1)
            m.SuccessfulBookings = await ScalarInt(conn, @"
                SELECT COUNT(*) FROM MED_Book WHERE DateInsert >= @S AND DateInsert <= @E AND IsActive = 1", start, end);

            m.FailedBookings = m.TotalBookings - m.SuccessfulBookings;
            if (m.TotalBookings > 0) m.SuccessRate = (decimal)m.SuccessfulBookings / m.TotalBookings * 100;

            // Errors
            m.ErrorsToday = await ScalarInt(conn, @"
                SELECT COUNT(*) FROM MED_BookError WHERE DateInsert >= @S AND DateInsert <= @E", start, end);

            // Cancel errors
            m.CancelErrors = await ScalarInt(conn, @"
                SELECT COUNT(*) FROM MED_CancelBookError WHERE DateInsert >= @S AND DateInsert <= @E", start, end);

            // Revenue from reservations
            m.RevenueThisWeek = await ScalarDouble(conn, @"
                SELECT ISNULL(SUM(AmountAfterTax), 0) FROM Med_Reservation WHERE DateInsert >= @S AND DateInsert <= @E", start, end);

            // Hourly breakdown
            await GatherHourly(conn, start, end, m);

            // Top errors
            await GatherTopErrors(conn, start, end, m);
        }
        catch (Exception ex) { _logger.LogWarning("GatherMetrics partial: {Err}", ex.Message); }
        return m;
    }

    private async Task GatherHourly(SqlConnection conn, DateTime start, DateTime end, BIMetrics m)
    {
        try
        {
            const string sql = @"
                SELECT DATEPART(HOUR, DateInsert) as Hr, COUNT(*) as Cnt,
                       SUM(CASE WHEN IsActive = 0 THEN 1 ELSE 0 END) as Errs
                FROM MED_Book WHERE DateInsert >= @S AND DateInsert <= @E
                GROUP BY DATEPART(HOUR, DateInsert) ORDER BY Hr";
            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@S", start);
            cmd.Parameters.AddWithValue("@E", end);
            cmd.CommandTimeout = 30;
            using var rdr = await cmd.ExecuteReaderAsync();
            int max = 0;
            while (await rdr.ReadAsync())
            {
                int h = rdr.GetInt32(0), c = rdr.GetInt32(1), e = rdr.GetInt32(2);
                m.HourlyBreakdown.Add(new HourlyBI { Hour = h, Bookings = c, Errors = e, SuccessRate = c > 0 ? (decimal)(c - e) / c * 100 : 0 });
                if (c > max) { max = c; m.PeakHour = h; }
            }
        }
        catch { /* swallow */ }
    }

    private async Task GatherTopErrors(SqlConnection conn, DateTime start, DateTime end, BIMetrics m)
    {
        try
        {
            const string sql = @"
                SELECT TOP 10 ISNULL(LEFT([Error], 80), 'Unknown') as ErrText, COUNT(*) as Cnt
                FROM MED_BookError WHERE DateInsert >= @S AND DateInsert <= @E
                GROUP BY ISNULL(LEFT([Error], 80), 'Unknown') ORDER BY Cnt DESC";
            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@S", start);
            cmd.Parameters.AddWithValue("@E", end);
            cmd.CommandTimeout = 30;
            using var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
                m.TopErrors.Add($"{rdr.GetString(0)}: {rdr.GetInt32(1)}x");
        }
        catch { /* swallow */ }
    }

    // ── Predictions ──────────────────────────────────────────────

    private List<PredictiveAlert> GeneratePredictions(BIMetrics m)
    {
        var alerts = new List<PredictiveAlert>();
        if (m.SuccessRate < 70 && m.TotalBookings > 10)
            alerts.Add(new() { Type = "PERFORMANCE_DEGRADATION", Message = $"שיעור הצלחה ירד ל-{m.SuccessRate:F1}%", Confidence = 0.85m, Severity = "High", Recommendation = "ניטור שגיאות ושקול הגדלת משאבים" });
        if (m.TopErrors.Count > 0 && m.FailedBookings > 5)
            alerts.Add(new() { Type = "ERROR_PATTERN", Message = "דפוס שגיאות חוזר מזוהה", Confidence = 0.75m, Severity = "Medium", Recommendation = "בדוק קטגוריות שגיאות מובילות" });
        var peak = new[] { 9, 10, 11, 14, 15, 16 };
        if (peak.Contains(DateTime.Now.Hour) && m.TotalBookings == 0)
            alerts.Add(new() { Type = "LOW_ACTIVITY", Message = $"אין הזמנות בשעת שיא {DateTime.Now.Hour}:00", Confidence = 0.90m, Severity = "Medium", Recommendation = "בדוק בריאות מערכת" });
        return alerts;
    }

    // ── Insights / Recommendations ───────────────────────────────

    private List<string> GenerateInsights(BIMetrics m)
    {
        var i = new List<string>();
        if (m.PeakHour > 0) i.Add($"שעת השיא: {m.PeakHour}:00");
        i.Add(m.SuccessRate switch { > 90 => "שיעור הצלחה מצוין - יציב", > 80 => "שיעור הצלחה טוב", > 70 => "שיעור בינוני - ניטור צמוד", _ => "שיעור נמוך - בדיקה מיידית" });
        if (m.HourlyBreakdown.Any()) { var worst = m.HourlyBreakdown.OrderBy(h => h.SuccessRate).First(); i.Add($"השעה הבעייתית: {worst.Hour}:00 ({worst.SuccessRate:F1}%)"); }
        if (m.RevenueThisWeek > 0) i.Add($"הכנסות בתקופה: ${m.RevenueThisWeek:N0}");
        return i;
    }

    private List<string> GenerateRecommendations(BIMetrics m)
    {
        var r = new List<string>();
        if (m.SuccessRate < 80) r.Add("הגדל ניטור והתראות");
        if (m.FailedBookings > m.SuccessfulBookings * 0.2) r.Add("בדוק שגיאות שכיחות");
        if (m.TotalBookings == 0) r.Add("בדוק חיבוריות");
        if (m.HourlyBreakdown.Any(h => h.SuccessRate < 50)) r.Add("חקור שעות בעייתיות");
        if (!r.Any()) r.Add("המערכת פועלת תקין");
        return r;
    }

    // ── Helpers ───────────────────────────────────────────────────

    private static async Task<int> ScalarInt(SqlConnection conn, string sql, DateTime start, DateTime end)
    {
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@S", start);
        cmd.Parameters.AddWithValue("@E", end);
        cmd.CommandTimeout = 30;
        return Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
    }

    private static async Task<double> ScalarDouble(SqlConnection conn, string sql, DateTime start, DateTime end)
    {
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@S", start);
        cmd.Parameters.AddWithValue("@E", end);
        cmd.CommandTimeout = 30;
        return Convert.ToDouble(await cmd.ExecuteScalarAsync() ?? 0.0);
    }

    private static (DateTime start, DateTime end) CalcPeriod(string p) => p.ToLower() switch
    {
        "today" => (DateTime.Now.Date, DateTime.Now.Date.AddDays(1).AddTicks(-1)),
        "yesterday" => (DateTime.Now.Date.AddDays(-1), DateTime.Now.Date.AddTicks(-1)),
        "week" => (DateTime.Now.Date.AddDays(-7), DateTime.Now.Date.AddDays(1).AddTicks(-1)),
        "month" => (DateTime.Now.Date.AddDays(-30), DateTime.Now.Date.AddDays(1).AddTicks(-1)),
        _ => (DateTime.Now.Date, DateTime.Now.Date.AddDays(1).AddTicks(-1))
    };
}
