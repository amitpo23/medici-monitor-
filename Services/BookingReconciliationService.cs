using Microsoft.Data.SqlClient;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Cross-system booking reconciliation — compares bookings across Medici DB,
/// Innstant API, and Hotel.Tools/Zenith (browser). Runs hourly, alerts on mismatches.
/// </summary>
public class BookingReconciliationService
{
    private readonly string _connStr;
    private readonly InnstantApiClient _innstant;
    private readonly BrowserReconciliationService _browser;
    private readonly NotificationService _notification;
    private readonly ILogger<BookingReconciliationService> _logger;

    private readonly List<ReconciliationReport> _history = new();
    private readonly object _lock = new();
    private const int MaxHistory = 100;

    public ReconciliationReport? LastReport { get; private set; }

    public BookingReconciliationService(
        IConfiguration config,
        InnstantApiClient innstant,
        BrowserReconciliationService browser,
        NotificationService notification,
        ILogger<BookingReconciliationService> logger)
    {
        _connStr = config.GetConnectionString("SqlServer")
            ?? throw new InvalidOperationException("Missing SqlServer connection string");
        _innstant = innstant;
        _browser = browser;
        _notification = notification;
        _logger = logger;
    }

    // ── Full Reconciliation Run ──────────────────────────────────

    public async Task<ReconciliationReport> RunReconciliation(int lookbackHours = 24)
    {
        var report = new ReconciliationReport
        {
            StartTime = DateTime.UtcNow,
            LookbackHours = lookbackHours
        };

        _logger.LogInformation("Starting booking reconciliation (lookback: {Hours}h)", lookbackHours);

        try
        {
            // Step 1: Load bookings from Medici DB
            var mediciBookings = await LoadMediciBookings(lookbackHours);
            report.MediciBookingsCount = mediciBookings.Count;

            var mediciReservations = await LoadMediciReservations(lookbackHours);
            report.MediciReservationsCount = mediciReservations.Count;

            var salesOrders = await LoadSalesOrders(lookbackHours);
            report.SalesOrdersCount = salesOrders.Count;

            // Step 2: Verify against Innstant API
            if (_innstant.IsConfigured)
            {
                var bookingIdsToVerify = mediciBookings
                    .Where(b => b.ContentBookingId > 0 && b.Source == 1) // Source 1 = Innstant
                    .Select(b => b.ContentBookingId)
                    .Distinct()
                    .ToList();

                if (bookingIdsToVerify.Any())
                {
                    var innstantResults = await _innstant.VerifyBookings(bookingIdsToVerify);
                    report.InnstantVerifiedCount = innstantResults.Count(r => r.Found);
                    report.InnstantMissingCount = innstantResults.Count(r => !r.Found);

                    // Detect mismatches
                    foreach (var innstant in innstantResults)
                    {
                        var medici = mediciBookings.FirstOrDefault(b => b.ContentBookingId == innstant.ContentBookingId);
                        if (medici == null) continue;

                        if (!innstant.Found)
                        {
                            report.Mismatches.Add(new BookingMismatch
                            {
                                Type = MismatchType.MissingInInnstant,
                                Severity = "Critical",
                                ContentBookingId = innstant.ContentBookingId,
                                MediciHotelId = medici.HotelId,
                                Description = $"הזמנה {innstant.ContentBookingId} קיימת במדיצי אך לא נמצאה ב-Innstant",
                                MediciData = $"Hotel: {medici.HotelId}, Price: {medici.Price:C}, Dates: {medici.StartDate:dd/MM} - {medici.EndDate:dd/MM}",
                                ExternalData = innstant.Error ?? "Not found"
                            });
                            continue;
                        }

                        // Price mismatch check
                        if (innstant.TotalPrice.HasValue && medici.Price > 0)
                        {
                            var priceDiff = Math.Abs(innstant.TotalPrice.Value - medici.Price);
                            var priceDiffPct = medici.Price > 0 ? (priceDiff / medici.Price) * 100 : 0;
                            if (priceDiffPct > 5) // >5% price difference
                            {
                                report.Mismatches.Add(new BookingMismatch
                                {
                                    Type = MismatchType.PriceMismatch,
                                    Severity = priceDiffPct > 20 ? "Critical" : "Warning",
                                    ContentBookingId = innstant.ContentBookingId,
                                    MediciHotelId = medici.HotelId,
                                    Description = $"פער מחיר {priceDiffPct:F1}% בהזמנה {innstant.ContentBookingId}",
                                    MediciData = $"Medici Price: {medici.Price:F2}",
                                    ExternalData = $"Innstant Price: {innstant.TotalPrice:F2} {innstant.Currency}"
                                });
                            }
                        }

                        // Status mismatch check
                        if (innstant.Status != null && innstant.Status.Contains("cancel", StringComparison.OrdinalIgnoreCase)
                            && medici.IsActive)
                        {
                            report.Mismatches.Add(new BookingMismatch
                            {
                                Type = MismatchType.StatusMismatch,
                                Severity = "Critical",
                                ContentBookingId = innstant.ContentBookingId,
                                MediciHotelId = medici.HotelId,
                                Description = $"הזמנה {innstant.ContentBookingId} בוטלה ב-Innstant אך עדיין פעילה במדיצי!",
                                MediciData = $"Medici: Active=true",
                                ExternalData = $"Innstant: Status={innstant.Status}"
                            });
                        }
                    }
                }
            }
            else
            {
                report.InnstantSkipped = true;
                _logger.LogWarning("Innstant API not configured — skipping API verification");
            }

            // Step 3: Cross-reference Medici Bookings vs Reservations
            foreach (var booking in mediciBookings.Where(b => b.IsSold))
            {
                var matchingRes = mediciReservations.FirstOrDefault(r =>
                    r.HotelCode == booking.HotelId.ToString() &&
                    r.DateFrom?.Date == booking.StartDate?.Date &&
                    r.DateTo?.Date == booking.EndDate?.Date);

                if (matchingRes == null && booking.StartDate > DateTime.UtcNow) // Only future bookings
                {
                    report.Mismatches.Add(new BookingMismatch
                    {
                        Type = MismatchType.MissingReservation,
                        Severity = "Warning",
                        ContentBookingId = booking.ContentBookingId,
                        MediciHotelId = booking.HotelId,
                        Description = $"הזמנה נמכרה (IsSold) אך לא נמצאה Reservation תואמת — Hotel {booking.HotelId}, {booking.StartDate:dd/MM}-{booking.EndDate:dd/MM}",
                        MediciData = $"BookId: {booking.PreBookId}, ContentBookingId: {booking.ContentBookingId}",
                        ExternalData = "No matching Med_Reservation found"
                    });
                }
            }

            // Step 4: Check for orphaned reservations (in Zenith but not in Medici)
            foreach (var res in mediciReservations.Where(r => r.ResStatus == "New" || r.ResStatus == "Commit"))
            {
                var matchingBook = mediciBookings.FirstOrDefault(b =>
                    b.HotelId.ToString() == res.HotelCode &&
                    b.StartDate?.Date == res.DateFrom?.Date &&
                    b.EndDate?.Date == res.DateTo?.Date &&
                    b.IsSold);

                if (matchingBook == null)
                {
                    report.Mismatches.Add(new BookingMismatch
                    {
                        Type = MismatchType.OrphanedReservation,
                        Severity = "Warning",
                        ContentBookingId = 0,
                        MediciHotelId = int.TryParse(res.HotelCode, out var hid) ? hid : 0,
                        Description = $"Reservation ב-Zenith ללא הזמנה תואמת במדיצי — {res.HotelCode}, {res.DateFrom:dd/MM}-{res.DateTo:dd/MM}",
                        MediciData = "No matching MED_Book",
                        ExternalData = $"Reservation: {res.UniqueId}, Status: {res.ResStatus}, Amount: {res.Amount}"
                    });
                }
            }

            // Step 5: Browser verification (Hotel.Tools / Innstant B2B portal)
            // Run with timeout to avoid blocking the entire reconciliation
            if (_browser.IsConfigured)
            {
                try
                {
                    using var browserCts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
                    var browserTask = _browser.VerifyViaB2BPortal(lookbackHours);
                    var completed = await Task.WhenAny(browserTask, Task.Delay(TimeSpan.FromSeconds(90)));
                    if (completed == browserTask)
                    {
                        report.BrowserCheckResults = await browserTask;
                        report.BrowserChecked = true;
                    }
                    else
                    {
                        report.BrowserChecked = false;
                        _logger.LogWarning("Browser reconciliation timed out after 90s — skipping");
                    }
                }
                catch (Exception ex)
                {
                    report.BrowserChecked = false;
                    _logger.LogWarning("Browser reconciliation failed: {Err}", ex.Message);
                }
            }
        }
        catch (Exception ex)
        {
            report.Error = ex.Message;
            _logger.LogError("Reconciliation failed: {Err}", ex.Message);
        }

        report.EndTime = DateTime.UtcNow;
        report.DurationMs = (int)(report.EndTime - report.StartTime).TotalMilliseconds;
        report.TotalMismatches = report.Mismatches.Count;
        report.CriticalMismatches = report.Mismatches.Count(m => m.Severity == "Critical");

        LastReport = report;
        lock (_lock)
        {
            _history.Add(report);
            while (_history.Count > MaxHistory) _history.RemoveAt(0);
        }

        // Always send WhatsApp update with full reconciliation summary
        await SendReconciliationUpdate(report);

        _logger.LogInformation("Reconciliation complete: {Total} bookings, {Mismatches} mismatches ({Critical} critical) in {Duration}ms",
            report.MediciBookingsCount, report.TotalMismatches, report.CriticalMismatches, report.DurationMs);

        return report;
    }

    // ── WhatsApp / Notification Update ─────────────────────────────

    private async Task SendReconciliationUpdate(ReconciliationReport report)
    {
        try
        {
            var statusEmoji = report.CriticalMismatches > 0 ? "🔴" : report.TotalMismatches > 0 ? "🟡" : "🟢";
            var statusText = report.CriticalMismatches > 0 ? "נמצאו אי-התאמות קריטיות!"
                : report.TotalMismatches > 0 ? "נמצאו אי-התאמות"
                : "הכל תקין — כל המערכות מסונכרנות";

            var msg = $"{statusEmoji} *סיכום בדיקת התאמה*\n" +
                      $"━━━━━━━━━━━━━━━━━━\n" +
                      $"📅 {report.EndTime:dd/MM/yyyy HH:mm} UTC\n" +
                      $"⏱️ טווח: {report.LookbackHours} שעות | משך: {report.DurationMs}ms\n\n" +
                      $"*מדיצי:* {report.MediciBookingsCount} הזמנות\n" +
                      $"*Zenith:* {report.MediciReservationsCount} reservations\n" +
                      $"*SalesOrders:* {report.SalesOrdersCount}\n";

            if (!report.InnstantSkipped)
                msg += $"*Innstant:* ✅ {report.InnstantVerifiedCount} אומתו | ❌ {report.InnstantMissingCount} חסרים\n";
            else
                msg += $"*Innstant:* ⚠️ API לא מוגדר\n";

            msg += $"*דפדפן:* {(report.BrowserChecked ? "✅ נבדק" : "❌ לא נבדק")}\n\n" +
                   $"*{statusText}*\n" +
                   $"סה\"כ אי-התאמות: *{report.TotalMismatches}*\n" +
                   $"קריטיות: *{report.CriticalMismatches}*";

            // Add top mismatches detail
            if (report.Mismatches.Any())
            {
                msg += "\n\n*פירוט:*\n";
                foreach (var m in report.Mismatches.OrderByDescending(m => m.Severity == "Critical").Take(5))
                {
                    var icon = m.Severity == "Critical" ? "🔴" : "🟡";
                    msg += $"{icon} {m.Description}\n";
                }
                if (report.Mismatches.Count > 5)
                    msg += $"_...ועוד {report.Mismatches.Count - 5} נוספים_";
            }

            var severity = report.CriticalMismatches > 0 ? "Critical" : report.TotalMismatches > 0 ? "Warning" : "Info";
            await _notification.SendAsync("בדיקת התאמת הזמנות", msg, severity, "Reconciliation");
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Failed to send reconciliation update: {Err}", ex.Message);
        }
    }

    // ── DB Queries ───────────────────────────────────────────────

    private async Task<List<MediciBookingRecord>> LoadMediciBookings(int lookbackHours)
    {
        var list = new List<MediciBookingRecord>();
        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();

            using var cmd = new SqlCommand(@"
                SELECT PreBookId, contentBookingID, HotelId, Source, Price, LastPrice,
                       StartDate, EndDate, IsSold, IsActive, CancellationTo, DateInsert
                FROM MED_Book
                WHERE IsActive = 1 AND DateInsert >= DATEADD(HOUR, -@Hours, GETDATE())
                ORDER BY DateInsert DESC", conn);
            cmd.Parameters.AddWithValue("@Hours", lookbackHours);
            cmd.CommandTimeout = 30;

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                list.Add(new MediciBookingRecord
                {
                    PreBookId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                    ContentBookingId = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                    HotelId = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                    Source = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                    Price = reader.IsDBNull(4) ? 0 : reader.GetDecimal(4),
                    LastPrice = reader.IsDBNull(5) ? 0 : reader.GetDecimal(5),
                    StartDate = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                    EndDate = reader.IsDBNull(7) ? null : reader.GetDateTime(7),
                    IsSold = !reader.IsDBNull(8) && reader.GetBoolean(8),
                    IsActive = !reader.IsDBNull(9) && reader.GetBoolean(9),
                    CancellationTo = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                    DateInsert = reader.IsDBNull(11) ? DateTime.MinValue : reader.GetDateTime(11)
                });
            }
        }
        catch (Exception ex) { _logger.LogWarning("LoadMediciBookings error: {Err}", ex.Message); }
        return list;
    }

    private async Task<List<MediciReservationRecord>> LoadMediciReservations(int lookbackHours)
    {
        var list = new List<MediciReservationRecord>();
        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();

            using var cmd = new SqlCommand(@"
                SELECT Id, HotelCode, UniqueId, ResStatus, Datefrom, Dateto,
                       AmountAfterTax, CurrencyCode, IsApproved, DateInsert
                FROM Med_Reservation
                WHERE DateInsert >= DATEADD(HOUR, -@Hours, GETDATE())
                ORDER BY DateInsert DESC", conn);
            cmd.Parameters.AddWithValue("@Hours", lookbackHours);
            cmd.CommandTimeout = 30;

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                list.Add(new MediciReservationRecord
                {
                    Id = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                    HotelCode = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    UniqueId = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    ResStatus = reader.IsDBNull(3) ? "" : reader.GetString(3),
                    DateFrom = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                    DateTo = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                    Amount = reader.IsDBNull(6) ? 0 : reader.GetDecimal(6),
                    Currency = reader.IsDBNull(7) ? "" : reader.GetString(7),
                    IsApproved = !reader.IsDBNull(8) && reader.GetBoolean(8),
                    DateInsert = reader.IsDBNull(9) ? DateTime.MinValue : reader.GetDateTime(9)
                });
            }
        }
        catch (Exception ex) { _logger.LogWarning("LoadMediciReservations error: {Err}", ex.Message); }
        return list;
    }

    private async Task<List<SalesOrderRecord>> LoadSalesOrders(int lookbackHours)
    {
        var list = new List<SalesOrderRecord>();
        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();

            // Try different table name variants
            var tableName = "SalesOfficeOrders";
            try
            {
                using var probe = new SqlCommand($"SELECT TOP 1 1 FROM [{tableName}]", conn);
                probe.CommandTimeout = 5;
                await probe.ExecuteScalarAsync();
            }
            catch
            {
                try { tableName = "[SalesOffice.Orders]"; using var p2 = new SqlCommand($"SELECT TOP 1 1 FROM {tableName}", conn); p2.CommandTimeout = 5; await p2.ExecuteScalarAsync(); }
                catch { tableName = "SalesOffice_Orders"; }
            }

            using var cmd = new SqlCommand($@"
                SELECT Id, WebJobStatus, DateFrom, DateTo, IsActive, DateInsert
                FROM {tableName}
                WHERE DateInsert >= DATEADD(HOUR, -@Hours, GETDATE())
                ORDER BY DateInsert DESC", conn);
            cmd.Parameters.AddWithValue("@Hours", lookbackHours);
            cmd.CommandTimeout = 30;

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                list.Add(new SalesOrderRecord
                {
                    Id = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                    WebJobStatus = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    DateFrom = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                    DateTo = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                    IsActive = !reader.IsDBNull(4) && reader.GetBoolean(4),
                    DateInsert = reader.IsDBNull(5) ? DateTime.MinValue : reader.GetDateTime(5)
                });
            }
        }
        catch (Exception ex) { _logger.LogWarning("LoadSalesOrders error: {Err}", ex.Message); }
        return list;
    }

    // ── History ──────────────────────────────────────────────────

    public List<ReconciliationReport> GetHistory(int last = 20)
    {
        lock (_lock)
        {
            return _history.OrderByDescending(r => r.StartTime).Take(last).ToList();
        }
    }
}

// ── Models ──────────────────────────────────────────────────────

public class ReconciliationReport
{
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public int DurationMs { get; set; }
    public int LookbackHours { get; set; }

    // Counts
    public int MediciBookingsCount { get; set; }
    public int MediciReservationsCount { get; set; }
    public int SalesOrdersCount { get; set; }
    public int InnstantVerifiedCount { get; set; }
    public int InnstantMissingCount { get; set; }
    public bool InnstantSkipped { get; set; }
    public bool BrowserChecked { get; set; }
    public BrowserCheckResult? BrowserCheckResults { get; set; }

    // Mismatches
    public List<BookingMismatch> Mismatches { get; set; } = new();
    public int TotalMismatches { get; set; }
    public int CriticalMismatches { get; set; }

    public string? Error { get; set; }
}

public class BookingMismatch
{
    public MismatchType Type { get; set; }
    public string Severity { get; set; } = "Warning";
    public int ContentBookingId { get; set; }
    public int MediciHotelId { get; set; }
    public string Description { get; set; } = "";
    public string? MediciData { get; set; }
    public string? ExternalData { get; set; }
    public DateTime DetectedAt { get; set; } = DateTime.UtcNow;
}

public enum MismatchType
{
    MissingInInnstant,
    MissingInMedici,
    MissingReservation,
    OrphanedReservation,
    PriceMismatch,
    StatusMismatch,
    DateMismatch,
    BrowserMismatch
}

public class MediciBookingRecord
{
    public int PreBookId { get; set; }
    public int ContentBookingId { get; set; }
    public int HotelId { get; set; }
    public int Source { get; set; }
    public decimal Price { get; set; }
    public decimal LastPrice { get; set; }
    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
    public bool IsSold { get; set; }
    public bool IsActive { get; set; }
    public DateTime? CancellationTo { get; set; }
    public DateTime DateInsert { get; set; }
}

public class MediciReservationRecord
{
    public int Id { get; set; }
    public string HotelCode { get; set; } = "";
    public string UniqueId { get; set; } = "";
    public string ResStatus { get; set; } = "";
    public DateTime? DateFrom { get; set; }
    public DateTime? DateTo { get; set; }
    public decimal Amount { get; set; }
    public string Currency { get; set; } = "";
    public bool IsApproved { get; set; }
    public DateTime DateInsert { get; set; }
}

public class SalesOrderRecord
{
    public int Id { get; set; }
    public string WebJobStatus { get; set; } = "";
    public DateTime? DateFrom { get; set; }
    public DateTime? DateTo { get; set; }
    public bool IsActive { get; set; }
    public DateTime DateInsert { get; set; }
}
