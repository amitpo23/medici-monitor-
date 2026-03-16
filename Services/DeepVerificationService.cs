using Microsoft.Data.SqlClient;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Deep cross-system verification — compares Medici DB data against Innstant API
/// and Zenith/Hotel.Tools data at field level. Detects anomalies, orphans, ghost records,
/// date mismatches, price drift, status conflicts, and missing cancellations.
/// </summary>
public class DeepVerificationService
{
    private readonly string _connStr;
    private readonly InnstantApiClient _innstant;
    private readonly ILogger<DeepVerificationService> _logger;
    private readonly NotificationService _notification;

    private readonly List<VerificationReport> _history = new();
    private readonly object _lock = new();
    public VerificationReport? LastReport { get; private set; }

    public DeepVerificationService(
        IConfiguration config,
        InnstantApiClient innstant,
        NotificationService notification,
        ILogger<DeepVerificationService> logger)
    {
        _connStr = config.GetConnectionString("SqlServer")
            ?? throw new InvalidOperationException("Missing SqlServer connection string");
        _innstant = innstant;
        _notification = notification;
        _logger = logger;
    }

    /// <summary>
    /// Run comprehensive cross-system verification.
    /// Checks every active booking in Medici against Innstant API + Zenith reservations.
    /// </summary>
    public async Task<VerificationReport> RunDeepVerification(int lookbackHours = 48)
    {
        var report = new VerificationReport { StartTime = DateTime.UtcNow, LookbackHours = lookbackHours };
        _logger.LogInformation("Starting deep verification (lookback: {Hours}h)", lookbackHours);

        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();

            // ── 1. Load all active bookings ──
            var bookings = await LoadAllBookings(conn, lookbackHours);
            report.TotalBookings = bookings.Count;

            // ── 2. Load all reservations ──
            var reservations = await LoadAllReservations(conn, lookbackHours);
            report.TotalReservations = reservations.Count;

            // ── 3. Load cancellations ──
            var cancellations = await LoadCancellations(conn, lookbackHours);
            var cancelErrors = await LoadCancelErrors(conn, lookbackHours);
            report.TotalCancellations = cancellations.Count;
            report.TotalCancelErrors = cancelErrors.Count;

            // ── 4. Verify each Innstant booking against API ──
            if (_innstant.IsConfigured)
            {
                var innstantBookings = bookings.Where(b => b.Source == 1 && b.ContentBookingId > 0).ToList();
                report.InnstantBookingsToVerify = innstantBookings.Count;

                foreach (var booking in innstantBookings)
                {
                    try
                    {
                        var apiResult = await _innstant.GetBookingDetails(booking.ContentBookingId);
                        if (apiResult == null || !apiResult.Found)
                        {
                            report.Anomalies.Add(new VerificationAnomaly
                            {
                                Type = AnomalyType.MissingInExternal,
                                Severity = "Critical",
                                System = "Innstant",
                                BookingId = booking.ContentBookingId,
                                PreBookId = booking.PreBookId,
                                HotelId = booking.HotelId,
                                Description = $"הזמנה {booking.ContentBookingId} לא נמצאה ב-Innstant API!",
                                MediciValue = $"Active, Price=${booking.Price:N0}, Hotel={booking.HotelId}",
                                ExternalValue = apiResult?.Error ?? "Not found"
                            });
                            continue;
                        }

                        // ── Field-level comparison ──

                        // Price check
                        if (apiResult.TotalPrice.HasValue && booking.Price > 0)
                        {
                            var diff = Math.Abs(apiResult.TotalPrice.Value - (decimal)booking.Price);
                            var pct = (decimal)booking.Price > 0 ? diff / (decimal)booking.Price * 100 : 0;
                            if (pct > 5)
                                report.Anomalies.Add(new VerificationAnomaly
                                {
                                    Type = AnomalyType.PriceMismatch,
                                    Severity = pct > 20 ? "Critical" : "Warning",
                                    System = "Innstant",
                                    BookingId = booking.ContentBookingId,
                                    PreBookId = booking.PreBookId,
                                    HotelId = booking.HotelId,
                                    Description = $"פער מחיר {pct:F1}% — Medici: ${booking.Price:N0}, Innstant: ${apiResult.TotalPrice:N0}",
                                    MediciValue = $"${booking.Price:N2}",
                                    ExternalValue = $"${apiResult.TotalPrice:N2} {apiResult.Currency}"
                                });
                        }

                        // Status check — cancelled in Innstant but active in Medici
                        if (apiResult.Status != null &&
                            (apiResult.Status.Contains("cancel", StringComparison.OrdinalIgnoreCase) ||
                             apiResult.Status.Contains("refund", StringComparison.OrdinalIgnoreCase)))
                        {
                            if (booking.IsActive)
                                report.Anomalies.Add(new VerificationAnomaly
                                {
                                    Type = AnomalyType.StatusConflict,
                                    Severity = "Critical",
                                    System = "Innstant",
                                    BookingId = booking.ContentBookingId,
                                    PreBookId = booking.PreBookId,
                                    HotelId = booking.HotelId,
                                    Description = $"סטטוס סותר! Medici=Active, Innstant={apiResult.Status}",
                                    MediciValue = "IsActive=true",
                                    ExternalValue = $"Status={apiResult.Status}"
                                });
                        }

                        // Date check
                        if (apiResult.CheckIn != null && booking.StartDate.HasValue)
                        {
                            if (DateTime.TryParse(apiResult.CheckIn, out var apiCheckIn) &&
                                apiCheckIn.Date != booking.StartDate.Value.Date)
                                report.Anomalies.Add(new VerificationAnomaly
                                {
                                    Type = AnomalyType.DateMismatch,
                                    Severity = "Warning",
                                    System = "Innstant",
                                    BookingId = booking.ContentBookingId,
                                    PreBookId = booking.PreBookId,
                                    HotelId = booking.HotelId,
                                    Description = $"תאריך Check-in שונה! Medici: {booking.StartDate:dd/MM}, Innstant: {apiCheckIn:dd/MM}",
                                    MediciValue = booking.StartDate?.ToString("yyyy-MM-dd"),
                                    ExternalValue = apiCheckIn.ToString("yyyy-MM-dd")
                                });
                        }

                        // Confirmation number tracking
                        if (!string.IsNullOrEmpty(apiResult.ConfirmationNumber))
                            report.VerifiedBookings.Add(new VerifiedBooking
                            {
                                ContentBookingId = booking.ContentBookingId,
                                PreBookId = booking.PreBookId,
                                HotelId = booking.HotelId,
                                MediciPrice = booking.Price,
                                InnstantPrice = (double)(apiResult.TotalPrice ?? 0),
                                InnstantStatus = apiResult.Status ?? "unknown",
                                ConfirmationNumber = apiResult.ConfirmationNumber,
                                Verified = true
                            });
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug("Verify booking {Id} error: {Err}", booking.ContentBookingId, ex.Message);
                    }

                    // Rate limit — small delay between API calls
                    await Task.Delay(200);
                }

                report.InnstantVerifiedOk = report.VerifiedBookings.Count;
            }

            // ── 5. Cross-reference: Sold bookings vs Zenith reservations ──
            foreach (var booking in bookings.Where(b => b.IsSold && b.IsActive))
            {
                var matchingRes = reservations.FirstOrDefault(r =>
                    r.HotelCode == booking.HotelId.ToString() &&
                    r.DateFrom?.Date == booking.StartDate?.Date &&
                    r.DateTo?.Date == booking.EndDate?.Date &&
                    (r.ResStatus == "New" || r.ResStatus == "Committed" || r.ResStatus == "Commit"));

                if (matchingRes == null && booking.StartDate > DateTime.UtcNow)
                {
                    report.Anomalies.Add(new VerificationAnomaly
                    {
                        Type = AnomalyType.MissingSale,
                        Severity = "Warning",
                        System = "Zenith",
                        BookingId = booking.ContentBookingId,
                        PreBookId = booking.PreBookId,
                        HotelId = booking.HotelId,
                        Description = $"הזמנה נמכרה אך אין Reservation ב-Zenith — Hotel {booking.HotelId}, {booking.StartDate:dd/MM}-{booking.EndDate:dd/MM}",
                        MediciValue = $"IsSold=true, Price=${booking.Price:N0}",
                        ExternalValue = "No matching reservation"
                    });
                }
                else if (matchingRes != null)
                {
                    // Price comparison between buy and sell
                    var sellPrice = matchingRes.Amount;
                    var buyPrice = booking.Price;
                    if (sellPrice > 0 && buyPrice > 0 && sellPrice < buyPrice)
                    {
                        var loss = buyPrice - sellPrice;
                        report.Anomalies.Add(new VerificationAnomaly
                        {
                            Type = AnomalyType.SellingAtLoss,
                            Severity = loss > 100 ? "Critical" : "Warning",
                            System = "Zenith",
                            BookingId = booking.ContentBookingId,
                            PreBookId = booking.PreBookId,
                            HotelId = booking.HotelId,
                            Description = $"מוכר בהפסד! קנייה: ${buyPrice:N0}, מכירה: ${sellPrice:N0}, הפסד: ${loss:N0}",
                            MediciValue = $"Buy=${buyPrice:N2}",
                            ExternalValue = $"Sell=${sellPrice:N2}"
                        });
                    }
                }
            }

            // ── 6. Orphaned reservations (in Zenith but no matching Medici booking) ──
            foreach (var res in reservations.Where(r => r.ResStatus == "New" || r.ResStatus == "Committed" || r.ResStatus == "Commit"))
            {
                var matchingBook = bookings.FirstOrDefault(b =>
                    b.HotelId.ToString() == res.HotelCode &&
                    b.StartDate?.Date == res.DateFrom?.Date &&
                    b.EndDate?.Date == res.DateTo?.Date &&
                    b.IsSold);

                if (matchingBook == null)
                    report.Anomalies.Add(new VerificationAnomaly
                    {
                        Type = AnomalyType.OrphanedRecord,
                        Severity = "Warning",
                        System = "Zenith",
                        BookingId = 0,
                        HotelId = int.TryParse(res.HotelCode, out var hid) ? hid : 0,
                        Description = $"Reservation יתומה ב-Zenith — {res.HotelCode}, {res.DateFrom:dd/MM}-{res.DateTo:dd/MM}, ${res.Amount:N0}",
                        MediciValue = "No matching booking",
                        ExternalValue = $"Res#{res.UniqueId}, Status={res.ResStatus}"
                    });
            }

            // ── 7. Ghost cancellations (inactive in Medici but no cancel record) ──
            var ghostSql = @"SELECT PreBookId, contentBookingID, HotelId, Price, DateInsert
                FROM MED_Book
                WHERE IsActive = 0 AND IsSold = 1
                  AND CancellationTo >= DATEADD(DAY, -90, GETDATE())
                  AND NOT EXISTS (SELECT 1 FROM MED_CancelBook c WHERE c.PreBookId = MED_Book.PreBookId)
                  AND NOT EXISTS (SELECT 1 FROM MED_CancelBookError e WHERE e.PreBookId = MED_Book.PreBookId)";
            using var ghostCmd = new SqlCommand(ghostSql, conn) { CommandTimeout = 15 };
            using var ghostRdr = await ghostCmd.ExecuteReaderAsync();
            while (await ghostRdr.ReadAsync())
            {
                report.Anomalies.Add(new VerificationAnomaly
                {
                    Type = AnomalyType.GhostCancellation,
                    Severity = "Critical",
                    System = "Medici",
                    BookingId = ghostRdr.IsDBNull(1) ? 0 : ghostRdr.GetInt32(1),
                    PreBookId = ghostRdr.GetInt32(0),
                    HotelId = ghostRdr.IsDBNull(2) ? 0 : ghostRdr.GetInt32(2),
                    Description = $"ביטול רפאי — הזמנה #{ghostRdr.GetInt32(0)} כובתה ב-DB ללא API cancel! ספק עלול לחייב.",
                    MediciValue = $"IsActive=false, IsSold=true, Price=${(ghostRdr.IsDBNull(3) ? 0 : ghostRdr.GetDouble(3)):N0}",
                    ExternalValue = "No cancel record in MED_CancelBook"
                });
            }

            // ── 8. Duplicate bookings (same hotel+dates bought multiple times) ──
            var dupSql = @"SELECT HotelId, StartDate, EndDate, COUNT(*) as Cnt, SUM(Price) as TotalPrice
                FROM MED_Book WHERE IsActive = 1 AND DateInsert >= DATEADD(DAY, -30, GETDATE())
                GROUP BY HotelId, StartDate, EndDate HAVING COUNT(*) >= 2
                ORDER BY SUM(Price) DESC";
            using var dupCmd = new SqlCommand(dupSql, conn) { CommandTimeout = 15 };
            using var dupRdr = await dupCmd.ExecuteReaderAsync();
            while (await dupRdr.ReadAsync())
            {
                report.Anomalies.Add(new VerificationAnomaly
                {
                    Type = AnomalyType.DuplicateBooking,
                    Severity = "Warning",
                    System = "Medici",
                    HotelId = dupRdr.GetInt32(0),
                    Description = $"הזמנה כפולה! Hotel {dupRdr.GetInt32(0)}: {dupRdr.GetInt32(3)} פעמים, סה\"כ ${(dupRdr.IsDBNull(4) ? 0 : dupRdr.GetDouble(4)):N0}",
                    MediciValue = $"{dupRdr.GetInt32(3)} duplicates",
                    ExternalValue = $"${(dupRdr.IsDBNull(4) ? 0 : dupRdr.GetDouble(4)):N0}"
                });
            }

            // ── 9. Expiring soon without sale (waste risk) ──
            var wasteSql = @"SELECT COUNT(*), ISNULL(SUM(Price), 0)
                FROM MED_Book
                WHERE IsActive = 1 AND (IsSold = 0 OR IsSold IS NULL)
                  AND CancellationTo BETWEEN GETDATE() AND DATEADD(HOUR, 24, GETDATE())";
            using var wasteCmd = new SqlCommand(wasteSql, conn) { CommandTimeout = 10 };
            using var wasteRdr = await wasteCmd.ExecuteReaderAsync();
            if (await wasteRdr.ReadAsync())
            {
                var wasteCount = wasteRdr.GetInt32(0);
                var wasteValue = wasteRdr.GetDouble(1);
                if (wasteCount > 0)
                    report.Anomalies.Add(new VerificationAnomaly
                    {
                        Type = AnomalyType.ExpiringUnsold,
                        Severity = wasteValue > 1000 ? "Critical" : "Warning",
                        System = "Medici",
                        Description = $"{wasteCount} חדרים לא נמכרו פגים ב-24 שעות! שווי: ${wasteValue:N0}",
                        MediciValue = $"{wasteCount} rooms, ${wasteValue:N0}",
                        ExternalValue = "Expiring within 24h"
                    });
            }
        }
        catch (Exception ex)
        {
            report.Error = ex.Message;
            _logger.LogError("Deep verification failed: {Err}", ex.Message);
        }

        report.EndTime = DateTime.UtcNow;
        report.DurationMs = (int)(report.EndTime - report.StartTime).TotalMilliseconds;
        report.TotalAnomalies = report.Anomalies.Count;
        report.CriticalAnomalies = report.Anomalies.Count(a => a.Severity == "Critical");

        LastReport = report;
        lock (_lock) { _history.Add(report); if (_history.Count > 50) _history.RemoveAt(0); }

        // Alert
        if (report.CriticalAnomalies > 0)
        {
            var msg = $"🔴 {report.CriticalAnomalies} אנומליות קריטיות מתוך {report.TotalAnomalies} סה\"כ\n\n" +
                      string.Join("\n", report.Anomalies.Where(a => a.Severity == "Critical").Take(5).Select(a => $"• {a.Description}"));
            await _notification.SendAsync("אנומליות בהזמנות!", msg, "Critical", "DeepVerification");
        }

        _logger.LogInformation("Deep verification complete: {Bookings} bookings, {Anomalies} anomalies ({Critical} critical) in {Ms}ms",
            report.TotalBookings, report.TotalAnomalies, report.CriticalAnomalies, report.DurationMs);

        return report;
    }

    // ── DB Queries ──

    private async Task<List<BookingRecord>> LoadAllBookings(SqlConnection conn, int hours)
    {
        var list = new List<BookingRecord>();
        using var cmd = new SqlCommand($@"
            SELECT b.PreBookId, b.contentBookingID, b.HotelId, b.Source, b.Price, b.LastPrice,
                   b.StartDate, b.EndDate, b.IsSold, b.IsActive, b.CancellationTo, b.DateInsert,
                   h.Name
            FROM MED_Book b LEFT JOIN Med_Hotels h ON h.HotelId = b.HotelId
            WHERE b.DateInsert >= DATEADD(HOUR, -@Hours, GETDATE())
            ORDER BY b.DateInsert DESC", conn) { CommandTimeout = 30 };
        cmd.Parameters.AddWithValue("@Hours", hours);
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
            list.Add(new BookingRecord
            {
                PreBookId = rdr.GetInt32(0),
                ContentBookingId = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1),
                HotelId = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2),
                Source = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3),
                Price = rdr.IsDBNull(4) ? 0 : rdr.GetDouble(4),
                LastPrice = rdr.IsDBNull(5) ? 0 : rdr.GetDouble(5),
                StartDate = rdr.IsDBNull(6) ? null : rdr.GetDateTime(6),
                EndDate = rdr.IsDBNull(7) ? null : rdr.GetDateTime(7),
                IsSold = !rdr.IsDBNull(8) && rdr.GetBoolean(8),
                IsActive = !rdr.IsDBNull(9) && rdr.GetBoolean(9),
                CancellationTo = rdr.IsDBNull(10) ? null : rdr.GetDateTime(10),
                DateInsert = rdr.GetDateTime(11),
                HotelName = rdr.IsDBNull(12) ? null : rdr.GetString(12)
            });
        return list;
    }

    private async Task<List<ReservationRecord>> LoadAllReservations(SqlConnection conn, int hours)
    {
        var list = new List<ReservationRecord>();
        using var cmd = new SqlCommand($@"
            SELECT Id, HotelCode, UniqueId, ResStatus, Datefrom, Dateto, AmountAfterTax, CurrencyCode, DateInsert
            FROM Med_Reservation WHERE DateInsert >= DATEADD(HOUR, -@Hours, GETDATE())
            ORDER BY DateInsert DESC", conn) { CommandTimeout = 30 };
        cmd.Parameters.AddWithValue("@Hours", hours);
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
            list.Add(new ReservationRecord
            {
                Id = rdr.GetInt32(0),
                HotelCode = rdr.IsDBNull(1) ? "" : rdr.GetString(1),
                UniqueId = rdr.IsDBNull(2) ? "" : rdr.GetString(2),
                ResStatus = rdr.IsDBNull(3) ? "" : rdr.GetString(3),
                DateFrom = rdr.IsDBNull(4) ? null : rdr.GetDateTime(4),
                DateTo = rdr.IsDBNull(5) ? null : rdr.GetDateTime(5),
                Amount = rdr.IsDBNull(6) ? 0 : rdr.GetDouble(6),
                Currency = rdr.IsDBNull(7) ? "" : rdr.GetString(7),
                DateInsert = rdr.GetDateTime(8)
            });
        return list;
    }

    private async Task<List<int>> LoadCancellations(SqlConnection conn, int hours)
    {
        var list = new List<int>();
        using var cmd = new SqlCommand($"SELECT PreBookId FROM MED_CancelBook WHERE DateInsert >= DATEADD(HOUR, -{hours}, GETDATE())", conn) { CommandTimeout = 10 };
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync()) list.Add(rdr.GetInt32(0));
        return list;
    }

    private async Task<List<int>> LoadCancelErrors(SqlConnection conn, int hours)
    {
        var list = new List<int>();
        using var cmd = new SqlCommand($"SELECT PreBookId FROM MED_CancelBookError WHERE DateInsert >= DATEADD(HOUR, -{hours}, GETDATE())", conn) { CommandTimeout = 10 };
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync()) if (!rdr.IsDBNull(0)) list.Add(rdr.GetInt32(0));
        return list;
    }

    public List<VerificationReport> GetHistory(int last = 20)
    {
        lock (_lock) { return _history.OrderByDescending(r => r.StartTime).Take(last).ToList(); }
    }
}

// ── Models ──

public class VerificationReport
{
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public int DurationMs { get; set; }
    public int LookbackHours { get; set; }
    public int TotalBookings { get; set; }
    public int TotalReservations { get; set; }
    public int TotalCancellations { get; set; }
    public int TotalCancelErrors { get; set; }
    public int InnstantBookingsToVerify { get; set; }
    public int InnstantVerifiedOk { get; set; }
    public int TotalAnomalies { get; set; }
    public int CriticalAnomalies { get; set; }
    public string? Error { get; set; }
    public List<VerificationAnomaly> Anomalies { get; set; } = new();
    public List<VerifiedBooking> VerifiedBookings { get; set; } = new();
}

public class VerificationAnomaly
{
    public AnomalyType Type { get; set; }
    public string Severity { get; set; } = "Warning";
    public string System { get; set; } = "";
    public int BookingId { get; set; }
    public int PreBookId { get; set; }
    public int HotelId { get; set; }
    public string Description { get; set; } = "";
    public string? MediciValue { get; set; }
    public string? ExternalValue { get; set; }
    public DateTime DetectedAt { get; set; } = DateTime.UtcNow;
}

public enum AnomalyType
{
    MissingInExternal,   // Booking in Medici but not in Innstant
    MissingInMedici,     // Booking in Innstant but not in Medici
    PriceMismatch,       // Price differs between systems
    StatusConflict,      // Active in one, cancelled in other
    DateMismatch,        // Check-in/out dates differ
    MissingSale,         // Sold but no Zenith reservation
    OrphanedRecord,      // Zenith reservation with no Medici booking
    GhostCancellation,   // Inactive in DB but no cancel API call
    DuplicateBooking,    // Same hotel+dates bought multiple times
    SellingAtLoss,       // Sell price < buy price
    ExpiringUnsold       // Rooms expiring in 24h without sale
}

public class VerifiedBooking
{
    public int ContentBookingId { get; set; }
    public int PreBookId { get; set; }
    public int HotelId { get; set; }
    public double MediciPrice { get; set; }
    public double InnstantPrice { get; set; }
    public string InnstantStatus { get; set; } = "";
    public string ConfirmationNumber { get; set; } = "";
    public bool Verified { get; set; }
}

public class BookingRecord
{
    public int PreBookId { get; set; }
    public int ContentBookingId { get; set; }
    public int HotelId { get; set; }
    public int Source { get; set; }
    public double Price { get; set; }
    public double LastPrice { get; set; }
    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
    public bool IsSold { get; set; }
    public bool IsActive { get; set; }
    public DateTime? CancellationTo { get; set; }
    public DateTime DateInsert { get; set; }
    public string? HotelName { get; set; }
}

public class ReservationRecord
{
    public int Id { get; set; }
    public string HotelCode { get; set; } = "";
    public string UniqueId { get; set; } = "";
    public string ResStatus { get; set; } = "";
    public DateTime? DateFrom { get; set; }
    public DateTime? DateTo { get; set; }
    public double Amount { get; set; }
    public string Currency { get; set; } = "";
    public DateTime DateInsert { get; set; }
}
