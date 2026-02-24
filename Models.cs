namespace MediciMonitor;

// ════════════════════════════════════════════════════════════════════════
//  Top-level Dashboard Response
// ════════════════════════════════════════════════════════════════════════
public class SystemStatus
{
    // ── Metadata ──
    public DateTime Timestamp { get; set; }
    public bool DbConnected { get; set; }
    public string? Error { get; set; }

    // ── 1. Bookings KPI ──
    public int TotalActiveBookings { get; set; }
    public int StuckCancellations { get; set; }
    public int UpcomingCancellations { get; set; }
    public int FutureBookings { get; set; }
    public int RoomsBoughtToday { get; set; }
    public int ActiveOpportunities { get; set; }

    // ── 2. Errors KPI ──
    public int BookingErrorsLast24h { get; set; }
    public int CancelErrorsLast24h { get; set; }
    public int CancelSuccessLast24h { get; set; }
    public int BackOfficeErrorsLast24h { get; set; }

    // ── 3. Push & Queue ──
    public int ActivePushOperations { get; set; }
    public int FailedPushOperations { get; set; }
    public int QueuePending { get; set; }
    public int QueueErrors { get; set; }

    // ── 4. SalesOffice ──
    public int SalesOfficePending { get; set; }
    public int SalesOfficeInProgress { get; set; }
    public int SalesOfficeCompleted { get; set; }
    public int SalesOfficeFailed { get; set; }

    // ── 5. NEW: Reservations (Zenith cockpit) ──
    public int ReservationsToday { get; set; }
    public int ReservationsThisWeek { get; set; }
    public int ReservationCancelsThisWeek { get; set; }
    public int ReservationModifiesThisWeek { get; set; }
    public double ReservationRevenueThisWeek { get; set; }

    // ── 6. NEW: Room Waste ──
    public int WasteRoomsTotal { get; set; }
    public double WasteTotalValue { get; set; }
    public int WasteExpiring24h { get; set; }
    public int WasteExpiring48h { get; set; }

    // ── 7. NEW: Conversion & Revenue ──
    public int TotalBought { get; set; }
    public int TotalSold { get; set; }
    public double ConversionRate { get; set; }
    public double TotalBoughtValue { get; set; }
    public double TotalSoldValue { get; set; }
    public double ProfitLoss { get; set; }
    public int BoughtToday { get; set; }
    public int SoldToday { get; set; }
    public double BoughtTodayValue { get; set; }
    public double SoldTodayValue { get; set; }

    // ── 8. NEW: Price Drift ──
    public int PriceDriftCount { get; set; }
    public double PriceDriftTotalImpact { get; set; }

    // ── 9. NEW: BuyRooms Heartbeat ──
    public DateTime? LastBookPurchaseTime { get; set; }
    public DateTime? LastPreBookTime { get; set; }
    public int MinutesSinceLastPurchase { get; set; }
    public bool BuyRoomsHealthy { get; set; }

    // ── Detailed Lists ──
    public List<StuckBookingInfo> StuckBookings { get; set; } = new();
    public List<CancelErrorInfo> RecentCancelErrors { get; set; } = new();
    public List<BookingErrorInfo> RecentBookingErrors { get; set; } = new();
    public List<PushFailureInfo> FailedPushItems { get; set; } = new();
    public List<QueueErrorInfo> QueueErrorItems { get; set; } = new();
    public List<BackOfficeErrorInfo> RecentBackOfficeErrors { get; set; } = new();
    public List<SalesOfficeOrderInfo> SalesOfficeStuckOrders { get; set; } = new();
    public List<ActiveBookingSummary> ActiveBookingsByHotel { get; set; } = new();
    public List<ReservationInfo> RecentReservations { get; set; } = new();
    public List<RoomWasteInfo> WasteRooms { get; set; } = new();
    public List<PriceDriftInfo> PriceDrifts { get; set; } = new();
    public List<ConversionByHotelInfo> ConversionByHotel { get; set; } = new();
}

// ════════════════════════════════════════════════════════════════════════
//  Existing detail models
// ════════════════════════════════════════════════════════════════════════

public class StuckBookingInfo
{
    public int PreBookId { get; set; }
    public string? ContentBookingId { get; set; }
    public DateTime? CancellationTo { get; set; }
    public int? Source { get; set; }
    public string SourceName { get; set; } = "";
    public int? HotelId { get; set; }
    public double? Price { get; set; }
    public int DaysStuck { get; set; }
    public int ErrorCount { get; set; }
    public string? LastError { get; set; }
}

public class CancelErrorInfo
{
    public int? PreBookId { get; set; }
    public string? ContentBookingId { get; set; }
    public string? Error { get; set; }
    public DateTime? DateInsert { get; set; }
}

public class BookingErrorInfo
{
    public int? PreBookId { get; set; }
    public string? Error { get; set; }
    public string? Code { get; set; }
    public DateTime? DateInsert { get; set; }
}

public class PushFailureInfo
{
    public int Id { get; set; }
    public int? BookId { get; set; }
    public int? OpportunityId { get; set; }
    public string? Error { get; set; }
    public DateTime? DateInsert { get; set; }
}

public class QueueErrorInfo
{
    public int Id { get; set; }
    public string? Status { get; set; }
    public string? Message { get; set; }
    public string? HotelName { get; set; }
    public DateTime? CreatedOn { get; set; }
}

public class BackOfficeErrorInfo
{
    public int? BackOfficeOptId { get; set; }
    public string? ErrorLog { get; set; }
    public DateTime? DateCreate { get; set; }
}

public class SalesOfficeOrderInfo
{
    public int Id { get; set; }
    public string? WebJobStatus { get; set; }
    public DateTime? DateFrom { get; set; }
    public DateTime? DateTo { get; set; }
    public bool IsActive { get; set; }
}

public class ActiveBookingSummary
{
    public int? HotelId { get; set; }
    public string? HotelName { get; set; }
    public int ActiveCount { get; set; }
    public int StuckCount { get; set; }
    public int SoldCount { get; set; }
}

// ════════════════════════════════════════════════════════════════════════
//  NEW: Reservations (Zenith cockpit sales)
// ════════════════════════════════════════════════════════════════════════

public class ReservationInfo
{
    public int Id { get; set; }
    public string? HotelCode { get; set; }
    public string? UniqueId { get; set; }
    public string? ResStatus { get; set; }
    public DateTime? DateFrom { get; set; }
    public DateTime? DateTo { get; set; }
    public double? AmountAfterTax { get; set; }
    public string? CurrencyCode { get; set; }
    public string? RoomTypeCode { get; set; }
    public string? RatePlanCode { get; set; }
    public bool? IsApproved { get; set; }
    public DateTime? DateInsert { get; set; }
    public string? Type { get; set; } // "New" | "Cancel" | "Modify"
}

// ════════════════════════════════════════════════════════════════════════
//  NEW: Room Waste (unsold rooms approaching cancellation)
// ════════════════════════════════════════════════════════════════════════

public class RoomWasteInfo
{
    public int PreBookId { get; set; }
    public string? ContentBookingId { get; set; }
    public int? HotelId { get; set; }
    public string? HotelName { get; set; }
    public double? Price { get; set; }
    public DateTime? CancellationTo { get; set; }
    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
    public int HoursUntilExpiry { get; set; }
    public string? Source { get; set; }
}

// ════════════════════════════════════════════════════════════════════════
//  NEW: Price Drift (supplier price changes)
// ════════════════════════════════════════════════════════════════════════

public class PriceDriftInfo
{
    public int PreBookId { get; set; }
    public int? HotelId { get; set; }
    public string? HotelName { get; set; }
    public double? OriginalPrice { get; set; }
    public double? LastPrice { get; set; }
    public double DriftAmount { get; set; }
    public double DriftPercent { get; set; }
    public DateTime? DateLastPrice { get; set; }
}

// ════════════════════════════════════════════════════════════════════════
//  NEW: Conversion by Hotel
// ════════════════════════════════════════════════════════════════════════

public class ConversionByHotelInfo
{
    public int? HotelId { get; set; }
    public string? HotelName { get; set; }
    public int Bought { get; set; }
    public int Sold { get; set; }
    public double BoughtValue { get; set; }
    public double SoldValue { get; set; }
    public double Conversion { get; set; }
}

// ════════════════════════════════════════════════════════════════════════
//  Azure Monitoring Models
// ════════════════════════════════════════════════════════════════════════

public class ApiHealthStatus
{
    public string Endpoint { get; set; } = "";
    public int ResponseTimeMs { get; set; }
    public int StatusCode { get; set; }
    public DateTime LastChecked { get; set; }
    public bool IsHealthy { get; set; }
    public string ErrorMessage { get; set; } = "";
}

public class AzureResourceStatus
{
    public string ResourceName { get; set; } = "";
    public string ResourceType { get; set; } = "";
    public string Status { get; set; } = "";
    public string Location { get; set; } = "";
    public DateTime LastUpdated { get; set; }
}

public class AzureAlert
{
    public DateTime Timestamp { get; set; }
    public string Level { get; set; } = "";
    public string Message { get; set; } = "";
    public string Source { get; set; } = "";
    public string ResourceId { get; set; } = "";
}

public class AzurePerformanceMetrics
{
    public DateTime Timestamp { get; set; }
    public double CpuUsage { get; set; }
    public double MemoryUsage { get; set; }
    public int RequestCount { get; set; }
    public double AvgResponseTime { get; set; }
}

// ════════════════════════════════════════════════════════════════════════
//  Business Intelligence Models
// ════════════════════════════════════════════════════════════════════════

public class BIMetrics
{
    public DateTime Period { get; set; }
    public int TotalBookings { get; set; }
    public int SuccessfulBookings { get; set; }
    public int FailedBookings { get; set; }
    public decimal SuccessRate { get; set; }
    public int PeakHour { get; set; }
    public int ErrorsToday { get; set; }
    public int CancelErrors { get; set; }
    public double RevenueThisWeek { get; set; }
    public List<HourlyBI> HourlyBreakdown { get; set; } = new();
    public List<string> TopErrors { get; set; } = new();
}

public class HourlyBI
{
    public int Hour { get; set; }
    public int Bookings { get; set; }
    public int Errors { get; set; }
    public decimal SuccessRate { get; set; }
}

public class PredictiveAlert
{
    public string Type { get; set; } = "";
    public string Message { get; set; } = "";
    public decimal Confidence { get; set; }
    public string Severity { get; set; } = "";
    public string Recommendation { get; set; } = "";
}

// ════════════════════════════════════════════════════════════════════════
//  Emergency Response Models
// ════════════════════════════════════════════════════════════════════════

public class EmergencyStatus
{
    public DateTime Timestamp { get; set; }
    public string CurrentStatus { get; set; } = "";
    public bool IsEmergency { get; set; }
    public int SeverityLevel { get; set; }
    public List<string> CriticalIssues { get; set; } = new();
    public List<EmergencyAction> SuggestedActions { get; set; } = new();
    public List<ApiHealthStatus> ApiHealth { get; set; } = new();
}

public class EmergencyAction
{
    public string ActionType { get; set; } = "";
    public string Description { get; set; } = "";
    public bool RequiresConfirmation { get; set; }
    public EmergencyAction() { }
    public EmergencyAction(string type, string desc, bool confirm) { ActionType = type; Description = desc; RequiresConfirmation = confirm; }
}

// ════════════════════════════════════════════════════════════════════════
//  Historical Data Models
// ════════════════════════════════════════════════════════════════════════

public class HistoricalSnapshot
{
    public DateTime TimeStamp { get; set; }
    public int BookingsCount { get; set; }
    public int ErrorsCount { get; set; }
    public int CancelErrorsCount { get; set; }
    public double ApiHealthRatio { get; set; }
    public double AverageResponseTime { get; set; }
    public bool DatabaseConnected { get; set; }
    public string OverallStatus { get; set; } = "";
}

// ════════════════════════════════════════════════════════════════════════
//  Alerting Models
// ════════════════════════════════════════════════════════════════════════

public class AlertInfo
{
    public string Id { get; set; } = "";
    public string Title { get; set; } = "";
    public string Message { get; set; } = "";
    public string Severity { get; set; } = "Info";
    public string Category { get; set; } = "";
    public DateTime Timestamp { get; set; }
    public bool IsActive { get; set; }
}
