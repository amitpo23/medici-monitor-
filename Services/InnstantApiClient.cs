using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Lightweight Innstant API client — used for booking verification only (read-only).
/// Calls booking-details endpoint to verify bookings exist in Innstant.
/// </summary>
public class InnstantApiClient
{
    private readonly HttpClient _http;
    private readonly ILogger<InnstantApiClient> _logger;
    private readonly string _bookUrl;
    private readonly string _accessToken;
    private readonly string _applicationKey;

    public InnstantApiClient(IConfiguration config, ILogger<InnstantApiClient> logger)
    {
        _logger = logger;
        var section = config.GetSection("Reconciliation:Innstant");
        _bookUrl = section["BookUrl"] ?? "https://book.mishor5.innstant-servers.com";
        _accessToken = section["AccessToken"] ?? "";
        _applicationKey = section["ApplicationKey"] ?? "";

        _http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        _http.DefaultRequestHeaders.Add("aether-access-token", _accessToken);
        _http.DefaultRequestHeaders.Add("aether-application-key", _applicationKey);
        _http.DefaultRequestHeaders.Add("cache-control", "no-cache");
    }

    public bool IsConfigured => !string.IsNullOrEmpty(_accessToken) && !string.IsNullOrEmpty(_applicationKey);

    /// <summary>
    /// Get booking details from Innstant by content booking ID.
    /// Returns parsed JSON or null if not found / error.
    /// </summary>
    public async Task<InnstantBookingInfo?> GetBookingDetails(int contentBookingId)
    {
        try
        {
            var url = $"{_bookUrl}/booking-details/{contentBookingId}";
            var response = await _http.GetAsync(url);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogDebug("Innstant booking-details/{Id} returned {Code}", contentBookingId, (int)response.StatusCode);
                return null;
            }

            var json = await response.Content.ReadAsStringAsync();
            if (string.IsNullOrWhiteSpace(json)) return null;

            var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            return new InnstantBookingInfo
            {
                ContentBookingId = contentBookingId,
                RawJson = json,
                Found = true,
                Status = GetStr(root, "status") ?? GetStr(root, "booking_status") ?? "unknown",
                HotelName = GetStr(root, "hotel_name"),
                CheckIn = GetStr(root, "check_in") ?? GetStr(root, "checkin_date"),
                CheckOut = GetStr(root, "check_out") ?? GetStr(root, "checkout_date"),
                TotalPrice = GetDecimal(root, "total_price") ?? GetDecimal(root, "price"),
                Currency = GetStr(root, "currency"),
                GuestName = GetStr(root, "guest_name") ?? GetStr(root, "lead_guest"),
                ConfirmationNumber = GetStr(root, "confirmation_number") ?? GetStr(root, "supplier_confirmation"),
                CancellationDeadline = GetStr(root, "cancellation_deadline") ?? GetStr(root, "free_cancellation_until")
            };
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Innstant booking-details/{Id} error: {Err}", contentBookingId, ex.Message);
            return new InnstantBookingInfo { ContentBookingId = contentBookingId, Found = false, Error = ex.Message };
        }
    }

    /// <summary>
    /// Batch-verify multiple bookings against Innstant.
    /// Rate-limited: max 5 concurrent calls.
    /// </summary>
    public async Task<List<InnstantBookingInfo>> VerifyBookings(List<int> contentBookingIds)
    {
        var results = new List<InnstantBookingInfo>();
        var semaphore = new SemaphoreSlim(5);

        var tasks = contentBookingIds.Select(async id =>
        {
            await semaphore.WaitAsync();
            try
            {
                var info = await GetBookingDetails(id);
                return info ?? new InnstantBookingInfo { ContentBookingId = id, Found = false };
            }
            finally { semaphore.Release(); }
        });

        results.AddRange(await Task.WhenAll(tasks));
        return results;
    }

    private static string? GetStr(JsonElement el, string prop)
    {
        if (el.TryGetProperty(prop, out var val) && val.ValueKind == JsonValueKind.String)
            return val.GetString();
        return null;
    }

    private static decimal? GetDecimal(JsonElement el, string prop)
    {
        if (el.TryGetProperty(prop, out var val))
        {
            if (val.ValueKind == JsonValueKind.Number) return val.GetDecimal();
            if (val.ValueKind == JsonValueKind.String && decimal.TryParse(val.GetString(), out var d)) return d;
        }
        return null;
    }
}

// ── Models ──

public class InnstantBookingInfo
{
    public int ContentBookingId { get; set; }
    public bool Found { get; set; }
    public string? Status { get; set; }
    public string? HotelName { get; set; }
    public string? CheckIn { get; set; }
    public string? CheckOut { get; set; }
    public decimal? TotalPrice { get; set; }
    public string? Currency { get; set; }
    public string? GuestName { get; set; }
    public string? ConfirmationNumber { get; set; }
    public string? CancellationDeadline { get; set; }
    public string? Error { get; set; }
    public string? RawJson { get; set; }
}
