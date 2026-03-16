using System.Diagnostics;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Browser-based reconciliation using agent-browser CLI.
/// Automates login to Innstant B2B portal and Hotel.Tools to verify bookings visually.
/// </summary>
public class BrowserReconciliationService
{
    private readonly ILogger<BrowserReconciliationService> _logger;
    private readonly string _b2bUrl;
    private readonly string _b2bAccount;
    private readonly string _b2bAgent;
    private readonly string _b2bPassword;
    private readonly string _hotelToolsUrl;
    private readonly string _hotelToolsAccount;
    private readonly string _hotelToolsAgent;
    private readonly string _hotelToolsPassword;

    public BrowserReconciliationService(IConfiguration config, ILogger<BrowserReconciliationService> logger)
    {
        _logger = logger;
        var section = config.GetSection("Reconciliation:Browser");

        _b2bUrl = section["InnstantB2BUrl"] ?? "https://b2b.innstantconnect.com/";
        _b2bAccount = section["InnstantAccount"] ?? "";
        _b2bAgent = section["InnstantAgent"] ?? "";
        _b2bPassword = section["InnstantPassword"] ?? "";

        _hotelToolsUrl = section["HotelToolsUrl"] ?? "https://hotel.tools/";
        _hotelToolsAccount = section["HotelToolsAccount"] ?? "";
        _hotelToolsAgent = section["HotelToolsAgent"] ?? "";
        _hotelToolsPassword = section["HotelToolsPassword"] ?? "";
    }

    public bool IsConfigured => !string.IsNullOrEmpty(_b2bAccount) && !string.IsNullOrEmpty(_b2bPassword);

    /// <summary>
    /// Launch agent-browser to check B2B portal for recent bookings.
    /// Returns structured results of what was found.
    /// </summary>
    public async Task<BrowserCheckResult> VerifyViaB2BPortal(int lookbackHours = 24)
    {
        var result = new BrowserCheckResult { StartTime = DateTime.UtcNow };

        try
        {
            // Check if agent-browser is available
            if (!await IsAgentBrowserInstalled())
            {
                result.Error = "agent-browser not installed. Run: npm install -g agent-browser && agent-browser install";
                result.Available = false;
                return result;
            }
            result.Available = true;

            // ── Innstant B2B Portal Check ──
            if (!string.IsNullOrEmpty(_b2bAccount))
            {
                _logger.LogInformation("Browser: checking Innstant B2B portal...");
                var b2bResult = await CheckInnstantB2B();
                result.InnstantB2B = b2bResult;
            }

            // ── Hotel.Tools Check ──
            if (!string.IsNullOrEmpty(_hotelToolsAccount))
            {
                _logger.LogInformation("Browser: checking Hotel.Tools portal...");
                var htResult = await CheckHotelTools();
                result.HotelTools = htResult;
            }
        }
        catch (Exception ex)
        {
            result.Error = ex.Message;
            _logger.LogError("Browser reconciliation error: {Err}", ex.Message);
        }

        result.EndTime = DateTime.UtcNow;
        result.DurationMs = (int)(result.EndTime - result.StartTime).TotalMilliseconds;
        return result;
    }

    private async Task<PortalCheckResult> CheckInnstantB2B()
    {
        var check = new PortalCheckResult { Portal = "Innstant B2B", Url = _b2bUrl };
        try
        {
            // Navigate to login page
            await RunAgentBrowser($"open \"{_b2bUrl}\"");
            await RunAgentBrowser("wait visible 3000");

            // Take a snapshot to understand the page
            var snapshot = await RunAgentBrowser("snapshot");

            // Try to login
            await RunAgentBrowser($"fill \"Account\" \"{_b2bAccount}\"");
            await RunAgentBrowser($"fill \"Agent\" \"{_b2bAgent}\"");
            await RunAgentBrowser($"fill \"Password\" \"{_b2bPassword}\"");
            await RunAgentBrowser("click \"Login\"");
            await RunAgentBrowser("wait networkidle 5000");

            // Navigate to bookings section
            await RunAgentBrowser("click \"Bookings\"");
            await RunAgentBrowser("wait networkidle 3000");

            // Get the page content
            var bookingsSnapshot = await RunAgentBrowser("snapshot");
            check.Snapshot = bookingsSnapshot;
            check.Success = true;

            // Take screenshot for evidence
            var screenshotPath = Path.Combine(Path.GetTempPath(), $"innstant_b2b_{DateTime.UtcNow:yyyyMMdd_HHmmss}.png");
            await RunAgentBrowser($"screenshot \"{screenshotPath}\"");
            check.ScreenshotPath = screenshotPath;

            // Parse booking count from snapshot if possible
            check.BookingsFound = ParseBookingCount(bookingsSnapshot);
        }
        catch (Exception ex)
        {
            check.Error = ex.Message;
            check.Success = false;
        }
        return check;
    }

    private async Task<PortalCheckResult> CheckHotelTools()
    {
        var check = new PortalCheckResult { Portal = "Hotel.Tools (הנובי)", Url = _hotelToolsUrl };
        try
        {
            // Navigate to Hotel.Tools
            await RunAgentBrowser($"open \"{_hotelToolsUrl}\"");
            await RunAgentBrowser("wait networkidle 5000");

            // Login
            await RunAgentBrowser($"fill \"Account\" \"{_hotelToolsAccount}\"");
            await RunAgentBrowser($"fill \"Agent\" \"{_hotelToolsAgent}\"");
            await RunAgentBrowser($"fill \"Password\" \"{_hotelToolsPassword}\"");
            await RunAgentBrowser("click \"Login\"");
            await RunAgentBrowser("wait networkidle 5000");

            // Navigate to reservations/bookings tab
            var clickResult = await RunAgentBrowser("click \"Reservations\"");
            if (string.IsNullOrEmpty(clickResult))
                await RunAgentBrowser("click \"Bookings\""); // Try alternative tab name
            await RunAgentBrowser("wait networkidle 3000");

            // Get full snapshot — this contains the reservation list
            var reservationsSnapshot = await RunAgentBrowser("snapshot");
            check.Snapshot = reservationsSnapshot;
            check.Success = true;

            var screenshotPath = Path.Combine(Path.GetTempPath(), $"hoteltools_{DateTime.UtcNow:yyyyMMdd_HHmmss}.png");
            await RunAgentBrowser($"screenshot \"{screenshotPath}\"");
            check.ScreenshotPath = screenshotPath;

            check.BookingsFound = ParseBookingCount(reservationsSnapshot);

            // Extract reservation details from snapshot for cross-referencing
            check.ExtractedReservations = ParseReservationsFromSnapshot(reservationsSnapshot);
        }
        catch (Exception ex)
        {
            check.Error = ex.Message;
            check.Success = false;
        }
        return check;
    }

    /// <summary>
    /// Parse reservation details from accessibility tree snapshot.
    /// Looks for patterns like hotel names, dates, amounts, confirmation numbers.
    /// </summary>
    private List<ExtractedReservation> ParseReservationsFromSnapshot(string snapshot)
    {
        var reservations = new List<ExtractedReservation>();
        if (string.IsNullOrEmpty(snapshot)) return reservations;

        try
        {
            var lines = snapshot.Split('\n');
            ExtractedReservation? current = null;

            foreach (var line in lines)
            {
                var trimmed = line.Trim();

                // Look for date patterns (dd/MM/yyyy or yyyy-MM-dd)
                var dateMatch = System.Text.RegularExpressions.Regex.Match(trimmed, @"(\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})");
                // Look for price patterns ($XXX or XXX.XX)
                var priceMatch = System.Text.RegularExpressions.Regex.Match(trimmed, @"\$?([\d,]+\.?\d{0,2})");
                // Look for confirmation/reference numbers
                var refMatch = System.Text.RegularExpressions.Regex.Match(trimmed, @"(?:Ref|Conf|#|ID)[\s:]*(\w+)", System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                if (trimmed.Contains("reservation", StringComparison.OrdinalIgnoreCase) ||
                    trimmed.Contains("booking", StringComparison.OrdinalIgnoreCase) ||
                    trimmed.Contains("hotel", StringComparison.OrdinalIgnoreCase))
                {
                    if (current != null) reservations.Add(current);
                    current = new ExtractedReservation { RawText = trimmed };
                }

                if (current != null)
                {
                    if (dateMatch.Success && current.Date == null) current.Date = dateMatch.Groups[1].Value;
                    if (refMatch.Success && current.Reference == null) current.Reference = refMatch.Groups[1].Value;
                }
            }
            if (current != null) reservations.Add(current);
        }
        catch { }

        return reservations;
    }

    // ── agent-browser CLI wrapper ────────────────────────────────

    private async Task<string> RunAgentBrowser(string command)
    {
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "agent-browser",
                Arguments = command,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            using var proc = Process.Start(psi);
            if (proc == null) return "";

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var output = await proc.StandardOutput.ReadToEndAsync();
            var error = await proc.StandardError.ReadToEndAsync();

            try { await proc.WaitForExitAsync(cts.Token); }
            catch (OperationCanceledException) { try { proc.Kill(); } catch { } return ""; }

            if (proc.ExitCode != 0 && !string.IsNullOrEmpty(error))
                _logger.LogDebug("agent-browser '{Cmd}' stderr: {Err}", command, error);

            return output.Trim();
        }
        catch (Exception ex)
        {
            _logger.LogDebug("agent-browser command failed: {Cmd} — {Err}", command, ex.Message);
            return "";
        }
    }

    private async Task<bool> IsAgentBrowserInstalled()
    {
        try
        {
            var output = await RunAgentBrowser("--version");
            return !string.IsNullOrEmpty(output);
        }
        catch { return false; }
    }

    private int ParseBookingCount(string snapshot)
    {
        // Try to extract booking count from accessibility tree
        // Look for patterns like "X bookings", "X results", numbers in tables
        try
        {
            var lines = snapshot.Split('\n');
            foreach (var line in lines)
            {
                if (line.Contains("booking", StringComparison.OrdinalIgnoreCase) ||
                    line.Contains("reservation", StringComparison.OrdinalIgnoreCase))
                {
                    var numbers = System.Text.RegularExpressions.Regex.Matches(line, @"\d+");
                    if (numbers.Count > 0 && int.TryParse(numbers[0].Value, out var count))
                        return count;
                }
            }
        }
        catch { }
        return -1; // Unknown
    }
}

// ── Models ──

public class BrowserCheckResult
{
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public int DurationMs { get; set; }
    public bool Available { get; set; }
    public PortalCheckResult? InnstantB2B { get; set; }
    public PortalCheckResult? HotelTools { get; set; }
    public string? Error { get; set; }
}

public class PortalCheckResult
{
    public string Portal { get; set; } = "";
    public string Url { get; set; } = "";
    public bool Success { get; set; }
    public int BookingsFound { get; set; } = -1;
    public string? Snapshot { get; set; }
    public string? ScreenshotPath { get; set; }
    public string? Error { get; set; }
    public List<ExtractedReservation>? ExtractedReservations { get; set; }
}

public class ExtractedReservation
{
    public string? HotelName { get; set; }
    public string? Date { get; set; }
    public string? Reference { get; set; }
    public string? Amount { get; set; }
    public string? Status { get; set; }
    public string RawText { get; set; } = "";
}
