namespace MediciMonitor.Services;

/// <summary>
/// Background service that periodically runs fail-safe scans.
/// - Runs every ScanIntervalSeconds (default 5 min)
/// - Optionally scans on startup
/// - Sends daily summary at configured hour
/// - Persists state periodically
/// </summary>
public class FailSafeBackgroundService : BackgroundService
{
    private readonly FailSafeService _failSafe;
    private readonly ILogger<FailSafeBackgroundService> _logger;

    public FailSafeBackgroundService(FailSafeService failSafe, ILogger<FailSafeBackgroundService> logger)
    {
        _failSafe = failSafe;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("FailSafe background scanner started — interval: {Sec}s", _failSafe.Config.ScanIntervalSeconds);

        // Optional: scan on startup
        if (_failSafe.Config.ScanOnStartup)
        {
            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken); // Wait for app warmup
            _ = await RunScan("Startup");
        }

        var lastStatus = "OK";
        while (!stoppingToken.IsCancellationRequested)
        {
            // Dynamic interval: 5 min when CRITICAL, normal otherwise
            var interval = lastStatus == "CRITICAL"
                ? Math.Max(300, 300)  // 5 min when critical
                : Math.Max(300, _failSafe.Config.ScanIntervalSeconds);
            await Task.Delay(TimeSpan.FromSeconds(interval), stoppingToken);

            if (!_failSafe.Config.Enabled) continue;

            lastStatus = await RunScan("Background");

            // Check if daily summary should be sent
            try { await _failSafe.SendDailySummaryIfDueAsync(); }
            catch (Exception ex) { _logger.LogWarning("Daily summary error: {Err}", ex.Message); }
        }
    }

    private async Task<string> RunScan(string source)
    {
        try
        {
            var result = await _failSafe.ScanAsync();
            _failSafe.LastBackgroundScanTime = DateTime.UtcNow;

            var status = result.Status;
            if (status == "CRITICAL")
                _logger.LogWarning("[FailSafe-{Source}] CRITICAL — {Msg}", source, result.Message);
            else if (status == "WARNING")
                _logger.LogInformation("[FailSafe-{Source}] Warning — {Msg}", source, result.Message);
            else
                _logger.LogDebug("[FailSafe-{Source}] OK — {Msg}", source, result.Message);
            return status;
        }
        catch (Exception ex)
        {
            _logger.LogError("[FailSafe-{Source}] Scan error: {Err}", source, ex.Message);
            return "ERROR";
        }
    }
}
