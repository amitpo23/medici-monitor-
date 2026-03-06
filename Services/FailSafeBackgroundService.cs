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
            await RunScan("Startup");
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            var interval = Math.Max(60, _failSafe.Config.ScanIntervalSeconds); // Min 60s
            await Task.Delay(TimeSpan.FromSeconds(interval), stoppingToken);

            if (!_failSafe.Config.Enabled) continue;

            await RunScan("Background");

            // Check if daily summary should be sent
            try { await _failSafe.SendDailySummaryIfDueAsync(); }
            catch (Exception ex) { _logger.LogWarning("Daily summary error: {Err}", ex.Message); }
        }
    }

    private async Task RunScan(string source)
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
        }
        catch (Exception ex)
        {
            _logger.LogError("[FailSafe-{Source}] Scan error: {Err}", source, ex.Message);
        }
    }
}
