namespace MediciMonitor.Services;

/// <summary>
/// Background service that runs booking reconciliation every hour.
/// Compares Medici DB, Innstant API, and Hotel.Tools/Zenith browser.
/// </summary>
public class ReconciliationBackgroundService : BackgroundService
{
    private readonly BookingReconciliationService _reconciliation;
    private readonly IConfiguration _config;
    private readonly ILogger<ReconciliationBackgroundService> _logger;

    public ReconciliationBackgroundService(
        BookingReconciliationService reconciliation,
        IConfiguration config,
        ILogger<ReconciliationBackgroundService> logger)
    {
        _reconciliation = reconciliation;
        _config = config;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var enabled = _config.GetValue<bool>("Reconciliation:Enabled");
        if (!enabled)
        {
            _logger.LogInformation("Reconciliation background service is disabled");
            return;
        }

        var intervalMinutes = _config.GetValue<int?>("Reconciliation:IntervalMinutes") ?? 60;
        var lookbackHours = _config.GetValue<int?>("Reconciliation:LookbackHours") ?? 24;

        _logger.LogInformation("Reconciliation background service started — interval: {Min} min, lookback: {Hours}h",
            intervalMinutes, lookbackHours);

        // Wait 2 minutes after startup before first run
        await Task.Delay(TimeSpan.FromMinutes(2), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _reconciliation.RunReconciliation(lookbackHours);
            }
            catch (Exception ex)
            {
                _logger.LogError("Reconciliation background run failed: {Err}", ex.Message);
            }

            await Task.Delay(TimeSpan.FromMinutes(intervalMinutes), stoppingToken);
        }
    }
}
