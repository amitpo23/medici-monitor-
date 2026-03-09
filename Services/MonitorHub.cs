using Microsoft.AspNetCore.SignalR;

namespace MediciMonitor.Services;

/// <summary>
/// SignalR hub for real-time dashboard updates.
/// Clients connect to receive live alerts, status changes, and breaker events.
/// </summary>
public class MonitorHub : Hub
{
    private readonly ILogger<MonitorHub> _logger;

    public MonitorHub(ILogger<MonitorHub> logger) => _logger = logger;

    public override Task OnConnectedAsync()
    {
        _logger.LogInformation("SignalR client connected: {Id}", Context.ConnectionId);
        return base.OnConnectedAsync();
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogInformation("SignalR client disconnected: {Id}", Context.ConnectionId);
        return base.OnDisconnectedAsync(exception);
    }

    /// <summary>Client can request a specific data refresh.</summary>
    public async Task RequestRefresh(string dataType)
    {
        _logger.LogDebug("Client {Id} requested refresh: {Type}", Context.ConnectionId, dataType);
        await Clients.Caller.SendAsync("RefreshRequested", dataType);
    }
}

/// <summary>
/// Helper service for pushing updates to all connected SignalR clients.
/// Inject this into background services to broadcast events.
/// </summary>
public class MonitorHubNotifier
{
    private readonly IHubContext<MonitorHub> _hubContext;

    public MonitorHubNotifier(IHubContext<MonitorHub> hubContext) => _hubContext = hubContext;

    public async Task SendAlerts(object alerts)
        => await _hubContext.Clients.All.SendAsync("AlertsUpdated", alerts);

    public async Task SendBreakerChange(string breakerName, bool isOpen)
        => await _hubContext.Clients.All.SendAsync("BreakerChanged", new { breakerName, isOpen, timestamp = DateTime.UtcNow });

    public async Task SendStatusUpdate(object status)
        => await _hubContext.Clients.All.SendAsync("StatusUpdated", status);

    public async Task SendNotification(string title, string message, string severity)
        => await _hubContext.Clients.All.SendAsync("Notification", new { title, message, severity, timestamp = DateTime.UtcNow });
}
