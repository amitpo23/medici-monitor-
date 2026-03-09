using SendGrid;
using SendGrid.Helpers.Mail;

namespace MediciMonitor.Services;

/// <summary>
/// Background service that periodically checks for critical alerts and sends email notifications.
/// Tracks notified alerts to avoid duplicate emails (cooldown: 1 hour per alert ID).
/// </summary>
public class AlertNotificationService : BackgroundService
{
    private readonly AlertingService _alerting;
    private readonly IConfiguration _config;
    private readonly ILogger<AlertNotificationService> _logger;
    private readonly MonitorHubNotifier _hubNotifier;
    private readonly Dictionary<string, DateTime> _lastNotified = new();
    private static readonly TimeSpan CooldownPeriod = TimeSpan.FromHours(1);
    private static readonly TimeSpan CheckInterval = TimeSpan.FromMinutes(5);

    public AlertNotificationService(AlertingService alerting, IConfiguration config, ILogger<AlertNotificationService> logger, MonitorHubNotifier hubNotifier)
    {
        _alerting = alerting;
        _config = config;
        _logger = logger;
        _hubNotifier = hubNotifier;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("AlertNotificationService started — checking every {Interval} minutes", CheckInterval.TotalMinutes);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var alerts = await _alerting.EvaluateAlerts();

                // Push all alerts to connected SignalR clients
                if (alerts.Any())
                    await _hubNotifier.SendAlerts(alerts);

                var critical = alerts.Where(a => a.Severity == "Critical").ToList();

                foreach (var alert in critical)
                {
                    if (_lastNotified.TryGetValue(alert.Id, out var lastTime) && DateTime.UtcNow - lastTime < CooldownPeriod)
                        continue;

                    var sent = await SendAlertEmail(alert);
                    if (sent)
                    {
                        _lastNotified[alert.Id] = DateTime.UtcNow;
                        _logger.LogWarning("Critical alert email sent: {AlertId} — {Title}: {Message}", alert.Id, alert.Title, alert.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("AlertNotificationService error: {Err}", ex.Message);
            }

            await Task.Delay(CheckInterval, stoppingToken);
        }
    }

    private async Task<bool> SendAlertEmail(AlertInfo alert)
    {
        var apiKey = _config["SendGrid:ApiKey"] ?? "";
        var fromEmail = _config["SendGrid:FromEmail"] ?? "";
        var recipients = _config["SendGrid:AlertRecipients"] ?? "";

        if (string.IsNullOrEmpty(apiKey) || string.IsNullOrEmpty(fromEmail) || string.IsNullOrEmpty(recipients))
        {
            _logger.LogWarning("SendGrid not configured — skipping email for alert {AlertId}", alert.Id);
            return false;
        }

        var client = new SendGridClient(apiKey);
        var from = new EmailAddress(fromEmail, "Medici Monitor");
        var tos = recipients.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(e => new EmailAddress(e))
            .ToList();

        var subject = $"[CRITICAL] Medici Alert: {alert.Title}";
        var body = $"""
            CRITICAL ALERT — Medici Hotels Monitor
            ========================================

            Alert ID:  {alert.Id}
            Title:     {alert.Title}
            Severity:  {alert.Severity}
            Category:  {alert.Category}
            Time:      {alert.Timestamp:yyyy-MM-dd HH:mm:ss} UTC

            Message:
            {alert.Message}

            ========================================
            Dashboard: https://medici-monitor-dashboard.azurewebsites.net/index.html
            This is an automated alert from Medici Monitor.
            """;

        var msg = new SendGridMessage
        {
            From = from,
            Subject = subject,
            PlainTextContent = body,
            Personalizations = new List<Personalization>
            {
                new Personalization { Tos = tos }
            }
        };

        var response = await client.SendEmailAsync(msg);
        return response.StatusCode == System.Net.HttpStatusCode.Accepted;
    }
}
