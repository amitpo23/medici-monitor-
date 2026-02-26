using System.Net;
using System.Net.Mail;
using System.Text;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Notification service — sends alerts via Webhook, Email, and Console/Log.
/// Channels are configurable at runtime via API.
/// </summary>
public class NotificationService
{
    private readonly ILogger<NotificationService> _logger;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };
    private readonly List<NotificationRecord> _history = new();
    private readonly object _lock = new();
    private const int MaxHistory = 500;

    // ── Configuration (runtime-editable) ──
    public NotificationConfig Config { get; set; } = new();

    public NotificationService(ILogger<NotificationService> logger) => _logger = logger;

    // ── Send notification across all enabled channels ──

    public async Task<NotificationResult> SendAsync(string title, string message, string severity = "Info", string? category = null)
    {
        var result = new NotificationResult { Title = title, Severity = severity, Timestamp = DateTime.UtcNow };
        var tasks = new List<Task>();

        if (Config.WebhookEnabled && !string.IsNullOrEmpty(Config.WebhookUrl))
            tasks.Add(SendWebhook(title, message, severity, category, result));

        if (Config.EmailEnabled && !string.IsNullOrEmpty(Config.SmtpHost))
            tasks.Add(SendEmail(title, message, severity, result));

        if (Config.SlackEnabled && !string.IsNullOrEmpty(Config.SlackWebhookUrl))
            tasks.Add(SendSlack(title, message, severity, result));

        if (Config.TeamsEnabled && !string.IsNullOrEmpty(Config.TeamsWebhookUrl))
            tasks.Add(SendTeams(title, message, severity, result));

        // Always log
        _logger.LogInformation("Notification [{Severity}] {Title}: {Message}", severity, title, message);
        result.Channels.Add(new ChannelResult { Channel = "Log", Success = true });

        if (tasks.Any())
            await Task.WhenAll(tasks);

        // Store in history
        lock (_lock)
        {
            _history.Add(new NotificationRecord
            {
                Title = title,
                Message = message,
                Severity = severity,
                Category = category,
                Timestamp = DateTime.UtcNow,
                ChannelResults = result.Channels.ToList()
            });
            while (_history.Count > MaxHistory) _history.RemoveAt(0);
        }

        return result;
    }

    // ── Webhook ──

    private async Task SendWebhook(string title, string message, string severity, string? category, NotificationResult result)
    {
        try
        {
            var payload = JsonSerializer.Serialize(new
            {
                title, message, severity, category,
                timestamp = DateTime.UtcNow,
                source = "MediciMonitor"
            });
            var content = new StringContent(payload, Encoding.UTF8, "application/json");
            var resp = await _http.PostAsync(Config.WebhookUrl, content);
            result.Channels.Add(new ChannelResult { Channel = "Webhook", Success = resp.IsSuccessStatusCode, Detail = $"HTTP {(int)resp.StatusCode}" });
        }
        catch (Exception ex)
        {
            result.Channels.Add(new ChannelResult { Channel = "Webhook", Success = false, Detail = ex.Message });
            _logger.LogWarning("Webhook notification failed: {Err}", ex.Message);
        }
    }

    // ── Email (SMTP) ──

    private async Task SendEmail(string title, string message, string severity, NotificationResult result)
    {
        try
        {
            using var smtp = new SmtpClient(Config.SmtpHost, Config.SmtpPort)
            {
                Credentials = new NetworkCredential(Config.SmtpUser, Config.SmtpPass),
                EnableSsl = Config.SmtpSsl
            };

            var severityEmoji = severity switch { "Critical" => "[CRITICAL]", "Warning" => "[WARNING]", _ => "[INFO]" };
            var mail = new MailMessage
            {
                From = new MailAddress(Config.SmtpFrom ?? Config.SmtpUser ?? "monitor@medici.com"),
                Subject = $"{severityEmoji} MediciMonitor: {title}",
                Body = $"Severity: {severity}\nTime: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss UTC}\n\n{message}\n\n-- MediciMonitor v2.1",
                IsBodyHtml = false
            };

            foreach (var to in (Config.EmailRecipients ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                mail.To.Add(to);

            if (mail.To.Count == 0)
            {
                result.Channels.Add(new ChannelResult { Channel = "Email", Success = false, Detail = "No recipients configured" });
                return;
            }

            await smtp.SendMailAsync(mail);
            result.Channels.Add(new ChannelResult { Channel = "Email", Success = true, Detail = $"Sent to {mail.To.Count} recipients" });
        }
        catch (Exception ex)
        {
            result.Channels.Add(new ChannelResult { Channel = "Email", Success = false, Detail = ex.Message });
            _logger.LogWarning("Email notification failed: {Err}", ex.Message);
        }
    }

    // ── Slack ──

    private async Task SendSlack(string title, string message, string severity, NotificationResult result)
    {
        try
        {
            var icon = severity switch { "Critical" => ":rotating_light:", "Warning" => ":warning:", _ => ":information_source:" };
            var payload = JsonSerializer.Serialize(new
            {
                text = $"{icon} *{title}*\n{message}\n_Severity: {severity} | {DateTime.UtcNow:HH:mm:ss UTC}_"
            });
            var content = new StringContent(payload, Encoding.UTF8, "application/json");
            var resp = await _http.PostAsync(Config.SlackWebhookUrl, content);
            result.Channels.Add(new ChannelResult { Channel = "Slack", Success = resp.IsSuccessStatusCode, Detail = $"HTTP {(int)resp.StatusCode}" });
        }
        catch (Exception ex)
        {
            result.Channels.Add(new ChannelResult { Channel = "Slack", Success = false, Detail = ex.Message });
            _logger.LogWarning("Slack notification failed: {Err}", ex.Message);
        }
    }

    // ── Microsoft Teams ──

    private async Task SendTeams(string title, string message, string severity, NotificationResult result)
    {
        try
        {
            var color = severity switch { "Critical" => "FF0000", "Warning" => "FFA500", _ => "0078D4" };
            var payload = JsonSerializer.Serialize(new
            {
                @type = "MessageCard",
                themeColor = color,
                title = $"MediciMonitor: {title}",
                text = message,
                sections = new[]
                {
                    new { facts = new[]
                    {
                        new { name = "Severity", value = severity },
                        new { name = "Time", value = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss UTC") },
                        new { name = "Source", value = "MediciMonitor v2.1" }
                    }}
                }
            });
            var content = new StringContent(payload, Encoding.UTF8, "application/json");
            var resp = await _http.PostAsync(Config.TeamsWebhookUrl, content);
            result.Channels.Add(new ChannelResult { Channel = "Teams", Success = resp.IsSuccessStatusCode, Detail = $"HTTP {(int)resp.StatusCode}" });
        }
        catch (Exception ex)
        {
            result.Channels.Add(new ChannelResult { Channel = "Teams", Success = false, Detail = ex.Message });
            _logger.LogWarning("Teams notification failed: {Err}", ex.Message);
        }
    }

    // ── Test notification ──

    public async Task<NotificationResult> SendTestAsync()
    {
        return await SendAsync(
            "בדיקת התראות",
            "זוהי הודעת בדיקה מ-MediciMonitor. אם אתה רואה הודעה זו, ערוץ ההתראות פועל תקין.",
            "Info", "Test");
    }

    // ── History ──

    public List<NotificationRecord> GetHistory(int last = 50)
    {
        lock (_lock)
        {
            return _history.OrderByDescending(n => n.Timestamp).Take(last).ToList();
        }
    }

    public NotificationConfig GetConfig() => Config;
}

// ── Models ──

public class NotificationConfig
{
    // Webhook
    public bool WebhookEnabled { get; set; }
    public string? WebhookUrl { get; set; }

    // Email
    public bool EmailEnabled { get; set; }
    public string? SmtpHost { get; set; }
    public int SmtpPort { get; set; } = 587;
    public string? SmtpUser { get; set; }
    public string? SmtpPass { get; set; }
    public string? SmtpFrom { get; set; }
    public bool SmtpSsl { get; set; } = true;
    public string? EmailRecipients { get; set; }

    // Slack
    public bool SlackEnabled { get; set; }
    public string? SlackWebhookUrl { get; set; }

    // Teams
    public bool TeamsEnabled { get; set; }
    public string? TeamsWebhookUrl { get; set; }

    // Behavior
    public int CooldownMinutes { get; set; } = 5;
    public string MinSeverity { get; set; } = "Warning";
}

public class NotificationResult
{
    public string Title { get; set; } = "";
    public string Severity { get; set; } = "";
    public DateTime Timestamp { get; set; }
    public List<ChannelResult> Channels { get; set; } = new();
}

public class ChannelResult
{
    public string Channel { get; set; } = "";
    public bool Success { get; set; }
    public string? Detail { get; set; }
}

public class NotificationRecord
{
    public string Title { get; set; } = "";
    public string Message { get; set; } = "";
    public string Severity { get; set; } = "";
    public string? Category { get; set; }
    public DateTime Timestamp { get; set; }
    public List<ChannelResult> ChannelResults { get; set; } = new();
}
