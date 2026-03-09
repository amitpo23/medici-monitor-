namespace MediciMonitor.Services;

public class ApiKeyMiddleware
{
    private readonly RequestDelegate _next;
    private readonly string? _apiKey;
    private readonly HashSet<string> _publicPaths = new(StringComparer.OrdinalIgnoreCase)
    {
        "/healthz", "/readyz", "/index.html", "/", "/favicon.ico"
    };

    public ApiKeyMiddleware(RequestDelegate next, IConfiguration config)
    {
        _next = next;
        _apiKey = config["Security:ApiKey"];
    }

    public async Task InvokeAsync(HttpContext context)
    {
        var path = context.Request.Path.Value ?? "";

        // Allow public paths and static files
        if (_publicPaths.Contains(path) || path.StartsWith("/css") || path.StartsWith("/js") || path.StartsWith("/img"))
        {
            await _next(context);
            return;
        }

        // Allow if no API key is configured (backward compatible)
        if (string.IsNullOrEmpty(_apiKey))
        {
            await _next(context);
            return;
        }

        // Check header: X-API-Key
        if (context.Request.Headers.TryGetValue("X-API-Key", out var headerKey) && headerKey == _apiKey)
        {
            await _next(context);
            return;
        }

        // Check query: ?apikey=...
        if (context.Request.Query.TryGetValue("apikey", out var queryKey) && queryKey == _apiKey)
        {
            await _next(context);
            return;
        }

        context.Response.StatusCode = 401;
        await context.Response.WriteAsJsonAsync(new { error = "Unauthorized", message = "Valid API key required in X-API-Key header" });
    }
}
