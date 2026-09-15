using System.Text.Json;
using System.Text.Json.Serialization;
using FluxRisk.Core;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton(_ => RiskDecisionEngine.CreateDefault());
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(
        new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)));

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.MapPost(
    "/v1/decisions",
    async (RiskEvent riskEvent, RiskDecisionEngine engine, CancellationToken cancellationToken) =>
    {
        try
        {
            var result = await engine.DecideAsync(riskEvent, cancellationToken);
            return Results.Ok(result);
        }
        catch (ArgumentException error)
        {
            return Results.BadRequest(new { error = error.Message });
        }
        catch (InvalidOperationException error)
        {
            return Results.Conflict(new { error = error.Message });
        }
    });

app.MapGet(
    "/v1/decisions/{eventId}",
    (string eventId, RiskDecisionEngine engine) =>
        engine.TryGetDecision(eventId, out var decision)
            ? Results.Ok(decision)
            : Results.NotFound(new { error = "Decision not found." }));

app.Run();
