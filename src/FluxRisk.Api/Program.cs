using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using FluxRisk.Api;
using FluxRisk.Core;
using FluxRisk.Infrastructure;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
var postgresConnection = builder.Configuration.GetConnectionString("FluxRisk");
var storageMode = string.IsNullOrWhiteSpace(postgresConnection) ? "memory" : "postgres";
var redpandaProxy = builder.Configuration["Redpanda:HttpProxy"];
var modelMode = Enum.TryParse<ModelExecutionMode>(builder.Configuration["RiskModel:Mode"], true, out var parsedMode)
    ? parsedMode
    : ModelExecutionMode.Shadow;

if (storageMode == "memory")
{
    builder.Services.AddSingleton<InMemoryRiskDecisionStore>();
    builder.Services.AddSingleton<IRiskDecisionStore>(services => services.GetRequiredService<InMemoryRiskDecisionStore>());
    builder.Services.AddSingleton<IOutboxStore>(services => services.GetRequiredService<InMemoryRiskDecisionStore>());
    builder.Services.AddSingleton<IReviewCaseStore>(services => services.GetRequiredService<InMemoryRiskDecisionStore>());
    builder.Services.AddSingleton<IReplaySource>(services => services.GetRequiredService<InMemoryRiskDecisionStore>());
}
else
{
    builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(postgresConnection!));
    builder.Services.AddSingleton<PostgresRiskDecisionStore>();
    builder.Services.AddSingleton<IRiskDecisionStore>(services => services.GetRequiredService<PostgresRiskDecisionStore>());
    builder.Services.AddSingleton<IOutboxStore>(services => services.GetRequiredService<PostgresRiskDecisionStore>());
    builder.Services.AddSingleton<IReviewCaseStore>(services => services.GetRequiredService<PostgresRiskDecisionStore>());
    builder.Services.AddSingleton<IReplaySource>(services => services.GetRequiredService<PostgresRiskDecisionStore>());
    builder.Services.AddSingleton<DatabaseMigrator>();
}

builder.Services.AddSingleton<IRiskModel, CalibratedAnomalyModel>();
builder.Services.AddSingleton(services => RiskDecisionEngine.CreateDefault(
    services.GetRequiredService<IRiskDecisionStore>(),
    model: services.GetRequiredService<IRiskModel>(),
    modelMode: modelMode));
builder.Services.AddSingleton<RiskReplayService>();
builder.Services.AddSingleton<DecisionTelemetry>();

if (!string.IsNullOrWhiteSpace(redpandaProxy))
{
    builder.Services.AddSingleton<IMessagePublisher>(_ => new RedpandaHttpPublisher(
        new HttpClient { BaseAddress = new Uri(redpandaProxy.TrimEnd('/') + "/") },
        builder.Configuration["Redpanda:Topic"] ?? "risk-decisions"));
    builder.Services.AddSingleton<OutboxRelay>();
    builder.Services.AddHostedService<OutboxRelayWorker>();
}

builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(
        new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)));

var app = builder.Build();
if (storageMode == "postgres")
{
    await app.Services.GetRequiredService<DatabaseMigrator>().MigrateAsync();
}

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    storage = storageMode,
    streaming = string.IsNullOrWhiteSpace(redpandaProxy) ? "disabled" : "redpanda",
    model = modelMode.ToString().ToLowerInvariant(),
}));

app.MapPost("/v1/decisions", async (
    RiskEvent riskEvent,
    RiskDecisionEngine engine,
    DecisionTelemetry telemetry,
    CancellationToken cancellationToken) =>
{
    var stopwatch = Stopwatch.StartNew();
    RiskAction? action = null;
    try
    {
        var result = await engine.DecideAsync(riskEvent, cancellationToken);
        action = result.Decision.Action;
        return Results.Ok(result);
    }
    catch (LateEventException error)
    {
        return Results.UnprocessableEntity(new
        {
            error = error.Message,
            error.EventId,
            error.OccurredAt,
            error.Watermark,
        });
    }
    catch (ArgumentException error)
    {
        return Results.BadRequest(new { error = error.Message });
    }
    catch (InvalidOperationException error)
    {
        return Results.Conflict(new { error = error.Message });
    }
    finally
    {
        telemetry.Record(stopwatch.Elapsed, action, action is null);
    }
});

app.MapGet("/v1/decisions/{eventId}", async (
    string eventId,
    RiskDecisionEngine engine,
    CancellationToken cancellationToken) =>
    await engine.GetDecisionAsync(eventId, cancellationToken) is { } decision
        ? Results.Ok(decision)
        : Results.NotFound(new { error = "Decision not found." }));

app.MapGet("/v1/replay/{accountId}", async (
    string accountId,
    IReplaySource source,
    RiskReplayService replay,
    CancellationToken cancellationToken) =>
{
    var events = await source.GetEventsForReplayAsync(accountId, cancellationToken);
    return Results.Ok(new
    {
        accountId,
        eventCount = events.Count,
        decisions = await replay.ReplayAsync(events, cancellationToken),
    });
});

app.MapGet("/v1/cases", async (
    string? status,
    int? limit,
    IReviewCaseStore cases,
    CancellationToken cancellationToken) =>
{
    ReviewCaseStatus? parsedStatus = null;
    if (!string.IsNullOrWhiteSpace(status))
    {
        if (!Enum.TryParse<ReviewCaseStatus>(status, true, out var parsed))
        {
            return Results.BadRequest(new { error = $"Unknown case status '{status}'." });
        }
        parsedStatus = parsed;
    }

    return Results.Ok(await cases.ListCasesAsync(parsedStatus, limit ?? 50, cancellationToken));
});

app.MapPost("/v1/cases/{id:guid}/resolve", async (
    Guid id,
    ResolveCaseCommand command,
    IReviewCaseStore cases,
    CancellationToken cancellationToken) =>
{
    try
    {
        var updated = await cases.ResolveCaseAsync(id, command, cancellationToken);
        return updated is null
            ? Results.NotFound(new { error = "Review case not found or version is stale." })
            : Results.Ok(updated);
    }
    catch (InvalidOperationException error)
    {
        return Results.Conflict(new { error = error.Message });
    }
});

app.MapGet("/v1/metrics", (DecisionTelemetry telemetry) => Results.Ok(telemetry.Snapshot()));
app.MapGet("/metrics", (DecisionTelemetry telemetry) => Results.Text(
    telemetry.ToPrometheus(),
    "text/plain; version=0.0.4"));

app.Run();
