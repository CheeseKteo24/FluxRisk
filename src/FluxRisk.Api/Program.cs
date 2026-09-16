using System.Text.Json;
using System.Text.Json.Serialization;
using FluxRisk.Core;
using FluxRisk.Infrastructure;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
var postgresConnection = builder.Configuration.GetConnectionString("FluxRisk");
var storageMode = string.IsNullOrWhiteSpace(postgresConnection) ? "memory" : "postgres";
if (string.IsNullOrWhiteSpace(postgresConnection))
{
    builder.Services.AddSingleton<IRiskDecisionStore, InMemoryRiskDecisionStore>();
}
else
{
    builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(postgresConnection));
    builder.Services.AddSingleton<IRiskDecisionStore, PostgresRiskDecisionStore>();
    builder.Services.AddSingleton<DatabaseMigrator>();
}

builder.Services.AddSingleton(serviceProvider =>
    RiskDecisionEngine.CreateDefault(serviceProvider.GetRequiredService<IRiskDecisionStore>()));
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(
        new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)));

var app = builder.Build();

if (storageMode == "postgres")
{
    await app.Services.GetRequiredService<DatabaseMigrator>().MigrateAsync();
}

app.MapGet("/health", () => Results.Ok(new { status = "ok", storage = storageMode }));

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
    async (string eventId, RiskDecisionEngine engine, CancellationToken cancellationToken) =>
        await engine.GetDecisionAsync(eventId, cancellationToken)
            is { } decision
            ? Results.Ok(decision)
            : Results.NotFound(new { error = "Decision not found." }));

app.Run();
