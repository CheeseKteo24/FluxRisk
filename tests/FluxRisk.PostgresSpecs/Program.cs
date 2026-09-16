using FluxRisk.Core;
using FluxRisk.Infrastructure;
using Npgsql;

var connectionString = Environment.GetEnvironmentVariable("FLUXRISK_POSTGRES");
if (string.IsNullOrWhiteSpace(connectionString))
{
    Console.WriteLine("SKIP PostgreSQL specs: FLUXRISK_POSTGRES is not set.");
    return 0;
}

await using var dataSource = NpgsqlDataSource.Create(connectionString);
await new DatabaseMigrator(dataSource).MigrateAsync();
await using (var reset = dataSource.CreateCommand(
    "TRUNCATE TABLE outbox_messages, risk_decisions, risk_events;"))
{
    await reset.ExecuteNonQueryAsync();
}

var specs = new (string Name, Func<Task> Run)[]
{
    ("decision survives engine restart", DecisionSurvivesRestart),
    ("duplicate does not duplicate outbox", DuplicateDoesNotDuplicateOutbox),
    ("window state survives engine restart", WindowStateSurvivesRestart),
    ("event id cannot move between accounts", EventCannotMoveAccounts),
};

var failures = 0;
foreach (var spec in specs)
{
    try
    {
        await spec.Run();
        Console.WriteLine($"PASS {spec.Name}");
    }
    catch (Exception error)
    {
        failures++;
        Console.Error.WriteLine($"FAIL {spec.Name}: {error.Message}");
    }
}

Console.WriteLine($"{specs.Length - failures}/{specs.Length} PostgreSQL specs passed");
return failures == 0 ? 0 : 1;

RiskDecisionEngine NewEngine() =>
    RiskDecisionEngine.CreateDefault(new PostgresRiskDecisionStore(dataSource));

RiskEvent Event(string id, string account = "account-pg", int second = 0) =>
    new(id, account, "device-1", 120m, "CNY", "CN",
        new DateTimeOffset(2026, 9, 16, 0, 0, second, TimeSpan.Zero));

async Task DecisionSurvivesRestart()
{
    var original = await NewEngine().DecideAsync(Event("pg-persist", "account-persist"));
    var loaded = await NewEngine().GetDecisionAsync("pg-persist");
    True(loaded is not null);
    Equal(original.Decision.EventId, loaded!.EventId);
    Equal(original.Decision.AccountId, loaded.AccountId);
    Equal(original.Decision.Action, loaded.Action);
    Equal(original.Decision.Score, loaded.Score);
    Equal(original.Decision.Features, loaded.Features);
    True(original.Decision.RuleHits.SequenceEqual(loaded.RuleHits));
    Equal(original.Decision.DecidedAt, loaded.DecidedAt);
}

async Task DuplicateDoesNotDuplicateOutbox()
{
    var first = await NewEngine().DecideAsync(Event("pg-duplicate", "account-duplicate"));
    var duplicate = await NewEngine().DecideAsync(Event("pg-duplicate", "account-duplicate"));
    True(!first.Duplicate && duplicate.Duplicate);
    await using var command = dataSource.CreateCommand(
        "SELECT count(*) FROM outbox_messages WHERE aggregate_id = 'pg-duplicate';");
    Equal(1L, (long)(await command.ExecuteScalarAsync() ?? -1L));
}

async Task WindowStateSurvivesRestart()
{
    DecisionResult? result = null;
    for (var index = 0; index < 5; index++)
    {
        result = await NewEngine().DecideAsync(
            Event($"pg-window-{index}", "account-window", second: index));
    }

    Equal(RiskAction.Block, result!.Decision.Action);
    Equal(5, result.Decision.Features.TransactionCount5Minutes);
}

async Task EventCannotMoveAccounts()
{
    await NewEngine().DecideAsync(Event("pg-owned", "account-owner"));
    try
    {
        await NewEngine().DecideAsync(Event("pg-owned", "account-attacker"));
        throw new InvalidOperationException("Expected conflicting event ownership to fail.");
    }
    catch (InvalidOperationException error) when (error.Message.Contains("another account", StringComparison.Ordinal))
    {
    }
}

static void Equal<T>(T expected, T? actual)
{
    if (!EqualityComparer<T>.Default.Equals(expected, actual))
    {
        throw new InvalidOperationException($"Expected {expected}, received {actual}.");
    }
}

static void True(bool value)
{
    if (!value)
    {
        throw new InvalidOperationException("Expected condition to be true.");
    }
}
