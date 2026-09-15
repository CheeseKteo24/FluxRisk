using FluxRisk.Core;

var specs = new (string Name, Func<Task> Run)[]
{
    ("normal transaction is allowed", NormalTransactionIsAllowed),
    ("high amount is sent to review", HighAmountIsReviewed),
    ("five-minute velocity blocks", VelocityBlocks),
    ("duplicate event is idempotent", DuplicateIsIdempotent),
    ("parallel accounts remain independent", ParallelAccountsRemainIndependent),
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

Console.WriteLine($"{specs.Length - failures}/{specs.Length} specs passed");
return failures == 0 ? 0 : 1;

static RiskEvent Event(
    string eventId,
    string accountId = "account-1",
    decimal amount = 120m,
    string deviceId = "device-1",
    string country = "CN",
    int seconds = 0) =>
    new(
        eventId,
        accountId,
        deviceId,
        amount,
        "CNY",
        country,
        new DateTimeOffset(2026, 9, 15, 0, 0, seconds, TimeSpan.Zero));

static async Task NormalTransactionIsAllowed()
{
    var result = await RiskDecisionEngine.CreateDefault().DecideAsync(Event("event-1"));
    Equal(RiskAction.Allow, result.Decision.Action);
    Equal(1, result.Decision.Features.TransactionCount5Minutes);
}

static async Task HighAmountIsReviewed()
{
    var result = await RiskDecisionEngine.CreateDefault()
        .DecideAsync(Event("event-1", amount: 15_000m));
    Equal(RiskAction.Review, result.Decision.Action);
    True(result.Decision.RuleHits.Any(hit => hit.Code == "HIGH_AMOUNT"));
}

static async Task VelocityBlocks()
{
    var engine = RiskDecisionEngine.CreateDefault();
    DecisionResult? result = null;
    for (var index = 0; index < 5; index++)
    {
        result = await engine.DecideAsync(Event($"event-{index}", seconds: index));
    }
    Equal(RiskAction.Block, result!.Decision.Action);
    Equal(5, result.Decision.Features.TransactionCount5Minutes);
}

static async Task DuplicateIsIdempotent()
{
    var engine = RiskDecisionEngine.CreateDefault();
    var first = await engine.DecideAsync(Event("event-1"));
    var duplicate = await engine.DecideAsync(Event("event-1"));
    Equal(first.Decision, duplicate.Decision);
    True(duplicate.Duplicate);

    for (var index = 2; index <= 4; index++)
    {
        await engine.DecideAsync(Event($"event-{index}", seconds: index));
    }
    var fourthUnique = await engine.DecideAsync(Event("event-read", seconds: 4));
    Equal(5, fourthUnique.Decision.Features.TransactionCount5Minutes);
}

static async Task ParallelAccountsRemainIndependent()
{
    var engine = RiskDecisionEngine.CreateDefault();
    var tasks = Enumerable.Range(0, 100)
        .Select(index => engine.DecideAsync(
            Event($"event-{index}", accountId: $"account-{index}", seconds: index % 60))
            .AsTask());
    var results = await Task.WhenAll(tasks);
    True(results.All(result => result.Decision.Action == RiskAction.Allow));
}

static void Equal<T>(T expected, T actual)
    where T : notnull
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
