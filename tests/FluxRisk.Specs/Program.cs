using FluxRisk.Core;

var specs = new (string Name, Func<Task> Run)[]
{
    ("normal transaction is allowed", NormalTransactionIsAllowed),
    ("high amount is sent to review", HighAmountIsReviewed),
    ("five-minute velocity blocks", VelocityBlocks),
    ("duplicate event is idempotent", DuplicateIsIdempotent),
    ("parallel accounts remain independent", ParallelAccountsRemainIndependent),
    ("bounded out-of-order event is accepted", BoundedOutOfOrderIsAccepted),
    ("event older than watermark is rejected", EventOlderThanWatermarkIsRejected),
    ("shadow model is audited without changing score", ShadowModelDoesNotChangeScore),
    ("assist model contributes to score", AssistModelContributes),
    ("replay reproduces decision sequence", ReplayReproducesSequence),
    ("outbox retries and dead-letters", OutboxRetriesAndDeadLetters),
    ("review case uses optimistic version", ReviewCaseUsesVersion),
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

static async Task BoundedOutOfOrderIsAccepted()
{
    var engine = RiskDecisionEngine.CreateDefault();
    await engine.DecideAsync(EventAt("event-first", 10));
    var result = await engine.DecideAsync(EventAt("event-late-but-valid", 9));
    True(result.Decision.EventTime!.OutOfOrder);
}

static async Task EventOlderThanWatermarkIsRejected()
{
    var engine = RiskDecisionEngine.CreateDefault();
    await engine.DecideAsync(EventAt("event-new", 10));
    try
    {
        await engine.DecideAsync(EventAt("event-too-late", 7));
        throw new InvalidOperationException("Expected event to be rejected by watermark.");
    }
    catch (LateEventException)
    {
    }
}

static async Task ShadowModelDoesNotChangeScore()
{
    var result = await RiskDecisionEngine.CreateDefault()
        .DecideAsync(Event("model-shadow", amount: 15_000m));
    Equal(45, result.Decision.Score);
    True(result.Decision.Model is { ShadowMode: true, ScoreContribution: > 0 });
}

static async Task AssistModelContributes()
{
    var engine = RiskDecisionEngine.CreateDefault(modelMode: ModelExecutionMode.Assist);
    var result = await engine.DecideAsync(Event("model-assist", amount: 15_000m));
    True(result.Decision.Score > 45);
    True(result.Decision.Model is { ShadowMode: false });
}

static async Task ReplayReproducesSequence()
{
    var events = Enumerable.Range(0, 5).Select(index => Event($"replay-{index}", seconds: index));
    var replay = await new RiskReplayService().ReplayAsync(events);
    Equal(5, replay.Count);
    Equal(RiskAction.Block, replay[^1].Action);
    Equal(5, replay[^1].Features.TransactionCount5Minutes);
}

static async Task OutboxRetriesAndDeadLetters()
{
    var store = new InMemoryRiskDecisionStore();
    await RiskDecisionEngine.CreateDefault(store).DecideAsync(Event("outbox-failure"));
    var relay = new OutboxRelay(
        store,
        new AlwaysFailPublisher(),
        options: new OutboxRelayOptions(MaxAttempts: 2, MaximumBackoff: TimeSpan.Zero));
    Equal(1, await relay.RunOnceAsync("worker"));
    Equal(1, await relay.RunOnceAsync("worker"));
    Equal(0, await relay.RunOnceAsync("worker"));
}

static async Task ReviewCaseUsesVersion()
{
    var store = new InMemoryRiskDecisionStore();
    await RiskDecisionEngine.CreateDefault(store)
        .DecideAsync(Event("case-review", amount: 15_000m));
    var reviewCase = (await store.ListCasesAsync(ReviewCaseStatus.Open, 10)).Single();
    var resolved = await store.ResolveCaseAsync(
        reviewCase.Id,
        new ResolveCaseCommand(ReviewCaseStatus.Approved, "reviewer", "verified", reviewCase.Version));
    Equal(ReviewCaseStatus.Approved, resolved!.Status);
    Equal(2, resolved.Version);
}

static RiskEvent EventAt(string id, int minute) =>
    new(id, "event-time-account", "device-1", 120m, "CNY", "CN",
        new DateTimeOffset(2026, 9, 15, 0, minute, 0, TimeSpan.Zero));

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

file sealed class AlwaysFailPublisher : IMessagePublisher
{
    public ValueTask<string> PublishAsync(
        OutboxDelivery message,
        CancellationToken cancellationToken = default) =>
        ValueTask.FromException<string>(new HttpRequestException($"Simulated failure for {message.Id}."));
}
