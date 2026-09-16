using System.Text.Json;
using System.Text.Json.Serialization;

namespace FluxRisk.Core;

public sealed class RiskDecisionEngine
{
    private static readonly TimeSpan Retention = TimeSpan.FromHours(24);
    private readonly IReadOnlyList<IRiskRule> _rules;
    private readonly IRiskDecisionStore _store;
    private readonly TimeProvider _timeProvider;
    private static readonly JsonSerializerOptions OutboxJsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
    };

    public RiskDecisionEngine(
        IEnumerable<IRiskRule> rules,
        IRiskDecisionStore store,
        TimeProvider? timeProvider = null)
    {
        _rules = rules.ToArray();
        _store = store;
        _timeProvider = timeProvider ?? TimeProvider.System;
        if (_rules.Count == 0)
        {
            throw new ArgumentException("At least one risk rule is required.", nameof(rules));
        }
    }

    public static RiskDecisionEngine CreateDefault(
        IRiskDecisionStore? store = null,
        TimeProvider? timeProvider = null) =>
        new(
            [
                new HighAmountRule(),
                new VelocityRule(),
                new DeviceBurstRule(),
                new CountryChangeRule(),
            ],
            store ?? new InMemoryRiskDecisionStore(),
            timeProvider);

    public async ValueTask<DecisionResult> DecideAsync(
        RiskEvent riskEvent,
        CancellationToken cancellationToken = default)
    {
        Validate(riskEvent);
        return await _store.ExecuteAccountTransactionAsync(
            riskEvent.AccountId,
            riskEvent.EventId,
            async (session, token) =>
            {
                var existing = await session.GetDecisionAsync(riskEvent.EventId, token)
                    .ConfigureAwait(false);
                if (existing is not null)
                {
                    if (!string.Equals(existing.AccountId, riskEvent.AccountId, StringComparison.Ordinal))
                    {
                        throw new InvalidOperationException(
                            $"Event ID {riskEvent.EventId} is already owned by another account.");
                    }

                    return new DecisionResult(existing, Duplicate: true);
                }

                var history = await session.GetAccountEventsAsync(
                    riskEvent.AccountId,
                    riskEvent.OccurredAt - Retention,
                    riskEvent.OccurredAt,
                    token).ConfigureAwait(false);
                var features = BuildFeatures(history.Append(riskEvent), riskEvent.OccurredAt);
                var hits = _rules
                    .Select(rule => rule.Evaluate(riskEvent, features))
                    .Where(hit => hit is not null)
                    .Cast<RuleHit>()
                    .OrderByDescending(hit => hit.Score)
                    .ThenBy(hit => hit.Code, StringComparer.Ordinal)
                    .ToArray();
                var score = Math.Min(100, hits.Sum(hit => hit.Score));
                var action = score >= 70
                    ? RiskAction.Block
                    : score >= 35
                        ? RiskAction.Review
                        : RiskAction.Allow;
                var decision = new RiskDecision(
                    riskEvent.EventId,
                    riskEvent.AccountId,
                    action,
                    score,
                    features,
                    hits,
                    _timeProvider.GetUtcNow());
                var outboxMessage = new OutboxMessage(
                    Guid.NewGuid(),
                    riskEvent.EventId,
                    "risk.decision.v1",
                    JsonSerializer.Serialize(decision, OutboxJsonOptions),
                    decision.DecidedAt);
                await session.SaveAsync(riskEvent, decision, outboxMessage, token)
                    .ConfigureAwait(false);
                return new DecisionResult(decision, Duplicate: false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<RiskDecision?> GetDecisionAsync(
        string eventId,
        CancellationToken cancellationToken = default) =>
        _store.GetDecisionAsync(eventId, cancellationToken);

    private static FeatureSnapshot BuildFeatures(
        IEnumerable<RiskEvent> history,
        DateTimeOffset currentTime)
    {
        var retained = history
            .Where(item => item.OccurredAt <= currentTime)
            .ToArray();
        var fiveMinutes = retained
            .Where(item => item.OccurredAt >= currentTime - TimeSpan.FromMinutes(5))
            .ToArray();
        var oneHour = retained
            .Where(item => item.OccurredAt >= currentTime - TimeSpan.FromHours(1));

        return new FeatureSnapshot(
            fiveMinutes.Length,
            fiveMinutes.Sum(item => item.Amount),
            oneHour.Select(item => item.DeviceId).Distinct(StringComparer.Ordinal).Count(),
            retained.Select(item => item.Country).Distinct(StringComparer.OrdinalIgnoreCase).Count());
    }

    private static void Validate(RiskEvent riskEvent)
    {
        if (string.IsNullOrWhiteSpace(riskEvent.EventId) ||
            string.IsNullOrWhiteSpace(riskEvent.AccountId) ||
            string.IsNullOrWhiteSpace(riskEvent.DeviceId) ||
            string.IsNullOrWhiteSpace(riskEvent.Currency) ||
            string.IsNullOrWhiteSpace(riskEvent.Country))
        {
            throw new ArgumentException("Event identity and location fields must not be empty.");
        }
        if (riskEvent.Amount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(riskEvent), "Amount must be positive.");
        }
    }
}
