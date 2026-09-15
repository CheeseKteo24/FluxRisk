using System.Collections.Concurrent;

namespace FluxRisk.Core;

public sealed class RiskDecisionEngine
{
    private static readonly TimeSpan Retention = TimeSpan.FromHours(24);
    private readonly IReadOnlyList<IRiskRule> _rules;
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _accountLocks = new();
    private readonly ConcurrentDictionary<string, string> _eventOwners = new();
    private readonly ConcurrentDictionary<string, RiskDecision> _decisions = new();
    private readonly ConcurrentDictionary<string, List<RiskEvent>> _eventsByAccount = new();

    public RiskDecisionEngine(
        IEnumerable<IRiskRule> rules,
        TimeProvider? timeProvider = null)
    {
        _rules = rules.ToArray();
        _timeProvider = timeProvider ?? TimeProvider.System;
        if (_rules.Count == 0)
        {
            throw new ArgumentException("At least one risk rule is required.", nameof(rules));
        }
    }

    public static RiskDecisionEngine CreateDefault(TimeProvider? timeProvider = null) =>
        new(
            [
                new HighAmountRule(),
                new VelocityRule(),
                new DeviceBurstRule(),
                new CountryChangeRule(),
            ],
            timeProvider);

    public async ValueTask<DecisionResult> DecideAsync(
        RiskEvent riskEvent,
        CancellationToken cancellationToken = default)
    {
        Validate(riskEvent);
        var owner = _eventOwners.GetOrAdd(riskEvent.EventId, riskEvent.AccountId);
        if (!string.Equals(owner, riskEvent.AccountId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Event ID {riskEvent.EventId} is already owned by another account.");
        }

        var gate = _accountLocks.GetOrAdd(riskEvent.AccountId, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_decisions.TryGetValue(riskEvent.EventId, out var existing))
            {
                return new DecisionResult(existing, Duplicate: true);
            }

            var history = _eventsByAccount.GetOrAdd(riskEvent.AccountId, _ => []);
            history.RemoveAll(item => item.OccurredAt < riskEvent.OccurredAt - Retention);
            history.Add(riskEvent);
            var features = BuildFeatures(history, riskEvent.OccurredAt);
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
            _decisions[riskEvent.EventId] = decision;
            return new DecisionResult(decision, Duplicate: false);
        }
        finally
        {
            gate.Release();
        }
    }

    public bool TryGetDecision(string eventId, out RiskDecision? decision) =>
        _decisions.TryGetValue(eventId, out decision);

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
