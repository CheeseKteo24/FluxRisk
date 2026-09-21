using System.Text.Json;
using System.Text.Json.Serialization;

namespace FluxRisk.Core;

public sealed class RiskDecisionEngine
{
    private static readonly TimeSpan Retention = TimeSpan.FromHours(24);
    private readonly IReadOnlyList<IRiskRule> _rules;
    private readonly IRiskDecisionStore _store;
    private readonly TimeProvider _timeProvider;
    private readonly IRiskModel _model;
    private readonly ModelExecutionMode _modelMode;
    private readonly EventTimeOptions _eventTimeOptions;
    private static readonly JsonSerializerOptions OutboxJsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
    };

    public RiskDecisionEngine(
        IEnumerable<IRiskRule> rules,
        IRiskDecisionStore store,
        TimeProvider? timeProvider = null,
        IRiskModel? model = null,
        ModelExecutionMode modelMode = ModelExecutionMode.Shadow,
        EventTimeOptions? eventTimeOptions = null)
    {
        _rules = rules.ToArray();
        _store = store;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _model = model ?? new CalibratedAnomalyModel();
        _modelMode = modelMode;
        _eventTimeOptions = eventTimeOptions ?? EventTimeOptions.Default;
        if (_rules.Count == 0)
        {
            throw new ArgumentException("At least one risk rule is required.", nameof(rules));
        }
    }

    public static RiskDecisionEngine CreateDefault(
        IRiskDecisionStore? store = null,
        TimeProvider? timeProvider = null,
        IRiskModel? model = null,
        ModelExecutionMode modelMode = ModelExecutionMode.Shadow,
        EventTimeOptions? eventTimeOptions = null) =>
        new(
            [
                new HighAmountRule(),
                new VelocityRule(),
                new DeviceBurstRule(),
                new CountryChangeRule(),
            ],
            store ?? new InMemoryRiskDecisionStore(),
            timeProvider,
            model,
            modelMode,
            eventTimeOptions);

    public async ValueTask<DecisionResult> DecideAsync(
        RiskEvent riskEvent,
        CancellationToken cancellationToken = default)
    {
        Validate(riskEvent);
        riskEvent = riskEvent with { OccurredAt = NormalizeTimestamp(riskEvent.OccurredAt) };
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

                var processingTime = NormalizeTimestamp(_timeProvider.GetUtcNow());
                if (_eventTimeOptions.FutureTolerance != TimeSpan.MaxValue &&
                    riskEvent.OccurredAt > processingTime + _eventTimeOptions.FutureTolerance)
                {
                    throw new ArgumentException(
                        $"Event {riskEvent.EventId} is too far in the future.",
                        nameof(riskEvent));
                }

                var latest = await session.GetLatestEventTimeAsync(riskEvent.AccountId, token)
                    .ConfigureAwait(false);
                var watermark = latest is null || _eventTimeOptions.AllowedLateness == TimeSpan.MaxValue
                    ? DateTimeOffset.MinValue
                    : latest.Value - _eventTimeOptions.AllowedLateness;
                if (riskEvent.OccurredAt < watermark)
                {
                    throw new LateEventException(riskEvent.EventId, riskEvent.OccurredAt, watermark);
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
                var modelScore = await _model.ScoreAsync(riskEvent, features, token)
                    .ConfigureAwait(false);
                var ruleScore = Math.Min(100, hits.Sum(hit => hit.Score));
                var score = _modelMode == ModelExecutionMode.Assist
                    ? Math.Min(100, ruleScore + modelScore.ScoreContribution)
                    : ruleScore;
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
                    processingTime,
                    new EventTimeAssessment(
                        watermark,
                        latest is not null && riskEvent.OccurredAt < latest,
                        _eventTimeOptions.AllowedLateness),
                    new ModelAssessment(
                        modelScore.Version,
                        modelScore.Probability,
                        modelScore.ScoreContribution,
                        _modelMode == ModelExecutionMode.Shadow));
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

    private static DateTimeOffset NormalizeTimestamp(DateTimeOffset timestamp)
    {
        var utc = timestamp.ToUniversalTime();
        return new DateTimeOffset(
            utc.Ticks - (utc.Ticks % TimeSpan.TicksPerMicrosecond),
            TimeSpan.Zero);
    }
}
