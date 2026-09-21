namespace FluxRisk.Core;

public sealed record RiskEvent(
    string EventId,
    string AccountId,
    string DeviceId,
    decimal Amount,
    string Currency,
    string Country,
    DateTimeOffset OccurredAt);

public enum RiskAction
{
    Allow,
    Review,
    Block,
}

public sealed record FeatureSnapshot(
    int TransactionCount5Minutes,
    decimal TransactionAmount5Minutes,
    int DistinctDevices1Hour,
    int DistinctCountries24Hours);

public sealed record RuleHit(
    string Code,
    int Score,
    string Explanation);

public sealed record RiskDecision(
    string EventId,
    string AccountId,
    RiskAction Action,
    int Score,
    FeatureSnapshot Features,
    IReadOnlyList<RuleHit> RuleHits,
    DateTimeOffset DecidedAt,
    EventTimeAssessment? EventTime = null,
    ModelAssessment? Model = null);

public sealed record DecisionResult(RiskDecision Decision, bool Duplicate);

public sealed record OutboxMessage(
    Guid Id,
    string AggregateId,
    string Type,
    string Payload,
    DateTimeOffset OccurredAt);

public sealed record EventTimeAssessment(
    DateTimeOffset Watermark,
    bool OutOfOrder,
    TimeSpan AllowedLateness);

public sealed record ModelAssessment(
    string Version,
    double Probability,
    int ScoreContribution,
    bool ShadowMode);
