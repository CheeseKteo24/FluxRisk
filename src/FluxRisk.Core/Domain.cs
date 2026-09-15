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
    DateTimeOffset DecidedAt);

public sealed record DecisionResult(RiskDecision Decision, bool Duplicate);
