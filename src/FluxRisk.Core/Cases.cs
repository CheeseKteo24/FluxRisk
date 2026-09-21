namespace FluxRisk.Core;

public enum ReviewCaseStatus
{
    Open,
    Approved,
    Rejected,
}

public sealed record ReviewCase(
    Guid Id,
    string EventId,
    string AccountId,
    RiskAction SuggestedAction,
    int Score,
    ReviewCaseStatus Status,
    string? Assignee,
    string? Notes,
    int Version,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);

public sealed record ResolveCaseCommand(
    ReviewCaseStatus Status,
    string Assignee,
    string? Notes,
    int ExpectedVersion);

public interface IReviewCaseStore
{
    ValueTask<IReadOnlyList<ReviewCase>> ListCasesAsync(
        ReviewCaseStatus? status,
        int limit,
        CancellationToken cancellationToken = default);

    ValueTask<ReviewCase?> ResolveCaseAsync(
        Guid id,
        ResolveCaseCommand command,
        CancellationToken cancellationToken = default);
}

public interface IReplaySource
{
    ValueTask<IReadOnlyList<RiskEvent>> GetEventsForReplayAsync(
        string accountId,
        CancellationToken cancellationToken = default);
}
