namespace FluxRisk.Core;

public sealed record EventTimeOptions(
    TimeSpan AllowedLateness,
    TimeSpan FutureTolerance)
{
    public static EventTimeOptions Default { get; } = new(
        TimeSpan.FromMinutes(2),
        TimeSpan.FromMinutes(1));
}

public sealed class LateEventException(
    string eventId,
    DateTimeOffset occurredAt,
    DateTimeOffset watermark)
    : InvalidOperationException(
        $"Event {eventId} occurred at {occurredAt:O}, before watermark {watermark:O}.")
{
    public string EventId { get; } = eventId;
    public DateTimeOffset OccurredAt { get; } = occurredAt;
    public DateTimeOffset Watermark { get; } = watermark;
}

public sealed record ReplayDecision(
    string EventId,
    RiskAction Action,
    int Score,
    FeatureSnapshot Features,
    IReadOnlyList<RuleHit> RuleHits,
    ModelAssessment? Model);

public sealed class RiskReplayService(IRiskModel? model = null)
{
    public async Task<IReadOnlyList<ReplayDecision>> ReplayAsync(
        IEnumerable<RiskEvent> events,
        CancellationToken cancellationToken = default)
    {
        var ordered = events
            .OrderBy(item => item.OccurredAt)
            .ThenBy(item => item.EventId, StringComparer.Ordinal)
            .ToArray();
        var store = new InMemoryRiskDecisionStore();
        var engine = RiskDecisionEngine.CreateDefault(
            store,
            model: model,
            eventTimeOptions: new EventTimeOptions(TimeSpan.MaxValue, TimeSpan.MaxValue));
        var results = new List<ReplayDecision>(ordered.Length);
        foreach (var riskEvent in ordered)
        {
            var result = await engine.DecideAsync(riskEvent, cancellationToken).ConfigureAwait(false);
            results.Add(new ReplayDecision(
                result.Decision.EventId,
                result.Decision.Action,
                result.Decision.Score,
                result.Decision.Features,
                result.Decision.RuleHits,
                result.Decision.Model));
        }

        return results;
    }
}
