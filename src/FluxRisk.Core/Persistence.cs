using System.Collections.Concurrent;

namespace FluxRisk.Core;

public interface IRiskDecisionStore
{
    ValueTask<T> ExecuteAccountTransactionAsync<T>(
        string accountId,
        string eventId,
        Func<IRiskDecisionSession, CancellationToken, ValueTask<T>> operation,
        CancellationToken cancellationToken = default);

    ValueTask<RiskDecision?> GetDecisionAsync(
        string eventId,
        CancellationToken cancellationToken = default);
}

public interface IRiskDecisionSession
{
    ValueTask<RiskDecision?> GetDecisionAsync(
        string eventId,
        CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<RiskEvent>> GetAccountEventsAsync(
        string accountId,
        DateTimeOffset fromInclusive,
        DateTimeOffset throughInclusive,
        CancellationToken cancellationToken = default);

    ValueTask<DateTimeOffset?> GetLatestEventTimeAsync(
        string accountId,
        CancellationToken cancellationToken = default);

    ValueTask SaveAsync(
        RiskEvent riskEvent,
        RiskDecision decision,
        OutboxMessage outboxMessage,
        CancellationToken cancellationToken = default);
}

public sealed class InMemoryRiskDecisionStore :
    IRiskDecisionStore,
    IOutboxStore,
    IReviewCaseStore,
    IReplaySource
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _accountLocks = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _eventLocks = new();
    private readonly ConcurrentDictionary<string, RiskDecision> _decisions = new();
    private readonly ConcurrentDictionary<string, List<RiskEvent>> _eventsByAccount = new();
    private readonly ConcurrentDictionary<Guid, OutboxEntry> _outbox = new();
    private readonly ConcurrentDictionary<Guid, ReviewCase> _cases = new();

    public async ValueTask<T> ExecuteAccountTransactionAsync<T>(
        string accountId,
        string eventId,
        Func<IRiskDecisionSession, CancellationToken, ValueTask<T>> operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        var eventGate = _eventLocks.GetOrAdd(eventId, _ => new SemaphoreSlim(1, 1));
        var accountGate = _accountLocks.GetOrAdd(accountId, _ => new SemaphoreSlim(1, 1));
        await eventGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await accountGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var session = new Session(_decisions, _eventsByAccount, _outbox, _cases);
                return await operation(session, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                accountGate.Release();
            }
        }
        finally
        {
            eventGate.Release();
        }
    }

    public ValueTask<RiskDecision?> GetDecisionAsync(
        string eventId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _decisions.TryGetValue(eventId, out var decision);
        return ValueTask.FromResult(decision);
    }

    public ValueTask<IReadOnlyList<RiskEvent>> GetEventsForReplayAsync(
        string accountId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        IReadOnlyList<RiskEvent> events = _eventsByAccount.TryGetValue(accountId, out var found)
            ? found.OrderBy(item => item.OccurredAt).ThenBy(item => item.EventId).ToArray()
            : [];
        return ValueTask.FromResult(events);
    }

    public ValueTask<IReadOnlyList<OutboxDelivery>> ClaimAsync(
        string workerId,
        int batchSize,
        TimeSpan lease,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var now = DateTimeOffset.UtcNow;
        var claimed = new List<OutboxDelivery>();
        foreach (var entry in _outbox.Values.OrderBy(item => item.Message.OccurredAt))
        {
            if (claimed.Count >= batchSize)
            {
                break;
            }

            lock (entry)
            {
                if (entry.Published || entry.DeadLettered || entry.AvailableAt > now ||
                    entry.LockedUntil > now)
                {
                    continue;
                }

                entry.Attempt++;
                entry.LockedUntil = now + lease;
                entry.LockedBy = workerId;
                claimed.Add(new OutboxDelivery(
                    entry.Message.Id,
                    entry.Message.AggregateId,
                    entry.Message.Type,
                    entry.Message.Payload,
                    entry.Message.OccurredAt,
                    entry.Attempt));
            }
        }

        return ValueTask.FromResult<IReadOnlyList<OutboxDelivery>>(claimed);
    }

    public ValueTask MarkPublishedAsync(
        Guid id,
        string brokerMetadata,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_outbox.TryGetValue(id, out var entry))
        {
            lock (entry)
            {
                entry.Published = true;
                entry.BrokerMetadata = brokerMetadata;
                entry.LockedUntil = null;
                entry.LockedBy = null;
            }
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask MarkFailedAsync(
        Guid id,
        string error,
        DateTimeOffset availableAt,
        bool deadLetter,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_outbox.TryGetValue(id, out var entry))
        {
            lock (entry)
            {
                entry.LastError = error;
                entry.AvailableAt = availableAt;
                entry.DeadLettered = deadLetter;
                entry.LockedUntil = null;
                entry.LockedBy = null;
            }
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<ReviewCase>> ListCasesAsync(
        ReviewCaseStatus? status,
        int limit,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        IReadOnlyList<ReviewCase> result = _cases.Values
            .Where(item => status is null || item.Status == status)
            .OrderByDescending(item => item.CreatedAt)
            .Take(Math.Clamp(limit, 1, 200))
            .ToArray();
        return ValueTask.FromResult(result);
    }

    public ValueTask<ReviewCase?> ResolveCaseAsync(
        Guid id,
        ResolveCaseCommand command,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        while (_cases.TryGetValue(id, out var current))
        {
            if (current.Version != command.ExpectedVersion)
            {
                throw new InvalidOperationException("Review case was modified by another reviewer.");
            }

            var updated = current with
            {
                Status = command.Status,
                Assignee = command.Assignee,
                Notes = command.Notes,
                Version = current.Version + 1,
                UpdatedAt = DateTimeOffset.UtcNow,
            };
            if (_cases.TryUpdate(id, updated, current))
            {
                return ValueTask.FromResult<ReviewCase?>(updated);
            }
        }

        return ValueTask.FromResult<ReviewCase?>(null);
    }

    private sealed class Session(
        ConcurrentDictionary<string, RiskDecision> decisions,
        ConcurrentDictionary<string, List<RiskEvent>> eventsByAccount,
        ConcurrentDictionary<Guid, OutboxEntry> outbox,
        ConcurrentDictionary<Guid, ReviewCase> cases) : IRiskDecisionSession
    {
        public ValueTask<RiskDecision?> GetDecisionAsync(
            string eventId,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            decisions.TryGetValue(eventId, out var decision);
            return ValueTask.FromResult(decision);
        }

        public ValueTask<IReadOnlyList<RiskEvent>> GetAccountEventsAsync(
            string accountId,
            DateTimeOffset fromInclusive,
            DateTimeOffset throughInclusive,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!eventsByAccount.TryGetValue(accountId, out var events))
            {
                return ValueTask.FromResult<IReadOnlyList<RiskEvent>>([]);
            }

            IReadOnlyList<RiskEvent> result = events
                .Where(item => item.OccurredAt >= fromInclusive && item.OccurredAt <= throughInclusive)
                .OrderBy(item => item.OccurredAt)
                .ToArray();
            return ValueTask.FromResult(result);
        }

        public ValueTask<DateTimeOffset?> GetLatestEventTimeAsync(
            string accountId,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            DateTimeOffset? latest = eventsByAccount.TryGetValue(accountId, out var events) && events.Count > 0
                ? events.Max(item => item.OccurredAt)
                : null;
            return ValueTask.FromResult(latest);
        }

        public ValueTask SaveAsync(
            RiskEvent riskEvent,
            RiskDecision decision,
            OutboxMessage outboxMessage,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!decisions.TryAdd(riskEvent.EventId, decision))
            {
                throw new InvalidOperationException($"Event ID {riskEvent.EventId} already exists.");
            }

            eventsByAccount.GetOrAdd(riskEvent.AccountId, _ => []).Add(riskEvent);
            if (!outbox.TryAdd(outboxMessage.Id, new OutboxEntry(outboxMessage)))
            {
                throw new InvalidOperationException($"Outbox ID {outboxMessage.Id} already exists.");
            }

            if (decision.Action is RiskAction.Review or RiskAction.Block)
            {
                var reviewCase = new ReviewCase(
                    Guid.NewGuid(),
                    decision.EventId,
                    decision.AccountId,
                    decision.Action,
                    decision.Score,
                    ReviewCaseStatus.Open,
                    null,
                    null,
                    1,
                    decision.DecidedAt,
                    decision.DecidedAt);
                cases.TryAdd(reviewCase.Id, reviewCase);
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class OutboxEntry(OutboxMessage message)
    {
        public OutboxMessage Message { get; } = message;
        public int Attempt { get; set; }
        public bool Published { get; set; }
        public bool DeadLettered { get; set; }
        public DateTimeOffset AvailableAt { get; set; } = message.OccurredAt;
        public DateTimeOffset? LockedUntil { get; set; }
        public string? LockedBy { get; set; }
        public string? LastError { get; set; }
        public string? BrokerMetadata { get; set; }
    }
}
