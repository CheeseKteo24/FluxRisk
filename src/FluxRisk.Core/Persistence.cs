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

    ValueTask SaveAsync(
        RiskEvent riskEvent,
        RiskDecision decision,
        OutboxMessage outboxMessage,
        CancellationToken cancellationToken = default);
}

public sealed class InMemoryRiskDecisionStore : IRiskDecisionStore
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _accountLocks = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _eventLocks = new();
    private readonly ConcurrentDictionary<string, RiskDecision> _decisions = new();
    private readonly ConcurrentDictionary<string, List<RiskEvent>> _eventsByAccount = new();
    private readonly ConcurrentDictionary<Guid, OutboxMessage> _outbox = new();

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
                var session = new Session(_decisions, _eventsByAccount, _outbox);
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

    private sealed class Session(
        ConcurrentDictionary<string, RiskDecision> decisions,
        ConcurrentDictionary<string, List<RiskEvent>> eventsByAccount,
        ConcurrentDictionary<Guid, OutboxMessage> outbox) : IRiskDecisionSession
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
            if (!outbox.TryAdd(outboxMessage.Id, outboxMessage))
            {
                throw new InvalidOperationException($"Outbox ID {outboxMessage.Id} already exists.");
            }

            return ValueTask.CompletedTask;
        }
    }
}
