namespace FluxRisk.Core;

public sealed record OutboxDelivery(
    Guid Id,
    string AggregateId,
    string Type,
    string Payload,
    DateTimeOffset OccurredAt,
    int Attempt);

public interface IOutboxStore
{
    ValueTask<IReadOnlyList<OutboxDelivery>> ClaimAsync(
        string workerId,
        int batchSize,
        TimeSpan lease,
        CancellationToken cancellationToken = default);

    ValueTask MarkPublishedAsync(
        Guid id,
        string brokerMetadata,
        CancellationToken cancellationToken = default);

    ValueTask MarkFailedAsync(
        Guid id,
        string error,
        DateTimeOffset availableAt,
        bool deadLetter,
        CancellationToken cancellationToken = default);
}

public interface IMessagePublisher
{
    ValueTask<string> PublishAsync(
        OutboxDelivery message,
        CancellationToken cancellationToken = default);
}

public sealed record OutboxRelayOptions(
    int BatchSize = 50,
    int MaxAttempts = 5,
    TimeSpan? Lease = null,
    TimeSpan? MaximumBackoff = null)
{
    public TimeSpan EffectiveLease => Lease ?? TimeSpan.FromSeconds(30);
    public TimeSpan EffectiveMaximumBackoff => MaximumBackoff ?? TimeSpan.FromMinutes(5);
}

public sealed class OutboxRelay(
    IOutboxStore store,
    IMessagePublisher publisher,
    TimeProvider? timeProvider = null,
    OutboxRelayOptions? options = null)
{
    private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
    private readonly OutboxRelayOptions _options = options ?? new OutboxRelayOptions();

    public async Task<int> RunOnceAsync(
        string workerId,
        CancellationToken cancellationToken = default)
    {
        var messages = await store.ClaimAsync(
            workerId,
            _options.BatchSize,
            _options.EffectiveLease,
            cancellationToken).ConfigureAwait(false);
        foreach (var message in messages)
        {
            try
            {
                var metadata = await publisher.PublishAsync(message, cancellationToken)
                    .ConfigureAwait(false);
                await store.MarkPublishedAsync(message.Id, metadata, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception error) when (error is not OperationCanceledException)
            {
                var deadLetter = message.Attempt >= _options.MaxAttempts;
                var seconds = Math.Min(
                    Math.Pow(2d, Math.Min(message.Attempt, 20)),
                    _options.EffectiveMaximumBackoff.TotalSeconds);
                await store.MarkFailedAsync(
                    message.Id,
                    error.Message,
                    _timeProvider.GetUtcNow() + TimeSpan.FromSeconds(seconds),
                    deadLetter,
                    cancellationToken).ConfigureAwait(false);
            }
        }

        return messages.Count;
    }
}
