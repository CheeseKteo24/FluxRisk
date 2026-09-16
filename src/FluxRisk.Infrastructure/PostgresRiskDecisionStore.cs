using System.Text.Json;
using FluxRisk.Core;
using Npgsql;

namespace FluxRisk.Infrastructure;

public sealed class PostgresRiskDecisionStore(NpgsqlDataSource dataSource) : IRiskDecisionStore
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public async ValueTask<T> ExecuteAccountTransactionAsync<T>(
        string accountId,
        string eventId,
        Func<IRiskDecisionSession, CancellationToken, ValueTask<T>> operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            await AcquireLockAsync(connection, transaction, eventId, 0, cancellationToken)
                .ConfigureAwait(false);
            await AcquireLockAsync(connection, transaction, accountId, 1, cancellationToken)
                .ConfigureAwait(false);

            var result = await operation(
                new Session(connection, transaction),
                cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask<RiskDecision?> GetDecisionAsync(
        string eventId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        return await Session.GetDecisionAsync(connection, null, eventId, cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task AcquireLockAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string value,
        int seed,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(hashtextextended(@value, @seed));",
            connection,
            transaction);
        command.Parameters.AddWithValue("value", value);
        command.Parameters.AddWithValue("seed", seed);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private sealed class Session(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction) : IRiskDecisionSession
    {
        public ValueTask<RiskDecision?> GetDecisionAsync(
            string eventId,
            CancellationToken cancellationToken = default) =>
            PostgresRiskDecisionStore.Session.GetDecisionAsync(
                connection,
                transaction,
                eventId,
                cancellationToken);

        public async ValueTask<IReadOnlyList<RiskEvent>> GetAccountEventsAsync(
            string accountId,
            DateTimeOffset fromInclusive,
            DateTimeOffset throughInclusive,
            CancellationToken cancellationToken = default)
        {
            const string sql = """
                SELECT event_id, account_id, device_id, amount, currency, country, occurred_at
                FROM risk_events
                WHERE account_id = @account_id
                  AND occurred_at >= @from_inclusive
                  AND occurred_at <= @through_inclusive
                ORDER BY occurred_at, event_id;
                """;
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("account_id", accountId);
            command.Parameters.AddWithValue("from_inclusive", fromInclusive.UtcDateTime);
            command.Parameters.AddWithValue("through_inclusive", throughInclusive.UtcDateTime);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);
            var events = new List<RiskEvent>();
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                events.Add(new RiskEvent(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetDecimal(3),
                    reader.GetString(4),
                    reader.GetString(5),
                    new DateTimeOffset(reader.GetDateTime(6))));
            }

            return events;
        }

        public async ValueTask SaveAsync(
            RiskEvent riskEvent,
            RiskDecision decision,
            OutboxMessage outboxMessage,
            CancellationToken cancellationToken = default)
        {
            await InsertEventAsync(riskEvent, cancellationToken).ConfigureAwait(false);
            await InsertDecisionAsync(decision, cancellationToken).ConfigureAwait(false);
            await InsertOutboxAsync(outboxMessage, cancellationToken).ConfigureAwait(false);
        }

        internal static async ValueTask<RiskDecision?> GetDecisionAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction? transaction,
            string eventId,
            CancellationToken cancellationToken)
        {
            const string sql = """
                SELECT event_id, account_id, action, score, features, rule_hits, decided_at
                FROM risk_decisions
                WHERE event_id = @event_id;
                """;
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("event_id", eventId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                return null;
            }

            var features = JsonSerializer.Deserialize<FeatureSnapshot>(reader.GetString(4), JsonOptions)
                ?? throw new InvalidOperationException("Stored feature snapshot is invalid.");
            var hits = JsonSerializer.Deserialize<RuleHit[]>(reader.GetString(5), JsonOptions)
                ?? throw new InvalidOperationException("Stored rule hits are invalid.");
            return new RiskDecision(
                reader.GetString(0),
                reader.GetString(1),
                Enum.Parse<RiskAction>(reader.GetString(2), ignoreCase: true),
                reader.GetInt32(3),
                features,
                hits,
                new DateTimeOffset(reader.GetDateTime(6)));
        }

        private async Task InsertEventAsync(
            RiskEvent riskEvent,
            CancellationToken cancellationToken)
        {
            const string sql = """
                INSERT INTO risk_events
                    (event_id, account_id, device_id, amount, currency, country, occurred_at)
                VALUES
                    (@event_id, @account_id, @device_id, @amount, @currency, @country, @occurred_at);
                """;
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("event_id", riskEvent.EventId);
            command.Parameters.AddWithValue("account_id", riskEvent.AccountId);
            command.Parameters.AddWithValue("device_id", riskEvent.DeviceId);
            command.Parameters.AddWithValue("amount", riskEvent.Amount);
            command.Parameters.AddWithValue("currency", riskEvent.Currency);
            command.Parameters.AddWithValue("country", riskEvent.Country);
            command.Parameters.AddWithValue("occurred_at", riskEvent.OccurredAt.UtcDateTime);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task InsertDecisionAsync(
            RiskDecision decision,
            CancellationToken cancellationToken)
        {
            const string sql = """
                INSERT INTO risk_decisions
                    (event_id, account_id, action, score, features, rule_hits, decided_at)
                VALUES
                    (@event_id, @account_id, @action, @score, @features::jsonb, @rule_hits::jsonb, @decided_at);
                """;
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("event_id", decision.EventId);
            command.Parameters.AddWithValue("account_id", decision.AccountId);
            command.Parameters.AddWithValue("action", decision.Action.ToString().ToLowerInvariant());
            command.Parameters.AddWithValue("score", decision.Score);
            command.Parameters.AddWithValue("features", JsonSerializer.Serialize(decision.Features, JsonOptions));
            command.Parameters.AddWithValue("rule_hits", JsonSerializer.Serialize(decision.RuleHits, JsonOptions));
            command.Parameters.AddWithValue("decided_at", decision.DecidedAt.UtcDateTime);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task InsertOutboxAsync(
            OutboxMessage message,
            CancellationToken cancellationToken)
        {
            const string sql = """
                INSERT INTO outbox_messages
                    (id, aggregate_id, message_type, payload, occurred_at)
                VALUES
                    (@id, @aggregate_id, @message_type, @payload::jsonb, @occurred_at);
                """;
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("id", message.Id);
            command.Parameters.AddWithValue("aggregate_id", message.AggregateId);
            command.Parameters.AddWithValue("message_type", message.Type);
            command.Parameters.AddWithValue("payload", message.Payload);
            command.Parameters.AddWithValue("occurred_at", message.OccurredAt.UtcDateTime);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
