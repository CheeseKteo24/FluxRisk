using System.Text.Json;
using FluxRisk.Core;
using Npgsql;

namespace FluxRisk.Infrastructure;

public sealed class PostgresRiskDecisionStore(NpgsqlDataSource dataSource) :
    IRiskDecisionStore,
    IOutboxStore,
    IReviewCaseStore,
    IReplaySource
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

    public async ValueTask<IReadOnlyList<RiskEvent>> GetEventsForReplayAsync(
        string accountId,
        CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        return await Session.GetAccountEventsAsync(
            connection,
            null,
            accountId,
            DateTimeOffset.MinValue,
            DateTimeOffset.MaxValue,
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<OutboxDelivery>> ClaimAsync(
        string workerId,
        int batchSize,
        TimeSpan lease,
        CancellationToken cancellationToken = default)
    {
        const string sql = """
            WITH candidates AS (
                SELECT id
                FROM outbox_messages
                WHERE published_at IS NULL
                  AND dead_lettered_at IS NULL
                  AND available_at <= now()
                  AND (locked_until IS NULL OR locked_until < now())
                ORDER BY occurred_at
                FOR UPDATE SKIP LOCKED
                LIMIT @batch_size
            )
            UPDATE outbox_messages AS message
            SET locked_by = @worker_id,
                locked_until = now() + @lease,
                attempts = attempts + 1
            FROM candidates
            WHERE message.id = candidates.id
            RETURNING message.id, message.aggregate_id, message.message_type,
                      message.payload::text, message.occurred_at, message.attempts;
            """;
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("batch_size", Math.Clamp(batchSize, 1, 500));
        command.Parameters.AddWithValue("worker_id", workerId);
        command.Parameters.AddWithValue("lease", lease);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);
        var messages = new List<OutboxDelivery>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            messages.Add(new OutboxDelivery(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                new DateTimeOffset(reader.GetDateTime(4)),
                reader.GetInt32(5)));
        }

        await reader.DisposeAsync().ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return messages;
    }

    public async ValueTask MarkPublishedAsync(
        Guid id,
        string brokerMetadata,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("""
            UPDATE outbox_messages
            SET published_at = now(), broker_metadata = @metadata,
                locked_by = NULL, locked_until = NULL, last_error = NULL
            WHERE id = @id;
            """);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("metadata", brokerMetadata);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask MarkFailedAsync(
        Guid id,
        string error,
        DateTimeOffset availableAt,
        bool deadLetter,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("""
            UPDATE outbox_messages
            SET last_error = @error,
                available_at = @available_at,
                dead_lettered_at = CASE WHEN @dead_letter THEN now() ELSE dead_lettered_at END,
                locked_by = NULL,
                locked_until = NULL
            WHERE id = @id;
            """);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("error", error[..Math.Min(error.Length, 2000)]);
        command.Parameters.AddWithValue("available_at", availableAt.UtcDateTime);
        command.Parameters.AddWithValue("dead_letter", deadLetter);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<ReviewCase>> ListCasesAsync(
        ReviewCaseStatus? status,
        int limit,
        CancellationToken cancellationToken = default)
    {
        const string sql = """
            SELECT id, event_id, account_id, suggested_action, score, status,
                   assignee, notes, version, created_at, updated_at
            FROM review_cases
            WHERE (@status::text IS NULL OR status = @status)
            ORDER BY created_at DESC
            LIMIT @limit;
            """;
        await using var command = dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("status", status?.ToString().ToLowerInvariant() ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("limit", Math.Clamp(limit, 1, 200));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var cases = new List<ReviewCase>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            cases.Add(ReadCase(reader));
        }

        return cases;
    }

    public async ValueTask<ReviewCase?> ResolveCaseAsync(
        Guid id,
        ResolveCaseCommand resolve,
        CancellationToken cancellationToken = default)
    {
        const string sql = """
            UPDATE review_cases
            SET status = @status, assignee = @assignee, notes = @notes,
                version = version + 1, updated_at = now()
            WHERE id = @id AND version = @expected_version
            RETURNING id, event_id, account_id, suggested_action, score, status,
                      assignee, notes, version, created_at, updated_at;
            """;
        await using var command = dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("status", resolve.Status.ToString().ToLowerInvariant());
        command.Parameters.AddWithValue("assignee", resolve.Assignee);
        command.Parameters.AddWithValue("notes", resolve.Notes ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("expected_version", resolve.ExpectedVersion);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? ReadCase(reader)
            : null;
    }

    private static ReviewCase ReadCase(NpgsqlDataReader reader) =>
        new(
            reader.GetGuid(0),
            reader.GetString(1),
            reader.GetString(2),
            Enum.Parse<RiskAction>(reader.GetString(3), true),
            reader.GetInt32(4),
            Enum.Parse<ReviewCaseStatus>(reader.GetString(5), true),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.GetInt32(8),
            new DateTimeOffset(reader.GetDateTime(9)),
            new DateTimeOffset(reader.GetDateTime(10)));

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
            return await PostgresRiskDecisionStore.Session.GetAccountEventsAsync(
                connection,
                transaction,
                accountId,
                fromInclusive,
                throughInclusive,
                cancellationToken).ConfigureAwait(false);
        }

        internal static async ValueTask<IReadOnlyList<RiskEvent>> GetAccountEventsAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction? transaction,
            string accountId,
            DateTimeOffset fromInclusive,
            DateTimeOffset throughInclusive,
            CancellationToken cancellationToken)
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

        public async ValueTask<DateTimeOffset?> GetLatestEventTimeAsync(
            string accountId,
            CancellationToken cancellationToken = default)
        {
            await using var command = new NpgsqlCommand(
                "SELECT max(occurred_at) FROM risk_events WHERE account_id = @account_id;",
                connection,
                transaction);
            command.Parameters.AddWithValue("account_id", accountId);
            var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return result is null or DBNull ? null : new DateTimeOffset((DateTime)result);
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
            if (decision.Action is RiskAction.Review or RiskAction.Block)
            {
                await InsertReviewCaseAsync(decision, cancellationToken).ConfigureAwait(false);
            }
        }

        internal static async ValueTask<RiskDecision?> GetDecisionAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction? transaction,
            string eventId,
            CancellationToken cancellationToken)
        {
            const string sql = """
                SELECT event_id, account_id, action, score, features, rule_hits, decided_at,
                       event_time, model_assessment
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
            var eventTime = reader.IsDBNull(7)
                ? null
                : JsonSerializer.Deserialize<EventTimeAssessment>(reader.GetString(7), JsonOptions);
            var model = reader.IsDBNull(8)
                ? null
                : JsonSerializer.Deserialize<ModelAssessment>(reader.GetString(8), JsonOptions);
            return new RiskDecision(
                reader.GetString(0),
                reader.GetString(1),
                Enum.Parse<RiskAction>(reader.GetString(2), ignoreCase: true),
                reader.GetInt32(3),
                features,
                hits,
                new DateTimeOffset(reader.GetDateTime(6)),
                eventTime,
                model);
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
                    (event_id, account_id, action, score, features, rule_hits, decided_at,
                     event_time, model_assessment)
                VALUES
                    (@event_id, @account_id, @action, @score, @features::jsonb, @rule_hits::jsonb,
                     @decided_at, @event_time::jsonb, @model_assessment::jsonb);
                """;
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("event_id", decision.EventId);
            command.Parameters.AddWithValue("account_id", decision.AccountId);
            command.Parameters.AddWithValue("action", decision.Action.ToString().ToLowerInvariant());
            command.Parameters.AddWithValue("score", decision.Score);
            command.Parameters.AddWithValue("features", JsonSerializer.Serialize(decision.Features, JsonOptions));
            command.Parameters.AddWithValue("rule_hits", JsonSerializer.Serialize(decision.RuleHits, JsonOptions));
            command.Parameters.AddWithValue("decided_at", decision.DecidedAt.UtcDateTime);
            command.Parameters.AddWithValue(
                "event_time",
                decision.EventTime is null
                    ? (object)DBNull.Value
                    : JsonSerializer.Serialize(decision.EventTime, JsonOptions));
            command.Parameters.AddWithValue(
                "model_assessment",
                decision.Model is null
                    ? (object)DBNull.Value
                    : JsonSerializer.Serialize(decision.Model, JsonOptions));
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task InsertReviewCaseAsync(
            RiskDecision decision,
            CancellationToken cancellationToken)
        {
            const string sql = """
                INSERT INTO review_cases
                    (id, event_id, account_id, suggested_action, score, created_at, updated_at)
                VALUES
                    (@id, @event_id, @account_id, @suggested_action, @score, @created_at, @created_at);
                """;
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            command.Parameters.AddWithValue("id", Guid.NewGuid());
            command.Parameters.AddWithValue("event_id", decision.EventId);
            command.Parameters.AddWithValue("account_id", decision.AccountId);
            command.Parameters.AddWithValue("suggested_action", decision.Action.ToString().ToLowerInvariant());
            command.Parameters.AddWithValue("score", decision.Score);
            command.Parameters.AddWithValue("created_at", decision.DecidedAt.UtcDateTime);
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
