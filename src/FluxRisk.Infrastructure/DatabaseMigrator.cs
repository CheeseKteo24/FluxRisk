using System.Reflection;
using Npgsql;

namespace FluxRisk.Infrastructure;

public sealed class DatabaseMigrator(NpgsqlDataSource dataSource)
{
    private const long MigrationLockId = 2_046_865_915;

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        await using (var lockCommand = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(@lock_id);",
            connection,
            transaction))
        {
            lockCommand.Parameters.AddWithValue("lock_id", MigrationLockId);
            await lockCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using (var bootstrap = new NpgsqlCommand(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version text PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT now()
            );
            """,
            connection,
            transaction))
        {
            await bootstrap.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var assembly = typeof(DatabaseMigrator).Assembly;
        var resources = assembly.GetManifestResourceNames()
            .Where(name => name.Contains(".Migrations.", StringComparison.Ordinal) &&
                           name.EndsWith(".sql", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal)
            .ToArray();

        foreach (var resource in resources)
        {
            var version = resource[(resource.LastIndexOf(".Migrations.", StringComparison.Ordinal) + 12)..^4];
            if (await IsAppliedAsync(connection, transaction, version, cancellationToken)
                .ConfigureAwait(false))
            {
                continue;
            }

            await using var stream = assembly.GetManifestResourceStream(resource)
                ?? throw new InvalidOperationException($"Embedded migration {resource} was not found.");
            using var reader = new StreamReader(stream);
            var sql = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
            await using (var migration = new NpgsqlCommand(sql, connection, transaction))
            {
                await migration.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await using var record = new NpgsqlCommand(
                "INSERT INTO schema_migrations (version) VALUES (@version);",
                connection,
                transaction);
            record.Parameters.AddWithValue("version", version);
            await record.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<bool> IsAppliedAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string version,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = @version);",
            connection,
            transaction);
        command.Parameters.AddWithValue("version", version);
        return (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)
            ?? false);
    }
}
