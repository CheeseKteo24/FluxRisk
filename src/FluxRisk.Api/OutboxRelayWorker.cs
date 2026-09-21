using FluxRisk.Core;

namespace FluxRisk.Api;

public sealed class OutboxRelayWorker(
    OutboxRelay relay,
    ILogger<OutboxRelayWorker> logger) : BackgroundService
{
    private readonly string _workerId = $"{Environment.MachineName}-{Guid.NewGuid():N}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processed = await relay.RunOnceAsync(_workerId, stoppingToken)
                    .ConfigureAwait(false);
                await Task.Delay(
                    processed == 0 ? TimeSpan.FromSeconds(1) : TimeSpan.FromMilliseconds(50),
                    stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception error)
            {
                logger.LogError(error, "Outbox relay iteration failed.");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken).ConfigureAwait(false);
            }
        }
    }
}
