using System.Net.Http.Json;
using System.Text.Json;
using FluxRisk.Core;

namespace FluxRisk.Infrastructure;

public sealed class RedpandaHttpPublisher(HttpClient httpClient, string topic) : IMessagePublisher
{
    public async ValueTask<string> PublishAsync(
        OutboxDelivery message,
        CancellationToken cancellationToken = default)
    {
        var request = new
        {
            records = new[] { new { key = message.AggregateId, value = message.Payload } },
        };
        using var response = await httpClient.PostAsJsonAsync(
            $"topics/{Uri.EscapeDataString(topic)}",
            request,
            cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken)
            .ConfigureAwait(false);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        var offset = document.RootElement.GetProperty("offsets")[0];
        return $"partition={offset.GetProperty("partition").GetInt32()};offset={offset.GetProperty("offset").GetInt64()}";
    }
}
