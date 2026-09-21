using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http.Json;

var baseUrl = args.ElementAtOrDefault(0) ?? "http://127.0.0.1:8080";
var requestCount = ParsePositive(args.ElementAtOrDefault(1), 1_000);
var concurrency = ParsePositive(args.ElementAtOrDefault(2), 32);
var latencies = new ConcurrentBag<double>();
var errors = 0;
var next = -1;
using var client = new HttpClient { BaseAddress = new Uri(baseUrl), Timeout = TimeSpan.FromSeconds(10) };

Console.WriteLine($"Target={baseUrl}, requests={requestCount}, concurrency={concurrency}");
var total = Stopwatch.StartNew();
await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async worker =>
{
    while (true)
    {
        var index = Interlocked.Increment(ref next);
        if (index >= requestCount) return;

        var request = new
        {
            eventId = $"load-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{worker}-{index}",
            accountId = $"account-{index % Math.Max(concurrency * 4, 1)}",
            deviceId = $"device-{index % 23}",
            amount = index % 17 == 0 ? 15_000m : 120m,
            currency = "CNY",
            country = index % 19 == 0 ? "US" : "CN",
            occurredAt = DateTimeOffset.UtcNow,
        };
        var elapsed = Stopwatch.StartNew();
        try
        {
            using var response = await client.PostAsJsonAsync("/v1/decisions", request);
            if (!response.IsSuccessStatusCode) Interlocked.Increment(ref errors);
        }
        catch
        {
            Interlocked.Increment(ref errors);
        }
        finally
        {
            latencies.Add(elapsed.Elapsed.TotalMilliseconds);
        }
    }
}));
total.Stop();

var values = latencies.Order().ToArray();
Console.WriteLine($"Completed={values.Length}, errors={errors}, errorRate={(double)errors / Math.Max(values.Length, 1):P2}");
Console.WriteLine($"Throughput={values.Length / Math.Max(total.Elapsed.TotalSeconds, 0.001):F1} req/s");
Console.WriteLine($"p50={Percentile(values, .50):F2} ms, p95={Percentile(values, .95):F2} ms, p99={Percentile(values, .99):F2} ms");
Environment.ExitCode = errors == 0 ? 0 : 1;

static int ParsePositive(string? value, int fallback) =>
    int.TryParse(value, out var parsed) && parsed > 0 ? parsed : fallback;

static double Percentile(double[] values, double quantile) =>
    values.Length == 0 ? 0d : values[Math.Clamp((int)Math.Ceiling(values.Length * quantile) - 1, 0, values.Length - 1)];
