using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using FluxRisk.Core;

namespace FluxRisk.Api;

public sealed class DecisionTelemetry
{
    private const int Capacity = 10_000;
    private readonly ConcurrentQueue<double> _latencies = new();
    private long _requests;
    private long _errors;
    private long _allow;
    private long _review;
    private long _block;

    public void Record(TimeSpan elapsed, RiskAction? action, bool error)
    {
        Interlocked.Increment(ref _requests);
        if (error) Interlocked.Increment(ref _errors);
        if (action is RiskAction.Allow) Interlocked.Increment(ref _allow);
        if (action is RiskAction.Review) Interlocked.Increment(ref _review);
        if (action is RiskAction.Block) Interlocked.Increment(ref _block);
        _latencies.Enqueue(elapsed.TotalMilliseconds);
        while (_latencies.Count > Capacity) _latencies.TryDequeue(out _);
    }

    public object Snapshot()
    {
        var samples = _latencies.Order().ToArray();
        return new
        {
            requests = Interlocked.Read(ref _requests),
            errors = Interlocked.Read(ref _errors),
            allow = Interlocked.Read(ref _allow),
            review = Interlocked.Read(ref _review),
            block = Interlocked.Read(ref _block),
            p50Ms = Percentile(samples, 0.50),
            p95Ms = Percentile(samples, 0.95),
            p99Ms = Percentile(samples, 0.99),
        };
    }

    public string ToPrometheus()
    {
        var samples = _latencies.Order().ToArray();
        var output = new StringBuilder();
        output.AppendLine($"fluxrisk_decisions_total {Interlocked.Read(ref _requests)}");
        output.AppendLine($"fluxrisk_decision_errors_total {Interlocked.Read(ref _errors)}");
        output.AppendLine($"fluxrisk_actions_total{{action=\"allow\"}} {Interlocked.Read(ref _allow)}");
        output.AppendLine($"fluxrisk_actions_total{{action=\"review\"}} {Interlocked.Read(ref _review)}");
        output.AppendLine($"fluxrisk_actions_total{{action=\"block\"}} {Interlocked.Read(ref _block)}");
        foreach (var quantile in new[] { 0.50, 0.95, 0.99 })
        {
            output.Append("fluxrisk_decision_latency_milliseconds{quantile=\"")
                .Append(quantile.ToString("0.00", CultureInfo.InvariantCulture))
                .Append("\"} ")
                .AppendLine(Percentile(samples, quantile).ToString("0.###", CultureInfo.InvariantCulture));
        }

        return output.ToString();
    }

    private static double Percentile(double[] values, double quantile)
    {
        if (values.Length == 0) return 0d;
        var index = (int)Math.Ceiling(quantile * values.Length) - 1;
        return values[Math.Clamp(index, 0, values.Length - 1)];
    }
}
