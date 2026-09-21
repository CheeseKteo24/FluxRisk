namespace FluxRisk.Core;

public interface IRiskModel
{
    ValueTask<ModelScore> ScoreAsync(
        RiskEvent riskEvent,
        FeatureSnapshot features,
        CancellationToken cancellationToken = default);
}

public sealed record ModelScore(string Version, double Probability, int ScoreContribution);

public enum ModelExecutionMode
{
    Shadow,
    Assist,
}

public sealed class CalibratedAnomalyModel : IRiskModel
{
    public ValueTask<ModelScore> ScoreAsync(
        RiskEvent riskEvent,
        FeatureSnapshot features,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var amount = Math.Log10(Math.Max(1d, (double)riskEvent.Amount));
        var logit = -8.2d
            + (1.25d * amount)
            + (0.52d * features.TransactionCount5Minutes)
            + (0.68d * features.DistinctDevices1Hour)
            + (0.44d * features.DistinctCountries24Hours);
        var probability = 1d / (1d + Math.Exp(-logit));
        var contribution = (int)Math.Round(probability * 30d, MidpointRounding.AwayFromZero);
        return ValueTask.FromResult(new ModelScore("logistic-baseline-v1", probability, contribution));
    }
}
