namespace FluxRisk.Core;

public interface IRiskRule
{
    RuleHit? Evaluate(RiskEvent riskEvent, FeatureSnapshot features);
}

public sealed class HighAmountRule(decimal threshold = 10_000m) : IRiskRule
{
    public RuleHit? Evaluate(RiskEvent riskEvent, FeatureSnapshot features)
    {
        _ = features;
        return riskEvent.Amount >= threshold
            ? new RuleHit(
                "HIGH_AMOUNT",
                45,
                $"Transaction amount {riskEvent.Amount} is at least {threshold}.")
            : null;
    }
}

public sealed class VelocityRule(int threshold = 5) : IRiskRule
{
    public RuleHit? Evaluate(RiskEvent riskEvent, FeatureSnapshot features)
    {
        _ = riskEvent;
        return features.TransactionCount5Minutes >= threshold
            ? new RuleHit(
                "HIGH_VELOCITY",
                70,
                $"Observed {features.TransactionCount5Minutes} transactions in five minutes.")
            : null;
    }
}

public sealed class DeviceBurstRule(int threshold = 3) : IRiskRule
{
    public RuleHit? Evaluate(RiskEvent riskEvent, FeatureSnapshot features)
    {
        _ = riskEvent;
        return features.DistinctDevices1Hour >= threshold
            ? new RuleHit(
                "DEVICE_BURST",
                35,
                $"Observed {features.DistinctDevices1Hour} devices in one hour.")
            : null;
    }
}

public sealed class CountryChangeRule(int threshold = 2) : IRiskRule
{
    public RuleHit? Evaluate(RiskEvent riskEvent, FeatureSnapshot features)
    {
        _ = riskEvent;
        return features.DistinctCountries24Hours >= threshold
            ? new RuleHit(
                "COUNTRY_CHANGE",
                30,
                $"Observed {features.DistinctCountries24Hours} countries in 24 hours.")
            : null;
    }
}
