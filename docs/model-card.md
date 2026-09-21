# Model card: logistic-baseline-v1

## Intended use

This deterministic scorer demonstrates how a model is integrated, versioned, audited, shadowed, replayed, and rolled back inside a risk system. It is suitable for engineering demonstrations and contract tests. It is not trained or validated for production fraud prevention.

## Inputs and output

Inputs are transaction amount plus five-minute transaction count, one-hour distinct devices, and 24-hour distinct countries. A fixed logistic function produces a probability and a 0–30 score contribution. The model uses no protected attributes.

## Deployment modes

- `Shadow` (default): persist probability/version/contribution, but do not affect the decision.
- `Assist`: add the contribution to rule score. Use only after offline evaluation and approval.

## Known limitations

Coefficients are hand-authored, not learned from labeled fraud. There is no proof of discrimination, calibration, robustness, subgroup parity, or resistance to adversarial adaptation. Window features may also drift as customer behavior changes.

## Required promotion evidence

PR-AUC, recall at the allowed review rate, calibration curve/error, subgroup false-positive rates, temporal holdout performance, feature-distribution drift, load latency, a shadow disagreement report, and written rollback thresholds.
