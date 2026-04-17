# Migration Timing Shift Summary

- Generated at: 2026-04-17 12:27:49 UTC
- Database: C:\vs-projects\bird-bird\backend\database\migration.db
- Rows used in analysis: 75
- Species count: 1
- Deployment groups: 23
- Formula: day_of_year ~ centered_year
- Models requested: both
- Models succeeded: ols, mixed

## Key Notes

- This analysis is observational and should not be interpreted as causal climate proof.
- Mixed-effects model uses deployment_id random intercept to reduce repeated-measure bias.
- If climate variables are included, they are currently joined by date and require separate spatial QA.

## Selected Effects

| Model | Term | Estimate | p-value |
|---|---|---:|---:|
| ols | centered_year | -12.070837 | 4.33689e-23 |
| mixed | centered_year | -0.988064 | 0.922706 |
