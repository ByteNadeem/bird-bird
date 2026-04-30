# Migration Timing Shift Summary

- Generated at: 2026-04-30 12:18:25 UTC
- Database: C:\vs-projects\bird-bird\backend\database\migration.db
- Rows used in analysis: 73
- Species count: 1
- Deployment groups: 23
- Formula: day_of_year ~ centered_year + tmean_c_roll14 + precip_mm_roll14
- Models requested: both
- Models succeeded: ols

## Key Notes

- This analysis is observational and should not be interpreted as causal climate proof.
- Mixed-effects model uses deployment_id random intercept to reduce repeated-measure bias.
- If climate variables are included, they are currently joined by date and require separate spatial QA.

## Selected Effects

| Model | Term | Estimate | p-value |
|---|---|---:|---:|
| ols | centered_year | -3.721732 | 0.00096738 |
