# Migration Timing Shift Summary

- Generated at: 2026-04-22 15:29:52 UTC
- Database: C:\vs-projects\bird-bird\backend\database\migration.db
- Rows used in analysis: 75
- Species count: 1
- Deployment groups: 23
- Formula: day_of_year ~ centered_year + tmean_c
- Models requested: both
- Models succeeded: ols, mixed

## Key Notes

- This analysis is observational and should not be interpreted as causal climate proof.
- Mixed-effects model uses deployment_id random intercept to reduce repeated-measure bias.
- If climate variables are included, they are currently joined by date and require separate spatial QA.

## Selected Effects

| Model | Term | Estimate | p-value |
|---|---|---:|---:|
| ols | centered_year | -6.636377 | 1.31456e-05 |
| mixed | centered_year | 1.449875 | 0.87849 |
