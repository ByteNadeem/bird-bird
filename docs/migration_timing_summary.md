# Migration Timing Shift Summary

- Generated at: 2026-05-02 17:13:11 UTC
- Database: C:\vs-projects\bird-bird\backend\database\migration.db
- Rows used in analysis: 126
- Species count: 2
- Deployment groups: 73
- Formula: day_of_year ~ centered_year + C(species_code) + centered_year:C(species_code)
- Models requested: both
- Models succeeded: ols

## Key Notes

- This analysis is observational and should not be interpreted as causal climate proof.
- Mixed-effects model uses deployment_id random intercept to reduce repeated-measure bias.
- If climate variables are included, they are currently joined by date and require separate spatial QA.

## Selected Effects

| Model | Term | Estimate | p-value |
|---|---|---:|---:|
| ols | C(species_code)[T.eurcur] | -19.720551 | 2.59354e-06 |
| ols | centered_year | -5.960454 | 3.11529e-22 |
| ols | centered_year:C(species_code)[T.eurcur] | -5.960454 | 3.11529e-22 |
