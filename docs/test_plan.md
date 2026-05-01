# Testing Plan

## User stories
1. As an end user, I want to access the site and view migration content so I can plan birdwatching trips.
2. As an admin, I want to securely run backend data pipelines and validation checks so the site data stays accurate.
3. As a system manager, I want to verify reliability and performance so the service remains stable for users.

## Testing types and strategies
- Functional testing: validates core user and admin workflows (site access, species selection, model runs).
- Data quality testing: checks for duplicates, invalid coordinates, and timestamp formats in cleaned data.
- Usability testing: confirms navigation and error messages are clear and understandable.
- Performance testing: checks model runtime with full datasets.
- Security testing: ensures admin tasks are local-only and no sensitive data is exposed or committed.
- Compatibility testing: confirms the backend API responds correctly in the target local environment.

## Test plan
| Test Number | Test Type     | Description                                       | Test Data/Test Step                                                                 | Expected Result                                      | Actual Result / Status | Automation |
|------------:|--------------|---------------------------------------------------|--------------------------------------------------------------------------------------|------------------------------------------------------|------------------------|-----------|
| 1           | Usability     | User can access homepage and view content         | Run `pytest tests/test_api_endpoints.py::test_ui_homepage_served` or open site        | Homepage loads, content visible                      | TBD                    | Automated |
| 2           | Functional    | User can select a species from dropdown           | Manual: choose species in dropdown; API: `pytest tests/test_api_endpoints.py::test_api_visualization_bundle` | Map + chart update to chosen species                 | TBD                    | Mixed     |
| 3           | Security      | No sensitive data exposed to user                 | Inspect page source and network calls                                                 | Only public endpoints visible                         | TBD                    | Manual    |
| 4           | Functional    | Admin can run migration DB build                  | Run `make init-migration-db`                                                         | DB created or replaced successfully                   | TBD                    | Manual    |
| 5           | Data Quality  | Admin can validate cleaned observations           | Run `make test` (test_module_2)                                                       | No duplicates, valid timestamps                       | TBD                    | Automated |
| 6           | Security      | Admin tasks restricted to local environment       | Attempt admin scripts outside repo or without DB                                      | Script fails safely, no data leakage                  | TBD                    | Manual    |
| 7           | Functional    | Admin can run models successfully                 | Run `make story5-models`                                                              | Outputs generated in docs/ and plots                  | TBD                    | Manual    |
| 8           | Usability     | Admin error messages are understandable           | Run admin task with missing input                                                     | Clear error message displayed                         | TBD                    | Manual    |
| 9           | Performance   | System handles full dataset without crashing      | Run scripts with full dataset                                                         | Scripts complete without crash                        | TBD                    | Manual    |
| 10          | Reliability   | Automated tests pass consistently                 | Run `make test` twice                                                                 | All tests pass consistently                           | TBD                    | Manual    |
| 11          | Security      | Data files not accidentally committed             | Run `git status` after data generation                                                | Large generated files remain ignored                  | TBD                    | Manual    |
| 12          | Compatibility | Backend API runs locally as expected              | Run `pytest tests/test_api_endpoints.py::test_api_species_limit`                      | API responds with 200 and JSON                        | TBD                    | Automated |

## Sample admin test steps
- Migration DB build:
  1. Open terminal in project root.
  2. Run `make init-migration-db`.
  3. Confirm `backend/database/migration.db` exists.
  4. Record console output and screenshot.

- Model run:
  1. Ensure migration DB exists.
  2. Run `make story5-models`.
  3. Confirm outputs in `docs/` and plots in `backend/data/plots/`.

## Sample system management test steps
- Reliability check:
  1. Run `make test`.
  2. Run `make test` again.
  3. Confirm both runs pass with no errors.

- Performance check:
  1. Run `make story5-models` on the full dataset.
  2. Record runtime and confirm process completes.

## Evidence checklist
- Screenshot of `make test` output (Test 11)
- Screenshot of homepage load (Test 1)
- Screenshot of species selection updating charts (Test 2)
- Screenshot of `make story5-models` output (Test 8)
- Screenshot of `git status` confirming ignored data files (Test 12)
- Screenshot of `/api/species` response (Test 12)
