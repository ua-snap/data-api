---
name: add-tests
description: Add and run automated tests for agent-created features and bug fixes in UA SNAP repositories. Use when implementing or changing Python application behavior, JavaScript/TypeScript browser behavior, APIs, or user-facing workflows; choose Pytest, Playwright, or both according to the behavior changed.
---

# UA SNAP Add Tests

Deliver code changes with meaningful automated coverage. Treat the test as part of the requested feature or bug fix, not as optional follow-up work.

## Inspect the repository

Before editing, read the repository's `AGENTS.md` and relevant contributor documentation. Inspect nearby source and tests, dependency files, test configuration, fixtures, CI workflows, and package scripts. Follow the repository's established naming, location, commands, and test style.

Do not introduce a duplicate framework when the repository already has an appropriate way to test the changed behavior. Repository-specific instructions override this skill.

## Select coverage

- Use Pytest for Python functions, services, data processing, Flask/FastAPI routes, and Python integrations.
- Use Playwright for browser-visible JavaScript or TypeScript behavior, navigation, forms, maps, accessibility interactions, and end-to-end workflows.
- Use both when a feature changes independently meaningful Python backend behavior and browser behavior.
- For a bug fix, add a regression test that fails because of the bug before relying on the fix.
- For a refactor with no intended behavior change, preserve or improve coverage of the behavior at risk.

If the repository uses a different established test runner for the relevant layer, extend it instead of forcing Pytest or Playwright. Prefer the narrowest test level that proves behavior through a stable public boundary.

## Write durable tests

Test observable behavior and user outcomes rather than implementation details. Cover the primary success path and important failure, boundary, permission, empty-data, or state-transition cases introduced by the change.

Keep tests deterministic and isolated:

- Reuse existing fixtures, factories, helpers, and Playwright page objects.
- Mock only external systems, nondeterministic inputs, or boundaries that the test is not meant to exercise.
- Do not call live production services or depend on real credentials.
- For Playwright, prefer accessible role, label, placeholder, and visible-text locators. Add a stable test ID only when no user-facing locator is reliable.
- Avoid fixed sleeps. Wait for a visible condition, URL, response, or application state.
- Do not weaken assertions merely to make a test pass.

When no suitable test infrastructure exists, add the smallest conventional setup needed for the selected framework, including a discoverable command. Avoid unrelated dependency or configuration changes.

## Verify

Run the new or changed test first. Then run the closest relevant suite and any repository-required lint, type-check, or test command when practical. Diagnose failures caused by the change; do not rewrite unrelated failing tests or conceal pre-existing failures.

Before finishing, report:

- which tests were added or changed and what behavior they prove;
- the exact verification commands run and whether they passed;
- any tests not run, environment limitations, or pre-existing failures.

Do not claim coverage or a passing suite without executing it. If a test cannot be added or run, explain the concrete blocker and leave the implementation in a state that makes the missing verification explicit.
