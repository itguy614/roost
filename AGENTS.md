# Codex instructions for Roost

Read `SPEC.md` before planning or editing. Treat it as the product baseline; flag ambiguities rather than silently changing requirements.

## Engineering priorities
1. Safety and correctness of process supervision over UI polish.
2. Build testable vertical slices; avoid creating unused abstractions or fake implementations.
3. Use idiomatic Rust, small crates, explicit errors, typed domain models and structured tracing.
4. Keep daemon independent of Tauri; both GUI and CLI use versioned IPC.
5. Avoid shell commands for child process execution; use explicit argv and resolved PHP paths.
6. Never terminate unrelated processes; verify ownership and process start identity.
7. Separate desired state, observed state, and configuration. Make restart behavior deterministic.
8. No secrets in logs, IPC discovery payloads, or project config files.
9. Treat queue names discovered by static inspection as suggestions, never exhaustive truth.
10. Keep scope to queue workers and schedulers for v0.1; defer general-purpose command runners.

## Workflow
- Before a milestone, propose a short plan with changed files, risks and tests.
- Implement the smallest working slice, run formatting/lint/tests, and report results truthfully.
- Add tests for every state transition and failure path.
- Document public IPC messages and config schema when introducing them.
- Prefer real macOS verification for launchd, process groups, notifications and packaging; mark untested platform behavior explicitly.
- Do not commit generated artifacts, secrets or machine-local paths.
- When an integration API is undocumented, build a discovery spike rather than inventing behavior.

## Desktop UI
- Use the published `@itguy614/clean-ui` package as Roost's Vue component system. Register `createCleanUI()` at app startup and import `@itguy614/clean-ui/styles` there. Import named components and composables from the public package root.
- Prefer existing Clean UI components and documented props, slots, and tokens. Use native macOS UI where appropriate. Keep Roost tokens in `apps/desktop/src/theme/roost.css` and follow `docs/ui/README.md`.
- For a genuinely missing shared component, check the Clean UI documentation and existing issues, then propose a scoped [Clean UI issue](https://github.com/itguy614/clean-ui/issues) with the use case, accessibility needs and a minimal example. Keep Roost's theme local and avoid duplicating library components.

## Definition of done for each change
- Behavior works end to end or is explicitly marked incomplete.
- Automated tests added and passing where executable.
- No undocumented breaking config or IPC changes.
- Error states are visible to CLI and eventual GUI.
- README or architecture notes updated when relevant.
