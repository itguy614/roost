# Native macOS notification feasibility spike

**Review outcome requested** — The helper lifecycle worked. Native notification authorization remains blocked on the tested Mac. Review this evidence before PR 1.

## Environment and evidence

Apple Silicon, macOS 26.6.2 build 25G83, Swift 6.3.3. The disposable Swift bundle used AppKit and UserNotifications with zero windows, a private Roost state directory, and a synthetic event. It called the existing CLI to negotiate v1 and read daemon status. Its single-entry journal is a fixture for state reuse during an upgrade.

| Check | Actual result |
| --- | --- |
| Lifecycle ownership | Started and stopped the helper independently of the Roost daemon. No main window was created. |
| Singleton | Forced a second bundle launch against the same state directory. It exited at the exclusive lock before asking for authorization. |
| Daemon reconnect | Stopped and restarted the real daemon. The same helper PID logged the disconnect and connected to the new daemon PID through the CLI. |
| Helper upgrade | Stopped build 1, replaced the bundle with build 2 under the same bundle identifier, and reused the private state directory. Build 2 reacquired the lock, reopened the synthetic journal, and connected to the running daemon. The ad hoc designated requirement changed when the bundle was re-signed. |
| Notification authorization | Tried an ad hoc bundle from the original shell and from Aqua, a locally signed bundle from Aqua, a locally signed regular-activation bundle with zero windows, and a locally signed bundle launched from an interactive Aqua Terminal. All returned `UNErrorDomain Code 1`, “Notifications are not allowed for this application.” The reported authorization remained undetermined. This differs from a user choosing Deny. |
| OS acceptance and presentation | The authorization failure prevented submission. User Allow and Deny, OS acceptance, Notification Center delivery, banner presentation, and permission continuity after an authorized upgrade remain unverified. |

Representative lifecycle output

```text
started pid=3111 build=1 windows=0
journal resumed -:accepted
daemon connected pid=3098
daemon disconnected
daemon connected pid=3141
helper stopped
duplicate helper rejected
started pid=3178 build=2 windows=0
journal resumed -:accepted
daemon connected pid=3141
helper stopped
```

The next feasibility check needs an app build and macOS session where authorization succeeds. Record the prompt decision, OS submission result, Notification Center presence, and any visually observed banner separately. Repeat with Deny, then upgrade an authorized build and check permission continuity. An OS list entry can establish presence in Notification Center. It cannot establish that the user saw a banner. [Apple delivered notification API](https://developer.apple.com/documentation/usernotifications/unusernotificationcenter/getdeliverednotifications%28completionhandler%3A%29?changes=_4_8)

## Reproduce

Run from the repository root in a logged-in macOS session. The commands create disposable state, use unique app identities, and print the directory containing the logs. The defaults use ad hoc signing. Provide `ROOST_SPIKE_SIGN_IDENTITY` in the environment to compare a local development identity. Set `ROOST_SPIKE_REGULAR=1` to test ordinary activation with zero windows.

```sh
cargo build --workspace --locked
bash docs/spikes/run-m4-notification-probe.sh lifecycle
bash docs/spikes/run-m4-notification-probe.sh allow
bash docs/spikes/run-m4-notification-probe.sh deny
```

Choose Allow or Deny when the corresponding test app prompts. `lifecycle` uses a synthetic journal record and never asks for permission. The helper and scripts are feasibility fixtures. Production cursor persistence, reconnection policy, and notification delivery remain in PR 4.

## Evidence for the contract clarifications

- A deterministic cadence model exercised worst-case two-second slots. Eight projects with 32 selectors each finished every ten-second round. Sets of 9, 16, 17, and 32 projects rotated fairly, including 65 selectors per project. With one unavailable slot, eight projects each received one turn over two rounds. PR 2 still needs measured backend and cleanup timings.
- Audited the exact database and Redis pending-count call paths in Laravel 12.50.0 and 13.43.0. A live fixture using Laravel 13.43.0 returned the expected counts on query-only SQLite with three `SELECT COUNT` queries and unchanged rows. An isolated Redis reader restricted to `LLEN` returned the expected counts and left the ready list, delayed set, and reserved set unchanged. The two-version end-to-end integration matrix remains a PR 2 gate.
