# Native macOS notification feasibility spike

**Follow-up result** — Moving the same signed bundles from a temporary directory to `~/Applications` resolved the observed authorization failure on this Mac. Both the conventional control and Roost's windowless probe obtained authorization, recorded OS acceptance, and found their test notification in Notification Center. PR 4 still needs production integration and the remaining permission and delivery tests.

## Initial run

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

## Controlled location comparison

On the same Apple Silicon Mac and Aqua login session, a conventional signed AppKit control installed its notification delegate before finishing launch, opened a visible key window, and requested authorization after becoming active. It failed from `/tmp` with Code 1 and undetermined authorization. A byte-preserving copy to a unique directory under `~/Applications` passed code-signature verification. With the same bundle identifier, signature, and launch command, authorization changed from undetermined to authorized after the prompt, the OS accepted the request, and the notification appeared in Notification Center.

Repeated the location-only comparison with the previously failing, development-signed Roost helper. The helper opened zero windows from `~/Applications`. It logged authorization granted, OS acceptance, a committed journal entry, and exactly one matching Notification Center entry. The earlier tests already demonstrated helper singleton enforcement and journal reuse across an upgrade. The owner confirmed seeing the control's permission prompt.

The comparison confirms a location-sensitive failure for these builds on this Mac. It does not establish Apple's underlying policy or a universal requirement to use one exact directory. Launch Services registration or a system policy related to temporary locations remain hypotheses. The same signature and bundle identity worked after relocation, and both activation modes passed in the Applications directory. A visual banner, a user choosing Deny, and permission continuity across an authorized upgrade still need verification. The desktop reference uses a resident Tauri app and its notification plugin. Roost's standalone helper can retain direct UserNotifications evidence for permission, acceptance, and Notification Center delivery.

## Reproduce

Run from the repository root in a logged-in macOS session. The commands create disposable state, use unique app identities, and print the directory containing the logs. The defaults use ad hoc signing. Provide `ROOST_SPIKE_SIGN_IDENTITY` in the environment to compare a local development identity. Set `ROOST_SPIKE_REGULAR=1` to test ordinary activation with zero windows.

```sh
cargo build --workspace --locked
bash docs/spikes/run-m4-notification-probe.sh lifecycle
bash docs/spikes/run-m4-notification-probe.sh allow
bash docs/spikes/run-m4-notification-probe.sh deny
```

Set `ROOST_SPIKE_APP_PARENT="$HOME/Applications"` to place the disposable signed bundle in the user's Applications directory. This leaves the Roost daemon state and logs in a temporary directory. The script prints both locations for review and cleanup.

Choose Allow or Deny when the corresponding test app prompts. `lifecycle` uses a synthetic journal record and never asks for permission. The helper and scripts are feasibility fixtures. Production cursor persistence, reconnection policy, and notification delivery remain in PR 4.

## Evidence for the contract clarifications

- A deterministic cadence model exercised worst-case two-second slots. Eight projects with 32 selectors each finished every ten-second round. Sets of 9, 16, 17, and 32 projects rotated fairly, including 65 selectors per project. With one unavailable slot, eight projects each received one turn over two rounds. PR 2 still needs measured backend and cleanup timings.
- Audited the exact database and Redis pending-count call paths in Laravel 12.50.0 and 13.43.0. A live fixture using Laravel 13.43.0 returned the expected counts on query-only SQLite with three `SELECT COUNT` queries and unchanged rows. An isolated Redis reader restricted to `LLEN` returned the expected counts and left the ready list, delayed set, and reserved set unchanged. The two-version end-to-end integration matrix remains a PR 2 gate.
