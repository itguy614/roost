# Roost UI — approved concept v3 implementation reference

## Status
Owner approved the **light-mode-first, proper-dark-mode** hybrid native macOS / modern developer-tool direction. This is an implementation reference, **not** a pixel-perfect generated-image specification. The existing concept board is illustrative, including fake data and rough labels. Resolve final labels and affordances against the actual IPC API.

## Design principles
1. **One focal point per page:** visually dominant metric, primary action, or next logical step. A default healthy state should not steal attention.
2. **Native macOS character:** system SF typography, standard window chrome, sidebar navigation, platform-native preference patterns and controls where practical.
3. **Light mode is canonical:** white/off-white surfaces, graphite text, understated blue for navigation and primary actions; implement a fully usable semantic-token dark mode with the same hierarchy.
4. **No generic AI-dashboard styling:** no purple-on-black, neon glows, gratuitous gradients, oversized hero banners, glassmorphism or ornamental charts.
5. **Cards and shadows only to express hierarchy:** prefer section headings, tables, spacing, separators, and alignment. No card for every number.
6. **Color conveys meaning:** running/healthy green, warning amber, failed red, selected/interactive blue; always pair status color with text/icon.
7. **Rooster brand:** retain colorful rooster personality in app icon; UI chrome remains restrained.

## Screen-level hierarchy and acceptance
### Dashboard
- **Primary:** actionable attention when something is wrong; otherwise **project table** with desired/observed workers and scheduler states.
- Compact aggregate status line only; **shrink/remove giant green checkmark and 'All Systems Healthy' banner**.
- One obvious next action for empty state: Add Project. Do not fill default dashboard with decorative cards.
- Unknown metrics are shown as unavailable/stale, not zero.

### Project detail
- **Primary:** running/desired worker count and relevant lifecycle action.
- Show **only the context-appropriate primary Start or Stop action**, not equally prominent Start and Stop. Restart may live in an overflow menu.
- Supporting sections: worker profiles, scheduler, PHP provider, queue connections vs incomplete queue-name suggestions, watcher, groups, resource usage.
- Uncertain lease/recovery blocked and backoff states must be explicit and must disable unsafe actions.

### Logs & metrics
- **Primary:** live, filterable log stream taking most of the viewport.
- Metrics, crashes, history as adjacent tabs, not competing chart cards below logs.
- Show source, timestamps, stderr/stdout, bounded output, disconnect/reconnect and no-data states.

### Settings
- **Primary:** currently selected preference pane.
- Native-style preference rows, no cards; section navigation for General, Providers, Watching, Notifications, Startup, Advanced.
- Clearly distinguish GUI start at login, daemon launch at login, and project autostart.
- Explain portable file-owned settings and conflicts before applying changes.

### Menu bar
- **Primary:** compact status and project-level actions; Open Roost visible.
- **Move Restart Daemon to an Advanced submenu** and confirm disruptive actions.
- Quit UI and closing main window must never imply stopping daemon-owned processes.

## Interaction states required
- Empty projects, loading, daemon offline, version mismatch, reconnect, stale event cursor.
- Starting, running, stopping, stopped, backoff, failed, recovery blocked.
- Concurrent CLI/GUI mutations, idempotency, revision conflicts, file-owned configuration.
- Queue suggestion incomplete/unavailable, metrics unsupported/unknown/stale.
- Keyboard access, VoiceOver labels, adequate contrast, reduced motion, light/dark parity.

## Brand asset
`docs/ui/roost-app-icon.svg` is an **original editable vector interpretation** of the rooster concept, not a pixel-perfect extraction of the generated concept board. Review visually at 16, 32, 128 and 1024 px; refine silhouette/optical alignment before producing macOS `.icns` and template menu-bar variants. The app icon can be colorful; menu-bar template icon should be monochrome.

## Implementation gate
- [ ] Confirm visual consistency of Dashboard, Project Detail, Logs & Metrics, Settings and Menu Bar with this reference.
- [ ] Verify every screen has one clear primary information/action hierarchy.
- [ ] Verify cards/shadows are only used for actual elevation or hierarchy.
- [ ] Verify both appearances, contrast, accessibility and all exceptional states.
- [ ] Review actual screenshot builds against the approved hierarchy before merging.
