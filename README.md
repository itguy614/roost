# Roost PR #18 final visual review

Native, unretouched single-window PNG captures of the optimized app at `52a1e1b` on Apple Silicon macOS 26.6.2. Both appearances use the same 12 disposable project registrations served by a real local daemon. Dispatch runs two fixture workers and one scheduler.

| State | Light | Dark |
| --- | --- | --- |
| Stale dashboard after actual daemon disconnect | [PNG](light-stale-dashboard.png) | [PNG](dark-stale-dashboard.png) |
| Twelve projects in the sidebar, eight rows on dashboard page one | [PNG](light-12-projects.png) | [PNG](dark-12-projects.png) |
| Minimum 720 × 460 pt native window | [PNG](light-minimum-window.png) | [PNG](dark-minimum-window.png) |

The stale views preserve the last successful project snapshot and show the offline message. In the same runs, restarting the daemon refreshed the live dashboard. The full-size windows measured 1016 × 670 pt. For the minimum captures, a local build override set the initial window dimensions to the existing minimum. Core Graphics confirmed the actual native window size. Retina screenshots are 2032 × 1340 px and 1440 × 920 px respectively.

[Draft PR #18](https://github.com/itguy614/roost/pull/18)
