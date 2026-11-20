import AppKit
import Darwin
import Foundation
import UserNotifications

// Disposable feasibility probe. The existing CLI exercises the daemon handshake.
// The event ID and delivery journal stand in for Milestone 4 event.history.
@main
struct NotificationProbe {
    static func log(_ message: String) {
        FileHandle.standardOutput.write(Data((message + "\n").utf8))
    }

    static func daemonPID(cli: String, state: URL) -> Int? {
        let process = Process()
        let output = Pipe()
        process.executableURL = URL(fileURLWithPath: cli)
        process.arguments = ["--state-dir", state.path, "daemon", "status"]
        process.standardOutput = output
        process.standardError = FileHandle.nullDevice
        do {
            try process.run()
            let bytes = output.fileHandleForReading.readDataToEndOfFile()
            process.waitUntilExit()
            guard process.terminationStatus == 0,
                  let reply = try JSONSerialization.jsonObject(with: bytes) as? [String: Any],
                  reply["protocol_version"] as? Int == 1 else { return nil }
            return reply["pid"] as? Int
        } catch {
            return nil
        }
    }

    @MainActor
    static func main() {
        let args = CommandLine.arguments
        guard args.count == 4 else {
            log("Usage: NotificationProbe STATE_DIR ROOST_CLI EVENT_ID")
            exit(2)
        }
        let app = NSApplication.shared
        if Bundle.main.object(forInfoDictionaryKey: "LSUIElement") as? Bool == true {
            app.setActivationPolicy(.accessory)
        } else {
            app.setActivationPolicy(.regular)
        }
        Task {
            let state = URL(fileURLWithPath: args[1]).resolvingSymlinksInPath()
            let lockPath = state.appendingPathComponent("notification-spike.lock").path
            let lock = open(lockPath, O_CREAT | O_RDWR | O_NOFOLLOW | O_CLOEXEC, 0o600)
            guard lock >= 0 else {
                log("lock unavailable")
                exit(2)
            }
            guard flock(lock, LOCK_EX | LOCK_NB) == 0 else {
                log("duplicate helper rejected")
                exit(3)
            }
            log("started pid=\(getpid()) bundle=\(Bundle.main.bundleIdentifier ?? "missing") build=\(Bundle.main.object(forInfoDictionaryKey: "CFBundleVersion") ?? "missing") windows=\(app.windows.count)")
            let stop = state.appendingPathComponent("notification-spike.stop")
            let timer = Timer.scheduledTimer(withTimeInterval: 0.5, repeats: true) { _ in
                if FileManager.default.fileExists(atPath: stop.path) {
                    Task { @MainActor in
                        log("helper stopped")
                        app.terminate(nil)
                    }
                }
            }
            await observe(app: app, state: state, cli: args[2], event: args[3])
            timer.invalidate()
            close(lock)
            app.terminate(nil)
        }
        app.run()
    }

    @MainActor
    static func observe(app: NSApplication, state: URL, cli: String, event: String) async {
        let center = UNUserNotificationCenter.current()
        let initial = await center.notificationSettings()
        log("authorization before=\(initial.authorizationStatus.rawValue)")
        if event != "-" && initial.authorizationStatus == .notDetermined {
            app.activate()
            do {
                let granted = try await center.requestAuthorization(options: [.alert])
                log("authorization request granted=\(granted)")
            } catch {
                log("authorization request failed: \(error)")
            }
        }
        let settings = await center.notificationSettings()
        log("authorization after=\(settings.authorizationStatus.rawValue) alerts=\(settings.alertSetting.rawValue)")

        let stop = state.appendingPathComponent("notification-spike.stop")
        let journal = state.appendingPathComponent("notification-spike.journal")
        let accepted = event + ":accepted"
        let denied = event + ":denied"
        let existing = try? String(contentsOf: journal, encoding: .utf8)
        var submitted = (existing == accepted || existing == denied)
        if submitted { log("journal resumed \(existing ?? "")") }
        var previousPID: Int?
        var recordedConnection = false
        var foundDelivered = false
        let deadline = ContinuousClock.now + .seconds(120)
        while ContinuousClock.now < deadline && !FileManager.default.fileExists(atPath: stop.path) {
            let connected = daemonPID(cli: cli, state: state)
            if !recordedConnection || connected != previousPID {
                log(connected.map { "daemon connected pid=\($0)" } ?? "daemon disconnected")
                previousPID = connected
                recordedConnection = true
            }
            if event != "-" && connected != nil && !submitted {
                submitted = true
                if settings.authorizationStatus == .authorized {
                    let content = UNMutableNotificationContent()
                    content.title = "Roost Notification Spike"
                    content.body = "Synthetic local notification"
                    do {
                        try await center.add(UNNotificationRequest(identifier: event, content: content, trigger: nil))
                        log("OS accepted event=\(event)")
                        _ = umask(0o077)
                        try accepted.write(to: journal, atomically: true, encoding: .utf8)
                        log("journal committed \(accepted)")
                    } catch {
                        log("delivery or journal failed: \(error)")
                    }
                } else if settings.authorizationStatus == .denied {
                    _ = umask(0o077)
                    do {
                        try denied.write(to: journal, atomically: true, encoding: .utf8)
                        log("skipped submission and committed \(denied)")
                    } catch {
                        log("journal failed: \(error)")
                    }
                } else {
                    log("submission paused because authorization is unresolved")
                }
            }
            if event != "-" && !foundDelivered {
                let delivered = await center.deliveredNotifications()
                let count = delivered.filter { $0.request.identifier == event }.count
                if count > 0 {
                    foundDelivered = true
                    log("Notification Center contains event=\(event) count=\(count)")
                }
            }
            try? await Task.sleep(for: .milliseconds(500))
        }
        log("helper stopped")
    }
}
