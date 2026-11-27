import AppKit
import Darwin
import Foundation
import NotificationCore
import UserNotifications

@MainActor private final class NativeSink: NSObject, NotificationSink, UNUserNotificationCenterDelegate {
    private let center = UNUserNotificationCenter.current()
    private var requestingPermission = false

    override init() {
        super.init()
        center.delegate = self
    }

    nonisolated func userNotificationCenter(_ center: UNUserNotificationCenter, willPresent notification: UNNotification,
                                            withCompletionHandler completion: @escaping (UNNotificationPresentationOptions) -> Void) {
        completion([.list, .banner])
    }

    func permission() async -> Permission {
        let current = await systemPermission()
        // macOS can report denied while an explicit consent prompt is visible.
        // Leave eligible receipts pending until that request has actually resolved.
        return requestingPermission && current == .denied ? .undetermined : current
    }

    private func systemPermission() async -> Permission {
        await bounded(.unknown) { complete in
            center.getNotificationSettings { settings in
                let result: Permission
                switch settings.authorizationStatus {
                case .notDetermined: result = .undetermined
                case .denied: result = .denied
                case .authorized: result = .authorized
                case .provisional: result = .provisional
                case .ephemeral: result = .ephemeral
                @unknown default: result = .unknown
                }
                complete(result)
            }
        }
    }

    func submit(identifier: String, kind: String) async -> SinkResult {
        // Recheck OS permission immediately before submitting. A revocation can
        // still race the OS call, which has its own acceptance result.
        let current = await permission()
        if current == .denied { return .denied }
        guard [.authorized, .provisional, .ephemeral].contains(current) else { return .retryableFailure }
        let content = UNMutableNotificationContent()
        content.title = "Roost"
        switch kind {
        case "process_crash": content.body = "A queue worker exited unexpectedly."
        case "permanent_recovery_failure": content.body = "Roost could not recover a queue worker."
        case "resource_threshold": content.body = "A queue worker exceeded a resource threshold."
        default: return .rejected
        }
        let request = UNNotificationRequest(identifier: identifier, content: content, trigger: nil)
        return await bounded(.retryableFailure) { complete in
            center.add(request) { error in
                guard let error = error as NSError? else { complete(.accepted); return }
                if error.domain == UNErrorDomain && error.code == UNError.Code.notificationsNotAllowed.rawValue {
                    complete(.denied)
                } else if error.domain == UNErrorDomain {
                    // Other known request errors are terminal. A transport
                    // interruption has no affirmative submission result.
                    complete(.rejected)
                } else {
                    complete(.retryableFailure)
                }
            }
        }
    }

    func requestPermission(app: NSApplication) async {
        guard await systemPermission() == .undetermined else { return }
        requestingPermission = true
        defer { requestingPermission = false }
        log("authorization request started")
        if #available(macOS 14, *) { app.activate() } else { app.activate(ignoringOtherApps: true) }
        do {
            let granted = try await center.requestAuthorization(options: [.alert])
            log("authorization request granted=\(granted)")
        } catch {
            let issue = error as NSError
            if issue.domain == UNErrorDomain {
                log("authorization request failed (UserNotifications code \(issue.code))")
            } else {
                log("authorization request failed")
            }
        }
    }

    func containsDelivered(_ identifier: String) async -> Bool {
        await bounded(false) { complete in
            center.getDeliveredNotifications { notifications in
                complete(notifications.contains { $0.request.identifier == identifier })
            }
        }
    }

    private func bounded<T: Sendable>(_ timeout: T, start: (@escaping @Sendable (T) -> Void) -> Void) async -> T {
        await withCheckedContinuation { continuation in
            var completed = false
            let finish: @MainActor (T) -> Void = { value in
                guard !completed else { return }
                completed = true
                continuation.resume(returning: value)
            }
            let deadline = Task {
                try? await Task.sleep(for: .seconds(2))
                if !Task.isCancelled { finish(timeout) }
            }
            start { value in
                Task { @MainActor in
                    deadline.cancel()
                    finish(value)
                }
            }
        }
    }
}

private func log(_ message: String) {
    FileHandle.standardOutput.write(Data((message + "\n").utf8))
}

private func wallSeconds() -> UInt64 {
    UInt64(max(0, Date().timeIntervalSince1970))
}

@main struct Agent {
    @MainActor static func main() {
        let args = Array(CommandLine.arguments.dropFirst())
        guard Bundle.main.bundleIdentifier != nil, args.count >= 2, args[0] == "--state-dir",
              Set(args.dropFirst(2)).isSubset(of: ["--request-permission", "--verify-center"]) else {
            log("Launch the helper app with --state-dir ABSOLUTE_PATH [--request-permission] [--verify-center]")
            exit(2)
        }
        let files: StateFiles
        let exclusive: HelperLock
        let journal: Journal
        do {
            files = try StateFiles(path: args[1])
            exclusive = try files.lock()
            journal = try files.readJournal()
        } catch StateFileError.duplicateHelper {
            log("duplicate helper rejected")
            exit(3)
        } catch let error as StateFileError {
            log("private notification helper state unavailable: \(error)")
            exit(2)
        } catch {
            log("private notification helper journal is invalid")
            exit(2)
        }
        let app = NSApplication.shared
        app.setActivationPolicy(.accessory)
        let signals = [SIGTERM, SIGINT].map { number in
            signal(number, SIG_IGN)
            let source = DispatchSource.makeSignalSource(signal: number, queue: .main)
            source.setEventHandler {
                MainActor.assumeIsolated { app.terminate(nil) }
            }
            source.resume()
            return source
        }
        let sink = NativeSink()
        let engine = Delivery(journal: journal, persist: files.writeJournal, sink: sink, now: wallSeconds)
        log("helper started pid=\(getpid()) windows=\(app.windows.count)")
        if args.contains("--request-permission") {
            Task { await sink.requestPermission(app: app) }
        }
        Task { await run(files: files, engine: engine, sink: sink, verify: args.contains("--verify-center")) }
        withExtendedLifetime((exclusive, signals)) { app.run() }
    }

    @MainActor private static func run(files: StateFiles, engine: Delivery, sink: NativeSink, verify: Bool) async {
        let client = RoostClient(state: files)
        var previous: Connection?
        var previousPermission: Permission?
        var previousDelivery: DeliveryState?
        var nextStatus = ContinuousClock.now
        var lastReceipt = engine.journal.lastReceived
        var previousGap = engine.journal.gap
        var lastOutcome = engine.journal.lastOutcome
        if engine.journal.cursor != nil { log("journal resumed") }
        var evidence: (String, ContinuousClock.Instant)?
        var delay: UInt64 = 2
        while !Task.isCancelled {
            var connection: Connection = .connecting
            var more = false
            do {
                guard engine.state != .journalUncertain else { throw StateFileError.journalDurabilityUncertain }
                let page = try await client.page(cursor: engine.journal.cursor)
                connection = .connected
                let consumed = try await engine.process(page, policy: client.policy(id:lineage:))
                more = consumed && page.has_more
                if verify, let outcome = engine.journal.lastOutcome, outcome != lastOutcome, outcome.state == .accepted {
                    evidence = ("roost.\(page.lineage).\(outcome.event_id.uuidString.lowercased())", ContinuousClock.now + .seconds(20))
                }
            } catch let error as RPCFailure where error.code == -32020 {
                do {
                    switch error.data?.kind {
                    case "cursor_expired":
                        guard let cursor = error.data?.recovery_cursor else { throw WireError.invalidFrame }
                        try engine.recover(kind: .retention, cursor: cursor, prunedThrough: error.data?.pruned_through,
                                           earliest: error.data?.earliest_retained)
                    case "cursor_lineage_changed": try engine.recover(kind: .lineageChanged, cursor: nil)
                    default: throw WireError.invalidFrame
                    }
                    connection = .connected
                } catch {
                    connection = .degraded
                }
            } catch WireError.incompatible {
                connection = .incompatible
            } catch WireError.transport {
                connection = .disconnected
            } catch {
                connection = .degraded
            }

            let permission = await sink.permission()
            let now = wallSeconds()
            if connection != previous { log("connection=\(connection.rawValue)") }
            if permission != previousPermission { log("permission=\(permission.rawValue)") }
            if engine.state != previousDelivery { log("delivery=\(engine.state.rawValue)") }
            if let receipt = engine.journal.lastReceived, receipt != lastReceipt { log("receipt=\(receipt)") }
            if let outcome = engine.journal.lastOutcome, outcome != lastOutcome { log("submission=\(outcome.state.rawValue) event=\(outcome.event_id)") }
            if let gap = engine.journal.gap, gap != previousGap { log("gap=\(gap.kind.rawValue)") }
            if connection != previous || permission != previousPermission || engine.state != previousDelivery
                || engine.journal.lastOutcome != lastOutcome
                || engine.journal.lastReceived != lastReceipt || engine.journal.gap != previousGap
                || ContinuousClock.now >= nextStatus {
                do {
                    try files.writeStatus(HelperStatus(pid: getpid(), now: now, connection: connection,
                                                       permission: permission, delivery: engine.state, journal: engine.journal))
                    nextStatus = ContinuousClock.now + .seconds(4)
                } catch {
                    log("notification status could not be written")
                }
            }
            if let (identifier, deadline) = evidence {
                if await sink.containsDelivered(identifier) {
                    log("Notification Center contains request=\(identifier)")
                    evidence = nil
                } else if ContinuousClock.now >= deadline {
                    log("Notification Center presence unconfirmed")
                    evidence = nil
                }
            }
            previous = connection
            previousPermission = permission
            previousDelivery = engine.state
            lastReceipt = engine.journal.lastReceived
            lastOutcome = engine.journal.lastOutcome
            previousGap = engine.journal.gap
            if connection == .connected { delay = 2 }
            try? await Task.sleep(for: more ? .milliseconds(250) : .seconds(delay))
            if connection != .connected { delay = min(8, delay * 2) }
        }
    }
}
