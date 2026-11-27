import Foundation
import Testing
@testable import NotificationCore

@MainActor private final class Sink: NotificationSink {
    var permissionValue = Permission.authorized
    var responses: [SinkResult] = [.accepted]
    var identifiers = [String]()
    var beforeSubmit: () -> Void = {}

    func permission() async -> Permission { permissionValue }

    func submit(identifier: String, kind: String) async -> SinkResult {
        beforeSubmit()
        identifiers.append(identifier)
        return responses.isEmpty ? .accepted : responses.removeFirst()
    }
}

struct DeliveryTests {
    private let lineage = String(repeating: "a", count: 32)

    @Test @MainActor func testReceiptAndAttemptPrecedeSubmissionAndTerminalOutcomeAdvancesCursor() async throws {
        let sink = Sink()
        var durable = Journal()
        let event = Event(id: UUID(), kind: "process_crash", at: 1_000)
        let delivery = Delivery(journal: durable, persist: { durable = $0 }, sink: sink, now: { 1_012 })
        sink.beforeSubmit = {
            #expect(durable.lastReceived == event.id)
            #expect(durable.cursor == nil)
            #expect(durable.active?.attempts == 1)
        }
        let consumed = try await delivery.process(page(event)) { id, epoch in
            #expect(id == event.id)
            #expect(epoch == self.lineage)
            #expect(durable.lastReceived == event.id)
            return .eligible
        }
        #expect(consumed)
        #expect(durable.cursor == "next")
        #expect(durable.lastOutcome?.state == .accepted)
        #expect(durable.active == nil)
    }

    @Test @MainActor func testJournalFailureAfterAcceptanceKeepsTheSameRequestIdentifierAcrossRestart() async throws {
        enum Fault: Error { case disk }
        let sink = Sink()
        let event = Event(id: UUID(), kind: "process_crash", at: 1_000)
        var durable = Journal()
        var clock: UInt64 = 1_012
        let first = Delivery(journal: durable, persist: { candidate in
            if candidate.lastOutcome != nil { throw Fault.disk }
            durable = candidate
        }, sink: sink, now: { clock })
        do {
            _ = try await first.process(page(event)) { _, _ in .eligible }
            Issue.record("expected terminal write failure")
        } catch Fault.disk {}
        #expect(durable.cursor == nil)
        #expect(durable.active?.attempts == 1)
        clock += 2
        let reopened = try JSONDecoder().decode(Journal.self, from: JSONEncoder().encode(durable))
        let second = Delivery(journal: reopened, persist: { durable = $0 }, sink: sink, now: { clock })
        let consumed = try await second.process(page(event)) { _, _ in .eligible }
        #expect(consumed)
        #expect(sink.identifiers.count == 2)
        #expect(sink.identifiers.first == sink.identifiers.last)
        #expect(durable.lastOutcome?.state == .accepted)
    }

    @Test @MainActor func testRetryBackoffIsBoundedAndKeepsOneIdentifier() async throws {
        let sink = Sink()
        sink.responses = [.retryableFailure, .retryableFailure, .retryableFailure]
        let event = Event(id: UUID(), kind: "resource_threshold", at: 1_000)
        var clock: UInt64 = 1_010
        let delivery = Delivery(journal: Journal(), persist: { _ in }, sink: sink, now: { clock })
        for wall in [1_010, 1_011, 1_012, 1_015] {
            clock = UInt64(wall)
            let consumed = try await delivery.process(page(event)) { _, _ in .eligible }
            #expect(!consumed)
            #expect(delivery.journal.cursor == nil)
        }
        #expect(sink.identifiers.count == 2)
        clock = 1_016
        let consumed = try await delivery.process(page(event)) { _, _ in .eligible }
        #expect(consumed)
        #expect(sink.identifiers.count == 3)
        #expect(Set(sink.identifiers).count == 1)
        #expect(delivery.journal.lastOutcome?.state == .failed)
    }

    @Test @MainActor func testPermissionAndPolicyAreRecheckedAndUnresolvedEventsExpire() async throws {
        let sink = Sink()
        let event = Event(id: UUID(), kind: "process_crash", at: 1_000)
        var clock: UInt64 = 1_001
        for (permission, policy, expected) in [(Permission.denied, Policy.eligible, Submission.denied),
                                               (.authorized, .optedOut, .optedOut),
                                               (.authorized, .suppressed, .suppressed)] {
            sink.permissionValue = permission
            let delivery = Delivery(journal: Journal(), persist: { _ in }, sink: sink, now: { clock })
            let consumed = try await delivery.process(page(event)) { _, _ in policy }
            #expect(consumed)
            #expect(delivery.journal.lastOutcome?.state == expected)
        }
        let waiting = Delivery(journal: Journal(), persist: { _ in }, sink: sink, now: { clock })
        sink.permissionValue = .undetermined
        let pending = try await waiting.process(page(event)) { _, _ in .eligible }
        #expect(!pending)
        #expect(waiting.journal.cursor == nil)
        clock = 1_600
        let expired = try await waiting.process(page(event)) { _, _ in Issue.record("expired event looked up"); return .eligible }
        #expect(expired)
        #expect(waiting.journal.lastOutcome?.state == .expired)
        #expect(sink.identifiers.isEmpty)
    }

    @Test @MainActor func testCursorRecoveryPersistsGapAndSeparatesLineages() async throws {
        let sink = Sink()
        let event = Event(id: UUID(), kind: "resource_threshold", at: 1_000)
        let delivery = Delivery(journal: Journal(), persist: { _ in }, sink: sink, now: { 1_001 })
        _ = try await delivery.process(page(event)) { _, _ in .eligible }
        let boundary = RetainedBoundary(sequence: "7", at: 999)
        try delivery.recover(kind: .retention, cursor: "resume", prunedThrough: "6", earliest: boundary)
        #expect(delivery.journal.gap?.earliest_retained == boundary)
        #expect(delivery.journal.cursor == "resume")
        #expect(delivery.journal.gap?.kind == .retention)
        let different = Page(lineage: String(repeating: "b", count: 32), items: [event], next_cursor: "new", has_more: false)
        _ = try await delivery.process(different) { _, _ in .eligible }
        #expect(delivery.journal.gap?.kind == .lineageChanged)
        #expect(delivery.journal.cursor == "new")
        #expect(Set(sink.identifiers).count == 2)
    }

    @Test @MainActor func testReconnectPreservesPendingReceiptAndRevalidatesOptIn() async throws {
        struct Unreachable: Error {}
        let sink = Sink()
        let event = Event(id: UUID(), kind: "process_crash", at: 1_000)
        var saved = Journal()
        let first = Delivery(journal: saved, persist: { saved = $0 }, sink: sink, now: { 1_010 })
        do {
            _ = try await first.process(page(event)) { _, _ in throw Unreachable() }
            Issue.record("expected a disconnect")
        } catch is Unreachable {}
        #expect(saved.lastReceived == event.id)
        #expect(saved.cursor == nil)
        #expect(saved.active?.attempts == 0)
        let reopened = Delivery(journal: saved, persist: { saved = $0 }, sink: sink, now: { 1_012 })
        let consumed = try await reopened.process(page(event)) { _, _ in .optedOut }
        #expect(consumed)
        #expect(saved.lastOutcome?.state == .optedOut)
        #expect(sink.identifiers.isEmpty)
    }

    @Test @MainActor func testEventThatExpiresDuringPolicyLookupIsNeverSubmitted() async throws {
        let sink = Sink()
        let event = Event(id: UUID(), kind: "process_crash", at: 1_000)
        var clock: UInt64 = 1_599
        let delivery = Delivery(journal: Journal(), persist: { _ in }, sink: sink, now: { clock })
        let consumed = try await delivery.process(page(event)) { _, _ in
            clock = 1_600
            return .eligible
        }
        #expect(consumed)
        #expect(delivery.journal.lastOutcome?.state == .expired)
        #expect(sink.identifiers.isEmpty)
    }

    @Test @MainActor func testRevokedPermissionPreventsRetryAfterAnAuthorizedAttempt() async throws {
        let sink = Sink()
        sink.responses = [.retryableFailure]
        let event = Event(id: UUID(), kind: "process_crash", at: 1_000)
        var clock: UInt64 = 1_010
        let delivery = Delivery(journal: Journal(), persist: { _ in }, sink: sink, now: { clock })
        let pending = try await delivery.process(page(event)) { _, _ in .eligible }
        #expect(!pending)
        clock += 2
        sink.permissionValue = .denied
        let consumed = try await delivery.process(page(event)) { _, _ in .eligible }
        #expect(consumed)
        #expect(delivery.journal.lastOutcome?.state == .denied)
        #expect(sink.identifiers.count == 1)
    }

    @Test func testPrivateJournalAndSingleton() throws {
        let parent = FileManager.default.temporaryDirectory.resolvingSymlinksInPath()
        let dir = parent.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
        defer { try? FileManager.default.removeItem(at: dir) }
        let files = try StateFiles(path: dir.path)
        let lock = try files.lock()
        _ = withExtendedLifetime(lock) {
            #expect(throws: StateFileError.duplicateHelper) { try files.lock() }
        }
        try files.writeJournal(Journal())
        #expect(try files.readJournal() == Journal())
        let journal = dir.appendingPathComponent("notification-helper-journal.json")
        let attributes = try FileManager.default.attributesOfItem(atPath: journal.path)
        #expect(attributes[.posixPermissions] as? Int == 0o600)
        try FileManager.default.removeItem(at: journal)
        try FileManager.default.createSymbolicLink(at: journal, withDestinationURL: dir)
        #expect(throws: StateFileError.unavailable) { try files.readJournal() }
    }

    private func page(_ event: Event) -> Page {
        Page(lineage: lineage, items: [event], next_cursor: "next", has_more: false)
    }
}
