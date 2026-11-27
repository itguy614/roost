import Foundation

public struct Event: Codable, Equatable, Sendable {
    public let id: UUID
    public let kind: String
    public let at: UInt64
}

public struct Page: Codable, Equatable, Sendable {
    public let lineage: String
    public let items: [Event]
    public let next_cursor: String
    public let has_more: Bool
}

public enum Policy: String, Codable, Sendable {
    case eligible, optedOut = "opted_out", suppressed, unsupported, eventUnavailable = "event_unavailable"
}

public enum Permission: String, Codable, Sendable {
    case unknown, undetermined, denied, authorized, provisional, ephemeral
}

public enum Submission: String, Codable, Sendable {
    case accepted, denied, optedOut = "opted_out", suppressed, unsupported, expired, unavailable, failed
}

public enum Connection: String, Codable, Sendable {
    case connecting, connected, disconnected, incompatible, degraded
}

public enum GapKind: String, Codable, Sendable {
    case retention, lineageChanged = "lineage_changed"
}

public struct RetainedBoundary: Codable, Equatable, Sendable {
    public let sequence: String
    public let at: UInt64
}

public struct Gap: Codable, Equatable, Sendable {
    public let at: UInt64
    public let kind: GapKind
    public let pruned_through: String?
    public let earliest_retained: RetainedBoundary?
}

public struct Outcome: Codable, Equatable, Sendable {
    public let event_id: UUID
    public let at: UInt64
    public let state: Submission
}

public struct Receipt: Codable, Equatable, Sendable {
    var event: Event
    var lineage: String
    var attempts: Int = 0
    var retryAt: UInt64 = 0
}

public struct Journal: Codable, Equatable, Sendable {
    var version: Int = 1
    public fileprivate(set) var cursor: String?
    fileprivate var lineage: String?
    internal fileprivate(set) var active: Receipt?
    public fileprivate(set) var lastReceived: UUID?
    public fileprivate(set) var lastOutcome: Outcome?
    public fileprivate(set) var gap: Gap?

    public init() {}
}

public struct HelperStatus: Encodable {
    let version = 1
    let pid: Int32
    let updated_at: UInt64
    let connection: Connection
    let permission: Permission
    let last_received_event_id: UUID?
    let last_submission: Outcome?
    let last_gap: Gap?

    public init(pid: Int32, now: UInt64, connection: Connection, permission: Permission, journal: Journal) {
        self.pid = pid
        self.updated_at = now
        self.connection = connection
        self.permission = permission
        self.last_received_event_id = journal.lastReceived
        self.last_submission = journal.lastOutcome
        self.last_gap = journal.gap
    }
}

public enum SinkResult: Equatable { case accepted, denied, retryableFailure, rejected }

@MainActor public protocol NotificationSink {
    func permission() async -> Permission
    func submit(identifier: String, kind: String) async -> SinkResult
}

public enum DeliveryError: Error { case invalidPage }

@MainActor public final class Delivery {
    public private(set) var journal: Journal
    private let persist: (Journal) throws -> Void
    private let sink: NotificationSink
    private let now: () -> UInt64
    private let maxAge: UInt64 = 600
    private let retryDelays: [UInt64] = [2, 4, 8]

    public init(journal: Journal, persist: @escaping (Journal) throws -> Void, sink: NotificationSink,
                now: @escaping () -> UInt64) {
        self.journal = journal
        self.persist = persist
        self.sink = sink
        self.now = now
    }

    private func update(_ edit: (inout Journal) -> Void) throws {
        var next = journal
        edit(&next)
        if next != journal {
            try persist(next)
            journal = next
        }
    }

    public func recover(kind: GapKind, cursor: String?, prunedThrough: String? = nil,
                        earliest: RetainedBoundary? = nil) throws {
        if let cursor, cursor.isEmpty || cursor.utf8.count > 1024 { throw DeliveryError.invalidPage }
        try update {
            if let active = $0.active {
                $0.lastOutcome = Outcome(event_id: active.event.id, at: now(), state: .unavailable)
            }
            $0.active = nil
            $0.cursor = cursor
            if kind == .lineageChanged { $0.lineage = nil }
            $0.gap = Gap(at: now(), kind: kind, pruned_through: prunedThrough, earliest_retained: earliest)
        }
    }

    /// Returns true only when this page has been durably consumed.
    public func process(_ page: Page, policy lookup: (UUID, String) async throws -> Policy) async throws -> Bool {
        guard page.items.count <= 1, page.lineage.count == 32,
              page.lineage.utf8.allSatisfy({ (48...57).contains($0) || (97...102).contains($0) }),
              !page.next_cursor.isEmpty, page.next_cursor.utf8.count <= 1024 else { throw DeliveryError.invalidPage }
        if let lineage = journal.lineage, lineage != page.lineage {
            try recover(kind: .lineageChanged, cursor: nil)
        }
        guard let event = page.items.first else {
            guard journal.active == nil, !page.has_more else { throw DeliveryError.invalidPage }
            try update { $0.cursor = page.next_cursor; $0.lineage = page.lineage }
            return true
        }
        if let active = journal.active {
            guard active.event == event, active.lineage == page.lineage else { throw DeliveryError.invalidPage }
        } else {
            try update {
                $0.lineage = page.lineage
                $0.active = Receipt(event: event, lineage: page.lineage)
                $0.lastReceived = event.id
            }
        }
        func finish(_ state: Submission) throws -> Bool {
            try update {
                $0.lastOutcome = Outcome(event_id: event.id, at: now(), state: state)
                $0.active = nil
                $0.cursor = page.next_cursor
            }
            return true
        }
        let wall = now()
        if event.at > wall {
            if event.at - wall > maxAge { return try finish(.expired) }
            return false
        }
        if wall - event.at >= maxAge { return try finish(.expired) }
        guard let record = journal.active, record.attempts >= 0 else { throw DeliveryError.invalidPage }
        if record.attempts >= retryDelays.count { return try finish(.failed) }
        if wall < record.retryAt { return false }
        let permission = await sink.permission()
        switch try await lookup(event.id, page.lineage) {
        case .eligible: break
        case .optedOut: return try finish(.optedOut)
        case .suppressed: return try finish(.suppressed)
        case .unsupported: return try finish(.unsupported)
        case .eventUnavailable: return try finish(.unavailable)
        }
        if permission == .denied { return try finish(.denied) }
        guard [.authorized, .provisional, .ephemeral].contains(permission) else { return false }
        guard ["process_crash", "permanent_recovery_failure", "resource_threshold"].contains(event.kind) else {
            return try finish(.unsupported)
        }
        let submitAt = now()
        guard submitAt >= event.at else { return false }
        if submitAt - event.at >= maxAge { return try finish(.expired) }
        // A crash or journal error after submitting can consume an attempt.
        // Retrying uses the same OS request identifier.
        try update {
            $0.active?.attempts += 1
            let delay = retryDelays[record.attempts]
            $0.active?.retryAt = min(submitAt, UInt64.max - delay) + delay
        }
        let identifier = "roost.\(page.lineage).\(event.id.uuidString.lowercased())"
        switch await sink.submit(identifier: identifier, kind: event.kind) {
        case .accepted: return try finish(.accepted)
        case .denied: return try finish(.denied)
        case .rejected: return try finish(.failed)
        case .retryableFailure:
            return record.attempts + 1 == retryDelays.count ? try finish(.failed) : false
        }
    }
}
