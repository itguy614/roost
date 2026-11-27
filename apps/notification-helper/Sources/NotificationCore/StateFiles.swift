import Darwin
import Foundation

public enum StateFileError: Error, Equatable {
    case journalDurabilityUncertain
    case unsafeDirectory, unsafeFile, unavailable, duplicateHelper, oversized, unsupportedJournal
}

enum JournalWritePhase { case beforeRename, afterRename }

public struct StateFiles: Sendable {
    public let directory: URL

    public init(path: String) throws {
        guard path.hasPrefix("/"), URL(fileURLWithPath: path).lastPathComponent != "." else {
            throw StateFileError.unsafeDirectory
        }
        let input = URL(fileURLWithPath: path)
        guard let canonical = realpath(input.deletingLastPathComponent().path, nil) else {
            throw StateFileError.unsafeDirectory
        }
        defer { free(canonical) }
        let parent = String(cString: canonical)
        directory = URL(fileURLWithPath: (parent as NSString).appendingPathComponent(input.lastPathComponent))
        var current = parent
        while true {
            var info = stat()
            guard lstat(current, &info) == 0, info.st_mode & S_IFMT == S_IFDIR,
                  info.st_uid == 0 || info.st_uid == geteuid(),
                  info.st_mode & 0o022 == 0 || info.st_mode & 0o1000 != 0 else {
                throw StateFileError.unsafeDirectory
            }
            if current == "/" { break }
            current = (current as NSString).deletingLastPathComponent
        }
        var info = stat()
        guard lstat(directory.path, &info) == 0, info.st_mode & S_IFMT == S_IFDIR,
              info.st_uid == geteuid(), info.st_mode & 0o777 == 0o700 else {
            throw StateFileError.unsafeDirectory
        }
    }

    public func lock() throws -> HelperLock {
        let fd = Darwin.open(directory.appendingPathComponent("notification-helper.lock").path,
                             O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK, 0o600)
        guard fd >= 0 else { throw StateFileError.unavailable }
        do {
            try Self.checkFile(fd, limit: 0)
            guard flock(fd, LOCK_EX | LOCK_NB) == 0 else { throw StateFileError.duplicateHelper }
            return HelperLock(fd)
        } catch {
            close(fd)
            throw error
        }
    }

    public func readJournal() throws -> Journal {
        let file = directory.appendingPathComponent("notification-helper-journal.json")
        let fd = Darwin.open(file.path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK)
        if fd < 0 {
            guard errno == ENOENT else { throw StateFileError.unavailable }
            return Journal()
        }
        let stream = FileHandle(fileDescriptor: fd, closeOnDealloc: true)
        try Self.checkFile(fd, limit: 16384)
        let data = try stream.read(upToCount: 16385) ?? Data()
        guard data.count <= 16384 else { throw StateFileError.oversized }
        let journal = try JSONDecoder().decode(Journal.self, from: data)
        guard journal.version == 1 else { throw StateFileError.unsupportedJournal }
        // A previous helper may have paused after rename but before directory sync.
        // Commit the directory entry that this launch actually reopened before delivering.
        try syncDirectory()
        return journal
    }

    public func writeJournal(_ journal: Journal) throws {
        try writeJournal(journal, faultAt: { _ in })
    }

    func writeJournal(_ journal: Journal, faultAt: (JournalWritePhase) throws -> Void) throws {
        try save(journal, as: "notification-helper-journal.json", durable: true, faultAt: faultAt)
    }

    public func writeStatus(_ status: HelperStatus) throws {
        try save(status, as: "notification-helper-status.json", durable: false)
    }

    private func save<T: Encodable>(_ value: T, as name: String, durable: Bool,
                                    faultAt: (JournalWritePhase) throws -> Void = { _ in }) throws {
        let data = try JSONEncoder().encode(value)
        guard data.count <= (durable ? 16384 : 8192) else { throw StateFileError.oversized }
        let temp = directory.appendingPathComponent(".notification-\(UUID())")
        let fd = Darwin.open(temp.path, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0o600)
        guard fd >= 0 else { throw StateFileError.unavailable }
        defer { try? FileManager.default.removeItem(at: temp) }
        let stream = FileHandle(fileDescriptor: fd, closeOnDealloc: true)
        try stream.write(contentsOf: data)
        if durable { try stream.synchronize() }
        try stream.close()
        try faultAt(.beforeRename)
        guard rename(temp.path, directory.appendingPathComponent(name).path) == 0 else {
            throw StateFileError.unavailable
        }
        if durable {
            do {
                try faultAt(.afterRename)
                try syncDirectory()
            } catch {
                throw StateFileError.journalDurabilityUncertain
            }
        }
    }

    private func syncDirectory() throws {
        let dir = Darwin.open(directory.path, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW)
        guard dir >= 0 else { throw StateFileError.unavailable }
        defer { close(dir) }
        guard fsync(dir) == 0 else { throw StateFileError.unavailable }
    }

    private static func checkFile(_ fd: Int32, limit: Int64) throws {
        var info = stat()
        guard fstat(fd, &info) == 0, info.st_mode & S_IFMT == S_IFREG,
              info.st_uid == geteuid(), info.st_mode & 0o777 == 0o600,
              info.st_size >= 0, info.st_size <= limit else { throw StateFileError.unsafeFile }
    }
}

public final class HelperLock {
    private let fd: Int32
    fileprivate init(_ fd: Int32) { self.fd = fd }
    deinit { close(fd) }
}
