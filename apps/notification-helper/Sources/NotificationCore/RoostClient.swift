import Darwin
import Foundation

public struct RPCFailure: Decodable, Error, Sendable {
    public struct Details: Decodable, Sendable {
        public let kind: String?
        public let recovery_cursor: String?
        public let pruned_through: String?
        public let earliest_retained: RetainedBoundary?
    }
    public let code: Int
    public let data: Details?
}

public enum WireError: Error { case transport, invalidFrame, incompatible }

public struct RoostClient: Sendable {
    let state: StateFiles

    public init(state: StateFiles) { self.state = state }

    public func page(cursor: String?) async throws -> Page {
        try await Task.detached(priority: .utility) {
            let socket = try Wire(state.directory.appendingPathComponent("roost.sock"))
            try socket.hello()
            var params: [String: Any] = ["limit": 1]
            if let cursor { params["cursor"] = cursor }
            return try socket.request("event.history", params, as: Page.self)
        }.value
    }

    public func policy(id: UUID, lineage: String) async throws -> Policy {
        struct Reply: Decodable { let policy: Policy }
        return try await Task.detached(priority: .utility) {
            let socket = try Wire(state.directory.appendingPathComponent("roost.sock"))
            try socket.hello()
            return try socket.request("notifications.event.policy", ["id": id.uuidString, "lineage": lineage], as: Reply.self).policy
        }.value
    }
}

private final class Wire {
    let fd: Int32
    var id = 0
    let maxFrame = 65_536

    init(_ endpoint: URL) throws {
        var info = stat()
        guard lstat(endpoint.path, &info) == 0, info.st_mode & S_IFMT == S_IFSOCK,
              info.st_uid == geteuid(), info.st_mode & 0o777 == 0o600 else { throw WireError.transport }
        var address = sockaddr_un()
        let path = Array(endpoint.path.utf8)
        guard !path.contains(0), path.count < MemoryLayout.size(ofValue: address.sun_path) else {
            throw WireError.transport
        }
        address.sun_len = UInt8(path.count + 3)
        address.sun_family = sa_family_t(AF_UNIX)
        withUnsafeMutableBytes(of: &address.sun_path) { bytes in
            bytes.copyBytes(from: path)
        }
        let socket = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard socket >= 0 else { throw WireError.transport }
        do {
            var enabled: Int32 = 1
            guard fcntl(socket, F_SETFD, FD_CLOEXEC) == 0,
                  fcntl(socket, F_SETFL, O_NONBLOCK) == 0,
                  setsockopt(socket, SOL_SOCKET, SO_NOSIGPIPE, &enabled, socklen_t(MemoryLayout<Int32>.size)) == 0 else {
                throw WireError.transport
            }
            let addressLength = socklen_t(address.sun_len)
            let connected = withUnsafePointer(to: &address) {
                $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { Darwin.connect(socket, $0, addressLength) }
            }
            if connected != 0 {
                guard errno == EINPROGRESS else { throw WireError.transport }
                try Self.wait(socket, Int16(POLLOUT), until: Self.deadline())
                var socketError: Int32 = 0
                var length = socklen_t(MemoryLayout<Int32>.size)
                guard getsockopt(socket, SOL_SOCKET, SO_ERROR, &socketError, &length) == 0,
                      socketError == 0 else { throw WireError.transport }
            }
            var peer: uid_t = 0
            var group: gid_t = 0
            guard getpeereid(socket, &peer, &group) == 0, peer == geteuid() else { throw WireError.transport }
        } catch {
            close(socket)
            throw error
        }
        fd = socket
    }

    deinit { close(fd) }

    func hello() throws {
        struct Hello: Decodable { let protocol_version: Int; let capabilities: [String] }
        do {
            let hello = try request("daemon.hello", ["protocol_version": 1], as: Hello.self)
            guard hello.protocol_version == 1, hello.capabilities.contains("event.history"),
                  hello.capabilities.contains("notifications.event.policy") else { throw WireError.incompatible }
        } catch let error as RPCFailure where error.code == -32001 || error.code == -32601 {
            throw WireError.incompatible
        }
    }

    func request<T: Decodable>(_ method: String, _ params: [String: Any], as type: T.Type) throws -> T {
        id += 1
        var request = try JSONSerialization.data(withJSONObject: ["jsonrpc": "2.0", "id": id, "method": method, "params": params])
        guard request.count < maxFrame else { throw WireError.invalidFrame }
        request.append(10)
        let deadline = Self.deadline()
        try request.withUnsafeBytes { bytes in
            guard let base = bytes.baseAddress else { throw WireError.invalidFrame }
            var sent = 0
            while sent < bytes.count {
                try Self.wait(fd, Int16(POLLOUT), until: deadline)
                let count = send(fd, base.advanced(by: sent), bytes.count - sent, 0)
                if count < 0 && (errno == EAGAIN || errno == EINTR) { continue }
                guard count > 0 else { throw WireError.transport }
                sent += count
            }
        }
        var response = Data()
        var buffer = [UInt8](repeating: 0, count: 4096)
        while response.count < maxFrame {
            try Self.wait(fd, Int16(POLLIN), until: deadline)
            let count = recv(fd, &buffer, buffer.count, 0)
            if count < 0 && (errno == EAGAIN || errno == EINTR) { continue }
            guard count > 0 else { throw WireError.transport }
            let chunk = buffer.prefix(count)
            if let end = chunk.firstIndex(of: 10) {
                guard end + 1 == count, response.count + end <= maxFrame else { throw WireError.invalidFrame }
                response.append(contentsOf: chunk.prefix(upTo: end))
                let reply = try JSONDecoder().decode(Response<T>.self, from: response)
                guard reply.jsonrpc == "2.0", reply.id == id else { throw WireError.invalidFrame }
                if let error = reply.error { throw error }
                guard let result = reply.result else { throw WireError.invalidFrame }
                return result
            }
            response.append(contentsOf: chunk)
        }
        throw WireError.invalidFrame
    }

    private struct Response<T: Decodable>: Decodable {
        let jsonrpc: String
        let id: Int
        let result: T?
        let error: RPCFailure?
    }

    private static func deadline() -> DispatchTime { .now() + .seconds(2) }

    private static func wait(_ fd: Int32, _ events: Int16, until deadline: DispatchTime) throws {
        while true {
            let now = DispatchTime.now().uptimeNanoseconds
            guard now < deadline.uptimeNanoseconds else { throw WireError.transport }
            let ms = Int32((deadline.uptimeNanoseconds - now + 999_999) / 1_000_000)
            var descriptor = pollfd(fd: fd, events: events, revents: 0)
            let result = poll(&descriptor, 1, ms)
            if result < 0, errno == EINTR { continue }
            guard result == 1, descriptor.revents & events != 0 else { throw WireError.transport }
            return
        }
    }
}
