// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "RoostNotifications",
    platforms: [.macOS(.v13)],
    targets: [
        .target(name: "NotificationCore"),
        .executableTarget(name: "RoostNotificationHelper", dependencies: ["NotificationCore"]),
        .testTarget(name: "NotificationCoreTests", dependencies: ["NotificationCore"]),
    ]
)
