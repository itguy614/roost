<?php
// This script runs inside the explicitly opted-in, trusted project bootstrap.
ob_start(static fn (string $buffer): string => '', 4096);
$level = ob_get_level();
try {
    $selectors = json_decode($argv[1], true, 8, JSON_THROW_ON_ERROR);
    if (!is_array($selectors) || count($selectors) < 1 || count($selectors) > 32) {
        exit(2);
    }
    require getcwd() . '/vendor/autoload.php';
    $app = require getcwd() . '/bootstrap/app.php';
    $app->make(\Illuminate\Contracts\Console\Kernel::class)->bootstrap();
    $allowed = in_array(\Illuminate\Foundation\Application::VERSION, ['12.50.0', '13.43.0'], true);
    $connections = $app->make('config')->get('queue.connections', []);
    $manager = $app->make('queue');
    $safe = static fn ($value): bool => is_string($value)
        && preg_match('/\A(?:[A-Za-z0-9]|\{)[A-Za-z0-9_:.{}-]{0,95}\z/D', $value) === 1
        && preg_match('/\A(?:ghp_|github_pat_|sk-|xox|akia|asia|eyj)/i', $value) !== 1;
    $failure = static fn (string $reason, ?string $queue = null): array =>
        ['queue' => $queue, 'pending' => null, 'reason' => $reason];
    $results = [];
    $counts = [];
    foreach ($selectors as $selector) {
        $connection = $selector['connection'];
        $requested = $selector['queue'];
        if (!$safe($connection) || ($requested !== null && !$safe($requested))) {
            exit(2);
        }
        if (!$allowed) {
            $results[] = $failure('framework_unsupported');
            continue;
        }
        $settings = $connections[$connection] ?? null;
        if (!is_array($settings)) {
            $results[] = $failure('connection_missing');
            continue;
        }
        $expected = match ($settings['driver'] ?? null) {
            'database' => \Illuminate\Queue\DatabaseQueue::class,
            'redis' => \Illuminate\Queue\RedisQueue::class,
            default => null,
        };
        if ($expected === null) {
            $results[] = $failure('driver_unsupported');
            continue;
        }
        $queue = $requested ?? ($settings['queue'] ?? null);
        if (!$safe($queue)) {
            $results[] = $failure('invalid_default');
            continue;
        }
        if (isset($counts[$connection][$queue])) {
            $results[] = $counts[$connection][$queue];
            continue;
        }
        try {
            // Resolve through the framework and reject extensions before the count.
            $adapter = $manager->connection($connection);
            if (get_class($adapter) !== $expected) {
                $reading = $failure('custom_connector', $queue);
            } else {
                $pending = $adapter->pendingSize($queue);
                $reading = is_int($pending) && $pending >= 0
                    ? ['queue' => $queue, 'pending' => $pending, 'reason' => null]
                    : $failure('backend_unavailable', $queue);
            }
        } catch (\Throwable $error) {
            $reading = $failure('backend_unavailable', $queue);
        }
        $counts[$connection][$queue] = $reading;
        $results[] = $reading;
    }
    $output = json_encode($results, JSON_THROW_ON_ERROR);
} catch (\Throwable $error) {
    exit(1);
}
while (ob_get_level() >= $level) {
    ob_end_clean();
}
fwrite(STDOUT, $output);
