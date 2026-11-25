<?php
use Illuminate\Foundation\Application;
use Illuminate\Foundation\Bootstrap\LoadConfiguration;

$base = dirname(__DIR__);
if (is_file($base . '/record-admissions')) {
    file_put_contents($base . '/admissions.log', microtime(true) . PHP_EOL, FILE_APPEND | LOCK_EX);
}
if (is_file($base . '/hold-probe')) {
    usleep(900000);
}
if (is_file($base . '/record-usage')) {
    register_shutdown_function(function () use ($base) {
        $usage = getrusage();
        $cpu = $usage['ru_utime.tv_sec'] + $usage['ru_stime.tv_sec']
             + ($usage['ru_utime.tv_usec'] + $usage['ru_stime.tv_usec']) / 1000000;
        file_put_contents($base . '/usage.jsonl', json_encode([
            'at' => microtime(true), 'cpu_seconds' => $cpu, 'peak_rss_bytes' => $usage['ru_maxrss'],
        ]) . PHP_EOL, FILE_APPEND | LOCK_EX);
    });
}
$app = Application::configure($base)->create();
$app->afterBootstrapping(LoadConfiguration::class, function ($app) use ($base) {
    $config = $app['config'];
    $config->set('queue.default', 'database');
    $default = is_file($base . '/queue-default.php') ? require $base . '/queue-default.php' : 'primary';
    $config->set('queue.connections', [
        'database' => ['driver' => 'database', 'queue' => 'primary', 'connection' => 'fixture', 'table' => 'jobs'],
        'redis' => ['driver' => 'redis', 'queue' => $default, 'connection' => 'fixture'],
        'sync' => ['driver' => 'sync'],
        'custom' => ['driver' => 'redis', 'queue' => 'primary', 'connection' => 'fixture'],
    ]);
    $config->set('database.default', 'fixture');
    $config->set('database.connections.fixture', [
        'driver' => 'sqlite',
        'database' => $base . '/jobs.sqlite3',
        'options' => [PDO::SQLITE_ATTR_OPEN_FLAGS => PDO::SQLITE_OPEN_READONLY],
    ]);
    $config->set('database.redis', [
        'client' => 'phpredis',
        'options' => ['prefix' => ''],
        'fixture' => [
            'host' => trim(file_get_contents($base . '/redis-socket')),
            'port' => 0,
            'username' => 'roost_probe',
            'password' => 'fixture-password',
            'database' => 0,
            'timeout' => 8.0,
            'read_timeout' => 8.0,
        ],
    ]);
});
$app->booted(function ($app) use ($base) {
    if (is_file($base . '/custom-connector')) {
        $app->extend('queue', fn ($manager) => new class($manager) {
            public function __construct(private $manager) {}
            public function connection($name) {
                if ($name === 'custom') {
                    return new stdClass();
                }
                return $this->manager->connection($name);
            }
        });
    }
    $app['db']->connection('fixture')->listen(function ($query) use ($base) {
        file_put_contents($base . '/sql.log', $query->sql . PHP_EOL, FILE_APPEND | LOCK_EX);
    });
});
return $app;
