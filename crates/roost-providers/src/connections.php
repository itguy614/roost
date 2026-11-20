<?php
// The process is explicitly invoked for a trusted local project. Suppress
// ordinary bootstrap output and serialize only the allowlisted queue fields.
ob_start(static fn (string $buffer): string => '', 4096);
try {
    require getcwd() . '/vendor/autoload.php';
    $app = require getcwd() . '/bootstrap/app.php';
    $app->make(\Illuminate\Contracts\Console\Kernel::class)->bootstrap();
    $config = $app->make('config');
    $connections = $config->get('queue.connections', []);
    if (!is_array($connections)) {
        exit(2);
    }
    $result = ['default' => null, 'connections' => [], 'omitted_connections' => 0];
    $safe = static fn ($value): bool => is_string($value)
        && preg_match('/\A(?:[A-Za-z0-9]|\{)[A-Za-z0-9_:.{}-]{0,95}\z/D', $value) === 1
        && preg_match('/\A(?:ghp_|github_pat_|sk-|xox|akia|asia|eyj)/i', $value) !== 1;
    $default = $config->get('queue.default');
    $result['default'] = $safe($default) ? $default : null;
    foreach ($connections as $name => $settings) {
        if (!$safe($name) || count($result['connections']) >= 128) {
            ++$result['omitted_connections'];
            continue;
        }
        $driver = is_array($settings) ? ($settings['driver'] ?? null) : null;
        $result['connections'][] = ['name' => $name, 'driver' => $safe($driver) ? $driver : null];
    }
    $output = json_encode($result, JSON_THROW_ON_ERROR);
} catch (\Throwable $error) {
    exit(1);
}
ob_end_clean();
fwrite(STDOUT, $output);
