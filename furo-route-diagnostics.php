<?php
declare(strict_types=1);

$DIAGNOSTICS_PASSKEY = 'change-this-passkey';
$DIAGNOSTICS_CONNECT_TIMEOUT_SEC = 5;
$DIAGNOSTICS_SESSION_NAME = 'furo_route_diagnostics';

session_name($DIAGNOSTICS_SESSION_NAME);
session_start([
    'cookie_httponly' => true,
    'cookie_samesite' => 'Lax',
    'use_strict_mode' => true,
]);

function h(mixed $value): string
{
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

function sanitizeRouteId(string $value, int $index): string
{
    $sanitized = preg_replace('/[^a-zA-Z0-9_\-\.]/', '', $value) ?? '';
    if ($sanitized === '') {
        return 'route_' . ($index + 1);
    }
    return $sanitized;
}

function isRouteEnabled(array $route): bool
{
    if (!array_key_exists('enabled', $route) || $route['enabled'] === null) {
        return true;
    }
    return (bool)$route['enabled'];
}

function validatePort(mixed $value, string $field): int
{
    if (!is_int($value)) {
        if (is_string($value) && ctype_digit($value)) {
            $value = (int)$value;
        } else {
            throw new InvalidArgumentException($field . ' must be between 1 and 65535');
        }
    }

    if ($value < 1 || $value > 65535) {
        throw new InvalidArgumentException($field . ' must be between 1 and 65535');
    }

    return $value;
}

function relayEndpointFromUrl(string $relayUrl): array
{
    $parts = parse_url($relayUrl);
    if (!is_array($parts) || empty($parts['host'])) {
        throw new InvalidArgumentException('relay_url must be a valid absolute URL');
    }

    $scheme = strtolower((string)($parts['scheme'] ?? ''));
    $port = $parts['port'] ?? null;
    if ($port === null) {
        if ($scheme === 'https') {
            $port = 443;
        } elseif ($scheme === 'http') {
            $port = 80;
        } else {
            throw new InvalidArgumentException('relay_url must include a port when scheme is not http or https');
        }
    }

    return [
        'scheme' => $scheme === '' ? 'http' : $scheme,
        'host' => (string)$parts['host'],
        'port' => validatePort($port, 'relay_url port'),
        'path' => (string)($parts['path'] ?? '/'),
    ];
}

function tcpTarget(string $host, int $port): string
{
    if (str_contains($host, ':') && !str_starts_with($host, '[')) {
        return '[' . $host . ']:' . $port;
    }
    return $host . ':' . $port;
}

function testTcpConnectivity(string $host, int $port, int $timeoutSec): array
{
    $errno = 0;
    $errstr = '';
    $start = microtime(true);
    $socket = @stream_socket_client(
        'tcp://' . tcpTarget($host, $port),
        $errno,
        $errstr,
        $timeoutSec,
        STREAM_CLIENT_CONNECT
    );
    $latencyMs = round((microtime(true) - $start) * 1000, 2);

    if (is_resource($socket)) {
        fclose($socket);
        return [
            'ok' => true,
            'latency_ms' => $latencyMs,
            'message' => 'TCP connection established',
        ];
    }

    $detail = trim($errstr);
    if ($detail === '') {
        $detail = 'connection failed';
    }
    if ($errno > 0) {
        $detail .= ' (' . $errno . ')';
    }

    return [
        'ok' => false,
        'latency_ms' => $latencyMs,
        'message' => $detail,
    ];
}

function buildDiagnosticsRoutes(array $config): array
{
    $topLevelPublicHost = trim((string)($config['public_host'] ?? ''));
    $topLevelPublicPort = $config['public_port'] ?? null;

    if (!empty($config['routes']) && is_array($config['routes'])) {
        $routes = [];
        foreach (array_values($config['routes']) as $index => $route) {
            if (!is_array($route)) {
                throw new InvalidArgumentException('routes[' . $index . '] must be an object');
            }

            $relayUrl = trim((string)($route['relay_url'] ?? ''));
            if ($relayUrl === '') {
                throw new InvalidArgumentException('routes[' . $index . '].relay_url is required');
            }

            $publicHost = trim((string)($route['public_host'] ?? ''));
            if ($publicHost === '') {
                $publicHost = $topLevelPublicHost;
            }
            if ($publicHost === '') {
                throw new InvalidArgumentException('routes[' . $index . '].public_host is required');
            }

            $publicPort = $route['public_port'] ?? $topLevelPublicPort;
            $serverHost = trim((string)($route['server_host'] ?? ''));
            if ($serverHost === '') {
                throw new InvalidArgumentException('routes[' . $index . '].server_host is required');
            }

            $sessionCount = $route['session_count'] ?? null;
            if (!is_int($sessionCount) && !(is_string($sessionCount) && ctype_digit($sessionCount))) {
                throw new InvalidArgumentException('routes[' . $index . '].session_count must be >= 1');
            }
            $sessionCount = (int)$sessionCount;
            if ($sessionCount < 1) {
                throw new InvalidArgumentException('routes[' . $index . '].session_count must be >= 1');
            }

            $routes[] = [
                'id' => sanitizeRouteId((string)($route['id'] ?? ''), $index),
                'enabled' => isRouteEnabled($route),
                'relay_url' => $relayUrl,
                'relay_endpoint' => relayEndpointFromUrl($relayUrl),
                'public_host' => $publicHost,
                'public_port' => validatePort($publicPort, 'routes[' . $index . '].public_port'),
                'server_host' => $serverHost,
                'server_port' => validatePort($route['server_port'] ?? null, 'routes[' . $index . '].server_port'),
                'session_count' => $sessionCount,
            ];
        }

        return $routes;
    }

    $relayUrl = trim((string)($config['relay_url'] ?? ''));
    if ($relayUrl === '') {
        throw new InvalidArgumentException('relay_url is required');
    }
    if ($topLevelPublicHost === '') {
        throw new InvalidArgumentException('public_host is required');
    }
    $serverHost = trim((string)($config['server_host'] ?? ''));
    if ($serverHost === '') {
        throw new InvalidArgumentException('server_host is required');
    }

    $sessionCount = $config['session_count'] ?? null;
    if (!is_int($sessionCount) && !(is_string($sessionCount) && ctype_digit($sessionCount))) {
        throw new InvalidArgumentException('session_count must be >= 1');
    }
    $sessionCount = (int)$sessionCount;
    if ($sessionCount < 1) {
        throw new InvalidArgumentException('session_count must be >= 1');
    }

    return [[
        'id' => 'primary',
        'enabled' => true,
        'relay_url' => $relayUrl,
        'relay_endpoint' => relayEndpointFromUrl($relayUrl),
        'public_host' => $topLevelPublicHost,
        'public_port' => validatePort($topLevelPublicPort, 'public_port'),
        'server_host' => $serverHost,
        'server_port' => validatePort($config['server_port'] ?? null, 'server_port'),
        'session_count' => $sessionCount,
    ]];
}

function runDiagnostics(array $routes, int $timeoutSec): array
{
    $results = [];
    $summary = [
        'routes' => count($routes),
        'checks' => 0,
        'reachable' => 0,
        'unreachable' => 0,
        'fastest_ms' => null,
    ];

    foreach ($routes as $route) {
        $checks = [
            [
                'label' => 'Relay endpoint',
                'target' => $route['relay_endpoint']['scheme'] . '://' . $route['relay_endpoint']['host'] . ':' . $route['relay_endpoint']['port'],
                'probe' => testTcpConnectivity($route['relay_endpoint']['host'], $route['relay_endpoint']['port'], $timeoutSec),
            ],
            [
                'label' => 'Client callback',
                'target' => $route['public_host'] . ':' . $route['public_port'],
                'probe' => testTcpConnectivity($route['public_host'], $route['public_port'], $timeoutSec),
            ],
            [
                'label' => 'Server agent',
                'target' => $route['server_host'] . ':' . $route['server_port'],
                'probe' => testTcpConnectivity($route['server_host'], $route['server_port'], $timeoutSec),
            ],
        ];

        $routeReachable = true;
        foreach ($checks as $check) {
            $summary['checks']++;
            if ($check['probe']['ok']) {
                $summary['reachable']++;
                if ($summary['fastest_ms'] === null || $check['probe']['latency_ms'] < $summary['fastest_ms']) {
                    $summary['fastest_ms'] = $check['probe']['latency_ms'];
                }
                continue;
            }

            $summary['unreachable']++;
            $routeReachable = false;
        }

        $results[] = [
            'id' => $route['id'],
            'enabled' => $route['enabled'],
            'session_count' => $route['session_count'],
            'relay_url' => $route['relay_url'],
            'client_target' => $route['public_host'] . ':' . $route['public_port'],
            'server_target' => $route['server_host'] . ':' . $route['server_port'],
            'ok' => $routeReachable,
            'checks' => $checks,
        ];
    }

    return [$results, $summary];
}

$configContent = '';
$accessError = '';
$configError = '';
$diagnostics = [];
$summary = null;

if (($_POST['action'] ?? '') === 'unlock') {
    $submittedPasskey = (string)($_POST['passkey'] ?? '');
    if (hash_equals($DIAGNOSTICS_PASSKEY, $submittedPasskey)) {
        session_regenerate_id(true);
        $_SESSION['diagnostics_unlocked'] = true;
        header('Location: ' . strtok($_SERVER['REQUEST_URI'] ?? '', '?'));
        exit;
    }
    $accessError = 'Invalid passkey.';
}

if (($_POST['action'] ?? '') === 'logout') {
    $_SESSION = [];
    if (ini_get('session.use_cookies')) {
        $params = session_get_cookie_params();
        setcookie(session_name(), '', time() - 42000, $params['path'], $params['domain'], (bool)$params['secure'], (bool)$params['httponly']);
    }
    session_destroy();
    header('Location: ' . strtok($_SERVER['REQUEST_URI'] ?? '', '?'));
    exit;
}

$isUnlocked = !empty($_SESSION['diagnostics_unlocked']);

if ($isUnlocked && ($_POST['action'] ?? '') === 'run') {
    $configContent = trim((string)($_POST['config_content'] ?? ''));

    if ($configContent === '') {
        $configError = 'Paste the full client config JSON first.';
    } else {
        try {
            $decoded = json_decode($configContent, true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($decoded)) {
                throw new InvalidArgumentException('The submitted JSON must decode to an object.');
            }

            $routes = buildDiagnosticsRoutes($decoded);
            [$diagnostics, $summary] = runDiagnostics($routes, $DIAGNOSTICS_CONNECT_TIMEOUT_SEC);
        } catch (JsonException $e) {
            $configError = 'Invalid JSON: ' . $e->getMessage();
        } catch (InvalidArgumentException $e) {
            $configError = $e->getMessage();
        }
    }
}
?>
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FURO Route Diagnostics</title>
<style>
    :root {
        color-scheme: light;
        --bg: #eef2ff;
        --bg-accent: radial-gradient(circle at top left, rgba(59, 130, 246, 0.22), transparent 34%), radial-gradient(circle at bottom right, rgba(16, 185, 129, 0.18), transparent 28%), linear-gradient(180deg, #f8fbff 0%, #eef2ff 100%);
        --panel: rgba(255, 255, 255, 0.8);
        --panel-strong: rgba(255, 255, 255, 0.92);
        --text: #10203a;
        --muted: #5a6a86;
        --border: rgba(148, 163, 184, 0.28);
        --shadow: 0 28px 60px rgba(15, 23, 42, 0.12);
        --success: #0f9f6e;
        --danger: #d1435b;
        --warning: #d07d11;
        --chip: rgba(15, 23, 42, 0.06);
        --textarea: rgba(248, 250, 252, 0.82);
        --button: linear-gradient(135deg, #2563eb, #0f766e);
        --button-text: #f8fbff;
    }

    [data-theme="dark"] {
        color-scheme: dark;
        --bg: #08111f;
        --bg-accent: radial-gradient(circle at top left, rgba(37, 99, 235, 0.35), transparent 30%), radial-gradient(circle at bottom right, rgba(20, 184, 166, 0.2), transparent 26%), linear-gradient(180deg, #07111d 0%, #0b1728 100%);
        --panel: rgba(8, 17, 31, 0.7);
        --panel-strong: rgba(8, 17, 31, 0.88);
        --text: #ebf3ff;
        --muted: #96a7c4;
        --border: rgba(148, 163, 184, 0.2);
        --shadow: 0 30px 80px rgba(2, 6, 23, 0.42);
        --success: #34d399;
        --danger: #fb7185;
        --warning: #fbbf24;
        --chip: rgba(255, 255, 255, 0.06);
        --textarea: rgba(15, 23, 42, 0.72);
        --button: linear-gradient(135deg, #3b82f6, #14b8a6);
        --button-text: #03111c;
    }

    * {
        box-sizing: border-box;
    }

    body {
        margin: 0;
        min-height: 100vh;
        font-family: "Segoe UI", "Noto Sans", sans-serif;
        background: var(--bg-accent);
        color: var(--text);
    }

    .shell {
        width: min(1180px, calc(100% - 32px));
        margin: 0 auto;
        padding: 28px 0 48px;
    }

    .topbar,
    .hero,
    .panel,
    .route-card {
        backdrop-filter: blur(18px);
        background: var(--panel);
        border: 1px solid var(--border);
        box-shadow: var(--shadow);
    }

    .topbar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 16px;
        border-radius: 24px;
        padding: 16px 20px;
        margin-bottom: 20px;
    }

    .brand {
        display: flex;
        align-items: center;
        gap: 14px;
    }

    .brand-mark {
        width: 44px;
        height: 44px;
        border-radius: 14px;
        background: linear-gradient(135deg, rgba(37, 99, 235, 0.16), rgba(20, 184, 166, 0.18));
        display: grid;
        place-items: center;
        overflow: hidden;
    }

    .brand-mark svg {
        width: 34px;
        height: 34px;
    }

    .brand-copy strong,
    .hero-copy h1,
    .access-card h2,
    .panel h2,
    .route-card h3 {
        display: block;
        letter-spacing: -0.03em;
    }

    .brand-copy small,
    .hero-copy p,
    .hint,
    .meta,
    .badge-muted,
    .check-note,
    textarea,
    input,
    button {
        font: inherit;
    }

    .brand-copy small,
    .hero-copy p,
    .hint,
    .meta,
    .check-note,
    .summary-card span,
    .badge-muted,
    .field-note {
        color: var(--muted);
    }

    .actions-inline {
        display: flex;
        align-items: center;
        gap: 12px;
        flex-wrap: wrap;
    }

    .theme-toggle,
    .ghost-button {
        display: inline-flex;
        align-items: center;
        gap: 10px;
        border: 1px solid var(--border);
        background: var(--panel-strong);
        color: var(--text);
        border-radius: 999px;
        padding: 10px 14px;
        cursor: pointer;
        transition: transform 160ms ease, border-color 160ms ease, background 160ms ease;
    }

    .theme-toggle:hover,
    .ghost-button:hover,
    .primary-button:hover {
        transform: translateY(-1px);
    }

    .theme-toggle svg,
    .ghost-button svg {
        width: 18px;
        height: 18px;
    }

    .hero {
        border-radius: 30px;
        padding: 30px;
        display: grid;
        grid-template-columns: minmax(0, 1.2fr) minmax(280px, 0.8fr);
        gap: 26px;
        margin-bottom: 20px;
    }

    .hero-copy h1 {
        font-size: clamp(2rem, 4vw, 3.25rem);
        margin: 0 0 14px;
        line-height: 0.96;
    }

    .hero-copy p {
        font-size: 1rem;
        line-height: 1.7;
        margin: 0 0 24px;
        max-width: 64ch;
    }

    .chip-row {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
    }

    .chip {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        border-radius: 999px;
        padding: 10px 14px;
        background: var(--chip);
        border: 1px solid rgba(148, 163, 184, 0.12);
        font-size: 0.95rem;
    }

    .hero-visual {
        position: relative;
        min-height: 260px;
        display: grid;
        place-items: center;
        overflow: hidden;
        border-radius: 24px;
        background:
            radial-gradient(circle at center, rgba(37, 99, 235, 0.18), transparent 30%),
            linear-gradient(160deg, rgba(255, 255, 255, 0.08), rgba(255, 255, 255, 0.02));
        border: 1px solid rgba(148, 163, 184, 0.12);
    }

    .hero-visual svg {
        width: min(100%, 360px);
        height: auto;
    }

    .orbit {
        transform-origin: center;
        animation: spin 16s linear infinite;
    }

    .pulse {
        transform-origin: center;
        animation: pulse 2.8s ease-in-out infinite;
    }

    .access-wrap {
        min-height: calc(100vh - 76px);
        display: grid;
        place-items: center;
    }

    .access-card {
        width: min(520px, 100%);
        border-radius: 30px;
        padding: 32px;
        background: var(--panel);
        border: 1px solid var(--border);
        box-shadow: var(--shadow);
        backdrop-filter: blur(20px);
    }

    .access-art {
        width: 92px;
        height: 92px;
        margin-bottom: 18px;
    }

    .access-art svg {
        width: 100%;
        height: 100%;
    }

    .access-card h2 {
        font-size: 2rem;
        margin: 0 0 12px;
    }

    .access-card p {
        margin: 0 0 22px;
        color: var(--muted);
        line-height: 1.7;
    }

    form {
        margin: 0;
    }

    label {
        display: block;
        font-size: 0.93rem;
        margin-bottom: 8px;
        font-weight: 600;
        color: var(--muted);
    }

    input,
    textarea {
        width: 100%;
        border-radius: 18px;
        border: 1px solid var(--border);
        background: var(--textarea);
        color: var(--text);
        padding: 14px 16px;
        outline: none;
        transition: border-color 160ms ease, box-shadow 160ms ease;
    }

    textarea {
        min-height: 300px;
        resize: vertical;
        font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
        line-height: 1.55;
    }

    input:focus,
    textarea:focus {
        border-color: rgba(37, 99, 235, 0.45);
        box-shadow: 0 0 0 4px rgba(37, 99, 235, 0.12);
    }

    .primary-button {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        gap: 10px;
        border: 0;
        border-radius: 999px;
        padding: 13px 18px;
        background: var(--button);
        color: var(--button-text);
        cursor: pointer;
        font-weight: 700;
        min-width: 170px;
        box-shadow: 0 18px 30px rgba(37, 99, 235, 0.24);
        transition: transform 160ms ease, filter 160ms ease;
    }

    .primary-button svg {
        width: 18px;
        height: 18px;
    }

    .primary-button.is-submitting .button-idle {
        display: none;
    }

    .primary-button .button-busy {
        display: none;
    }

    .primary-button.is-submitting .button-busy {
        display: inline-flex;
        align-items: center;
        gap: 10px;
    }

    .primary-button.is-submitting .button-busy svg {
        animation: spin 0.9s linear infinite;
    }

    .field-note {
        display: block;
        margin-top: 8px;
        font-size: 0.93rem;
        line-height: 1.5;
    }

    .grid {
        display: grid;
        gap: 20px;
    }

    .panel {
        border-radius: 28px;
        padding: 24px;
    }

    .summary-grid,
    .routes-grid {
        display: grid;
        gap: 16px;
    }

    .summary-grid {
        grid-template-columns: repeat(4, minmax(0, 1fr));
        margin-top: 18px;
    }

    .summary-card {
        border-radius: 22px;
        padding: 18px;
        background: var(--panel-strong);
        border: 1px solid var(--border);
    }

    .summary-card strong {
        display: block;
        font-size: 1.65rem;
        margin-bottom: 6px;
    }

    .route-card {
        border-radius: 24px;
        padding: 22px;
    }

    .route-head,
    .check-row,
    .form-head {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 14px;
        flex-wrap: wrap;
    }

    .route-head {
        margin-bottom: 18px;
    }

    .route-card h3,
    .panel h2 {
        margin: 0;
        font-size: 1.3rem;
    }

    .badge-row {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
    }

    .badge,
    .badge-muted {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        border-radius: 999px;
        padding: 8px 12px;
        font-size: 0.88rem;
        border: 1px solid var(--border);
        background: var(--chip);
    }

    .badge-ok {
        color: var(--success);
        border-color: rgba(15, 159, 110, 0.25);
    }

    .badge-bad {
        color: var(--danger);
        border-color: rgba(209, 67, 91, 0.24);
    }

    .badge-disabled {
        color: var(--warning);
        border-color: rgba(208, 125, 17, 0.22);
    }

    .checks {
        display: grid;
        gap: 12px;
    }

    .check-row {
        padding: 16px 18px;
        border-radius: 20px;
        background: var(--panel-strong);
        border: 1px solid var(--border);
    }

    .check-copy strong {
        display: block;
        margin-bottom: 4px;
    }

    .check-copy code,
    .meta code {
        font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
        font-size: 0.94em;
    }

    .check-stats {
        text-align: right;
        min-width: 132px;
    }

    .latency {
        display: block;
        font-size: 1.1rem;
        font-weight: 700;
        margin-bottom: 4px;
    }

    .flash {
        margin-bottom: 16px;
        border-radius: 18px;
        padding: 14px 16px;
        border: 1px solid transparent;
        line-height: 1.55;
    }

    .flash-error {
        color: var(--danger);
        border-color: rgba(209, 67, 91, 0.18);
        background: rgba(209, 67, 91, 0.08);
    }

    .empty-state {
        border-radius: 24px;
        padding: 24px;
        border: 1px dashed var(--border);
        background: rgba(255, 255, 255, 0.04);
        line-height: 1.7;
        color: var(--muted);
    }

    @keyframes spin {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
    }

    @keyframes pulse {
        0%, 100% { transform: scale(1); opacity: 0.65; }
        50% { transform: scale(1.08); opacity: 1; }
    }

    @media (max-width: 980px) {
        .hero {
            grid-template-columns: 1fr;
        }

        .summary-grid {
            grid-template-columns: repeat(2, minmax(0, 1fr));
        }
    }

    @media (max-width: 640px) {
        .shell {
            width: min(100% - 20px, 100%);
            padding-top: 20px;
        }

        .topbar,
        .hero,
        .panel,
        .route-card,
        .access-card {
            border-radius: 22px;
        }

        .hero,
        .panel,
        .route-card,
        .access-card {
            padding: 20px;
        }

        .summary-grid {
            grid-template-columns: 1fr;
        }

        .check-stats {
            text-align: left;
            min-width: 0;
        }
    }
</style>
</head>
<body>
<div class="shell">
    <div class="topbar">
        <div class="brand">
            <div class="brand-mark" aria-hidden="true">
                <svg viewBox="0 0 64 64" fill="none">
                    <circle class="pulse" cx="32" cy="32" r="22" stroke="currentColor" stroke-opacity=".22" stroke-width="4"/>
                    <path d="M18 33.5h11.5L35 19l6 27 5.5-12.5H58" stroke="currentColor" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>
                </svg>
            </div>
            <div class="brand-copy">
                <strong>FURO Route Diagnostics</strong>
                <small>Relay-side TCP reachability for client and route endpoints.</small>
            </div>
        </div>
        <div class="actions-inline">
            <button type="button" class="theme-toggle" id="themeToggle" aria-label="Toggle theme">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
                    <path d="M12 3v2.2M12 18.8V21M4.9 4.9l1.6 1.6M17.5 17.5l1.6 1.6M3 12h2.2M18.8 12H21M4.9 19.1l1.6-1.6M17.5 6.5l1.6-1.6"/>
                    <circle cx="12" cy="12" r="4.6"/>
                </svg>
                <span>Theme</span>
            </button>
            <?php if ($isUnlocked): ?>
                <form method="post">
                    <input type="hidden" name="action" value="logout">
                    <button type="submit" class="ghost-button">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
                            <path d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4"/>
                            <path d="M10 17l5-5-5-5"/>
                            <path d="M15 12H3"/>
                        </svg>
                        <span>Lock</span>
                    </button>
                </form>
            <?php endif; ?>
        </div>
    </div>

    <?php if (!$isUnlocked): ?>
        <div class="access-wrap">
            <div class="access-card">
                <div class="access-art" aria-hidden="true">
                    <svg viewBox="0 0 120 120" fill="none">
                        <circle class="pulse" cx="60" cy="60" r="44" stroke="currentColor" stroke-opacity=".16" stroke-width="4"/>
                        <circle class="pulse" cx="60" cy="60" r="30" stroke="currentColor" stroke-opacity=".28" stroke-width="4" style="animation-delay: .25s"/>
                        <rect x="36" y="52" width="48" height="34" rx="12" fill="currentColor" fill-opacity=".1" stroke="currentColor" stroke-opacity=".42" stroke-width="4"/>
                        <path d="M45 52V40c0-8.28 6.72-15 15-15s15 6.72 15 15v12" stroke="currentColor" stroke-opacity=".7" stroke-width="4" stroke-linecap="round"/>
                        <circle cx="60" cy="68" r="4" fill="currentColor"/>
                        <path d="M60 72v6" stroke="currentColor" stroke-width="4" stroke-linecap="round"/>
                    </svg>
                </div>
                <h2>Enter passkey</h2>
                <p>This page exposes relay-side diagnostics, so it stays locked until the built-in passkey matches.</p>
                <?php if ($accessError !== ''): ?>
                    <div class="flash flash-error"><?= h($accessError) ?></div>
                <?php endif; ?>
                <form method="post" autocomplete="off">
                    <input type="hidden" name="action" value="unlock">
                    <label for="passkey">Passkey</label>
                    <input id="passkey" name="passkey" type="password" placeholder="Enter passkey" autofocus required>
                    <button type="submit" class="primary-button" style="margin-top: 18px;">
                        <span class="button-idle">Unlock diagnostics</span>
                        <span class="button-busy">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M12 3a9 9 0 1 0 9 9"/>
                            </svg>
                            <span>Unlocking</span>
                        </span>
                    </button>
                </form>
            </div>
        </div>
    <?php else: ?>
        <section class="hero">
            <div class="hero-copy">
                <h1>Paste the client config and probe every route from the relay host.</h1>
                <p>The page parses the same route fields FURO uses, then measures TCP reachability and connect latency for the relay origin, the client callback endpoint, and the server agent on every discovered route.</p>
                <div class="chip-row">
                    <div class="chip">Hardcoded passkey gate</div>
                    <div class="chip">Theme preference saved locally</div>
                    <div class="chip">No config draft stored in localStorage</div>
                </div>
            </div>
            <div class="hero-visual" aria-hidden="true">
                <svg viewBox="0 0 360 280" fill="none">
                    <g class="orbit">
                        <circle cx="180" cy="140" r="96" stroke="currentColor" stroke-opacity=".16" stroke-width="2" stroke-dasharray="10 10"/>
                        <circle cx="180" cy="140" r="126" stroke="currentColor" stroke-opacity=".12" stroke-width="2" stroke-dasharray="6 12"/>
                    </g>
                    <g class="pulse">
                        <rect x="128" y="88" width="104" height="104" rx="28" fill="currentColor" fill-opacity=".08" stroke="currentColor" stroke-opacity=".18"/>
                        <path d="M108 140h46l18-42 22 86 18-44h40" stroke="currentColor" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/>
                    </g>
                    <circle cx="84" cy="102" r="12" fill="currentColor" fill-opacity=".14"/>
                    <circle cx="272" cy="82" r="10" fill="currentColor" fill-opacity=".18"/>
                    <circle cx="288" cy="192" r="14" fill="currentColor" fill-opacity=".12"/>
                    <circle cx="70" cy="198" r="8" fill="currentColor" fill-opacity=".18"/>
                </svg>
            </div>
        </section>

        <section class="panel">
            <div class="form-head">
                <div>
                    <h2>Client Config Input</h2>
                    <p class="hint">Paste the full `config.client.json` content. Routes with per-route `public_host` and `public_port` overrides are handled automatically.</p>
                </div>
            </div>

            <?php if ($configError !== ''): ?>
                <div class="flash flash-error"><?= h($configError) ?></div>
            <?php endif; ?>

            <form method="post" id="diagnosticsForm">
                <input type="hidden" name="action" value="run">
                <label for="config_content">Client config JSON</label>
                <textarea id="config_content" name="config_content" spellcheck="false" placeholder="{&#10;  &quot;public_host&quot;: &quot;37.32.14.205&quot;,&#10;  &quot;routes&quot;: [...]&#10;}"><?= h($configContent) ?></textarea>
                <span class="field-note">Only the theme preference is saved locally. The pasted config stays in the current request/session render only.</span>
                <button type="submit" class="primary-button" id="runButton" style="margin-top: 18px;">
                    <span class="button-idle">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M5 12h14M13 5l7 7-7 7"/>
                        </svg>
                        <span>Run diagnostics</span>
                    </span>
                    <span class="button-busy">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M12 3a9 9 0 1 0 9 9"/>
                        </svg>
                        <span>Probing endpoints</span>
                    </span>
                </button>
            </form>
        </section>

        <section class="grid" style="margin-top: 20px;">
            <?php if ($summary !== null): ?>
                <div class="panel">
                    <h2>Summary</h2>
                    <div class="summary-grid">
                        <div class="summary-card">
                            <strong><?= h($summary['routes']) ?></strong>
                            <span>Routes analyzed</span>
                        </div>
                        <div class="summary-card">
                            <strong><?= h($summary['checks']) ?></strong>
                            <span>Endpoint checks</span>
                        </div>
                        <div class="summary-card">
                            <strong><?= h($summary['reachable']) ?></strong>
                            <span>Reachable checks</span>
                        </div>
                        <div class="summary-card">
                            <strong><?= $summary['fastest_ms'] !== null ? h($summary['fastest_ms']) . ' ms' : 'n/a' ?></strong>
                            <span>Fastest successful TCP connect</span>
                        </div>
                    </div>
                </div>

                <div class="routes-grid">
                    <?php foreach ($diagnostics as $route): ?>
                        <article class="route-card">
                            <div class="route-head">
                                <div>
                                    <h3><?= h($route['id']) ?></h3>
                                    <div class="meta">Relay URL: <code><?= h($route['relay_url']) ?></code></div>
                                </div>
                                <div class="badge-row">
                                    <span class="badge <?= $route['ok'] ? 'badge-ok' : 'badge-bad' ?>">
                                        <?= $route['ok'] ? 'Reachable' : 'Attention needed' ?>
                                    </span>
                                    <?php if (!$route['enabled']): ?>
                                        <span class="badge badge-disabled">Disabled in config</span>
                                    <?php endif; ?>
                                    <span class="badge-muted">Sessions: <?= h($route['session_count']) ?></span>
                                </div>
                            </div>

                            <div class="checks">
                                <?php foreach ($route['checks'] as $check): ?>
                                    <div class="check-row">
                                        <div class="check-copy">
                                            <strong><?= h($check['label']) ?></strong>
                                            <div class="meta"><code><?= h($check['target']) ?></code></div>
                                            <div class="check-note"><?= h($check['probe']['message']) ?></div>
                                        </div>
                                        <div class="check-stats">
                                            <span class="latency"><?= h($check['probe']['latency_ms']) ?> ms</span>
                                            <span class="<?= $check['probe']['ok'] ? 'badge-ok' : 'badge-bad' ?>">
                                                <?= $check['probe']['ok'] ? 'TCP reachable' : 'TCP failed' ?>
                                            </span>
                                        </div>
                                    </div>
                                <?php endforeach; ?>
                            </div>
                        </article>
                    <?php endforeach; ?>
                </div>
            <?php else: ?>
                <div class="panel">
                    <div class="empty-state">No diagnostics have been run yet. Paste a client config above to inspect every route from the PHP host’s point of view.</div>
                </div>
            <?php endif; ?>
        </section>
    <?php endif; ?>
</div>

<script>
    (function () {
        const storageKey = 'furo-route-diagnostics-theme';
        const root = document.documentElement;
        const toggle = document.getElementById('themeToggle');
        const saved = localStorage.getItem(storageKey);
        const preferred = saved || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
        root.setAttribute('data-theme', preferred);

        if (toggle) {
            toggle.addEventListener('click', function () {
                const next = root.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
                root.setAttribute('data-theme', next);
                localStorage.setItem(storageKey, next);
            });
        }

        const form = document.getElementById('diagnosticsForm');
        const button = document.getElementById('runButton');
        if (form && button) {
            form.addEventListener('submit', function () {
                button.classList.add('is-submitting');
            });
        }
    }());
</script>
</body>
</html>
