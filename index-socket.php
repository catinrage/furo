<?php
declare(strict_types=1);

$API_KEY = 'my_super_secret_123456789';
$CONNECT_TIMEOUT_SEC = 8;
$READ_IDLE_TIMEOUT_SEC = 300;
$BUFFER_SIZE = 262144;

ignore_user_abort(true);
set_time_limit(0);

function auth(string $apiKey): void
{
    $key = $_SERVER['HTTP_X_API_KEY'] ?? ($_GET['key'] ?? null);
    if ($key !== $apiKey) {
        http_response_code(403);
        header('Content-Type: application/json');
        echo json_encode(['error' => 'Unauthorized']);
        exit;
    }
}

function failJson(string $message, int $code = 400, array $extra = []): void
{
    http_response_code($code);
    header('Content-Type: application/json');
    echo json_encode(array_merge(['error' => $message], $extra), JSON_UNESCAPED_SLASHES);
    exit;
}

function connectTcp(string $host, int $port, int $timeoutSec): Socket
{
    $socket = @socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
    if ($socket === false) {
        throw new RuntimeException('socket_create failed');
    }

    @socket_set_option($socket, SOL_SOCKET, SO_SNDTIMEO, ['sec' => $timeoutSec, 'usec' => 0]);
    @socket_set_option($socket, SOL_SOCKET, SO_RCVTIMEO, ['sec' => $timeoutSec, 'usec' => 0]);
    @socket_set_option($socket, SOL_TCP, TCP_NODELAY, 1);
    @socket_set_option($socket, SOL_SOCKET, SO_KEEPALIVE, 1);
    @socket_set_option($socket, SOL_SOCKET, SO_RCVBUF, 262144);
    @socket_set_option($socket, SOL_SOCKET, SO_SNDBUF, 262144);

    if (!@socket_connect($socket, $host, $port)) {
        $err = socket_strerror(socket_last_error($socket));
        @socket_close($socket);
        throw new RuntimeException("socket_connect failed to {$host}:{$port}: {$err}");
    }

    return $socket;
}

function setSocketTimeout(Socket $socket, int $seconds): void
{
    @socket_set_option($socket, SOL_SOCKET, SO_SNDTIMEO, ['sec' => $seconds, 'usec' => 0]);
    @socket_set_option($socket, SOL_SOCKET, SO_RCVTIMEO, ['sec' => $seconds, 'usec' => 0]);
}

function shutdownWrite(Socket $socket): void
{
    @socket_shutdown($socket, 1);
}

function closeSocket(?Socket $socket): void
{
    if ($socket instanceof Socket) {
        @socket_close($socket);
    }
}

function bridgeSockets(Socket $left, Socket $right, int $bufferSize, int $idleTimeoutSec): array
{
    $leftToRight = 0;
    $rightToLeft = 0;
    $lastActivity = time();

    while (true) {
        $read = [$left, $right];
        $write = null;
        $except = null;
        $changed = @socket_select($read, $write, $except, 1, 0);
        if ($changed === false) {
            throw new RuntimeException('socket_select failed');
        }
        if ($changed === 0) {
            if (time() - $lastActivity > $idleTimeoutSec) {
                break;
            }
            continue;
        }

        foreach ($read as $source) {
            $data = @socket_read($source, $bufferSize, PHP_BINARY_READ);
            if ($data === false || $data === '') {
                shutdownWrite($left);
                shutdownWrite($right);
                return [
                    'left_to_right_bytes' => $leftToRight,
                    'right_to_left_bytes' => $rightToLeft,
                ];
            }

            $lastActivity = time();
            if ($source === $left) {
                socket_write_all($right, $data);
                $leftToRight += strlen($data);
            } else {
                socket_write_all($left, $data);
                $rightToLeft += strlen($data);
            }
        }
    }

    return [
        'left_to_right_bytes' => $leftToRight,
        'right_to_left_bytes' => $rightToLeft,
    ];
}

function socket_write_all(Socket $socket, string $payload): void
{
    $offset = 0;
    $length = strlen($payload);
    while ($offset < $length) {
        $written = @socket_write($socket, substr($payload, $offset), $length - $offset);
        if ($written === false || $written === 0) {
            throw new RuntimeException('socket_write failed: ' . socket_strerror(socket_last_error($socket)));
        }
        $offset += $written;
    }
}

auth($API_KEY);
$action = $_GET['action'] ?? null;

if ($action !== 'session') {
    failJson('Invalid action', 400, ['expected' => 'session']);
}

if (!function_exists('socket_create')) {
    failJson('PHP sockets extension is not available', 500);
}

$iranHost = (string)($_GET['iran_host'] ?? '');
$iranPort = (int)($_GET['iran_port'] ?? 0);
$outerHost = (string)($_GET['outer_host'] ?? '');
$outerPort = (int)($_GET['outer_port'] ?? 0);

if ($iranHost === '' || $iranPort < 1 || $iranPort > 65535 || $outerHost === '' || $outerPort < 1 || $outerPort > 65535) {
    failJson('Missing or invalid parameters', 400, [
        'required' => ['iran_host', 'iran_port', 'outer_host', 'outer_port'],
    ]);
}

$iranSocket = null;
$outerSocket = null;

try {
    $iranSocket = connectTcp($iranHost, $iranPort, $CONNECT_TIMEOUT_SEC);
    $outerSocket = connectTcp($outerHost, $outerPort, $CONNECT_TIMEOUT_SEC);

    setSocketTimeout($iranSocket, 0);
    setSocketTimeout($outerSocket, 0);

    header('Content-Type: text/plain; charset=UTF-8');
    header('X-Accel-Buffering: no');
    echo "OK\n";
    @ob_flush();
    @flush();

    $stats = bridgeSockets($iranSocket, $outerSocket, $BUFFER_SIZE, $READ_IDLE_TIMEOUT_SEC);
    closeSocket($iranSocket);
    closeSocket($outerSocket);
    error_log(sprintf('[INDEX-SOCKET] session bridged %d/%d bytes', $stats['left_to_right_bytes'], $stats['right_to_left_bytes']));
    exit;
} catch (Throwable $e) {
    closeSocket($iranSocket);
    closeSocket($outerSocket);
    failJson('Session bridge failed', 502, ['detail' => $e->getMessage()]);
}
