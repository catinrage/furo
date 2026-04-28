<?php
declare(strict_types=1);

/*
|--------------------------------------------------------------------------
| FURO relay configuration
|--------------------------------------------------------------------------
*/
$RELAY_API_KEY = 'my_super_secret_123456789';
$RELAY_CONNECT_TIMEOUT_SEC = 8;
$RELAY_IDLE_TIMEOUT_SEC = 300;
$RELAY_BUFFER_SIZE = 256 * 1024;
$RELAY_IO_CHUNK_SIZE = 128 * 1024;
$RELAY_MAX_PENDING_BYTES = 4 * 1024 * 1024;
$RELAY_ENABLE_LOGS = false;

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

function relayLog(string $message): void
{
    global $RELAY_ENABLE_LOGS;
    if (!$RELAY_ENABLE_LOGS) {
        return;
    }
    error_log('[FURO-RELAY] ' . $message);
}

function cleanToken(string $value): string
{
    return preg_replace('/[^a-zA-Z0-9_\-\.]/', '', $value) ?? '';
}

function isConnectInProgressError(int $errorCode): bool
{
    $pending = [];
    foreach (['SOCKET_EINPROGRESS', 'SOCKET_EALREADY', 'SOCKET_EWOULDBLOCK'] as $name) {
        if (defined($name)) {
            $pending[] = constant($name);
        }
    }

    return in_array($errorCode, $pending, true);
}

function isWouldBlockError(int $errorCode): bool
{
    $pending = [];
    foreach (['SOCKET_EAGAIN', 'SOCKET_EWOULDBLOCK', 'SOCKET_EINPROGRESS'] as $name) {
        if (defined($name)) {
            $pending[] = constant($name);
        }
    }

    return in_array($errorCode, $pending, true);
}

function waitForConnect(Socket $socket, string $host, int $port, int $timeoutSec): void
{
    $read = [];
    $write = [$socket];
    $except = [$socket];
    $changed = @socket_select($read, $write, $except, $timeoutSec, 0);
    if ($changed === false) {
        $err = socket_last_error($socket);
        throw new RuntimeException(
            sprintf(
                'socket_connect wait failed to %s:%d: %s',
                $host,
                $port,
                socket_strerror($err)
            )
        );
    }
    if ($changed === 0) {
        throw new RuntimeException(
            sprintf('socket_connect timed out to %s:%d', $host, $port)
        );
    }

    $soError = @socket_get_option($socket, SOL_SOCKET, SO_ERROR);
    if (!is_int($soError)) {
        throw new RuntimeException(
            sprintf('socket_connect state check failed to %s:%d', $host, $port)
        );
    }
    if ($soError !== 0) {
        throw new RuntimeException(
            sprintf(
                'socket_connect failed to %s:%d: %s',
                $host,
                $port,
                socket_strerror($soError)
            )
        );
    }
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
    @socket_set_option($socket, SOL_SOCKET, SO_RCVBUF, 1024 * 1024);
    @socket_set_option($socket, SOL_SOCKET, SO_SNDBUF, 1024 * 1024);

    if (!@socket_connect($socket, $host, $port)) {
        $errCode = socket_last_error($socket);
        if (isConnectInProgressError($errCode)) {
            try {
                waitForConnect($socket, $host, $port, $timeoutSec);
            } catch (Throwable $e) {
                @socket_close($socket);
                throw $e;
            }
        } else {
            $err = socket_strerror($errCode);
            @socket_close($socket);
            throw new RuntimeException("socket_connect failed to {$host}:{$port}: {$err}");
        }
    }

    return $socket;
}

function setSocketTimeout(Socket $socket, int $seconds): void
{
    @socket_set_option($socket, SOL_SOCKET, SO_SNDTIMEO, ['sec' => $seconds, 'usec' => 0]);
    @socket_set_option($socket, SOL_SOCKET, SO_RCVTIMEO, ['sec' => $seconds, 'usec' => 0]);
}

function socketReadLine(Socket $socket, int $limit = 2048): string
{
    $line = '';
    while (strlen($line) < $limit) {
        $chunk = @socket_read($socket, 1, PHP_BINARY_READ);
        if ($chunk === false || $chunk === '') {
            throw new RuntimeException('socket_read failed: ' . socket_strerror(socket_last_error($socket)));
        }
        if ($chunk === "\n") {
            return rtrim($line, "\r");
        }
        $line .= $chunk;
    }

    throw new RuntimeException('line too long');
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

function setSocketNonBlocking(Socket $socket): void
{
    @socket_set_nonblock($socket);
}

function flushSocketBuffer(Socket $socket, string &$buffer, int $chunkSize): int
{
    $flushed = 0;
    while ($buffer !== '') {
        $writeLen = min(strlen($buffer), $chunkSize);
        $written = @socket_write($socket, $buffer, $writeLen);
        if ($written === false) {
            $err = socket_last_error($socket);
            if (isWouldBlockError($err)) {
                return $flushed;
            }
            throw new RuntimeException('socket_write failed: ' . socket_strerror($err));
        }
        if ($written === 0) {
            throw new RuntimeException('socket_write returned 0');
        }

        $flushed += $written;
        if ($written === strlen($buffer)) {
            $buffer = '';
            return $flushed;
        }
        $buffer = (string)substr($buffer, $written);
    }

    return $flushed;
}

function bridgeSockets(Socket $left, Socket $right, int $bufferSize, int $idleTimeoutSec, int $chunkSize, int $maxPendingBytes): array
{
    $leftToRight = 0;
    $rightToLeft = 0;
    $lastActivity = time();
    $leftClosed = false;
    $rightClosed = false;
    $leftWriteShutdown = false;
    $rightWriteShutdown = false;
    $leftToRightBuffer = '';
    $rightToLeftBuffer = '';

    setSocketNonBlocking($left);
    setSocketNonBlocking($right);
    $chunkSize = max(4096, min($bufferSize, $chunkSize));

    while (true) {
        $read = [];
        if (!$leftClosed && strlen($leftToRightBuffer) < $maxPendingBytes) {
            $read[] = $left;
        }
        if (!$rightClosed && strlen($rightToLeftBuffer) < $maxPendingBytes) {
            $read[] = $right;
        }

        $write = [];
        if ($leftToRightBuffer !== '') {
            $write[] = $right;
        }
        if ($rightToLeftBuffer !== '') {
            $write[] = $left;
        }
        $except = null;
        $changed = @socket_select($read, $write, $except, 1, 0);
        if ($changed === false) {
            $leftErr = socket_last_error($left);
            $rightErr = socket_last_error($right);
            throw new RuntimeException(
                sprintf(
                    'socket_select failed left=%s right=%s',
                    socket_strerror($leftErr),
                    socket_strerror($rightErr)
                )
            );
        }
        if ($changed === 0) {
            if (time() - $lastActivity > $idleTimeoutSec) {
                return [
                    'left_to_right_bytes' => $leftToRight,
                    'right_to_left_bytes' => $rightToLeft,
                    'reason' => 'idle-timeout',
                ];
            }
            continue;
        }

        foreach ($read as $source) {
            $data = @socket_read($source, $chunkSize, PHP_BINARY_READ);
            if ($data === false) {
                $err = socket_last_error($source);
                if (isWouldBlockError($err)) {
                    continue;
                }
                throw new RuntimeException('socket_read failed: ' . socket_strerror($err));
            }
            if ($data === '') {
                if ($source === $left) {
                    $leftClosed = true;
                    if ($leftToRightBuffer === '' && !$rightWriteShutdown) {
                        shutdownWrite($right);
                        $rightWriteShutdown = true;
                    }
                } else {
                    $rightClosed = true;
                    if ($rightToLeftBuffer === '' && !$leftWriteShutdown) {
                        shutdownWrite($left);
                        $leftWriteShutdown = true;
                    }
                }
                continue;
            }

            $lastActivity = time();
            if ($source === $left) {
                $leftToRight += strlen($data);
                $leftToRightBuffer .= $data;
            } else {
                $rightToLeft += strlen($data);
                $rightToLeftBuffer .= $data;
            }
        }

        if ($leftToRightBuffer !== '') {
            $written = flushSocketBuffer($right, $leftToRightBuffer, $chunkSize);
            if ($written > 0) {
                $lastActivity = time();
            }
            if ($leftClosed && $leftToRightBuffer === '' && !$rightWriteShutdown) {
                shutdownWrite($right);
                $rightWriteShutdown = true;
            }
        }
        if ($rightToLeftBuffer !== '') {
            $written = flushSocketBuffer($left, $rightToLeftBuffer, $chunkSize);
            if ($written > 0) {
                $lastActivity = time();
            }
            if ($rightClosed && $rightToLeftBuffer === '' && !$leftWriteShutdown) {
                shutdownWrite($left);
                $leftWriteShutdown = true;
            }
        }

        if ($leftClosed && $rightClosed && $leftToRightBuffer === '' && $rightToLeftBuffer === '') {
            return [
                'left_to_right_bytes' => $leftToRight,
                'right_to_left_bytes' => $rightToLeft,
                'reason' => 'both-closed',
            ];
        }
    }

    return [
        'left_to_right_bytes' => $leftToRight,
        'right_to_left_bytes' => $rightToLeft,
        'reason' => 'loop-exit',
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

auth($RELAY_API_KEY);
$action = $_GET['action'] ?? null;

if ($action !== 'session') {
    failJson('Invalid action', 400, ['expected' => 'session']);
}

if (!function_exists('socket_create')) {
    failJson('PHP sockets extension is not available', 500);
}

$clientHost = (string)($_GET['client_host'] ?? '');
$clientPort = (int)($_GET['client_port'] ?? 0);
$serverHost = (string)($_GET['server_host'] ?? '');
$serverPort = (int)($_GET['server_port'] ?? 0);
$sessionId = cleanToken((string)($_GET['sid'] ?? ''));

if ($clientHost === '' || $clientPort < 1 || $clientPort > 65535 || $serverHost === '' || $serverPort < 1 || $serverPort > 65535 || $sessionId === '') {
    failJson('Missing or invalid parameters', 400, [
        'required' => ['sid', 'client_host', 'client_port', 'server_host', 'server_port'],
    ]);
}

$clientSocket = null;
$serverSocket = null;
$startedAt = microtime(true);

try {
    relayLog(sprintf(
        'session_start sid=%s client=%s:%d server=%s:%d',
        $sessionId,
        $clientHost,
        $clientPort,
        $serverHost,
        $serverPort
    ));

    $clientSocket = connectTcp($clientHost, $clientPort, $RELAY_CONNECT_TIMEOUT_SEC);
    socket_write_all($clientSocket, sprintf("SESSION %s %s\n", $RELAY_API_KEY, $sessionId));
    $clientReply = socketReadLine($clientSocket);
    if ($clientReply !== 'OK') {
        throw new RuntimeException('Client session attach failed: ' . $clientReply);
    }

    $serverSocket = connectTcp($serverHost, $serverPort, $RELAY_CONNECT_TIMEOUT_SEC);
    socket_write_all($serverSocket, sprintf("SESSION %s %s\n", $RELAY_API_KEY, $sessionId));
    $serverReply = socketReadLine($serverSocket);
    if ($serverReply !== 'OK') {
        throw new RuntimeException('Server session attach failed: ' . $serverReply);
    }

    setSocketTimeout($clientSocket, 0);
    setSocketTimeout($serverSocket, 0);

    header('Content-Type: text/plain; charset=UTF-8');
    header('X-Accel-Buffering: no');
    echo "OK\n";
    @ob_flush();
    @flush();

    $stats = bridgeSockets(
        $clientSocket,
        $serverSocket,
        $RELAY_BUFFER_SIZE,
        $RELAY_IDLE_TIMEOUT_SEC,
        $RELAY_IO_CHUNK_SIZE,
        $RELAY_MAX_PENDING_BYTES
    );
    closeSocket($clientSocket);
    closeSocket($serverSocket);
    relayLog(sprintf(
        'session_end sid=%s reason=%s dur_ms=%.0f client_to_server=%d server_to_client=%d',
        $sessionId,
        $stats['reason'] ?? 'unknown',
        (microtime(true) - $startedAt) * 1000,
        $stats['left_to_right_bytes'],
        $stats['right_to_left_bytes']
    ));
    exit;
} catch (Throwable $e) {
    closeSocket($clientSocket);
    closeSocket($serverSocket);
    relayLog(sprintf(
        'session_fail sid=%s dur_ms=%.0f error=%s',
        $sessionId !== '' ? $sessionId : 'unknown',
        (microtime(true) - $startedAt) * 1000,
        $e->getMessage()
    ));
    failJson('Session bridge failed', 502, ['detail' => $e->getMessage()]);
}
