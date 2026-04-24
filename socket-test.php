<?php
declare(strict_types=1);

header('Content-Type: application/json; charset=UTF-8');

$host = (string)($_GET['host'] ?? '');
$port = (int)($_GET['port'] ?? 0);
$mode = strtolower((string)($_GET['mode'] ?? 'ping'));
$bytes = max(1, (int)($_GET['bytes'] ?? 1048576));
$rounds = max(1, min(20, (int)($_GET['rounds'] ?? 3)));
$timeoutSec = max(1.0, min(30.0, (float)($_GET['timeout'] ?? 5.0)));

if ($host === '' || $port <= 0 || $port > 65535) {
    http_response_code(400);
    echo json_encode([
        'error' => 'Missing or invalid host/port',
        'example' => '?host=YOUR_IRAN_VPS_IP&port=19090&mode=ping',
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
    exit;
}

if (!function_exists('socket_create')) {
    http_response_code(500);
    echo json_encode(['error' => 'PHP sockets extension is not available'], JSON_PRETTY_PRINT);
    exit;
}

function fail(string $message, array $extra = [], int $code = 500): void
{
    http_response_code($code);
    echo json_encode(array_merge(['error' => $message], $extra), JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
    exit;
}

function socketWriteAll(Socket $socket, string $data): void
{
    $offset = 0;
    $length = strlen($data);

    while ($offset < $length) {
        $written = @socket_write($socket, substr($data, $offset), $length - $offset);
        if ($written === false || $written === 0) {
            fail('socket_write failed', ['socket_error' => socket_strerror(socket_last_error($socket))]);
        }
        $offset += $written;
    }
}

function socketReadLine(Socket $socket, int $maxLen = 8192): string
{
    $line = '';
    while (strlen($line) < $maxLen) {
        $chunk = @socket_read($socket, 1, PHP_BINARY_READ);
        if ($chunk === '' || $chunk === false) {
            fail('socket_read line failed', ['socket_error' => socket_strerror(socket_last_error($socket))]);
        }
        if ($chunk === "\n") {
            return rtrim($line, "\r");
        }
        $line .= $chunk;
    }

    fail('Line too long');
}

function socketReadExact(Socket $socket, int $length): string
{
    $data = '';
    while (strlen($data) < $length) {
        $chunk = @socket_read($socket, $length - strlen($data), PHP_BINARY_READ);
        if ($chunk === '' || $chunk === false) {
            fail('socket_read exact failed', ['socket_error' => socket_strerror(socket_last_error($socket))]);
        }
        $data .= $chunk;
    }
    return $data;
}

function ms(float $start, float $end): float
{
    return round(($end - $start) * 1000, 2);
}

function mbps(int $bytes, float $seconds): float
{
    if ($seconds <= 0) {
        return 0.0;
    }
    return round(($bytes / 1048576) / $seconds, 3);
}

$socket = @socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
if ($socket === false) {
    fail('socket_create failed');
}

$sec = (int)$timeoutSec;
$usec = (int)(($timeoutSec - $sec) * 1000000);
@socket_set_option($socket, SOL_SOCKET, SO_SNDTIMEO, ['sec' => $sec, 'usec' => $usec]);
@socket_set_option($socket, SOL_SOCKET, SO_RCVTIMEO, ['sec' => $sec, 'usec' => $usec]);
@socket_set_option($socket, SOL_TCP, TCP_NODELAY, 1);
@socket_set_option($socket, SOL_SOCKET, SO_KEEPALIVE, 1);

$connectStart = microtime(true);
$connected = @socket_connect($socket, $host, $port);
$connectEnd = microtime(true);

if ($connected === false) {
    fail('socket_connect failed', [
        'host' => $host,
        'port' => $port,
        'socket_error' => socket_strerror(socket_last_error($socket)),
        'connect_ms' => ms($connectStart, $connectEnd),
    ]);
}

$result = [
    'host' => $host,
    'port' => $port,
    'mode' => $mode,
    'connect_ms' => ms($connectStart, $connectEnd),
];

try {
    if ($mode === 'ping') {
        $samples = [];
        for ($i = 0; $i < $rounds; $i++) {
            $t0 = microtime(true);
            socketWriteAll($socket, "PING\n");
            $reply = socketReadLine($socket);
            $t1 = microtime(true);

            if ($reply !== 'PONG') {
                fail('Unexpected ping reply', ['reply' => $reply]);
            }
            $samples[] = ms($t0, $t1);
        }

        $result['rounds'] = $rounds;
        $result['rtt_ms'] = $samples;
        $result['avg_rtt_ms'] = round(array_sum($samples) / count($samples), 2);
        $result['min_rtt_ms'] = min($samples);
        $result['max_rtt_ms'] = max($samples);
    } elseif ($mode === 'push') {
        $payload = random_bytes($bytes);
        $t0 = microtime(true);
        socketWriteAll($socket, "PUSH {$bytes}\n");
        $ready = socketReadLine($socket);
        if ($ready !== 'READY') {
            fail('Unexpected push prelude', ['reply' => $ready]);
        }
        socketWriteAll($socket, $payload);
        $reply = socketReadLine($socket);
        $t1 = microtime(true);

        if ($reply !== "OK {$bytes}") {
            fail('Unexpected push reply', ['reply' => $reply]);
        }

        $seconds = $t1 - $t0;
        $result['bytes'] = $bytes;
        $result['elapsed_ms'] = ms($t0, $t1);
        $result['throughput_mib_s'] = mbps($bytes, $seconds);
    } elseif ($mode === 'pull') {
        $t0 = microtime(true);
        socketWriteAll($socket, "PULL {$bytes}\n");
        $header = socketReadLine($socket);
        if ($header !== "DATA {$bytes}") {
            fail('Unexpected pull header', ['reply' => $header]);
        }
        $data = socketReadExact($socket, $bytes);
        $reply = socketReadLine($socket);
        $t1 = microtime(true);

        if (strlen($data) !== $bytes) {
            fail('Unexpected pull size', ['received' => strlen($data), 'expected' => $bytes]);
        }
        if ($reply !== "OK {$bytes}") {
            fail('Unexpected pull trailer', ['reply' => $reply]);
        }

        $seconds = $t1 - $t0;
        $result['bytes'] = $bytes;
        $result['elapsed_ms'] = ms($t0, $t1);
        $result['throughput_mib_s'] = mbps($bytes, $seconds);
        $result['sha1'] = sha1($data);
    } elseif ($mode === 'echo') {
        $payload = random_bytes($bytes);
        $expectedHash = sha1($payload);

        $t0 = microtime(true);
        socketWriteAll($socket, "ECHO {$bytes}\n");
        $ready = socketReadLine($socket);
        if ($ready !== 'READY') {
            fail('Unexpected echo prelude', ['reply' => $ready]);
        }
        socketWriteAll($socket, $payload);
        $header = socketReadLine($socket);
        if ($header !== "DATA {$bytes}") {
            fail('Unexpected echo header', ['reply' => $header]);
        }
        $echoed = socketReadExact($socket, $bytes);
        $reply = socketReadLine($socket);
        $t1 = microtime(true);

        if ($reply !== "OK {$bytes}") {
            fail('Unexpected echo trailer', ['reply' => $reply]);
        }

        $seconds = $t1 - $t0;
        $result['bytes'] = $bytes;
        $result['elapsed_ms'] = ms($t0, $t1);
        $result['roundtrip_mib_s'] = mbps($bytes * 2, $seconds);
        $result['sent_sha1'] = $expectedHash;
        $result['recv_sha1'] = sha1($echoed);
        $result['match'] = hash_equals($expectedHash, sha1($echoed));
    } else {
        fail('Unsupported mode', ['mode' => $mode], 400);
    }
} finally {
    @socket_close($socket);
}

echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
