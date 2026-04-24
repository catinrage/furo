<?php
declare(strict_types=1);

// ================= CONFIG =================
$API_KEY = 'my_super_secret_123456789';
$CONNECTION_TTL = 300; // seconds

$REDIS_HOST = '37.32.14.205';
$REDIS_PORT = 6379;
$REDIS_PASSWORD = 'hesoyam';
$REDIS_TIMEOUT = 2.5;
$REDIS_READ_TIMEOUT = 2.5;
$REDIS_PERSISTENT_ID = 'host-tun-relay';
$REDIS_PREFIX = 'host_tun:';

$LONG_POLL_MS = 60;
$LONG_POLL_STEP_US = 5000;
$MAX_SYNC_CONNS = 256;

// ================= HELPERS =================
function respond(array $data, int $code = 200): void
{
    http_response_code($code);
    header('Content-Type: application/json');
    echo json_encode($data, JSON_UNESCAPED_SLASHES);
    exit;
}

function respondBinary(string $data, int $code = 200): void
{
    http_response_code($code);
    header('Content-Type: application/octet-stream');
    header('Content-Length: ' . strlen($data));
    echo $data;
    exit;
}

function auth(string $apiKey): void
{
    $key = $_SERVER['HTTP_X_API_KEY'] ?? ($_GET['key'] ?? null);
    if ($key !== $apiKey) {
        respond(['error' => 'Unauthorized'], 403);
    }
}

function clean(string $str): string
{
    return preg_replace('/[^a-zA-Z0-9_\-]/', '', $str) ?? '';
}

function redisClient(
    string $host,
    int $port,
    string $password,
    float $timeout,
    float $readTimeout,
    string $persistentId
): Redis {
    if (!class_exists('Redis')) {
        respond(['error' => 'Redis extension is not loaded'], 500);
    }

    $redis = new Redis();

    try {
        $ok = $redis->pconnect($host, $port, $timeout, $persistentId, 0, 0.0, ['read_timeout' => $readTimeout]);
        if (!$ok) {
            respond(['error' => 'Redis connect failed'], 500);
        }

        if ($password !== '' && !$redis->auth($password)) {
            respond(['error' => 'Redis auth failed'], 500);
        }

        $redis->setOption(Redis::OPT_SERIALIZER, Redis::SERIALIZER_NONE);
        if (defined('Redis::OPT_COMPRESSION') && defined('Redis::COMPRESSION_NONE')) {
            $redis->setOption(Redis::OPT_COMPRESSION, Redis::COMPRESSION_NONE);
        }
    } catch (Throwable $e) {
        respond(['error' => 'Redis unavailable', 'detail' => $e->getMessage()], 500);
    }

    return $redis;
}

function keyMeta(string $prefix, string $conn): string
{
    return $prefix . 'conn:' . $conn . ':meta';
}

function keyQueue(string $prefix, string $conn, string $dir): string
{
    return $prefix . 'conn:' . $conn . ':' . $dir;
}

function keyReady(string $prefix, string $dir): string
{
    return $prefix . 'ready:' . $dir;
}

function keyOpens(string $prefix): string
{
    return $prefix . 'opens';
}

function keyConns(string $prefix): string
{
    return $prefix . 'conns';
}

function fetchMeta(Redis $redis, string $prefix, string $conn): ?array
{
    $meta = $redis->hGetAll(keyMeta($prefix, $conn));
    if (!is_array($meta) || $meta === []) {
        return null;
    }

    return $meta;
}

function closeConnection(Redis $redis, string $prefix, string $conn): void
{
    $pipe = $redis->multi(Redis::PIPELINE);
    $pipe->del(keyMeta($prefix, $conn));
    $pipe->del(keyQueue($prefix, $conn, 'up'));
    $pipe->del(keyQueue($prefix, $conn, 'down'));
    $pipe->sRem(keyReady($prefix, 'up'), $conn);
    $pipe->sRem(keyReady($prefix, 'down'), $conn);
    $pipe->zRem(keyConns($prefix), $conn);
    $pipe->exec();
}

function hasPendingWork(Redis $redis, string $prefix, string $dir): bool
{
    if ($dir === 'up' && (int)$redis->lLen(keyOpens($prefix)) > 0) {
        return true;
    }

    return (int)$redis->sCard(keyReady($prefix, $dir)) > 0;
}

function drainQueue(Redis $redis, string $prefix, string $conn, string $dir): array
{
    static $lua = <<<'LUA'
local listKey = KEYS[1]
local readyKey = KEYS[2]
local connId = ARGV[1]
local items = redis.call('LRANGE', listKey, 0, -1)
if #items > 0 then
    redis.call('DEL', listKey)
end
redis.call('SREM', readyKey, connId)
return items
LUA;

    $result = $redis->eval(
        $lua,
        [keyQueue($prefix, $conn, $dir), keyReady($prefix, $dir), $conn],
        2
    );

    return is_array($result) ? $result : [];
}

function pushFrameData(string &$out, string $conn, string $payload): void
{
    if ($conn === '' || strlen($conn) > 255) {
        return;
    }

    $out .= chr(0);
    $out .= chr(strlen($conn));
    $out .= $conn;
    $out .= pack('N', strlen($payload));
    $out .= $payload;
}

function pushFrameEOF(string &$out, string $conn): void
{
    if ($conn === '' || strlen($conn) > 255) {
        return;
    }

    $out .= chr(1);
    $out .= chr(strlen($conn));
    $out .= $conn;
}

function pushFrameOpen(string &$out, string $conn, string $host, int $port): void
{
    if ($conn === '' || $host === '' || strlen($conn) > 255 || strlen($host) > 255) {
        return;
    }

    $out .= chr(2);
    $out .= chr(strlen($conn));
    $out .= $conn;
    $out .= chr(strlen($host));
    $out .= $host;
    $out .= pack('n', $port);
}

function readU8(string $body, int &$offset): ?int
{
    if ($offset >= strlen($body)) {
        return null;
    }

    return ord($body[$offset++]);
}

function readBytes(string $body, int &$offset, int $length): ?string
{
    if ($length < 0 || strlen($body) - $offset < $length) {
        return null;
    }

    $data = substr($body, $offset, $length);
    $offset += $length;
    return $data;
}

function readU32BE(string $body, int &$offset): ?int
{
    $raw = readBytes($body, $offset, 4);
    if ($raw === null) {
        return null;
    }

    $decoded = unpack('Nlen', $raw);
    return is_array($decoded) ? (int)$decoded['len'] : null;
}

function parseBinaryFrames(string $body): array
{
    $frames = [];
    $offset = 0;
    $bodyLen = strlen($body);

    while ($offset < $bodyLen) {
        $type = readU8($body, $offset);
        $connLen = readU8($body, $offset);
        if ($type === null || $connLen === null) {
            return [false, 'Invalid frame header'];
        }

        $conn = readBytes($body, $offset, $connLen);
        if ($conn === null) {
            return [false, 'Invalid conn id'];
        }

        if ($type === 0) {
            $dataLen = readU32BE($body, $offset);
            if ($dataLen === null) {
                return [false, 'Invalid data length'];
            }

            $data = readBytes($body, $offset, $dataLen);
            if ($data === null) {
                return [false, 'Truncated data frame'];
            }

            $frames[] = ['type' => 0, 'conn' => $conn, 'data' => $data];
            continue;
        }

        if ($type === 1) {
            $frames[] = ['type' => 1, 'conn' => $conn];
            continue;
        }

        if ($type === 2) {
            $hostLen = readU8($body, $offset);
            if ($hostLen === null) {
                return [false, 'Invalid host length'];
            }

            $host = readBytes($body, $offset, $hostLen);
            $portRaw = readBytes($body, $offset, 2);
            if ($host === null || $portRaw === null) {
                return [false, 'Invalid open frame'];
            }

            $decoded = unpack('nport', $portRaw);
            $frames[] = [
                'type' => 2,
                'conn' => $conn,
                'host' => $host,
                'port' => is_array($decoded) ? (int)$decoded['port'] : 0,
            ];
            continue;
        }

        return [false, 'Unknown frame type'];
    }

    return [true, $frames];
}

function drainOpensBinary(Redis $redis, string $prefix, int $limit, string &$out): int
{
    $count = 0;

    for ($i = 0; $i < $limit; $i++) {
        $conn = $redis->lPop(keyOpens($prefix));
        if ($conn === false || $conn === null) {
            break;
        }

        $conn = clean((string)$conn);
        if ($conn === '') {
            continue;
        }

        $meta = fetchMeta($redis, $prefix, $conn);
        if ($meta === null) {
            continue;
        }

        $host = (string)($meta['host'] ?? '');
        $port = (int)($meta['port'] ?? 0);
        pushFrameOpen($out, $conn, $host, $port);
        $count++;
    }

    return $count;
}

function drainReadyBinary(Redis $redis, string $prefix, string $dir, int $limit, string &$out): int
{
    $readyConnIDs = $redis->sMembers(keyReady($prefix, $dir));
    if (!is_array($readyConnIDs) || $readyConnIDs === []) {
        return 0;
    }

    $count = 0;
    foreach ($readyConnIDs as $connID) {
        $connID = clean((string)$connID);
        if ($connID === '') {
            continue;
        }

        $items = drainQueue($redis, $prefix, $connID, $dir);
        foreach ($items as $item) {
            if (!is_string($item) || $item === '') {
                continue;
            }

            if ($item === 'EOF') {
                pushFrameEOF($out, $connID);
            } else {
                pushFrameData($out, $connID, $item);
            }
        }

        $count++;
        if ($count >= $limit) {
            break;
        }
    }

    return $count;
}

auth($API_KEY);
$action = $_GET['action'] ?? null;
if (!$action) {
    respond(['error' => 'Missing action'], 400);
}

$redis = redisClient(
    $REDIS_HOST,
    $REDIS_PORT,
    $REDIS_PASSWORD,
    $REDIS_TIMEOUT,
    $REDIS_READ_TIMEOUT,
    $REDIS_PERSISTENT_ID
);

// ================= ACTION: OPEN =================
if ($action === 'open') {
    $conn = clean((string)($_GET['conn'] ?? ''));
    if ($conn === '') {
        respond(['error' => 'Missing conn'], 400);
    }

    $host = (string)($_GET['host'] ?? '');
    $port = (int)($_GET['port'] ?? 0);
    $now = time();

    $metaKey = keyMeta($REDIS_PREFIX, $conn);
    $pipe = $redis->multi(Redis::PIPELINE);
    $pipe->hMSet($metaKey, [
        'created' => (string)$now,
        'updated' => (string)$now,
        'status' => 'open',
        'host' => $host,
        'port' => (string)$port,
    ]);
    $pipe->expire($metaKey, $CONNECTION_TTL * 2);
    $pipe->zAdd(keyConns($REDIS_PREFIX), $now, $conn);
    $pipe->rPush(keyOpens($REDIS_PREFIX), $conn);
    $pipe->exec();

    respond(['status' => 'opened']);
}

// ================= ACTION: CLOSE =================
if ($action === 'close') {
    $conn = clean((string)($_GET['conn'] ?? ''));
    if ($conn === '') {
        respond(['error' => 'Missing conn'], 400);
    }

    closeConnection($redis, $REDIS_PREFIX, $conn);
    respond(['status' => 'closed']);
}

// ================= ACTION: SEND BATCH =================
if ($action === 'send_batch') {
    $dir = $_GET['dir'] ?? null;
    if (!in_array($dir, ['up', 'down'], true)) {
        respond(['error' => 'Invalid direction'], 400);
    }

    $contentType = $_SERVER['CONTENT_TYPE'] ?? '';
    if (stripos($contentType, 'application/octet-stream') === false) {
        respond(['error' => 'Expected binary payload'], 415);
    }

    $body = file_get_contents('php://input');
    if ($body === false || $body === '') {
        respond(['status' => 'ok']);
    }

    [$ok, $frames] = parseBinaryFrames($body);
    if (!$ok) {
        respond(['error' => 'Invalid binary payload', 'detail' => $frames], 400);
    }

    $now = time();
    $pipe = $redis->multi(Redis::PIPELINE);
    $queued = 0;

    foreach ($frames as $frame) {
        $connID = clean((string)$frame['conn']);
        if ($connID === '') {
            continue;
        }

        if ($frame['type'] === 0) {
            if (!$redis->exists(keyMeta($REDIS_PREFIX, $connID))) {
                continue;
            }

            $queueKey = keyQueue($REDIS_PREFIX, $connID, $dir);
            $pipe->rPush($queueKey, $frame['data']);
            $pipe->expire($queueKey, $CONNECTION_TTL * 2);
            $pipe->sAdd(keyReady($REDIS_PREFIX, $dir), $connID);
            $pipe->hSet(keyMeta($REDIS_PREFIX, $connID), 'updated', (string)$now);
            $pipe->expire(keyMeta($REDIS_PREFIX, $connID), $CONNECTION_TTL * 2);
            $pipe->zAdd(keyConns($REDIS_PREFIX), $now, $connID);
            $queued++;
            continue;
        }

        if ($frame['type'] === 1) {
            if (!$redis->exists(keyMeta($REDIS_PREFIX, $connID))) {
                continue;
            }

            $queueKey = keyQueue($REDIS_PREFIX, $connID, $dir);
            $pipe->rPush($queueKey, 'EOF');
            $pipe->expire($queueKey, $CONNECTION_TTL * 2);
            $pipe->sAdd(keyReady($REDIS_PREFIX, $dir), $connID);
            $pipe->hSet(keyMeta($REDIS_PREFIX, $connID), 'updated', (string)$now);
            $pipe->expire(keyMeta($REDIS_PREFIX, $connID), $CONNECTION_TTL * 2);
            $pipe->zAdd(keyConns($REDIS_PREFIX), $now, $connID);
            $queued++;
            continue;
        }

        if ($frame['type'] === 2 && $dir === 'up') {
            $host = (string)$frame['host'];
            $port = (int)$frame['port'];
            $metaKey = keyMeta($REDIS_PREFIX, $connID);
            $pipe->hMSet($metaKey, [
                'created' => (string)$now,
                'updated' => (string)$now,
                'status' => 'open',
                'host' => $host,
                'port' => (string)$port,
            ]);
            $pipe->expire($metaKey, $CONNECTION_TTL * 2);
            $pipe->zAdd(keyConns($REDIS_PREFIX), $now, $connID);
            $pipe->rPush(keyOpens($REDIS_PREFIX), $connID);
            $queued++;
        }
    }

    if ($queued > 0) {
        $pipe->exec();
    }

    respond(['status' => 'ok']);
}

// ================= ACTION: SYNC =================
if ($action === 'sync') {
    $dir = $_GET['dir'] ?? null;
    if (!in_array($dir, ['up', 'down'], true)) {
        respond(['error' => 'Invalid direction'], 400);
    }

    $deadline = microtime(true) + ($LONG_POLL_MS / 1000);
    while (!hasPendingWork($redis, $REDIS_PREFIX, $dir) && microtime(true) < $deadline) {
        usleep($LONG_POLL_STEP_US);
    }

    $out = '';
    if ($dir === 'up') {
        drainOpensBinary($redis, $REDIS_PREFIX, $MAX_SYNC_CONNS, $out);
    }
    drainReadyBinary($redis, $REDIS_PREFIX, $dir, $MAX_SYNC_CONNS, $out);

    if ($out === '') {
        http_response_code(204);
        exit;
    }

    respondBinary($out);
}

// ================= ACTION: CLEANUP =================
if ($action === 'cleanup') {
    $cutoff = time() - $CONNECTION_TTL;
    $expired = $redis->zRangeByScore(keyConns($REDIS_PREFIX), '-inf', (string)$cutoff);
    $cleaned = 0;

    if (is_array($expired)) {
        foreach ($expired as $conn) {
            $conn = clean((string)$conn);
            if ($conn === '') {
                continue;
            }

            closeConnection($redis, $REDIS_PREFIX, $conn);
            $cleaned++;
        }
    }

    respond(['status' => 'cleaned', 'count' => $cleaned]);
}

respond(['error' => 'Invalid action'], 400);
