<?php
// ================= CONFIG =================
$API_KEY = 'my_super_secret_123456789';
$STORAGE_DIR = __DIR__ . '/storage';
$MAX_CHUNK_SIZE = 1024 * 32; // 32KB
$CONNECTION_TTL = 300; // seconds

if (!is_dir($STORAGE_DIR)) {
    mkdir($STORAGE_DIR, 0700, true);
}

// ================= HELPERS =================
function respond($data, $code = 200) {
    http_response_code($code);
    header('Content-Type: application/json');
    echo json_encode($data);
    exit;
}

function auth($API_KEY) {
    $key = $_SERVER['HTTP_X_API_KEY'] ?? ($_GET['key'] ?? null);
    if ($key !== $API_KEY) {
        respond(['error' => 'Unauthorized'], 403);
    }
}

function clean($str) {
    return preg_replace('/[^a-zA-Z0-9_\-]/', '', $str);
}

// ================= AUTH =================
auth($API_KEY);

// ================= INPUT =================
$action = $_GET['action'] ?? null;
$conn   = clean($_GET['conn'] ?? '');
$dir    = $_GET['dir'] ?? null; // up / down

if (!$action || !$conn) {
    respond(['error' => 'Missing parameters'], 400);
}

if ($dir && !in_array($dir, ['up', 'down'])) {
    respond(['error' => 'Invalid direction'], 400);
}

$metaFile = "$STORAGE_DIR/{$conn}.meta";
$queueFile = $dir ? "$STORAGE_DIR/{$conn}_{$dir}.queue" : null;

// ================= CREATE =================
if ($action === 'open') {
    $meta = [
        'created' => time(),
        'updated' => time(),
        'status'  => 'open'
    ];
    file_put_contents($metaFile, json_encode($meta));
    respond(['status' => 'opened']);
}

// ================= CLOSE =================
if ($action === 'close') {
    @unlink("$STORAGE_DIR/{$conn}_up.queue");
    @unlink("$STORAGE_DIR/{$conn}_down.queue");
    @unlink($metaFile);
    respond(['status' => 'closed']);
}

// ================= SEND =================
if ($action === 'send') {
    if (!$dir) respond(['error' => 'Missing direction'], 400);

    $data = file_get_contents('php://input');
    if (!$data || strlen($data) > $MAX_CHUNK_SIZE) {
        respond(['error' => 'Invalid chunk'], 400);
    }

    $fp = fopen($queueFile, 'a');
    if (!$fp) respond(['error' => 'Storage error'], 500);

    flock($fp, LOCK_EX);
    fwrite($fp, base64_encode($data) . "\n");
    flock($fp, LOCK_UN);
    fclose($fp);

    touch($metaFile);
    respond(['status' => 'ok']);
}

// ================= RECEIVE =================
if ($action === 'receive') {
    if (!$dir) respond(['error' => 'Missing direction'], 400);

    if (!file_exists($queueFile)) {
        respond(['chunks' => []]);
    }

    $fp = fopen($queueFile, 'c+');
    if (!$fp) respond(['error' => 'Storage error'], 500);

    flock($fp, LOCK_EX);

    $lines = file($queueFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    file_put_contents($queueFile, '');

    flock($fp, LOCK_UN);
    fclose($fp);

    $chunks = array_map(fn($l) => base64_decode($l), $lines);

    touch($metaFile);
    respond(['chunks' => $chunks]);
}

// ================= CLEANUP =================
if ($action === 'cleanup') {
    $files = glob("$STORAGE_DIR/*.meta");

    foreach ($files as $file) {
        $meta = json_decode(file_get_contents($file), true);
        if (!$meta) continue;

        if (time() - $meta['updated'] > $CONNECTION_TTL) {
            $id = basename($file, '.meta');
            @unlink("$STORAGE_DIR/{$id}_up.queue");
            @unlink("$STORAGE_DIR/{$id}_down.queue");
            @unlink($file);
        }
    }

    respond(['status' => 'cleaned']);
}

// ================= DEFAULT =================
respond(['error' => 'Invalid action'], 400);