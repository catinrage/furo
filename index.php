<?php
// ================= CONFIG =================
$API_KEY = 'my_super_secret_123456789';
$STORAGE_DIR = __DIR__ . '/storage';
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
$action = $_GET['action'] ?? null;
if (!$action) respond(['error' => 'Missing action'], 400);

// ================= ACTION: OPEN =================
if ($action === 'open') {
    $conn = clean($_GET['conn'] ?? '');
    if (!$conn) respond(['error' => 'Missing conn'], 400);
    
    $meta =[
        'created' => time(),
        'updated' => time(),
        'status'  => 'open',
        'host'    => $_GET['host'] ?? '',
        'port'    => (int)($_GET['port'] ?? 0),
        'fetched' => false // outer.go will toggle this to true when processing
    ];
    file_put_contents("$STORAGE_DIR/{$conn}.meta", json_encode($meta), LOCK_EX);
    respond(['status' => 'opened']);
}

// ================= ACTION: CLOSE =================
if ($action === 'close') {
    $conn = clean($_GET['conn'] ?? '');
    if (!$conn) respond(['error' => 'Missing conn'], 400);

    @unlink("$STORAGE_DIR/{$conn}_up.queue");
    @unlink("$STORAGE_DIR/{$conn}_down.queue");
    @unlink("$STORAGE_DIR/{$conn}.meta");
    respond(['status' => 'closed']);
}

// ================= ACTION: SEND BATCH =================
if ($action === 'send_batch') {
    $dir = $_GET['dir'] ?? null;
    if (!in_array($dir, ['up', 'down'])) respond(['error' => 'Invalid direction'], 400);

    $json = file_get_contents('php://input');
    $payload = json_decode($json, true);
    
    if (is_array($payload)) {
        foreach ($payload as $connID => $b64data) {
            $connID = clean($connID);
            $queueFile = "$STORAGE_DIR/{$connID}_{$dir}.queue";
            
            // Append Base64 payload block safely
            $fp = fopen($queueFile, 'a');
            if ($fp) {
                flock($fp, LOCK_EX);
                fwrite($fp, $b64data . "\n");
                flock($fp, LOCK_UN);
                fclose($fp);
            }
            
            // Update timestamp
            $metaFile = "$STORAGE_DIR/{$connID}.meta";
            if (file_exists($metaFile)) {
                $meta = json_decode(file_get_contents($metaFile), true);
                if ($meta) {
                    $meta['updated'] = time();
                    file_put_contents($metaFile, json_encode($meta), LOCK_EX);
                }
            }
        }
    }
    respond(['status' => 'ok']);
}

// ================= ACTION: SYNC =================
if ($action === 'sync') {
    $dir = $_GET['dir'] ?? null;
    if (!in_array($dir, ['up', 'down'])) respond(['error' => 'Invalid direction'], 400);

    $response = ['chunks' => []];
    
    // Pass newly opened connections to outer.go
    if ($dir === 'up') {
        $response['opens'] =[];
        $files = glob("$STORAGE_DIR/*.meta");
        if ($files !== false) {
            foreach ($files as $file) {
                $meta = json_decode(file_get_contents($file), true);
                if ($meta && empty($meta['fetched'])) {
                    $connID = basename($file, '.meta');
                    $response['opens'][] =[
                        'conn' => $connID,
                        'host' => $meta['host'],
                        'port' => (int)$meta['port']
                    ];
                    $meta['fetched'] = true;
                    $meta['updated'] = time();
                    file_put_contents($file, json_encode($meta), LOCK_EX);
                }
            }
        }
    }

    // Process queued arrays securely
    $queueFiles = glob("$STORAGE_DIR/*_{$dir}.queue");
    if ($queueFiles !== false) {
        foreach ($queueFiles as $qFile) {
            $connID = str_replace("_{$dir}.queue", '', basename($qFile));
            
            $fp = fopen($qFile, 'c+');
            if ($fp) {
                flock($fp, LOCK_EX);
                $content = stream_get_contents($fp);
                ftruncate($fp, 0); // Clear immediately
                flock($fp, LOCK_UN);
                fclose($fp);
                
                if (!empty($content)) {
                    $lines = array_filter(explode("\n", trim($content)), 'strlen');
                    if (!empty($lines)) {
                        $response['chunks'][$connID] = array_values($lines);
                    }
                }
            }
        }
    }

    respond($response);
}

// ================= ACTION: CLEANUP =================
if ($action === 'cleanup') {
    $files = glob("$STORAGE_DIR/*.meta");
    if ($files !== false) {
        foreach ($files as $file) {
            $meta = json_decode(file_get_contents($file), true);
            if ($meta && (time() - $meta['updated'] > $CONNECTION_TTL)) {
                $id = basename($file, '.meta');
                @unlink("$STORAGE_DIR/{$id}_up.queue");
                @unlink("$STORAGE_DIR/{$id}_down.queue");
                @unlink($file);
            }
        }
    }
    respond(['status' => 'cleaned']);
}

respond(['error' => 'Invalid action'], 400);