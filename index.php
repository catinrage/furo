<?php
// ================= CONFIG =================
$API_KEY = 'my_super_secret_123456789';
$STORAGE_DIR = '/dev/shm/furo_storage'; 
$CONNECTION_TTL = 300; 

if (!is_dir($STORAGE_DIR)) {
    mkdir($STORAGE_DIR, 0700, true);
}

function respond($data, $code = 200) {
    http_response_code($code);
    header('Content-Type: application/json');
    echo json_encode($data);
    exit;
}

$key = $_SERVER['HTTP_X_API_KEY'] ?? ($_GET['key'] ?? null);
if ($key !== $API_KEY) {
    respond(['error' => 'Unauthorized'], 403);
}

$action = $_GET['action'] ?? null;

// ================= ACTION: OPEN & CLOSE & CLEANUP =================
if ($action === 'open') {
    $conn = preg_replace('/[^a-zA-Z0-9_\-]/', '', $_GET['conn'] ?? '');
    $meta = [
        'updated' => time(),
        'host'    => $_GET['host'] ?? '',
        'port'    => (int)($_GET['port'] ?? 0),
        'fetched' => false
    ];
    file_put_contents("$STORAGE_DIR/{$conn}.meta", json_encode($meta), LOCK_EX);
    respond(['status' => 'opened']);
}

if ($action === 'close') {
    $conn = preg_replace('/[^a-zA-Z0-9_\-]/', '', $_GET['conn'] ?? '');
    @unlink("$STORAGE_DIR/{$conn}_up.queue");
    @unlink("$STORAGE_DIR/{$conn}_down.queue");
    @unlink("$STORAGE_DIR/{$conn}_up.eof");
    @unlink("$STORAGE_DIR/{$conn}_down.eof");
    @unlink("$STORAGE_DIR/{$conn}.meta");
    respond(['status' => 'closed']);
}

if ($action === 'cleanup') {
    $files = glob("$STORAGE_DIR/*.meta");
    if ($files !== false) {
        foreach ($files as $file) {
            $meta = json_decode(file_get_contents($file), true);
            if ($meta && (time() - $meta['updated'] > $CONNECTION_TTL)) {
                $id = basename($file, '.meta');
                @unlink("$STORAGE_DIR/{$id}_up.queue");
                @unlink("$STORAGE_DIR/{$id}_down.queue");
                @unlink("$STORAGE_DIR/{$id}_up.eof");
                @unlink("$STORAGE_DIR/{$id}_down.eof");
                @unlink($file);
            }
        }
    }
    respond(['status' => 'cleaned']);
}

// ================= ACTION: SEND BATCH (BINARY) =================
if ($action === 'send_batch') {
    $dir = $_GET['dir'] ?? null;
    $input = file_get_contents('php://input');
    $len = strlen($input);
    $offset = 0;

    // Parse Binary Frames
    while ($offset < $len) {
        if ($offset + 2 > $len) break;
        $type = ord($input[$offset++]);
        $connLen = ord($input[$offset++]);
        
        if ($offset + $connLen > $len) break;
        $conn = preg_replace('/[^a-zA-Z0-9_\-]/', '', substr($input, $offset, $connLen));
        $offset += $connLen;

        if (empty($conn)) continue;

        if ($type === 0) { // Data Frame
            if ($offset + 4 > $len) break;
            $dataLen = unpack('N', substr($input, $offset, 4))[1]; 
            $offset += 4;
            
            if ($offset + $dataLen > $len) break;
            $data = substr($input, $offset, $dataLen); 
            $offset += $dataLen;

            $queueFile = "$STORAGE_DIR/{$conn}_{$dir}.queue";
            $fp = fopen($queueFile, 'a');
            if ($fp) {
                flock($fp, LOCK_EX);
                fwrite($fp, $data);
                flock($fp, LOCK_UN);
                fclose($fp);
            }
        } elseif ($type === 1) { // EOF Frame
            touch("$STORAGE_DIR/{$conn}_{$dir}.eof");
        }
    }
    respond(['status' => 'ok']);
}

// ================= ACTION: SYNC (LONG POLLING + BINARY) =================
if ($action === 'sync') {
    $dir = $_GET['dir'] ?? null;
    $output = '';

    // Long Polling: Wait up to 40ms (8 iterations of 5ms)
    for ($iter = 0; $iter < 8; $iter++) {
        
        // 1. Check for New Connections
        if ($dir === 'up') {
            $files = glob("$STORAGE_DIR/*.meta");
            if ($files !== false) {
                foreach ($files as $file) {
                    $fp = fopen($file, 'c+');
                    if ($fp) {
                        if (flock($fp, LOCK_EX | LOCK_NB)) { 
                            $meta = json_decode(stream_get_contents($fp), true);
                            if ($meta && empty($meta['fetched'])) {
                                $meta['fetched'] = true;
                                ftruncate($fp, 0); rewind($fp);
                                fwrite($fp, json_encode($meta));
                                
                                $conn = basename($file, '.meta');
                                $output .= chr(2) . chr(strlen($conn)) . $conn;
                                $output .= chr(strlen($meta['host'])) . $meta['host'];
                                $output .= pack('n', (int)$meta['port']);
                            }
                            flock($fp, LOCK_UN);
                        }
                        fclose($fp);
                    }
                }
            }
        }

        // 2. Check for Payload Data
        $queueFiles = glob("$STORAGE_DIR/*_{$dir}.queue");
        if ($queueFiles !== false) {
            foreach ($queueFiles as $qFile) {
                $conn = str_replace("_{$dir}.queue", '', basename($qFile));
                
                $fp = fopen($qFile, 'c+');
                if ($fp) {
                    if (flock($fp, LOCK_EX | LOCK_NB)) {
                        $content = stream_get_contents($fp);
                        if (strlen($content) > 0) {
                            ftruncate($fp, 0);
                            $output .= chr(0) . chr(strlen($conn)) . $conn . pack('N', strlen($content)) . $content;
                        }
                        flock($fp, LOCK_UN);
                    }
                    fclose($fp);
                }

                // 3. Check for EOF marker
                $eofFile = "$STORAGE_DIR/{$conn}_{$dir}.eof";
                clearstatcache(true, $qFile);
                if (file_exists($eofFile) && filesize($qFile) === 0) {
                    $output .= chr(1) . chr(strlen($conn)) . $conn;
                    @unlink($eofFile);
                }
            }
        }

        // Output binary data instantly if found
        if (strlen($output) > 0) {
            header('Content-Type: application/octet-stream');
            echo $output;
            exit;
        }

        // Wait 5ms before checking again
        usleep(5000); 
    }

    // 204 No Content means timeout reached with zero data
    http_response_code(204);
    exit;
}

respond(['error' => 'Invalid action'], 400);