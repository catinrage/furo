const RELAY = process.env.RELAY_URL || "https://hidaco.site/tools/rel/index.php";
const API_KEY = process.env.API_KEY || "my_super_secret_123456789";

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

const sockets: Record<string, any> = {};
const seen = new Set<string>();

// Data buffers
const sendBuffer: Record<string, Buffer[]> = {};
const upBuffer: Record<string, Buffer[]> = {}; // Queues data while Bun is connecting
const connecting = new Set<string>(); // Tracks sockets that are currently opening

async function relayClose(conn: string) {
  try {
    await fetch(`${RELAY}?action=close&conn=${conn}`, { headers: { "X-API-KEY": API_KEY } });
    console.log(`[FR] 🛑 Sent CLOSE to PHP for ${conn}`);
  } catch (e) {}
}

async function openTCP(conn: string, host: string, port: number) {
  connecting.add(conn); // Mark socket as pending
  
  try {
    const sock = await Bun.connect({
      hostname: host,
      port,
      socket: {
        data(socket, data) {
          const safeData = Buffer.from(data);
          console.log(`[FR] 📦 Remote server sent ${safeData.length} bytes for ${conn}`);
          if (!sendBuffer[conn]) sendBuffer[conn] = [];
          sendBuffer[conn].push(safeData);
        },
        close(socket) {
          console.log(`[FR] 🔌 Remote server closed connection ${conn}`);
          delete sockets[conn];
          relayClose(conn);
        },
        error(socket, error) {
          console.log(`[FR] ⚠️ Socket error ${conn}: ${error?.message}`);
          socket.end();
        }
      }
    });
    
    sockets[conn] = sock;
    connecting.delete(conn);
    console.log(`[FR] 🔗 CONNECTED ${conn} → ${host}:${port}`);

    // FLUSH QUEUE: If any packets arrived while we were connecting, send them now!
    if (upBuffer[conn] && upBuffer[conn].length > 0) {
      console.log(`[FR] ➡️ Flushing ${upBuffer[conn].length} queued packets for ${conn}`);
      for (const buf of upBuffer[conn]) {
        sock.write(buf);
      }
      delete upBuffer[conn];
    }

  } catch (e: any) {
    console.log(`[FR] ❌ FAILED ${conn} → ${host}:${port} (${e.message})`);
    connecting.delete(conn);
    delete upBuffer[conn];
    relayClose(conn); 
  }
}

// Cleanup cron
setInterval(async () => {
  try { await fetch(`${RELAY}?action=cleanup`, { headers: { "X-API-KEY": API_KEY } }); } catch {}
}, 60000);

console.log("🚀 OUTER SCRIPT RUNNING");

// ================= GLOBAL BATCH POLLING =================
(async () => {
  while (true) {
    let hasActivity = false;

    try {
      // 1. RECEIVE SYNC AND NEW OPENS (UP)
      const res = await fetch(`${RELAY}?action=sync&dir=up`, { headers: { "X-API-KEY": API_KEY } });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      
      const text = await res.text();
      let data: any = {};
      try { data = JSON.parse(text); } catch { console.error(`[FR] ❌ Invalid JSON from PHP: ${text.substring(0, 50)}...`); }

      // Handle new connections
      for (const o of data.opens ||[]) {
        if (seen.has(o.conn)) continue;
        seen.add(o.conn);
        if (o.host && o.port) {
            console.log(`[FR] 🔔 New Open Request: ${o.conn} → ${o.host}:${o.port}`);
            openTCP(o.conn, o.host, Number(o.port));
        }
      }

      // Handle incoming up-stream data
      const chunkKeys = Object.keys(data.chunks || {});
      if (chunkKeys.length > 0) {
        hasActivity = true;
        console.log(`[FR] ⬇️ Polled UP-batch from PHP (${chunkKeys.length} conns)`);
        
        for (const[conn, chunks] of Object.entries(data.chunks || {})) {
          for (const b64 of (chunks as string[])) {
            const buf = Buffer.from(b64, 'base64');
            
            if (sockets[conn]) {
              // Socket is ready, write immediately
              console.log(`[FR] ➡️ Writing ${buf.length} bytes to remote server for ${conn}`);
              sockets[conn].write(buf);
            } else if (connecting.has(conn)) {
              // Socket is still opening, queue the data!
              console.log(`[FR] ⏳ Queuing ${buf.length} bytes for ${conn} (socket opening...)`);
              if (!upBuffer[conn]) upBuffer[conn] = [];
              upBuffer[conn].push(buf);
            } else {
              // Socket is totally dead/closed
              console.log(`[FR] ⚠️ Dropping UP data, socket ${conn} is not open`);
            }
          }
        }
      }

      // 2. SEND BATCH (DOWN)
      const activeSends = Object.keys(sendBuffer);
      if (activeSends.length > 0) {
        const payload: Record<string, string> = {};
        for (const conn of activeSends) {
          payload[conn] = Buffer.concat(sendBuffer[conn]).toString('base64');
          delete sendBuffer[conn];
        }
        hasActivity = true;
        
        console.log(`[FR] ⬆️ POSTing DOWN-batch to PHP (${activeSends.length} conns)`);
        const pRes = await fetch(`${RELAY}?action=send_batch&dir=down`, {
          method: "POST",
          headers: { "X-API-KEY": API_KEY, "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });
        if (!pRes.ok) console.error(`[FR] ❌ DOWN-batch failed HTTP ${pRes.status}`);
      }

    } catch (e: any) {
      console.error(`[FR] ❌ Outer Loop Error: ${e.message}`);
      await sleep(50);
    }

    await sleep(hasActivity ? 5 : 50);
  }
})();