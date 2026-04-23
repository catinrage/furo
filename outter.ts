const RELAY = process.env.RELAY_URL || "https://hidaco.site/tools/rel/index.php";
const API_KEY = process.env.API_KEY || "my_super_secret_123456789";

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

const sockets: Record<string, any> = {};
const seen = new Set<string>();

async function recv(conn: string): Promise<Uint8Array[]> {
  try {
    const res = await fetch(`${RELAY}?action=receive&conn=${conn}&dir=up`, {
      headers: { "X-API-KEY": API_KEY }
    });
    if (!res.ok) return [];
    const json = await res.json();
    return (json.chunks ||[]).map((b64: string) => Buffer.from(b64, 'base64'));
  } catch (e) {
    return[];
  }
}

async function send(conn: string, data: Uint8Array) {
  try {
    await fetch(`${RELAY}?action=send&conn=${conn}&dir=down`, {
      method: "POST",
      headers: { "X-API-KEY": API_KEY },
      body: data
    });
  } catch (e) {}
}

async function relayClose(conn: string) {
  try {
    await fetch(`${RELAY}?action=close&conn=${conn}`, {
      headers: { "X-API-KEY": API_KEY },
    });
  } catch (e) {}
}

async function openTCP(conn: string, host: string, port: number) {
  console.log(`[FR] OPEN ${conn} → ${host}:${port}`);

  try {
    const sock = await Bun.connect({
      hostname: host,
      port,
      socket: {
        data(socket, data) {
          send(conn, data);
        },
        close(socket) {
          console.log(`[${conn}] closed`);
          delete sockets[conn];
          relayClose(conn);
        },
        error(socket, error) {
          console.error(`[${conn}] socket error:`, error?.message);
        }
      }
    });
    
    console.log(`[${conn}] connected`);
    sockets[conn] = sock;
  } catch (e: any) {
    console.error(`[${conn}] failed to connect: ${e?.message}`);
    relayClose(conn); 
  }
}

// MAIN LOOP
console.log("🚀 Outer script running...");

(async () => {
  // Periodically prompt the relay server to purge dead states
  setInterval(async () => {
    try {
      await fetch(`${RELAY}?action=cleanup`, {
        headers: { "X-API-KEY": API_KEY }
      });
    } catch {}
  }, 60000);

while (true) {
    try {
      // STEP 1: check for new OPEN requests
      const res = await fetch(`${RELAY}?action=open_queue`, {
        headers: { "X-API-KEY": API_KEY }
      });
      const opens = await res.json().catch(() => ({ connections:[] }));

      for (const o of opens.connections ||[]) {
        if (seen.has(o.conn)) continue;
        seen.add(o.conn);
        openTCP(o.conn, o.host, Number(o.port));
      }

      // STEP 2: normal traffic loop (CONCURRENT)
      let hasData = false;
      
      // Process all active connections at the exact same time
      const tasks = Array.from(seen).map(async (conn) => {
        // If socket was closed, remove from our watch list to prevent memory leaks
        if (!sockets[conn]) {
          seen.delete(conn);
          return;
        }
        
        const chunks = await recv(conn);
        if (chunks.length > 0) hasData = true;
        
        for (const buf of chunks) {
          if (sockets[conn]) {
            sockets[conn].write(buf);
          }
        }
      });
      
      // Wait for all HTTP requests to finish
      await Promise.all(tasks);
      
      await sleep(hasData ? 10 : 50); // adaptive polling latency

    } catch (e) {
      console.error("[Outer] Loop error", e);
      await sleep(50);
    }
  }
})();