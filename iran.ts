import net from "net";

const RELAY = process.env.RELAY_URL || "https://hidaco.site/tools/rel/index.php";
const API_KEY = process.env.API_KEY || "my_super_secret_123456789";

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

let counter = 0;
const sockets: Record<string, net.Socket> = {};
const sendBuffer: Record<string, Buffer[]> = {};

async function relayOpen(conn: string, host: string, port: number) {
  try {
    const res = await fetch(`${RELAY}?action=open&conn=${conn}&host=${encodeURIComponent(host)}&port=${port}`, {
      headers: { "X-API-KEY": API_KEY },
    });
    if (!res.ok) console.error(`[IR] ❌ Open Failed HTTP ${res.status}`);
  } catch (e: any) {
    console.error(`[IR] ❌ Open Error:`, e.message);
  }
}

async function relayClose(conn: string) {
  try {
    await fetch(`${RELAY}?action=close&conn=${conn}`, { headers: { "X-API-KEY": API_KEY } });
    console.log(`[IR] 🛑 Sent CLOSE to PHP for ${conn}`);
  } catch (e) {}
}

// SOCKS5 SERVER
net.createServer((socket) => {
  const conn = `c_${Date.now()}_${counter++}`;
  sockets[conn] = socket;
  
  let stage = 0;

  socket.on("data", async (data: Buffer) => {
    // Safely copy buffer to prevent Node/Bun memory reuse issues
    const safeData = Buffer.from(data);

    if (stage === 0) {
      socket.write(Buffer.from([0x05, 0x00]));
      stage = 1;
      return;
    }

    if (stage === 1) {
      if (safeData.length < 7) return socket.destroy();
      
      let host = "", port = 0, offset = 0;
      const atyp = safeData[3];

      if (atyp === 0x01) {
        host = `${safeData[4]}.${safeData[5]}.${safeData[6]}.${safeData[7]}`;
        port = safeData.readUInt16BE(8);
        offset = 10;
      } else if (atyp === 0x03) {
        const hostLen = safeData[4];
        host = safeData.subarray(5, 5 + hostLen).toString();
        port = safeData.readUInt16BE(5 + hostLen);
        offset = 5 + hostLen + 2;
      } else if (atyp === 0x04) {
        const ipv6 = safeData.subarray(4, 20);
        const parts =[];
        for (let i = 0; i < 16; i += 2) parts.push(ipv6.readUInt16BE(i).toString(16));
        host = parts.join(':');
        port = safeData.readUInt16BE(20);
        offset = 22;
      } else {
        return socket.destroy();
      }

      console.log(`[IR] 🔗 CONNECT ${conn} → ${host}:${port}`);
      await relayOpen(conn, host, port);

      socket.write(Buffer.from([0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0]));
      stage = 2;

      if (safeData.length > offset) {
        console.log(`[IR] 📦 Stage 1 Piggyback Data: ${safeData.length - offset} bytes for ${conn}`);
        sendBuffer[conn] =[safeData.subarray(offset)];
      }
      return;
    }

    if (stage === 2) {
      console.log(`[IR] 📦 Client sent ${safeData.length} bytes for ${conn}`);
      if (!sendBuffer[conn]) sendBuffer[conn] = [];
      sendBuffer[conn].push(safeData);
    }
  });

  socket.on("close", () => {
    console.log(`[IR] 🔌 Client closed connection ${conn}`);
    delete sockets[conn];
    relayClose(conn);
  });
  socket.on("error", (e) => {
    console.log(`[IR] ⚠️ Socket error ${conn}: ${e.message}`);
    socket.destroy();
  });

}).listen(1080, () => {
  console.log("🚀 IRAN SOCKS5 RUNNING ON :1080");
});

// ================= GLOBAL BATCH POLLING =================
(async () => {
  while (true) {
    let hasActivity = false;

    // 1. SEND BATCH (UP)
    const activeSends = Object.keys(sendBuffer);
    if (activeSends.length > 0) {
      const payload: Record<string, string> = {};
      for (const conn of activeSends) {
        payload[conn] = Buffer.concat(sendBuffer[conn]).toString('base64');
        delete sendBuffer[conn];
      }
      hasActivity = true;
      try {
        console.log(`[IR] ⬆️ POSTing UP-batch to PHP (${activeSends.length} conns)`);
        const res = await fetch(`${RELAY}?action=send_batch&dir=up`, {
          method: "POST",
          headers: { "X-API-KEY": API_KEY, "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });
        if (!res.ok) console.error(`[IR] ❌ UP-batch failed HTTP ${res.status}`);
      } catch (e: any) {
        console.error(`[IR] ❌ UP-batch exception: ${e.message}`);
      }
    }

    // 2. RECEIVE SYNC (DOWN)
    if (Object.keys(sockets).length > 0) {
      try {
        const res = await fetch(`${RELAY}?action=sync&dir=down`, { headers: { "X-API-KEY": API_KEY } });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        
        const text = await res.text();
        let data: any = {};
        try { data = JSON.parse(text); } catch { console.error(`[IR] ❌ Invalid JSON from PHP: ${text.substring(0, 50)}...`); }
        
        const chunkKeys = Object.keys(data.chunks || {});
        if (chunkKeys.length > 0) {
          hasActivity = true;
          console.log(`[IR] ⬇️ Polled DOWN-batch from PHP (${chunkKeys.length} conns)`);
          
          for (const [conn, chunks] of Object.entries(data.chunks || {})) {
            if (sockets[conn]) {
              for (const b64 of (chunks as string[])) {
                const buf = Buffer.from(b64, 'base64');
                console.log(`[IR] ⬅️ Writing ${buf.length} bytes to local client for ${conn}`);
                sockets[conn].write(buf);
              }
            } else {
              console.log(`[IR] ⚠️ Dropping DOWN data, socket ${conn} already closed`);
            }
          }
        }
      } catch (e: any) {
         console.error(`[IR] ❌ SYNC-DOWN error: ${e.message}`);
      }
    }

    await sleep(hasActivity ? 5 : 50);
  }
})();