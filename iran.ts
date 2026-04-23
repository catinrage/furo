import net from "net";

const RELAY = process.env.RELAY_URL || "https://hidaco.site/tools/rel/index.php";
const API_KEY = process.env.API_KEY || "my_super_secret_123456789";

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

let counter = 0;
function connId() {
  return `c_${Date.now()}_${counter++}`;
}

async function relaySend(conn: string, dir: string, data: Buffer | Uint8Array) {
  try {
    await fetch(`${RELAY}?action=send&conn=${conn}&dir=${dir}`, {
      method: "POST",
      headers: { "X-API-KEY": API_KEY },
      body: data,
    });
  } catch (e) {
    // silently fail to prevent crash loops
  }
}

async function relayRecv(conn: string, dir: string): Promise<Buffer[]> {
  try {
    const res = await fetch(`${RELAY}?action=receive&conn=${conn}&dir=${dir}`, {
      headers: { "X-API-KEY": API_KEY },
    });
    if (!res.ok) return[];
    const json = await res.json();
    return (json.chunks ||[]).map((b64: string) => Buffer.from(b64, 'base64'));
  } catch (e) {
    return[];
  }
}

async function relayOpen(conn: string, host: string, port: number) {
  try {
    await fetch(`${RELAY}?action=open&conn=${conn}&host=${encodeURIComponent(host)}&port=${port}`, {
      headers: { "X-API-KEY": API_KEY },
    });
    console.log(`[IR] OPEN sent → ${host}:${port}`);
  } catch (e) {
    console.error(`[${conn}] Open error`, e);
  }
}

async function relayClose(conn: string) {
  try {
    await fetch(`${RELAY}?action=close&conn=${conn}`, {
      headers: { "X-API-KEY": API_KEY },
    });
  } catch (e) { }
}

// SOCKS5 SERVER
net.createServer((socket) => {
  const conn = connId();
  console.log(`\n[SOCKS] new ${conn}`);

  let stage = 0;
  let active = true;

  socket.on("data", async (data: Buffer) => {
    // -------------------------
    // STAGE 0: handshake
    // -------------------------
    if (stage === 0) {
      socket.write(Buffer.from([0x05, 0x00]));
      stage = 1;
      return;
    }

    // -------------------------
    // STAGE 1: CONNECT parse
    // -------------------------
    if (stage === 1) {
      if (data.length < 7 || data[0] !== 0x05 || data[1] !== 0x01) {
        socket.destroy();
        return;
      }

      let host = "";
      let port = 0;
      let offset = 0;
      const atyp = data[3];

      if (atyp === 0x01) { // IPv4
        host = `${data[4]}.${data[5]}.${data[6]}.${data[7]}`;
        port = data.readUInt16BE(8);
        offset = 10;
      } else if (atyp === 0x03) { // Domain name
        const hostLen = data[4];
        host = data.subarray(5, 5 + hostLen).toString();
        port = data.readUInt16BE(5 + hostLen);
        offset = 5 + hostLen + 2;
      } else if (atyp === 0x04) { // IPv6
        const ipv6 = data.subarray(4, 20);
        const parts =[];
        for (let i = 0; i < 16; i += 2) parts.push(ipv6.readUInt16BE(i).toString(16));
        host = parts.join(':');
        port = data.readUInt16BE(20);
        offset = 22;
      } else {
        socket.destroy();
        return;
      }

      console.log(`[${conn}] CONNECT ${host}:${port}`);
      await relayOpen(conn, host, port);

      // Standard 10-byte SOCKS5 Success Reply
      const reply = Buffer.from([0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0]);
      socket.write(reply);
      stage = 2;

      // Start response polling loop
      (async () => {
        while (active) {
          const chunks = await relayRecv(conn, "down");
          for (const c of chunks) {
            if (active) socket.write(c);
          }
          await sleep(chunks.length > 0 ? 10 : 50); // adaptive polling latency
        }
      })();

      // If payload is bundled via pipelining immediately after CONNECT block, forward it
      if (data.length > offset) {
        await relaySend(conn, "up", data.subarray(offset));
      }
      return;
    }

    // -------------------------
    // STAGE 2: STREAM
    // -------------------------
    if (stage === 2) {
      await relaySend(conn, "up", data);
    }
  });

  socket.on("close", () => {
    console.log(`[${conn}] closed`);
    active = false;
    relayClose(conn);
  });

  socket.on("error", (e) => {
    console.error(`[${conn}] Error: ${e.message}`);
  });

}).listen(1080, () => {
  console.log("🚀 IRAN SOCKS5 RUNNING ON :1080");
});