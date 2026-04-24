package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

var (
	relayURL = flag.String("relay", "https://hidaco.site/tools/rel/index.php", "PHP Relay URL")
	apiKey   = flag.String("key", "my_super_secret_123456789", "API Key")
)

var (
	sockets    = sync.Map{}
	seen       = sync.Map{}

	upBuffer = struct {
		sync.Mutex
		m map[string][][]byte
	}{m: make(map[string][][]byte)}

	sendBuffer = struct {
		sync.Mutex
		m map[string][][]byte
	}{m: make(map[string][][]byte)}

	eofLock  sync.Mutex
	eofSends[]string

	httpClient = &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        500,
			MaxIdleConnsPerHost: 500,
			IdleConnTimeout:     120 * time.Second,
			DisableKeepAlives:   false,
			ForceAttemptHTTP2:   true,
		},
	}
)

// 🚀 UPGRADED: 16 MB chunk limiter for high-speed PHP servers
const MaxChunkSize = 16 * 1024 * 1024 

type OpenReq struct {
	Conn string `json:"conn"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

func relayClose(connID string) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s?action=close&conn=%s", *relayURL, connID), nil)
	req.Header.Set("X-API-KEY", *apiKey)
	go httpClient.Do(req)
	log.Printf("[FR] 🛑 Relay Cleaned %s", connID)
}

func openTCP(connID, host string, port int) {
	target := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		log.Printf("[FR] ❌ FAILED %s → %s (%v)", connID, target, err)
		upBuffer.Lock()
		delete(upBuffer.m, connID)
		upBuffer.Unlock()
		seen.Delete(connID)

		eofLock.Lock()
		eofSends = append(eofSends, connID)
		eofLock.Unlock()
		return
	}

	sockets.Store(connID, conn)
	log.Printf("[FR] 🔗 CONNECTED %s → %s", connID, target)

	upBuffer.Lock()
	if queued, exists := upBuffer.m[connID]; exists {
		for _, buf := range queued {
			conn.Write(buf)
		}
		delete(upBuffer.m, connID)
	}
	upBuffer.Unlock()

	go func() {
		defer func() {
			conn.Close()
			sockets.Delete(connID)
			seen.Delete(connID)
			eofLock.Lock()
			eofSends = append(eofSends, connID)
			eofLock.Unlock()
		}()

		// 🚀 UPGRADED: 256 KB TCP read buffer for faster internet slurping
		buf := make([]byte, 256*1024)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])

				sendBuffer.Lock()
				sendBuffer.m[connID] = append(sendBuffer.m[connID], data)
				sendBuffer.Unlock()
			}
			if err != nil {
				break
			}
		}
	}()
}

// ⚡ FULL-DUPLEX: Dedicated Receiver Loop
func receiver() {
	for {
		hasActivity := false
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s?action=sync&dir=up", *relayURL), nil)
		req.Header.Set("X-API-KEY", *apiKey)

		if resp, err := httpClient.Do(req); err == nil {
			var data struct {
				Chunks map[string][]string `json:"chunks"`
				Opens[]OpenReq           `json:"opens"`
			}
			if json.NewDecoder(resp.Body).Decode(&data) == nil {
				for _, o := range data.Opens {
					if _, exists := seen.Load(o.Conn); !exists {
						seen.Store(o.Conn, true)
						if o.Host != "" && o.Port != 0 {
							log.Printf("[FR] 🔔 New Request: %s → %s:%d", o.Conn, o.Host, o.Port)
							go openTCP(o.Conn, o.Host, o.Port)
						}
					}
				}
				if len(data.Chunks) > 0 {
					hasActivity = true
					for connID, chunks := range data.Chunks {
						for _, b64 := range chunks {
							if b64 == "EOF" {
								log.Printf("[FR] 🏁 Remote EOF %s", connID)
								if val, ok := sockets.Load(connID); ok {
									val.(net.Conn).Close()
								}
								sockets.Delete(connID)
								seen.Delete(connID)
								relayClose(connID)
								continue
							}
							if buf, err := base64.StdEncoding.DecodeString(b64); err == nil {
								var conn net.Conn
								upBuffer.Lock()
								if val, ok := sockets.Load(connID); ok {
									conn = val.(net.Conn)
								} else if _, isSeen := seen.Load(connID); isSeen {
									upBuffer.m[connID] = append(upBuffer.m[connID], buf)
								}
								upBuffer.Unlock()
								if conn != nil {
									conn.Write(buf)
								}
							}
						}
					}
				}
			}
			resp.Body.Close()
		}

		if hasActivity {
			time.Sleep(1 * time.Millisecond)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// ⚡ FULL-DUPLEX: Dedicated Sender Loop
func sender() {
	for {
		hasActivity := false
		sendBuffer.Lock()
		payload := make(map[string]string)

		if len(sendBuffer.m) > 0 {
			for connID, bufs := range sendBuffer.m {
				var flat[]byte
				for _, b := range bufs {
					flat = append(flat, b...)
				}
				// 🚀 UPGRADED: Chunk Limiter to 16MB
				if len(flat) > MaxChunkSize {
					payload[connID] = base64.StdEncoding.EncodeToString(flat[:MaxChunkSize])
					sendBuffer.m[connID] = [][]byte{flat[MaxChunkSize:]} // Keep remainder
				} else {
					payload[connID] = base64.StdEncoding.EncodeToString(flat)
					delete(sendBuffer.m, connID)
				}
			}
		}
		sendBuffer.Unlock()

		eofLock.Lock()
		if len(eofSends) > 0 {
			var nextEof[]string
			for _, connID := range eofSends {
				if _, ok := payload[connID]; !ok {
					payload[connID] = "EOF"
				} else {
					nextEof = append(nextEof, connID) // wait until data is cleared
				}
			}
			eofSends = nextEof
		}
		eofLock.Unlock()

		if len(payload) > 0 {
			hasActivity = true
			jsonData, _ := json.Marshal(payload)
			req, _ := http.NewRequest("POST", fmt.Sprintf("%s?action=send_batch&dir=down", *relayURL), bytes.NewBuffer(jsonData))
			req.Header.Set("X-API-KEY", *apiKey)
			req.Header.Set("Content-Type", "application/json")
			if resp, err := httpClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}

		if hasActivity {
			time.Sleep(1 * time.Millisecond)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func main() {
	flag.Parse()
	log.Println("🚀 FURO (FRANCE) EXIT NODE RUNNING")

	go func() {
		for {
			req, _ := http.NewRequest("GET", fmt.Sprintf("%s?action=cleanup", *relayURL), nil)
			req.Header.Set("X-API-KEY", *apiKey)
			httpClient.Do(req)
			time.Sleep(1 * time.Minute)
		}
	}()

	// Start concurrent processing
	go receiver()
	go sender()

	// Block main thread forever
	select {}
}