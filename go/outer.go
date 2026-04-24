package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
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

const MaxChunkSize = 16 * 1024 * 1024

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

// ⚡ BINARY + LONG-POLLING RECEIVER
func receiver() {
	for {
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s?action=sync&dir=up", *relayURL), nil)
		req.Header.Set("X-API-KEY", *apiKey)

		resp, err := httpClient.Do(req)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.StatusCode == 200 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			buf := bytes.NewReader(body)

			for buf.Len() > 0 {
				t, _ := buf.ReadByte()
				cLen, _ := buf.ReadByte()
				connBytes := make([]byte, cLen)
				buf.Read(connBytes)
				connID := string(connBytes)

				if t == 0 { // Data Frame
					var dLen uint32
					binary.Read(buf, binary.BigEndian, &dLen)
					data := make([]byte, dLen)
					buf.Read(data)

					var conn net.Conn
					upBuffer.Lock()
					if val, ok := sockets.Load(connID); ok {
						conn = val.(net.Conn)
					} else if _, isSeen := seen.Load(connID); isSeen {
						upBuffer.m[connID] = append(upBuffer.m[connID], data)
					}
					upBuffer.Unlock()

					if conn != nil {
						conn.Write(data)
					}

				} else if t == 1 { // EOF Frame
					log.Printf("[FR] 🏁 Remote EOF %s", connID)
					if val, ok := sockets.Load(connID); ok {
						val.(net.Conn).Close()
					}
					sockets.Delete(connID)
					seen.Delete(connID)
					relayClose(connID)

				} else if t == 2 { // Open Request Frame
					hLen, _ := buf.ReadByte()
					hBytes := make([]byte, hLen)
					buf.Read(hBytes)
					var port uint16
					binary.Read(buf, binary.BigEndian, &port)

					if _, exists := seen.Load(connID); !exists {
						seen.Store(connID, true)
						log.Printf("[FR] 🔔 New Request: %s → %s:%d", connID, string(hBytes), port)
						go openTCP(connID, string(hBytes), int(port))
					}
				}
			}
			// No sleep, loop instantly because data is flowing
		} else {
			resp.Body.Close()
			// 204 No Content -> Long poll timed out cleanly. Loop instantly.
		}
	}
}

// ⚡ BINARY SENDER
func sender() {
	for {
		var outBuf bytes.Buffer

		sendBuffer.Lock()
		for connID, bufs := range sendBuffer.m {
			var flat[]byte
			for _, b := range bufs {
				flat = append(flat, b...)
			}
			
			if len(flat) > MaxChunkSize {
				chunk := flat[:MaxChunkSize]
				sendBuffer.m[connID] = [][]byte{flat[MaxChunkSize:]}
				
				outBuf.WriteByte(0)
				outBuf.WriteByte(byte(len(connID)))
				outBuf.WriteString(connID)
				binary.Write(&outBuf, binary.BigEndian, uint32(len(chunk)))
				outBuf.Write(chunk)
			} else {
				outBuf.WriteByte(0)
				outBuf.WriteByte(byte(len(connID)))
				outBuf.WriteString(connID)
				binary.Write(&outBuf, binary.BigEndian, uint32(len(flat)))
				outBuf.Write(flat)
				delete(sendBuffer.m, connID)
			}
		}
		sendBuffer.Unlock()

		eofLock.Lock()
		if len(eofSends) > 0 {
			var nextEof[]string
			for _, connID := range eofSends {
				sendBuffer.Lock()
				_, hasPending := sendBuffer.m[connID]
				sendBuffer.Unlock()

				if hasPending {
					nextEof = append(nextEof, connID)
				} else {
					outBuf.WriteByte(1)
					outBuf.WriteByte(byte(len(connID)))
					outBuf.WriteString(connID)
				}
			}
			eofSends = nextEof
		}
		eofLock.Unlock()

		if outBuf.Len() > 0 {
			req, _ := http.NewRequest("POST", fmt.Sprintf("%s?action=send_batch&dir=down", *relayURL), &outBuf)
			req.Header.Set("X-API-KEY", *apiKey)
			req.Header.Set("Content-Type", "application/octet-stream")
			if resp, err := httpClient.Do(req); err == nil {
				resp.Body.Close()
			}
		} else {
			time.Sleep(5 * time.Millisecond) // local idle wait
		}
	}
}

func main() {
	flag.Parse()
	log.Println("🚀 FURO (FRANCE) EXIT NODE RUNNING [BINARY + LONG POLL]")

	go func() {
		for {
			req, _ := http.NewRequest("GET", fmt.Sprintf("%s?action=cleanup", *relayURL), nil)
			req.Header.Set("X-API-KEY", *apiKey)
			httpClient.Do(req)
			time.Sleep(1 * time.Minute)
		}
	}()

	go receiver()
	go sender()
	select {}
}