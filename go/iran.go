package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/armon/go-socks5"
)

var (
	relayURL = flag.String("relay", "https://hidaco.site/tools/rel/index.php", "PHP Relay URL")
	apiKey   = flag.String("key", "my_super_secret_123456789", "API Key")
	listenIP = flag.String("listen", "127.0.0.1", "SOCKS5 listen IP")
	port     = flag.String("port", "1080", "SOCKS5 listen port")
)

var (
	connCounter int64
	connMutex   sync.Mutex
	sockets     = sync.Map{}

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

func genConnID() string {
	connMutex.Lock()
	defer connMutex.Unlock()
	connCounter++
	return fmt.Sprintf("c_%d_%d", time.Now().UnixMilli(), connCounter)
}

type TunnelConn struct {
	id     string
	cond   *sync.Cond
	rxBuf[]byte
	closed bool
	once   sync.Once
}

func newTunnelConn(id string) *TunnelConn {
	return &TunnelConn{
		id:   id,
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

func (c *TunnelConn) Read(b[]byte) (n int, err error) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	for len(c.rxBuf) == 0 && !c.closed {
		c.cond.Wait()
	}

	if len(c.rxBuf) > 0 {
		n = copy(b, c.rxBuf)
		c.rxBuf = c.rxBuf[n:]
		if len(c.rxBuf) == 0 {
			c.rxBuf = nil
		}
		return n, nil
	}
	return 0, io.EOF
}

func (c *TunnelConn) Write(b[]byte) (n int, err error) {
	c.cond.L.Lock()
	isClosed := c.closed
	c.cond.L.Unlock()

	if isClosed {
		return 0, io.EOF
	}

	data := make([]byte, len(b))
	copy(data, b)

	sendBuffer.Lock()
	sendBuffer.m[c.id] = append(sendBuffer.m[c.id], data)
	sendBuffer.Unlock()
	return len(b), nil
}

func (c *TunnelConn) Close() error {
	c.once.Do(func() {
		c.cond.L.Lock()
		c.closed = true
		c.cond.Broadcast()
		c.cond.L.Unlock()

		sockets.Delete(c.id)

		eofLock.Lock()
		eofSends = append(eofSends, c.id)
		eofLock.Unlock()

		log.Printf("[IR] 🛑 Closed %s", c.id)
	})
	return nil
}

func (c *TunnelConn) LocalAddr() net.Addr                { return &net.TCPAddr{IP: net.IPv4(0, 0, 0, 0), Port: 0} }
func (c *TunnelConn) RemoteAddr() net.Addr               { return &net.TCPAddr{IP: net.IPv4(0, 0, 0, 0), Port: 0} }
func (c *TunnelConn) SetDeadline(t time.Time) error      { return nil }
func (c *TunnelConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *TunnelConn) SetWriteDeadline(t time.Time) error { return nil }

func relayOpen(connID, host, port string) {
	reqURL := fmt.Sprintf("%s?action=open&conn=%s&host=%s&port=%s", *relayURL, connID, url.QueryEscape(host), port)
	req, _ := http.NewRequest("GET", reqURL, nil)
	req.Header.Set("X-API-KEY", *apiKey)
	httpClient.Do(req)
}

// ⚡ BINARY + LONG-POLLING RECEIVER
func receiver() {
	for {
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s?action=sync&dir=down", *relayURL), nil)
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

					if val, ok := sockets.Load(connID); ok {
						tConn := val.(*TunnelConn)
						tConn.cond.L.Lock()
						tConn.rxBuf = append(tConn.rxBuf, data...)
						tConn.cond.Signal()
						tConn.cond.L.Unlock()
					}
				} else if t == 1 { // EOF Frame
					log.Printf("[IR] 🏁 Remote EOF %s", connID)
					if val, ok := sockets.Load(connID); ok {
						val.(*TunnelConn).Close()
					}
				}
			}
			// No sleep
		} else {
			resp.Body.Close()
			// 204 No Content -> wait handled by PHP, loop instantly
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
			req, _ := http.NewRequest("POST", fmt.Sprintf("%s?action=send_batch&dir=up", *relayURL), &outBuf)
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

	conf := &socks5.Config{
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, targetPort, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}

			connID := genConnID()
			log.Printf("[IR] 🔗 CONNECT %s → %s:%s", connID, host, targetPort)

			go relayOpen(connID, host, targetPort)

			tConn := newTunnelConn(connID)
			sockets.Store(connID, tConn)
			return tConn, nil
		},
	}

	server, err := socks5.New(conf)
	if err != nil {
		log.Fatalf("Failed to create SOCKS5 server: %v", err)
	}

	go func() {
		addr := fmt.Sprintf("%s:%s", *listenIP, *port)
		log.Printf("🚀 FURO (IRAN) SOCKS5 RUNNING ON %s [BINARY + LONG POLL]", addr)
		if err := server.ListenAndServe("tcp", addr); err != nil {
			log.Fatalf("SOCKS5 Serve error: %v", err)
		}
	}()

	go receiver()
	go sender()
	select {}
}