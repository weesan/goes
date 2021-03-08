package goes

import (
	"log"
	"net"
	"time"
)

const defaultDiscoveryRefreshTimer = 5 * time.Second

type node struct {
	name string
	ip   string
}

type nodes map[string]*node

func newNode(name string, discovery string) (*node, error) {
	//listen(discovery)
	//sendHeartBeat(name, discovery)

	return &node{
		name: name,
		ip:   "localhost",
	}, nil
}

func listen(discovery string) {
	go func() {
		for {
			addr, err := net.ResolveUDPAddr("udp", discovery)
			if err != nil {
				log.Fatal(err)
			}

			conn, err := net.ListenMulticastUDP("udp", nil, addr)
			if err != nil {
				log.Fatal(err)
			}

			conn.SetReadBuffer(8192)

			for {
				buf := make([]byte, 8192)
				n, src, err := conn.ReadFromUDP(buf)
				if err != nil {
					log.Fatal("Failed to read from discovery:", err)
				}
				log.Printf("Received from %s: %s (%d)", src, string(buf)[:n], n)
			}
		}
	}()
}

func sendHeartBeat(name string, discovery string) {
	refreshTicker := time.NewTicker(defaultDiscoveryRefreshTimer)
	go func() {
		for {
			<-refreshTicker.C

			addr, err := net.ResolveUDPAddr("udp", discovery)
			if err != nil {
				log.Fatal(err)
			}

			conn, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				log.Fatal(err)
			}

			conn.Write([]byte("from " + name))
		}
	}()
}
