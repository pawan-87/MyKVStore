package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"time"

	"github.com/pawan-87/MyKVStore/client"
)

func main() {

	/*
		Start node1 first in separate terminal

		Start one node

		 go run . \
		  --id 1 \
		  --listen-client 127.0.0.1:2379 \
		  --listen-peer http://127.0.0.1:2380 \
		  --initial-cluster "1=http://127.0.0.1:2380" \
		  --data-dir ./data/node1
	*/

	allPeers := map[uint64]string{
		1: "http://127.0.0.1:2380",
	}

	node1Client, err := client.New("127.0.0.1:2379")
	if err != nil {
		log.Fatalf("clouldn't connect to node1: %v", err)
	}
	log.Println("Connected to node1 (127.0.0.1:2379)")

	listMembers(node1Client)

	putAndGet(node1Client, "name", "pawan")
	putAndGet(node1Client, "city", "bangalore")
	putAndGet(node1Client, "project", "MyKVStore")
	putAndGet(node1Client, "age", "24")

	// add node2
	_, err = addNode(node1Client,
		"http://127.0.0.1:2480",
		true,
		"127.0.0.1:2479",
		"./data/node2",
		allPeers,
	)
	if err != nil {
		log.Fatalf("Failed to add node2: %v", err)
	}
	waitForNode("http://127.0.0.1:2480", 120*time.Second) // wait until node2's peer transport is online
	time.Sleep(3 * time.Second)                           // extra time for raft sync

	putAndGet(node1Client, "afternode2", "afternode2val")

	listMembers(node1Client)

	// add node 3
	node3ID, err := addNode(node1Client,
		"http://127.0.0.1:2580",
		true,
		"127.0.0.1:2579",
		"./data/node3",
		allPeers,
	)
	if err != nil {
		log.Fatalf("Failed to add node3: %v", err)
	}
	waitForNode("http://127.0.0.1:2580", 120*time.Second) // wait until node3's peer transport is online
	time.Sleep(3 * time.Second)                           // extra time for raft sync

	putAndGet(node1Client, "afternode3", "afternode3val")
	listMembers(node1Client)

	// add node 4
	node4ID, err := addNode(node1Client,
		"http://127.0.0.1:2680",
		true,
		"127.0.0.1:2679",
		"./data/node4",
		allPeers,
	)
	if err != nil {
		log.Fatalf("Failed to add node4: %v", err)
	}
	waitForNode("http://127.0.0.1:2680", 120*time.Second) // wait until node4's peer transport is online
	time.Sleep(3 * time.Second)                           // extra time for raft sync

	log.Printf("🔍 Verifying node 3 is reachable before promotion...")
	if !isNodeReachable("http://127.0.0.1:2580") {
		log.Fatal("Node 3 is NOT reachable — cannot safely promote!")
	}
	log.Printf("Node 3 is reachable, promoting to voter...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = node1Client.Cluster.MemberPromote(ctx, node3ID)
	cancel()
	if err != nil {
		log.Printf("Promote node3 failed: %v", err)
	} else {
		log.Printf("Node 3 promoted to voter")
	}

	time.Sleep(5 * time.Second)

	putAndGet(node1Client, "afterpromote", "afterpromoteval")
	listMembers(node1Client)

	putAndGet(node1Client, "name3", "pawan")
	putAndGet(node1Client, "city3", "bangalore")
	putAndGet(node1Client, "project3", "MyKVStore")
	putAndGet(node1Client, "age3", "24")

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = node1Client.Cluster.MemberRemove(ctx, node4ID)
	if err != nil {
		log.Printf("❌ MemberRemove failed: %v", err)
	}

	listMembers(node1Client)

	putAndGet(node1Client, "afterremovingnode3", "afterremovingnode3val")

	listMembers(node1Client)
}

func waitForNode(peerURL string, timeout time.Duration) {
	u, err := url.Parse(peerURL)
	if err != nil {
		log.Fatalf("invalid peer URL %q: %v", peerURL, err)
	}
	host := u.Host

	log.Printf("Waiting for "+
		"node at %s to come online (timeout %s)...", peerURL, timeout)

	deadline := time.Now().Add(timeout)
	attempt := 0
	for time.Now().Before(deadline) {
		attempt++
		conn, err := net.DialTimeout("tcp", host, 1*time.Second)
		if err == nil {
			conn.Close()
			log.Printf("Node at %s is online! (took %d attempts)", peerURL, attempt)
			return
		}
		if attempt%10 == 0 {
			remaining := time.Until(deadline).Round(time.Second)
			log.Printf("Still waiting for %s ... (%s remaining)", peerURL, remaining)
		}
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("TIMEOUT: Node at %s never came online after %s. Did you start it?", peerURL, timeout)
}

func isNodeReachable(peerURL string) bool {
	u, err := url.Parse(peerURL)
	if err != nil {
		return false
	}
	conn, err := net.DialTimeout("tcp", u.Host, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func addNode(c *client.Client, peerURL string, isLearner bool, clientAddr string, dataDir string, allPeers map[uint64]string) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.Cluster.MemberAdd(ctx, []string{peerURL}, isLearner)
	if err != nil {
		return 0, fmt.Errorf("MemberAdd failed: %w", err)
	}

	newID := resp.Member.ID
	allPeers[newID] = peerURL

	log.Printf("✅ Member added! ID=%d PeerURL=%s Learner=%v", newID, peerURL, isLearner)

	clusterStr := ""
	for id, url := range allPeers {
		if clusterStr != "" {
			clusterStr += ","
		}
		clusterStr += fmt.Sprintf("%d=%s", id, url)
	}

	fmt.Printf(`
go run . \
  --id %d \
  --join \
  --listen-client %s \
  --listen-peer %s \
  --initial-cluster "%s" \
  --data-dir %s
`, newID, clientAddr, peerURL, clusterStr, dataDir)

	return newID, nil
}

func listMembers(c *client.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.Cluster.MemberList(ctx)
	if err != nil {
		log.Printf("MemberList failed: %v", err)
		return
	}

	log.Printf("Cluster members:")
	for _, m := range resp.Members {
		role := "voter"
		if m.IsLearner {
			role = "learner"
		}
		log.Printf("   ID=%-20d Name=%-12s PeerURLs=%-30v Role=%s", m.ID, m.Name, m.PeerURLs, role)
	}
}

func putAndGet(c *client.Client, key string, val string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.KV.Put(ctx, key, val)
	if err != nil {
		log.Printf("❌put failed: key: %s - value: %s\n", key, val)
		return
	}
	log.Printf("✅ Put: key:%s - value:%s", key, val)

	resp, err := c.KV.Get(ctx, key)
	if err != nil {
		log.Printf("❌ Get(%s) failed: %v", key, err)
	}

	for _, kv := range resp.Kvs {
		log.Printf("✅ Get: %s = %s  (rev=%d)", string(kv.Key), string(kv.Value), kv.ModRevision)
	}
}
