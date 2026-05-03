package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

var (
	masterConfigPath = flag.String("c", "config.server-master.json", "Path to server master config JSON")
	masterVersion    = flag.Bool("version", false, "Print version and exit")
	pingClientURL    = flag.String("ping-client", "", "Ping a furo-client admin URL, for example http://CLIENT_IP:19080")
)

var (
	appVersion   = "dev"
	appCommit    = "unknown"
	appBuildDate = "unknown"
)

type masterConfigFile struct {
	Namespace                string `json:"namespace"`
	APIKey                   string `json:"api_key"`
	Listen                   string `json:"listen"`
	PublicURL                string `json:"public_url"`
	AdminListen              string `json:"admin_listen"`
	CaasifyToken             string `json:"caasify_token"`
	StateFile                string `json:"state_file"`
	LogFile                  string `json:"log_file"`
	RelayURL                 string `json:"relay_url"`
	RelayHealthHost          string `json:"relay_health_host"`
	RelayHealthPort          int    `json:"relay_health_port"`
	ServerAgentPort          int    `json:"server_agent_port"`
	BackupCount              int    `json:"backup_count"`
	NodeCheckIntervalSeconds int    `json:"node_check_interval_seconds"`
	NodeFailureThreshold     int    `json:"node_failure_threshold"`
	PublishIntervalSeconds   int    `json:"publish_interval_seconds"`
	ProductID                int    `json:"product_id"`
	Template                 string `json:"template"`
	NotePrefix               string `json:"note_prefix"`
	IPv4                     int    `json:"ipv4"`
	IPv6                     int    `json:"ipv6"`
	BootstrapScriptPath      string `json:"bootstrap_script_path"`
}

func defaultMasterConfig() masterConfigFile {
	return masterConfigFile{
		Namespace:                "default",
		APIKey:                   "my_super_secret_123456789",
		Listen:                   "0.0.0.0:19082",
		AdminListen:              "127.0.0.1:19083",
		StateFile:                "furo-server-master-state.json",
		RelayHealthHost:          "f2.ra1n.xyz",
		RelayHealthPort:          443,
		ServerAgentPort:          8443,
		BackupCount:              1,
		NodeCheckIntervalSeconds: 10,
		NodeFailureThreshold:     3,
		PublishIntervalSeconds:   60,
		ProductID:                3776,
		Template:                 "ubuntu-24.04",
		NotePrefix:               "furo-server",
		IPv4:                     1,
		IPv6:                     1,
		BootstrapScriptPath:      "scripts/bootstrap-server-node.sh.example",
	}
}

func loadMasterConfig(path string) (masterConfigFile, error) {
	cfg := defaultMasterConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	if cfg.APIKey == "" {
		return cfg, errors.New("api_key is required")
	}
	cfg.Namespace = sanitizeMasterID(cfg.Namespace)
	if cfg.Namespace == "" {
		cfg.Namespace = "default"
	}
	if cfg.CaasifyToken == "" {
		return cfg, errors.New("caasify_token is required")
	}
	if cfg.RelayURL == "" {
		return cfg, errors.New("relay_url is required")
	}
	if cfg.Listen == "" {
		return cfg, errors.New("listen is required")
	}
	if cfg.PublicURL == "" {
		cfg.PublicURL = "http://" + cfg.Listen
	}
	if cfg.BackupCount < 0 {
		return cfg, errors.New("backup_count must be >= 0")
	}
	if cfg.NodeCheckIntervalSeconds <= 0 {
		cfg.NodeCheckIntervalSeconds = 10
	}
	if cfg.NodeFailureThreshold <= 0 {
		cfg.NodeFailureThreshold = 3
	}
	if cfg.PublishIntervalSeconds <= 0 {
		cfg.PublishIntervalSeconds = 60
	}
	if cfg.ServerAgentPort <= 0 {
		cfg.ServerAgentPort = 8443
	}
	if cfg.RelayHealthPort <= 0 {
		cfg.RelayHealthPort = 443
	}
	return cfg, nil
}

type masterNode struct {
	ID              string `json:"id"`
	Namespace       string `json:"namespace"`
	OrderID         string `json:"order_id"`
	IP              string `json:"ip"`
	Role            string `json:"role"`
	Status          string `json:"status"`
	Password        string `json:"password,omitempty"`
	CreatedAt       string `json:"created_at"`
	LastReportAt    string `json:"last_report_at,omitempty"`
	LastReportState string `json:"last_report_state,omitempty"`
}

type masterState struct {
	Namespace  string       `json:"namespace"`
	FleetID    string       `json:"fleet_id"`
	Generation int64        `json:"generation"`
	ActiveID   string       `json:"active_id"`
	Nodes      []masterNode `json:"nodes"`
	Retired    []string     `json:"retired"`
	UpdatedAt  string       `json:"updated_at"`
}

func defaultMasterState(namespace string) masterState {
	if namespace == "" {
		namespace = "default"
	}
	return masterState{Namespace: namespace, FleetID: "furo-" + namespace, Generation: 1, Nodes: []masterNode{}}
}

func sanitizeMasterID(value string) string {
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "fleet"
	}
	return b.String()
}

type masterApp struct {
	cfg       masterConfigFile
	statePath string
	mu        sync.Mutex
	state     masterState
	caasify   *caasifyClient
	startedAt time.Time
}

func newMasterApp(cfg masterConfigFile) *masterApp {
	return &masterApp{
		cfg:       cfg,
		statePath: resolvePath(filepath.Dir(*masterConfigPath), cfg.StateFile),
		caasify:   newCaasifyClient(cfg.CaasifyToken),
		startedAt: time.Now(),
	}
}

func resolvePath(base, value string) string {
	if filepath.IsAbs(value) {
		return value
	}
	return filepath.Join(base, value)
}

func (a *masterApp) loadState() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	data, err := os.ReadFile(a.statePath)
	if errors.Is(err, os.ErrNotExist) {
		a.state = defaultMasterState(a.cfg.Namespace)
		return a.saveStateLocked()
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &a.state); err != nil {
		return err
	}
	if a.state.Namespace == "" {
		a.state.Namespace = a.cfg.Namespace
	}
	if a.state.Namespace != a.cfg.Namespace {
		return fmt.Errorf("state namespace %q does not match config namespace %q", a.state.Namespace, a.cfg.Namespace)
	}
	if a.state.FleetID == "" {
		a.state.FleetID = defaultMasterState(a.cfg.Namespace).FleetID
	}
	if a.state.Generation <= 0 {
		a.state.Generation = 1
	}
	return nil
}

func (a *masterApp) saveStateLocked() error {
	a.state.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	data, err := json.MarshalIndent(a.state, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(a.statePath), 0755); err != nil {
		return err
	}
	tmp := a.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, a.statePath)
}

func (a *masterApp) findNodeLocked(id string) *masterNode {
	for idx := range a.state.Nodes {
		if a.state.Nodes[idx].Namespace == a.cfg.Namespace && a.state.Nodes[idx].ID == id {
			return &a.state.Nodes[idx]
		}
	}
	return nil
}

func (a *masterApp) activeNodeLocked() *masterNode {
	if a.state.ActiveID == "" {
		return nil
	}
	return a.findNodeLocked(a.state.ActiveID)
}

func (a *masterApp) standbyNodesLocked() []masterNode {
	var standby []masterNode
	for _, node := range a.state.Nodes {
		if node.Namespace == a.cfg.Namespace && node.Role == "standby" && node.Status != "deleted" && node.Status != "deleting" {
			standby = append(standby, node)
		}
	}
	return standby
}

func (a *masterApp) nextNodeID(role string) string {
	return fmt.Sprintf("%s-%s-%s-%d", a.cfg.NotePrefix, a.cfg.Namespace, role, time.Now().Unix())
}

func (a *masterApp) ensureFleet(ctx context.Context) error {
	a.mu.Lock()
	needActive := a.activeNodeLocked() == nil
	standbyNeed := a.cfg.BackupCount - len(a.standbyNodesLocked())
	a.mu.Unlock()
	if needActive {
		if _, err := a.createAndBootstrapNode(ctx, "active"); err != nil {
			return err
		}
	}
	for standbyNeed > 0 {
		if _, err := a.createAndBootstrapNode(ctx, "standby"); err != nil {
			return err
		}
		standbyNeed--
	}
	return a.publishRouteMap(ctx)
}

func (a *masterApp) createAndBootstrapNode(ctx context.Context, role string) (masterNode, error) {
	nodeID := a.nextNodeID(role)
	order, err := a.caasify.createVPS(ctx, caasifyCreateRequest{
		ProductID: a.cfg.ProductID,
		Note:      nodeID,
		Template:  a.cfg.Template,
		IPv4:      a.cfg.IPv4,
		IPv6:      a.cfg.IPv6,
	})
	if err != nil {
		return masterNode{}, err
	}
	info, err := a.caasify.waitForOrderReady(ctx, order.OrderID)
	if err != nil {
		return masterNode{}, err
	}
	node := masterNode{
		ID:        nodeID,
		Namespace: a.cfg.Namespace,
		OrderID:   order.OrderID,
		IP:        info.IP,
		Password:  info.Password,
		Role:      role,
		Status:    "created",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := a.bootstrapNode(ctx, node); err != nil {
		return masterNode{}, err
	}
	if role == "standby" {
		if err := a.verifyNodeRelayAccess(ctx, node); err != nil {
			_ = a.caasify.deleteOrder(context.Background(), node.OrderID)
			return masterNode{}, err
		}
	}
	node.Status = "ready"
	a.mu.Lock()
	if role == "active" {
		a.state.ActiveID = node.ID
	}
	a.state.Nodes = append(a.state.Nodes, node)
	a.state.Generation++
	err = a.saveStateLocked()
	a.mu.Unlock()
	if err != nil {
		return masterNode{}, err
	}
	return node, nil
}

func (a *masterApp) bootstrapNode(ctx context.Context, node masterNode) error {
	scriptPath := resolvePath(filepath.Dir(*masterConfigPath), a.cfg.BootstrapScriptPath)
	data, err := os.ReadFile(scriptPath)
	if err != nil {
		return err
	}
	rendered := string(data)
	replacements := map[string]string{
		"{{api_key}}":                a.cfg.APIKey,
		"{{namespace}}":              a.cfg.Namespace,
		"{{node_id}}":                node.ID,
		"{{node_role}}":              node.Role,
		"{{master_url}}":             a.cfg.PublicURL,
		"{{relay_health_host}}":      a.cfg.RelayHealthHost,
		"{{relay_health_port}}":      strconv.Itoa(a.cfg.RelayHealthPort),
		"{{check_interval_seconds}}": strconv.Itoa(a.cfg.NodeCheckIntervalSeconds),
		"{{failure_threshold}}":      strconv.Itoa(a.cfg.NodeFailureThreshold),
		"{{server_agent_port}}":      strconv.Itoa(a.cfg.ServerAgentPort),
	}
	for old, newValue := range replacements {
		rendered = strings.ReplaceAll(rendered, old, newValue)
	}
	return runSSHScript(ctx, node.IP, node.Password, rendered)
}

func (a *masterApp) verifyNodeRelayAccess(ctx context.Context, node masterNode) error {
	if a.cfg.RelayHealthHost == "" || a.cfg.RelayHealthPort <= 0 {
		return nil
	}
	command := fmt.Sprintf("nc -z -w3 %s %d", shellQuote(a.cfg.RelayHealthHost), a.cfg.RelayHealthPort)
	output, err := runSSHCommand(ctx, node.IP, node.Password, command)
	if err != nil {
		return fmt.Errorf("relay health check failed on node %s: %w output=%s", node.ID, err, strings.TrimSpace(output))
	}
	return nil
}

func runSSHScript(ctx context.Context, host, password, script string) error {
	output, err := runSSHCommand(ctx, host, password, "bash -s", strings.NewReader(script))
	if err != nil {
		return fmt.Errorf("bootstrap failed: %w output=%s", err, strings.TrimSpace(output))
	}
	return nil
}

func runSSHCommand(ctx context.Context, host, password, command string, stdin ...io.Reader) (string, error) {
	deadline := time.Now().Add(10 * time.Minute)
	addr := net.JoinHostPort(host, "22")
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}
		config := &ssh.ClientConfig{
			User:            "root",
			Auth:            []ssh.AuthMethod{ssh.Password(password)},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			Timeout:         10 * time.Second,
		}
		client, err := ssh.Dial("tcp", addr, config)
		if err != nil {
			lastErr = err
			time.Sleep(5 * time.Second)
			continue
		}
		defer client.Close()
		session, err := client.NewSession()
		if err != nil {
			return "", err
		}
		defer session.Close()
		if len(stdin) > 0 && stdin[0] != nil {
			session.Stdin = stdin[0]
		}
		output, err := session.CombinedOutput(command)
		return string(output), err
	}
	return "", fmt.Errorf("ssh not ready: %w", lastErr)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func (a *masterApp) handleDeadNode(ctx context.Context, nodeID, reason string) error {
	var oldOrderID string
	a.mu.Lock()
	dead := a.findNodeLocked(nodeID)
	if dead == nil {
		a.mu.Unlock()
		return nil
	}
	dead.Status = "dead"
	dead.LastReportState = reason
	dead.LastReportAt = time.Now().UTC().Format(time.RFC3339)
	oldOrderID = dead.OrderID
	if dead.Role == "active" && a.state.ActiveID == nodeID {
		var promoted *masterNode
		for idx := range a.state.Nodes {
			if a.state.Nodes[idx].Role == "standby" && a.state.Nodes[idx].Status != "deleted" && a.state.Nodes[idx].Status != "deleting" {
				promoted = &a.state.Nodes[idx]
				break
			}
		}
		if promoted != nil {
			promoted.Role = "active"
			promoted.Status = "ready"
			a.state.ActiveID = promoted.ID
			a.state.Retired = append(a.state.Retired, nodeID)
			a.state.Generation++
		}
	}
	err := a.saveStateLocked()
	a.mu.Unlock()
	if err != nil {
		return err
	}
	if err := a.publishRouteMap(ctx); err != nil {
		log.Printf("[FURO-MASTER] publish after dead failed: %v", err)
	}
	if oldOrderID != "" {
		go func() {
			deleteCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			if err := a.caasify.deleteOrder(deleteCtx, oldOrderID); err != nil {
				log.Printf("[FURO-MASTER] delete old node order=%s failed: %v", oldOrderID, err)
			}
		}()
	}
	return a.ensureFleet(ctx)
}

type relayRouteMap struct {
	Namespace  string           `json:"namespace"`
	FleetID    string           `json:"fleet_id"`
	Generation int64            `json:"generation"`
	Active     *relayRouteSpec  `json:"active"`
	Standby    []relayRouteSpec `json:"standby"`
	Retired    []string         `json:"retired"`
	UpdatedAt  string           `json:"updated_at"`
}

type relayRouteSpec struct {
	ID           string `json:"id"`
	RelayURL     string `json:"relay_url"`
	ServerHost   string `json:"server_host"`
	ServerPort   int    `json:"server_port"`
	SessionCount int    `json:"session_count"`
}

func (a *masterApp) routeMapLocked() relayRouteMap {
	routeMap := relayRouteMap{
		Namespace:  a.state.Namespace,
		FleetID:    a.state.FleetID,
		Generation: a.state.Generation,
		Standby:    []relayRouteSpec{},
		Retired:    append([]string(nil), a.state.Retired...),
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	for _, node := range a.state.Nodes {
		if node.Status == "deleted" || node.Status == "deleting" || node.IP == "" {
			continue
		}
		spec := relayRouteSpec{
			ID:           node.ID,
			RelayURL:     a.cfg.RelayURL,
			ServerHost:   node.IP,
			ServerPort:   a.cfg.ServerAgentPort,
			SessionCount: 4,
		}
		if node.ID == a.state.ActiveID && node.Role == "active" {
			routeMap.Active = &spec
			continue
		}
		if node.Role == "standby" {
			routeMap.Standby = append(routeMap.Standby, spec)
		}
	}
	return routeMap
}

func (a *masterApp) publishRouteMap(ctx context.Context) error {
	a.mu.Lock()
	routeMap := a.routeMapLocked()
	a.mu.Unlock()
	endpoint, err := relayRouteMapURL(a.cfg.RelayURL, a.cfg.Namespace)
	if err != nil {
		return err
	}
	data, err := json.Marshal(routeMap)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-KEY", a.cfg.APIKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("relay route-map publish status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func relayRouteMapURL(base string, namespace ...string) (string, error) {
	parsed, err := urlParse(base)
	if err != nil {
		return "", err
	}
	values := parsed.Query()
	values.Set("action", "route-map")
	if len(namespace) > 0 && strings.TrimSpace(namespace[0]) != "" {
		values.Set("namespace", sanitizeMasterID(namespace[0]))
	}
	parsed.RawQuery = values.Encode()
	return parsed.String(), nil
}

func urlParse(value string) (*url.URL, error) { return url.Parse(value) }

type nodeReport struct {
	Namespace  string `json:"namespace"`
	NodeID     string `json:"node_id"`
	Role       string `json:"role"`
	Status     string `json:"status"`
	Reason     string `json:"reason"`
	ReportedAt string `json:"reported_at"`
}

func (a *masterApp) authenticate(r *http.Request) bool {
	return r.Header.Get("X-API-KEY") == a.cfg.APIKey || r.URL.Query().Get("key") == a.cfg.APIKey
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func (a *masterApp) runControlServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	mux.HandleFunc("/node/report", func(w http.ResponseWriter, r *http.Request) {
		if !a.authenticate(r) {
			http.Error(w, "unauthorized", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var report nodeReport
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&report); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if report.NodeID == "" {
			http.Error(w, "node_id is required", http.StatusBadRequest)
			return
		}
		reportNamespace := sanitizeMasterID(report.Namespace)
		if reportNamespace == "" {
			reportNamespace = "default"
		}
		if reportNamespace != a.cfg.Namespace {
			http.Error(w, fmt.Sprintf("wrong namespace %q", report.Namespace), http.StatusConflict)
			return
		}
		if report.Status == "dead" {
			if err := a.handleDeadNode(r.Context(), report.NodeID, report.Reason); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"ok": true, "action": "replace", "role": "dead"})
			return
		}
		role := report.Role
		a.mu.Lock()
		if node := a.findNodeLocked(report.NodeID); node != nil {
			node.LastReportAt = time.Now().UTC().Format(time.RFC3339)
			node.LastReportState = report.Status
			role = node.Role
			_ = a.saveStateLocked()
		}
		a.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "role": role})
	})
	log.Printf("[FURO-MASTER] control listener on %s", a.cfg.Listen)
	return http.ListenAndServe(a.cfg.Listen, mux)
}

func (a *masterApp) runAdminServer() error {
	if a.cfg.AdminListen == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		state := a.state
		a.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"service":    "furo-server-master",
			"version":    appVersion,
			"commit":     appCommit,
			"uptime_sec": int64(time.Since(a.startedAt).Seconds()),
			"state":      state,
		})
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	log.Printf("[FURO-MASTER] admin listener on %s", a.cfg.AdminListen)
	return http.ListenAndServe(a.cfg.AdminListen, mux)
}

func (a *masterApp) runPublishLoop() {
	ticker := time.NewTicker(time.Duration(a.cfg.PublishIntervalSeconds) * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		if err := a.publishRouteMap(ctx); err != nil {
			log.Printf("[FURO-MASTER] route-map publish failed: %v", err)
		}
		cancel()
	}
}

func setupMasterLogging(cfg masterConfigFile) (*os.File, error) {
	if strings.TrimSpace(cfg.LogFile) == "" {
		return nil, nil
	}
	path := resolvePath(filepath.Dir(*masterConfigPath), cfg.LogFile)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	log.SetOutput(file)
	log.Printf("[FURO-MASTER] log file: %s", path)
	return file, nil
}

func pingHTTP(ctx context.Context, target, apiKey string) error {
	parsed, err := url.Parse(target)
	if err != nil {
		return err
	}
	if parsed.Path == "" || parsed.Path == "/" {
		parsed.Path = "/healthz"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return err
	}
	if apiKey != "" {
		req.Header.Set("X-API-KEY", apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	fmt.Printf("ping ok: %s\n%s\n", parsed.String(), strings.TrimSpace(string(body)))
	return nil
}

type caasifyClient struct {
	token  string
	client *http.Client
}

func newCaasifyClient(token string) *caasifyClient {
	return &caasifyClient{token: token, client: &http.Client{Timeout: 30 * time.Second}}
}

type caasifyCreateRequest struct {
	ProductID int
	Note      string
	Template  string
	IPv4      int
	IPv6      int
}

type caasifyCreateResult struct {
	OrderID string
}

type caasifyOrderInfo struct {
	IP       string
	Password string
}

func (c *caasifyClient) createVPS(ctx context.Context, input caasifyCreateRequest) (caasifyCreateResult, error) {
	body := map[string]any{
		"product_id": input.ProductID,
		"note":       input.Note,
		"Template":   input.Template,
		"IPv4":       input.IPv4,
		"IPv6":       input.IPv6,
	}
	data, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api-panel.caasify.com/webhook/panel/createVPS", bytes.NewReader(data))
	if err != nil {
		return caasifyCreateResult{}, err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("token", c.token)
	resp, err := c.client.Do(req)
	if err != nil {
		return caasifyCreateResult{}, err
	}
	defer resp.Body.Close()
	data, _ = io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return caasifyCreateResult{}, fmt.Errorf("create vps status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return caasifyCreateResult{}, err
	}
	orderID := jsonString(decoded, "id")
	if nested, ok := decoded["data"].(map[string]any); ok && orderID == "" {
		orderID = jsonString(nested, "id")
	}
	if orderID == "" {
		return caasifyCreateResult{}, fmt.Errorf("create response missing order id: %s", strings.TrimSpace(string(data)))
	}
	return caasifyCreateResult{OrderID: orderID}, nil
}

func (c *caasifyClient) showOrder(ctx context.Context, orderID string) (caasifyOrderInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.caasify.com/api/orders/"+orderID+"/show", nil)
	if err != nil {
		return caasifyOrderInfo{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return caasifyOrderInfo{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return caasifyOrderInfo{}, fmt.Errorf("show order status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return caasifyOrderInfo{}, err
	}
	root, _ := decoded["data"].(map[string]any)
	password := jsonString(root, "secret")
	ip := firstIPv4(root)
	return caasifyOrderInfo{IP: ip, Password: password}, nil
}

func (c *caasifyClient) waitForOrderReady(ctx context.Context, orderID string) (caasifyOrderInfo, error) {
	var last caasifyOrderInfo
	for i := 0; i < 90; i++ {
		info, err := c.showOrder(ctx, orderID)
		if err == nil {
			last = info
			if info.IP != "" && info.Password != "" {
				return info, nil
			}
		}
		select {
		case <-ctx.Done():
			return caasifyOrderInfo{}, ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}
	return caasifyOrderInfo{}, fmt.Errorf("order not ready ip=%q password_set=%t", last.IP, last.Password != "")
}

func (c *caasifyClient) deleteOrder(ctx context.Context, orderID string) error {
	form := "order_id=" + urlQueryEscape(orderID) + "&button_id=10"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api-panel.caasify.com/webhook/panel/orderButton", strings.NewReader(form))
	if err != nil {
		return err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("content-type", "application/x-www-form-urlencoded")
	req.Header.Set("token", c.token)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("delete status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err == nil {
		if ok, _ := decoded["ok"].(bool); !ok {
			return fmt.Errorf("delete not accepted: %s", strings.TrimSpace(string(data)))
		}
	}
	return nil
}

func jsonString(raw map[string]any, key string) string {
	if raw == nil {
		return ""
	}
	switch value := raw[key].(type) {
	case string:
		return value
	case float64:
		return strconv.FormatInt(int64(value), 10)
	default:
		return ""
	}
}

func firstIPv4(root map[string]any) string {
	view, _ := root["view"].(map[string]any)
	refs, _ := view["references"].([]any)
	for _, value := range refs {
		item, _ := value.(map[string]any)
		ref, _ := item["reference"].(map[string]any)
		if jsonString(ref, "type") == "ipv4" {
			if ip := jsonString(item, "value"); ip != "" {
				return ip
			}
		}
	}
	return ""
}

func urlQueryEscape(value string) string {
	return url.QueryEscape(value)
}

func main() {
	flag.Parse()
	if *masterVersion {
		fmt.Printf("furo-server-master version=%s commit=%s built=%s\n", appVersion, appCommit, appBuildDate)
		return
	}
	cfg, err := loadMasterConfig(*masterConfigPath)
	if err != nil {
		log.Fatalf("failed to load master config: %v", err)
	}
	logFile, err := setupMasterLogging(cfg)
	if err != nil {
		log.Fatalf("failed to setup logging: %v", err)
	}
	if logFile != nil {
		defer logFile.Close()
	}
	if *pingClientURL != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := pingHTTP(ctx, *pingClientURL, cfg.APIKey); err != nil {
			log.Fatalf("ping client failed: %v", err)
		}
		return
	}
	app := newMasterApp(cfg)
	if err := app.loadState(); err != nil {
		log.Fatalf("failed to load state: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	if err := app.ensureFleet(ctx); err != nil {
		cancel()
		log.Fatalf("failed to ensure fleet: %v", err)
	}
	cancel()
	go app.runPublishLoop()
	if cfg.AdminListen != "" {
		go func() {
			if err := app.runAdminServer(); err != nil {
				log.Fatalf("admin server failed: %v", err)
			}
		}()
	}
	if err := app.runControlServer(); err != nil {
		log.Fatalf("control server failed: %v", err)
	}
}
