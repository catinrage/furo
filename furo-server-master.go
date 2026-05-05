package main

import (
	"bytes"
	"context"
	"crypto/ecdh"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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
	syncNodesFlag    = flag.Bool("sync-nodes", false, "Sync current master-rendered node config to all active and standby nodes, then restart them")
	publishFlag      = flag.Bool("publish", false, "Publish the current route-map to configured relay caches and exit")
	listNodesFlag    = flag.Bool("list-nodes", false, "List known master nodes and exit")
	resetFlag        = flag.Bool("reset", false, "Delete current nodes, reset local state, create a fresh fleet, publish, and exit")
)

var (
	appVersion   = "dev"
	appCommit    = "unknown"
	appBuildDate = "unknown"
)

type masterConfigFile struct {
	Namespace                string                   `json:"namespace"`
	APIKey                   string                   `json:"api_key"`
	Listen                   string                   `json:"listen"`
	PublicURL                string                   `json:"public_url"`
	AdminListen              string                   `json:"admin_listen"`
	ProviderBackend          string                   `json:"provider_backend"`
	CaasifyToken             string                   `json:"caasify_token"`
	DopraxAPIKey             string                   `json:"doprax_api_key"`
	DopraxUsername           string                   `json:"doprax_username"`
	DopraxPassword           string                   `json:"doprax_password"`
	DopraxBaseURL            string                   `json:"doprax_base_url"`
	DopraxProductVersionID   string                   `json:"doprax_product_version_id"`
	DopraxLocationOptionID   string                   `json:"doprax_location_option_id"`
	DopraxOSOptionID         string                   `json:"doprax_os_option_id"`
	DopraxAccessMethod       string                   `json:"doprax_access_method"`
	DopraxLoginRetryAttempts int                      `json:"doprax_login_retry_attempts"`
	DopraxLoginRetryDelaySec int                      `json:"doprax_login_retry_delay_seconds"`
	StaticEgress             masterStaticEgressConfig `json:"static_egress"`
	StaticGressAlias         masterStaticEgressConfig `json:"static_gress"`
	StateFile                string                   `json:"state_file"`
	LogFile                  string                   `json:"log_file"`
	RelayURL                 string                   `json:"relay_url"`
	Relays                   []masterRelayConfig      `json:"relays"`
	RelayHealthHost          string                   `json:"relay_health_host"`
	RelayHealthPort          int                      `json:"relay_health_port"`
	ServerAgentPort          int                      `json:"server_agent_port"`
	NodeMaxSessions          int                      `json:"node_max_sessions"`
	RouteSessionCount        int                      `json:"route_session_count"`
	BackupCount              int                      `json:"backup_count"`
	NodeCheckIntervalSeconds int                      `json:"node_check_interval_seconds"`
	NodeFailureThreshold     int                      `json:"node_failure_threshold"`
	PublishIntervalSeconds   int                      `json:"publish_interval_seconds"`
	ProductID                int                      `json:"product_id"`
	Template                 string                   `json:"template"`
	NotePrefix               string                   `json:"note_prefix"`
	IPv4                     int                      `json:"ipv4"`
	IPv6                     int                      `json:"ipv6"`
	ProviderPool             []caasifyProviderOption  `json:"provider_pool"`
	BootstrapScriptPath      string                   `json:"bootstrap_script_path"`
}

type caasifyProviderOption struct {
	Name      string `json:"name,omitempty"`
	Note      string `json:"note,omitempty"`
	ProductID int    `json:"product_id"`
	Template  string `json:"template"`
	IPv4      int    `json:"ipv4"`
	IPv6      int    `json:"ipv6"`
}

type masterRelayConfig struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type masterStaticEgressConfig struct {
	Enabled             bool   `json:"enabled"`
	Interface           string `json:"interface"`
	ListenPort          int    `json:"listen_port"`
	Subnet              string `json:"subnet"`
	MasterTunnelIP      string `json:"master_tunnel_ip"`
	NodeRouteTable      int    `json:"node_route_table"`
	NodeRoutePriority   int    `json:"node_route_priority"`
	AutoInstallPackages bool   `json:"auto_install_packages"`
}

func defaultMasterConfig() masterConfigFile {
	return masterConfigFile{
		Namespace:                "default",
		APIKey:                   "my_super_secret_123456789",
		Listen:                   "0.0.0.0:19082",
		AdminListen:              "127.0.0.1:19083",
		DopraxBaseURL:            "https://www.doprax.com",
		DopraxProductVersionID:   "4034ee51-9731-4663-95ee-a162dc47b119",
		DopraxLocationOptionID:   "ec5bc1aa-db5f-48a8-8d88-ef654a2a6dc8",
		DopraxOSOptionID:         "ec9473a2-caa6-4197-95a4-7ee7e2b59dba",
		DopraxAccessMethod:       "password",
		DopraxLoginRetryAttempts: 3,
		DopraxLoginRetryDelaySec: 5,
		StaticEgress: masterStaticEgressConfig{
			Interface:           "wg-furo",
			ListenPort:          51820,
			Subnet:              "10.66.0.0/24",
			MasterTunnelIP:      "10.66.0.1",
			NodeRouteTable:      51820,
			NodeRoutePriority:   1000,
			AutoInstallPackages: true,
		},
		StateFile:                "furo-server-master-state.json",
		RelayHealthHost:          "f2.ra1n.xyz",
		RelayHealthPort:          443,
		ServerAgentPort:          8443,
		NodeMaxSessions:          33,
		RouteSessionCount:        4,
		BackupCount:              1,
		NodeCheckIntervalSeconds: 10,
		NodeFailureThreshold:     3,
		PublishIntervalSeconds:   60,
		ProductID:                3776,
		Template:                 "ubuntu-24.04",
		NotePrefix:               "furo-server",
		IPv4:                     1,
		IPv6:                     1,
		ProviderPool:             defaultCaasifyProviderPool(),
		BootstrapScriptPath:      "scripts/bootstrap-server-node.sh.example",
	}
}

func defaultCaasifyProviderPool() []caasifyProviderOption {
	return []caasifyProviderOption{
		{Name: "germany", Note: "VPS-Germany", ProductID: 3776, Template: "ubuntu-24.04", IPv4: 1, IPv6: 1},
		{Name: "france", Note: "VPS-France-1C-1GB", ProductID: 103, Template: "2284", IPv4: 1, IPv6: 1},
		{Name: "spain", Note: "VPS-Spain-1C-1GB", ProductID: 106, Template: "2284", IPv4: 1, IPv6: 1},
		{Name: "netherlands", Note: "VPS-Netherlands-1C-1GB", ProductID: 97, Template: "2284", IPv4: 1, IPv6: 1},
		{Name: "sweden", Note: "VPS-Sweden-1C-1GB", ProductID: 110, Template: "2284", IPv4: 1, IPv6: 1},
		{Name: "finland", Note: "VPS-Finland-2C-4GB", ProductID: 3784, Template: "ubuntu-24.04", IPv4: 1, IPv6: 1},
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
	cfg.Relays = normalizeMasterRelays(cfg.RelayURL, cfg.Relays)
	if cfg.RelayURL == "" && len(cfg.Relays) > 0 {
		cfg.RelayURL = cfg.Relays[0].URL
	}
	if cfg.RelayURL == "" {
		return cfg, errors.New("relay_url or relays is required")
	}
	cfg.ProviderBackend = resolvedMasterProviderBackend(cfg)
	switch cfg.ProviderBackend {
	case "doprax":
		if cfg.DopraxAPIKey == "" {
			return cfg, errors.New("doprax_api_key is required when provider_backend=doprax")
		}
		if cfg.DopraxUsername == "" {
			return cfg, errors.New("doprax_username is required when provider_backend=doprax")
		}
		if cfg.DopraxPassword == "" {
			return cfg, errors.New("doprax_password is required when provider_backend=doprax")
		}
	case "caasify":
		if cfg.CaasifyToken == "" {
			return cfg, errors.New("caasify_token is required when provider_backend=caasify")
		}
	default:
		return cfg, fmt.Errorf("provider_backend must be doprax or caasify, got %q", cfg.ProviderBackend)
	}
	if cfg.Listen == "" {
		return cfg, errors.New("listen is required")
	}
	if cfg.PublicURL == "" {
		cfg.PublicURL = "http://" + cfg.Listen
	}
	if cfg.StateFile == "" || cfg.StateFile == "furo-server-master-state.json" || cfg.StateFile == "furo-server-master-default-state.json" {
		cfg.StateFile = fmt.Sprintf("furo-server-master-%s-state.json", cfg.Namespace)
	}
	if cfg.LogFile == "./furo-server-master.log" || cfg.LogFile == "furo-server-master.log" || cfg.LogFile == "./furo-server-master-default.log" {
		cfg.LogFile = fmt.Sprintf("./furo-server-master-%s.log", cfg.Namespace)
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
	if cfg.NodeMaxSessions <= 0 {
		cfg.NodeMaxSessions = 33
	}
	if cfg.RouteSessionCount <= 0 {
		cfg.RouteSessionCount = 4
	}
	if cfg.RelayHealthPort <= 0 {
		cfg.RelayHealthPort = 443
	}
	if cfg.DopraxBaseURL == "" {
		cfg.DopraxBaseURL = "https://www.doprax.com"
	}
	cfg.DopraxBaseURL = strings.TrimRight(cfg.DopraxBaseURL, "/")
	if cfg.DopraxProductVersionID == "" {
		cfg.DopraxProductVersionID = "4034ee51-9731-4663-95ee-a162dc47b119"
	}
	if cfg.DopraxLocationOptionID == "" {
		cfg.DopraxLocationOptionID = "ec5bc1aa-db5f-48a8-8d88-ef654a2a6dc8"
	}
	if cfg.DopraxOSOptionID == "" {
		cfg.DopraxOSOptionID = "ec9473a2-caa6-4197-95a4-7ee7e2b59dba"
	}
	if cfg.DopraxAccessMethod == "" {
		cfg.DopraxAccessMethod = "password"
	}
	if cfg.DopraxLoginRetryAttempts <= 0 {
		cfg.DopraxLoginRetryAttempts = 3
	}
	if cfg.DopraxLoginRetryDelaySec <= 0 {
		cfg.DopraxLoginRetryDelaySec = 5
	}
	cfg.StaticEgress = normalizeMasterStaticEgressConfig(cfg.StaticEgress, cfg.StaticGressAlias)
	if cfg.StaticEgress.Enabled {
		if err := validateMasterStaticEgressConfig(cfg.StaticEgress); err != nil {
			return cfg, err
		}
	}
	cfg.ProviderPool = normalizeCaasifyProviderPool(cfg)
	return cfg, nil
}

func normalizeMasterRelays(primaryURL string, relays []masterRelayConfig) []masterRelayConfig {
	normalized := make([]masterRelayConfig, 0, len(relays)+1)
	seenIDs := make(map[string]struct{})
	seenURLs := make(map[string]struct{})
	addRelay := func(relay masterRelayConfig, fallbackID string) {
		relay.URL = strings.TrimSpace(relay.URL)
		if relay.URL == "" {
			return
		}
		relay.ID = sanitizeMasterID(strings.TrimSpace(relay.ID))
		if relay.ID == "" || relay.ID == "fleet" {
			relay.ID = fallbackID
		}
		baseID := relay.ID
		for i := 2; ; i++ {
			if _, exists := seenIDs[relay.ID]; !exists {
				break
			}
			relay.ID = fmt.Sprintf("%s-%d", baseID, i)
		}
		if _, exists := seenURLs[relay.URL]; exists {
			return
		}
		seenIDs[relay.ID] = struct{}{}
		seenURLs[relay.URL] = struct{}{}
		normalized = append(normalized, relay)
	}
	addRelay(masterRelayConfig{ID: "primary", URL: primaryURL}, "primary")
	for idx, relay := range relays {
		addRelay(relay, fmt.Sprintf("relay-%d", idx+1))
	}
	return normalized
}

func normalizeMasterStaticEgressConfig(cfg, alias masterStaticEgressConfig) masterStaticEgressConfig {
	if !cfg.Enabled && alias.Enabled {
		cfg = alias
	}
	if cfg.Interface == "" {
		cfg.Interface = "wg-furo"
	}
	if cfg.ListenPort == 0 {
		cfg.ListenPort = 51820
	}
	if cfg.Subnet == "" {
		cfg.Subnet = "10.66.0.0/24"
	}
	if cfg.MasterTunnelIP == "" {
		cfg.MasterTunnelIP = "10.66.0.1"
	}
	if cfg.NodeRouteTable == 0 {
		cfg.NodeRouteTable = 51820
	}
	if cfg.NodeRoutePriority == 0 {
		cfg.NodeRoutePriority = 1000
	}
	return cfg
}

func validateMasterStaticEgressConfig(cfg masterStaticEgressConfig) error {
	if cfg.ListenPort < 1 || cfg.ListenPort > 65535 {
		return errors.New("static_egress.listen_port must be between 1 and 65535")
	}
	if cfg.NodeRouteTable <= 0 {
		return errors.New("static_egress.node_route_table must be > 0")
	}
	if cfg.NodeRoutePriority <= 0 {
		return errors.New("static_egress.node_route_priority must be > 0")
	}
	if sanitizeMasterID(cfg.Interface) != cfg.Interface {
		return errors.New("static_egress.interface contains unsupported characters")
	}
	ip := net.ParseIP(cfg.MasterTunnelIP).To4()
	if ip == nil {
		return errors.New("static_egress.master_tunnel_ip must be an IPv4 address")
	}
	_, subnet, err := net.ParseCIDR(cfg.Subnet)
	if err != nil {
		return fmt.Errorf("parse static_egress.subnet: %w", err)
	}
	if !subnet.Contains(ip) {
		return errors.New("static_egress.master_tunnel_ip must be inside static_egress.subnet")
	}
	return nil
}

func normalizeCaasifyProviderPool(cfg masterConfigFile) []caasifyProviderOption {
	pool := append([]caasifyProviderOption(nil), cfg.ProviderPool...)
	if len(pool) == 0 {
		pool = []caasifyProviderOption{{
			Name:      "configured",
			Note:      "configured top-level product",
			ProductID: cfg.ProductID,
			Template:  cfg.Template,
			IPv4:      cfg.IPv4,
			IPv6:      cfg.IPv6,
		}}
	}
	for idx := range pool {
		if pool[idx].ProductID <= 0 {
			pool[idx].ProductID = cfg.ProductID
		}
		if pool[idx].Template == "" {
			pool[idx].Template = cfg.Template
		}
		if pool[idx].IPv4 == 0 && pool[idx].IPv6 == 0 {
			pool[idx].IPv4 = cfg.IPv4
			pool[idx].IPv6 = cfg.IPv6
		}
		if pool[idx].IPv4 == 0 && pool[idx].IPv6 == 0 {
			pool[idx].IPv4 = 1
			pool[idx].IPv6 = 1
		}
		if pool[idx].Name == "" {
			pool[idx].Name = sanitizeMasterID(pool[idx].Note)
		}
		if pool[idx].Name == "" {
			pool[idx].Name = fmt.Sprintf("product-%d", pool[idx].ProductID)
		}
	}
	return pool
}

func resolvedMasterProviderBackend(cfg masterConfigFile) string {
	backend := strings.ToLower(strings.TrimSpace(cfg.ProviderBackend))
	if backend != "" {
		return backend
	}
	if cfg.DopraxAPIKey != "" || cfg.DopraxUsername != "" || cfg.DopraxPassword != "" {
		return "doprax"
	}
	if cfg.CaasifyToken != "" {
		return "caasify"
	}
	return "doprax"
}

func randomIndex(max int) int {
	if max <= 1 {
		return 0
	}
	value, err := crand.Int(crand.Reader, big.NewInt(int64(max)))
	if err == nil {
		return int(value.Int64())
	}
	return int(time.Now().UnixNano() % int64(max))
}

type masterNode struct {
	ID                  string `json:"id"`
	Namespace           string `json:"namespace"`
	OrderID             string `json:"order_id"`
	ServiceID           string `json:"service_id,omitempty"`
	IP                  string `json:"ip"`
	Role                string `json:"role"`
	Status              string `json:"status"`
	Password            string `json:"password,omitempty"`
	CreatedAt           string `json:"created_at"`
	UpdatedAt           string `json:"updated_at,omitempty"`
	ReadyAt             string `json:"ready_at,omitempty"`
	ProvisionAttempts   int    `json:"provision_attempts,omitempty"`
	LastError           string `json:"last_error,omitempty"`
	LastReportAt        string `json:"last_report_at,omitempty"`
	LastReportState     string `json:"last_report_state,omitempty"`
	EgressEnabled       bool   `json:"egress_enabled,omitempty"`
	EgressTunnelIP      string `json:"egress_tunnel_ip,omitempty"`
	WireGuardPrivateKey string `json:"wireguard_private_key,omitempty"`
	WireGuardPublicKey  string `json:"wireguard_public_key,omitempty"`
}

type masterState struct {
	Namespace    string                  `json:"namespace"`
	FleetID      string                  `json:"fleet_id"`
	Generation   int64                   `json:"generation"`
	ActiveID     string                  `json:"active_id"`
	Nodes        []masterNode            `json:"nodes"`
	Retired      []string                `json:"retired"`
	StaticEgress masterStaticEgressState `json:"static_egress,omitempty"`
	UpdatedAt    string                  `json:"updated_at"`
}

type masterStaticEgressState struct {
	PrivateKey string `json:"private_key,omitempty"`
	PublicKey  string `json:"public_key,omitempty"`
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
	cfg                       masterConfigFile
	statePath                 string
	mu                        sync.Mutex
	reconcileMu               sync.Mutex
	state                     masterState
	caasify                   caasifyAPI
	startedAt                 time.Time
	staticEgressAvailable     bool
	bootstrapNodeFunc         func(context.Context, masterNode) error
	verifyNodeRelayAccessFunc func(context.Context, masterNode) error
}

func newMasterApp(cfg masterConfigFile) *masterApp {
	cfg.ProviderBackend = resolvedMasterProviderBackend(cfg)
	app := &masterApp{
		cfg:       cfg,
		statePath: resolvePath(filepath.Dir(*masterConfigPath), cfg.StateFile),
		caasify:   masterProviderFromConfig(cfg),
		startedAt: time.Now(),
	}
	app.bootstrapNodeFunc = app.bootstrapNode
	app.verifyNodeRelayAccessFunc = app.verifyNodeRelayAccess
	return app
}

func masterProviderFromConfig(cfg masterConfigFile) caasifyAPI {
	if resolvedMasterProviderBackend(cfg) == "caasify" {
		return newCaasifyClient(cfg.CaasifyToken)
	}
	return newDopraxClient(cfg)
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
		log.Printf("[FURO-MASTER] state file missing; creating namespace=%s fleet_id=%s path=%s", a.state.Namespace, a.state.FleetID, a.statePath)
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
	if a.state.Nodes == nil {
		a.state.Nodes = []masterNode{}
	}
	if a.state.Retired == nil {
		a.state.Retired = []string{}
	}
	for idx := range a.state.Nodes {
		if a.state.Nodes[idx].Namespace == "" {
			a.state.Nodes[idx].Namespace = a.cfg.Namespace
		}
		if a.state.Nodes[idx].UpdatedAt == "" {
			a.state.Nodes[idx].UpdatedAt = a.state.UpdatedAt
		}
	}
	if a.state.Generation <= 0 {
		a.state.Generation = 1
	}
	log.Printf("[FURO-MASTER] state loaded namespace=%s fleet_id=%s generation=%d active_id=%s nodes=%d retired=%d path=%s", a.state.Namespace, a.state.FleetID, a.state.Generation, a.state.ActiveID, len(a.state.Nodes), len(a.state.Retired), a.statePath)
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

func (a *masterApp) ensureMasterStaticEgressStateLocked() error {
	if !a.cfg.StaticEgress.Enabled {
		return nil
	}
	if a.state.StaticEgress.PrivateKey != "" && a.state.StaticEgress.PublicKey != "" {
		return nil
	}
	privateKey, publicKey, err := generateWireGuardKeyPair()
	if err != nil {
		return err
	}
	a.state.StaticEgress.PrivateKey = privateKey
	a.state.StaticEgress.PublicKey = publicKey
	return nil
}

func (a *masterApp) ensureNodeStaticEgressLocked(node *masterNode) error {
	if !a.cfg.StaticEgress.Enabled || node == nil {
		return nil
	}
	if err := a.ensureMasterStaticEgressStateLocked(); err != nil {
		return err
	}
	node.EgressEnabled = true
	if node.EgressTunnelIP == "" {
		ip, err := a.nextNodeTunnelIPLocked()
		if err != nil {
			return err
		}
		node.EgressTunnelIP = ip
	}
	if node.WireGuardPrivateKey == "" || node.WireGuardPublicKey == "" {
		privateKey, publicKey, err := generateWireGuardKeyPair()
		if err != nil {
			return err
		}
		node.WireGuardPrivateKey = privateKey
		node.WireGuardPublicKey = publicKey
	}
	return nil
}

func (a *masterApp) nextNodeTunnelIPLocked() (string, error) {
	_, subnet, err := net.ParseCIDR(a.cfg.StaticEgress.Subnet)
	if err != nil {
		return "", err
	}
	base := subnet.IP.To4()
	if base == nil {
		return "", errors.New("static egress subnet must be IPv4")
	}
	used := map[string]struct{}{a.cfg.StaticEgress.MasterTunnelIP: {}}
	for _, node := range a.state.Nodes {
		if node.EgressTunnelIP != "" {
			used[node.EgressTunnelIP] = struct{}{}
		}
	}
	baseNum := binary.BigEndian.Uint32(base)
	for offset := uint32(2); offset < 254; offset++ {
		var raw [4]byte
		binary.BigEndian.PutUint32(raw[:], baseNum+offset)
		ip := net.IP(raw[:]).String()
		if !subnet.Contains(net.IP(raw[:])) {
			continue
		}
		if _, exists := used[ip]; !exists {
			return ip, nil
		}
	}
	return "", errors.New("no free static egress tunnel IPs")
}

func (a *masterApp) findNodeLocked(id string) *masterNode {
	for idx := range a.state.Nodes {
		if a.state.Nodes[idx].Namespace == a.cfg.Namespace && a.state.Nodes[idx].ID == id {
			return &a.state.Nodes[idx]
		}
	}
	return nil
}

func nodeUnavailable(status string) bool {
	switch status {
	case "dead", "deleted", "deleting", "delete_failed", "verify_failed":
		return true
	default:
		return false
	}
}

func nodeNeedsProvision(status string) bool {
	switch status {
	case "", "creating", "create_wait_failed", "created", "bootstrapping", "bootstrap_failed", "verifying":
		return true
	default:
		return false
	}
}

func (a *masterApp) retireNodeIDLocked(nodeID string) {
	for _, retiredID := range a.state.Retired {
		if retiredID == nodeID {
			return
		}
	}
	a.state.Retired = append(a.state.Retired, nodeID)
}

func (a *masterApp) activeNodeLocked() *masterNode {
	if a.state.ActiveID == "" {
		return nil
	}
	node := a.findNodeLocked(a.state.ActiveID)
	if node == nil || nodeUnavailable(node.Status) {
		return nil
	}
	return node
}

func (a *masterApp) standbyNodesLocked() []masterNode {
	var standby []masterNode
	for _, node := range a.state.Nodes {
		if node.Namespace == a.cfg.Namespace && node.Role == "standby" && !nodeUnavailable(node.Status) {
			standby = append(standby, node)
		}
	}
	return standby
}

func (a *masterApp) activeReadyLocked() bool {
	node := a.activeNodeLocked()
	return node != nil && node.Status == "ready"
}

func (a *masterApp) pendingProvisionNodeIDsLocked() []string {
	var ids []string
	for _, node := range a.state.Nodes {
		if node.Namespace != a.cfg.Namespace || nodeUnavailable(node.Status) {
			continue
		}
		if nodeNeedsProvision(node.Status) {
			ids = append(ids, node.ID)
		}
	}
	return ids
}

func (a *masterApp) nextNodeID(role string) string {
	return fmt.Sprintf("%s-%s-%s-%d", a.cfg.NotePrefix, a.cfg.Namespace, role, time.Now().UnixNano())
}

func (a *masterApp) selectCaasifyProvider() caasifyProviderOption {
	pool := a.cfg.ProviderPool
	if len(pool) == 0 {
		pool = normalizeCaasifyProviderPool(a.cfg)
	}
	return pool[randomIndex(len(pool))]
}

func (a *masterApp) parseManagedNodeID(value string) (string, string, bool) {
	value = strings.TrimSpace(value)
	activePrefix := fmt.Sprintf("%s-%s-active-", a.cfg.NotePrefix, a.cfg.Namespace)
	standbyPrefix := fmt.Sprintf("%s-%s-standby-", a.cfg.NotePrefix, a.cfg.Namespace)
	switch {
	case strings.HasPrefix(value, activePrefix):
		return value, "active", true
	case strings.HasPrefix(value, standbyPrefix):
		return value, "standby", true
	default:
		return "", "", false
	}
}

func nodeTimestamp(id string) int64 {
	parts := strings.Split(id, "-")
	if len(parts) == 0 {
		return 0
	}
	value, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	return value
}

func (a *masterApp) importProviderNodes(ctx context.Context) error {
	orders, err := a.caasify.listOrders(ctx)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	knownOrders := make(map[string]struct{}, len(a.state.Nodes))
	knownIDs := make(map[string]struct{}, len(a.state.Nodes))
	providerOrders := make(map[string]struct{}, len(orders))
	for _, node := range a.state.Nodes {
		if node.OrderID != "" {
			knownOrders[node.OrderID] = struct{}{}
		}
		knownIDs[node.ID] = struct{}{}
	}
	for _, order := range orders {
		if order.OrderID != "" {
			providerOrders[order.OrderID] = struct{}{}
		}
	}

	imported := 0
	changed := false
	now := time.Now().UTC().Format(time.RFC3339)
	for _, order := range orders {
		id, role, ok := a.parseManagedNodeID(order.Note)
		if !ok {
			continue
		}
		switch strings.ToLower(order.Status) {
		case "passive", "cancelled", "canceled", "deleted", "terminated":
			log.Printf("[FURO-MASTER] provider node skipped id=%s order=%s status=%s reason=inactive_order", id, order.OrderID, order.Status)
			continue
		}
		if _, ok := knownOrders[order.OrderID]; ok {
			continue
		}
		if _, ok := knownIDs[id]; ok {
			continue
		}
		node := masterNode{
			ID:        id,
			Namespace: a.cfg.Namespace,
			OrderID:   order.OrderID,
			ServiceID: order.ServiceID,
			IP:        order.IP,
			Role:      role,
			Status:    "created",
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := a.ensureNodeStaticEgressLocked(&node); err != nil {
			return err
		}
		a.state.Nodes = append(a.state.Nodes, node)
		knownOrders[order.OrderID] = struct{}{}
		knownIDs[id] = struct{}{}
		imported++
		changed = true
		log.Printf("[FURO-MASTER] imported provider node id=%s role=%s order=%s ip=%s provider_status=%s reason=matching_namespace_note", node.ID, node.Role, node.OrderID, node.IP, order.Status)
	}

	for idx := range a.state.Nodes {
		node := &a.state.Nodes[idx]
		if node.Namespace != a.cfg.Namespace || node.OrderID == "" || nodeUnavailable(node.Status) {
			continue
		}
		if _, exists := providerOrders[node.OrderID]; exists {
			continue
		}
		node.Status = "deleted"
		node.LastError = "provider node missing from list"
		node.UpdatedAt = now
		a.retireNodeIDLocked(node.ID)
		if a.state.ActiveID == node.ID {
			a.state.ActiveID = ""
		}
		changed = true
		log.Printf("[FURO-MASTER] marked missing provider node deleted id=%s role=%s order=%s ip=%s reason=not_found_in_provider_list", node.ID, node.Role, node.OrderID, node.IP)
	}

	if a.activeNodeLocked() == nil {
		bestID := ""
		var bestTS int64
		for _, node := range a.state.Nodes {
			if node.Namespace != a.cfg.Namespace || node.Role != "active" || nodeUnavailable(node.Status) {
				continue
			}
			if ts := nodeTimestamp(node.ID); bestID == "" || ts > bestTS {
				bestID = node.ID
				bestTS = ts
			}
		}
		if bestID != "" {
			a.state.ActiveID = bestID
			changed = true
			log.Printf("[FURO-MASTER] selected imported active id=%s reason=no_active_in_state", bestID)
		}
	}

	if !changed {
		log.Printf("[FURO-MASTER] provider import complete imported=0 missing=0 matching_namespace=%s", a.cfg.Namespace)
		return nil
	}
	a.state.Generation++
	if err := a.saveStateLocked(); err != nil {
		return err
	}
	log.Printf("[FURO-MASTER] provider import saved imported=%d generation=%d", imported, a.state.Generation)
	return nil
}

func (a *masterApp) pruneSurplusNodes() {
	a.mu.Lock()
	var deleteNodes []masterNode
	now := time.Now().UTC().Format(time.RFC3339)

	active := a.activeNodeLocked()
	if active == nil {
		bestID := ""
		var bestTS int64
		for _, node := range a.state.Nodes {
			if node.Namespace != a.cfg.Namespace || node.Role != "active" || nodeUnavailable(node.Status) {
				continue
			}
			if ts := nodeTimestamp(node.ID); bestID == "" || ts > bestTS {
				bestID = node.ID
				bestTS = ts
			}
		}
		if bestID != "" {
			a.state.ActiveID = bestID
			active = a.findNodeLocked(bestID)
			a.state.Generation++
			log.Printf("[FURO-MASTER] selected active id=%s reason=surplus_prune_no_active", bestID)
		}
	}

	for idx := range a.state.Nodes {
		node := &a.state.Nodes[idx]
		if node.Namespace != a.cfg.Namespace || node.Role != "active" || nodeUnavailable(node.Status) {
			continue
		}
		if active != nil && node.ID == active.ID {
			continue
		}
		node.Status = "deleting"
		node.LastError = "surplus active node; another active is selected"
		node.UpdatedAt = now
		a.retireNodeIDLocked(node.ID)
		deleteNodes = append(deleteNodes, *node)
		a.state.Generation++
		log.Printf("[FURO-MASTER] marked surplus active for deletion id=%s order=%s ip=%s selected_active=%s", node.ID, node.OrderID, node.IP, a.state.ActiveID)
	}

	var standbys []*masterNode
	for idx := range a.state.Nodes {
		node := &a.state.Nodes[idx]
		if node.Namespace == a.cfg.Namespace && node.Role == "standby" && !nodeUnavailable(node.Status) {
			standbys = append(standbys, node)
		}
	}
	sort.Slice(standbys, func(i, j int) bool {
		return nodeTimestamp(standbys[i].ID) > nodeTimestamp(standbys[j].ID)
	})
	for idx, node := range standbys {
		if idx < a.cfg.BackupCount {
			continue
		}
		node.Status = "deleting"
		node.LastError = "surplus standby node above backup_count"
		node.UpdatedAt = now
		a.retireNodeIDLocked(node.ID)
		deleteNodes = append(deleteNodes, *node)
		a.state.Generation++
		log.Printf("[FURO-MASTER] marked surplus standby for deletion id=%s order=%s ip=%s backup_count=%d", node.ID, node.OrderID, node.IP, a.cfg.BackupCount)
	}

	if len(deleteNodes) > 0 {
		if err := a.saveStateLocked(); err != nil {
			log.Printf("[FURO-MASTER] save surplus prune failed err=%v", err)
		}
	}
	a.mu.Unlock()

	for _, node := range deleteNodes {
		go a.deleteNodeOrder(context.Background(), node, "surplus")
	}
}

func (a *masterApp) pruneVerifyFailedStandbys() {
	a.mu.Lock()
	var deleteNodes []masterNode
	now := time.Now().UTC().Format(time.RFC3339)
	for idx := range a.state.Nodes {
		node := &a.state.Nodes[idx]
		if node.Namespace != a.cfg.Namespace || node.Role != "standby" || node.Status != "verify_failed" {
			continue
		}
		node.Status = "deleting"
		if node.LastError == "" {
			node.LastError = "standby relay verification failed"
		}
		node.UpdatedAt = now
		a.retireNodeIDLocked(node.ID)
		deleteNodes = append(deleteNodes, *node)
		a.state.Generation++
		log.Printf("[FURO-MASTER] marked verify-failed standby for deletion id=%s order=%s ip=%s err=%s", node.ID, node.OrderID, node.IP, node.LastError)
	}
	if len(deleteNodes) > 0 {
		if err := a.saveStateLocked(); err != nil {
			log.Printf("[FURO-MASTER] save verify-failed prune state failed err=%v", err)
		}
	}
	a.mu.Unlock()

	for _, node := range deleteNodes {
		go a.deleteNodeOrder(context.Background(), node, "standby_verify_failed")
	}
}

func (a *masterApp) deleteNodeOrder(ctx context.Context, node masterNode, reason string) {
	if node.OrderID == "" {
		log.Printf("[FURO-MASTER] delete skipped id=%s reason=%s err=missing_order_id", node.ID, reason)
		return
	}
	log.Printf("[FURO-MASTER] deleting node id=%s role=%s order=%s ip=%s reason=%s", node.ID, node.Role, node.OrderID, node.IP, reason)
	deleteCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	err := a.caasify.deleteOrder(deleteCtx, node.OrderID)
	a.mu.Lock()
	defer a.mu.Unlock()
	current := a.findNodeLocked(node.ID)
	if current == nil {
		if err != nil {
			log.Printf("[FURO-MASTER] delete finished for missing node id=%s order=%s err=%v", node.ID, node.OrderID, err)
		}
		return
	}
	current.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	if err != nil {
		current.Status = "delete_failed"
		current.LastError = err.Error()
		log.Printf("[FURO-MASTER] delete node failed id=%s order=%s err=%v", node.ID, node.OrderID, err)
	} else {
		current.Status = "deleted"
		current.LastError = ""
		log.Printf("[FURO-MASTER] delete node accepted id=%s order=%s reason=%s", node.ID, node.OrderID, reason)
	}
	if saveErr := a.saveStateLocked(); saveErr != nil {
		log.Printf("[FURO-MASTER] save delete state failed id=%s err=%v", node.ID, saveErr)
	}
	if err == nil && a.cfg.StaticEgress.Enabled {
		go func() {
			if refreshErr := a.configureMasterStaticEgress(context.Background()); refreshErr != nil {
				log.Printf("[FURO-MASTER] static egress refresh after delete failed id=%s err=%v", node.ID, refreshErr)
			}
		}()
	}
}

func (a *masterApp) ensureFleet(ctx context.Context) error {
	a.reconcileMu.Lock()
	defer a.reconcileMu.Unlock()

	log.Printf("[FURO-MASTER] reconcile started namespace=%s backup_count=%d", a.cfg.Namespace, a.cfg.BackupCount)
	if err := a.importProviderNodes(ctx); err != nil {
		log.Printf("[FURO-MASTER] provider import failed err=%v", err)
	}
	if a.cfg.StaticEgress.Enabled {
		if err := a.configureMasterStaticEgress(ctx); err != nil {
			log.Printf("[FURO-MASTER] static egress reconcile refresh failed err=%v", err)
		}
	}
	a.pruneVerifyFailedStandbys()
	a.pruneSurplusNodes()

	a.mu.Lock()
	pendingIDs := a.pendingProvisionNodeIDsLocked()
	needActive := a.activeNodeLocked() == nil
	activeReady := a.activeReadyLocked()
	standbyNeed := 0
	if activeReady {
		standbyNeed = a.cfg.BackupCount - len(a.standbyNodesLocked())
	}
	log.Printf("[FURO-MASTER] reconcile state active_needed=%t active_ready=%t standby_needed=%d pending=%d generation=%d active_id=%s nodes=%d", needActive, activeReady, standbyNeed, len(pendingIDs), a.state.Generation, a.state.ActiveID, len(a.state.Nodes))
	a.mu.Unlock()

	var reconcileErr error
	for _, nodeID := range pendingIDs {
		if err := a.provisionNode(ctx, nodeID); err != nil {
			reconcileErr = err
			log.Printf("[FURO-MASTER] provision pending node failed id=%s err=%v", nodeID, err)
		}
	}

	a.mu.Lock()
	needActive = a.activeNodeLocked() == nil
	activeReady = a.activeReadyLocked()
	standbyNeed = 0
	if activeReady {
		standbyNeed = a.cfg.BackupCount - len(a.standbyNodesLocked())
	}
	a.mu.Unlock()
	if needActive {
		log.Printf("[FURO-MASTER] creating active reason=no_active_node")
		if _, err := a.createAndBootstrapNode(ctx, "active"); err != nil {
			reconcileErr = err
			log.Printf("[FURO-MASTER] create active failed err=%v", err)
		} else {
			a.mu.Lock()
			activeReady = a.activeReadyLocked()
			if activeReady {
				standbyNeed = a.cfg.BackupCount - len(a.standbyNodesLocked())
			}
			a.mu.Unlock()
		}
	}
	if !activeReady {
		log.Printf("[FURO-MASTER] standby creation skipped reason=active_not_ready")
	}
	for standbyNeed > 0 {
		log.Printf("[FURO-MASTER] creating standby reason=below_backup_count remaining=%d", standbyNeed)
		if _, err := a.createAndBootstrapNode(ctx, "standby"); err != nil {
			reconcileErr = err
			log.Printf("[FURO-MASTER] create standby failed err=%v", err)
			break
		}
		standbyNeed--
	}
	if err := a.publishRouteMap(ctx); err != nil {
		reconcileErr = err
		log.Printf("[FURO-MASTER] reconcile publish failed err=%v", err)
	}
	if reconcileErr != nil {
		log.Printf("[FURO-MASTER] reconcile finished with error err=%v", reconcileErr)
		return reconcileErr
	}
	log.Printf("[FURO-MASTER] reconcile finished ok")
	return nil
}

func (a *masterApp) createAndBootstrapNode(ctx context.Context, role string) (masterNode, error) {
	nodeID := a.nextNodeID(role)
	input := caasifyCreateRequest{Note: nodeID}
	providerName := a.cfg.ProviderBackend
	if a.cfg.ProviderBackend == "caasify" {
		provider := a.selectCaasifyProvider()
		providerName = provider.Name
		input.ProductID = provider.ProductID
		input.Template = provider.Template
		input.IPv4 = provider.IPv4
		input.IPv6 = provider.IPv6
		log.Printf("[FURO-MASTER] caasify provider selected id=%s role=%s provider=%s note=%s product=%d template=%s ipv4=%d ipv6=%d pool_size=%d", nodeID, role, provider.Name, provider.Note, provider.ProductID, provider.Template, provider.IPv4, provider.IPv6, len(a.cfg.ProviderPool))
		log.Printf("[FURO-MASTER] caasify create requested id=%s role=%s provider=%s product=%d template=%s ipv4=%d ipv6=%d", nodeID, role, provider.Name, provider.ProductID, provider.Template, provider.IPv4, provider.IPv6)
	} else {
		log.Printf("[FURO-MASTER] doprax create requested id=%s role=%s product_version=%s location_option=%s os_option=%s access_method=%s", nodeID, role, a.cfg.DopraxProductVersionID, a.cfg.DopraxLocationOptionID, a.cfg.DopraxOSOptionID, a.cfg.DopraxAccessMethod)
	}
	order, err := a.caasify.createVPS(ctx, input)
	if err != nil {
		return masterNode{}, err
	}
	if order.OrderID == "" {
		return masterNode{}, fmt.Errorf("provider create accepted but returned empty order id")
	}
	log.Printf("[FURO-MASTER] provider create accepted backend=%s id=%s role=%s order=%s provider=%s ip=%s password_set=%t", a.cfg.ProviderBackend, nodeID, role, order.OrderID, providerName, order.IP, order.Password != "")
	now := time.Now().UTC().Format(time.RFC3339)
	status := "creating"
	if order.IP != "" && order.Password != "" {
		status = "created"
	}
	node := masterNode{
		ID:        nodeID,
		Namespace: a.cfg.Namespace,
		OrderID:   order.OrderID,
		ServiceID: order.ServiceID,
		IP:        order.IP,
		Role:      role,
		Status:    status,
		Password:  order.Password,
		CreatedAt: now,
		UpdatedAt: now,
	}
	a.mu.Lock()
	if err := a.ensureNodeStaticEgressLocked(&node); err != nil {
		a.mu.Unlock()
		return masterNode{}, err
	}
	if role == "active" {
		a.state.ActiveID = node.ID
	}
	a.state.Nodes = append(a.state.Nodes, node)
	err = a.saveStateLocked()
	a.mu.Unlock()
	if err != nil {
		return masterNode{}, err
	}
	log.Printf("[FURO-MASTER] node saved before provisioning id=%s role=%s order=%s status=%s", node.ID, node.Role, node.OrderID, node.Status)
	if a.cfg.StaticEgress.Enabled {
		if err := a.configureMasterStaticEgress(ctx); err != nil {
			log.Printf("[FURO-MASTER] static egress refresh failed before node bootstrap id=%s err=%v", node.ID, err)
		}
	}
	if err := a.provisionNode(ctx, node.ID); err != nil {
		return node, err
	}
	a.mu.Lock()
	ready := a.findNodeLocked(node.ID)
	if ready != nil {
		node = *ready
	}
	a.mu.Unlock()
	return node, nil
}

func (a *masterApp) provisionNode(ctx context.Context, nodeID string) error {
	a.mu.Lock()
	node := a.findNodeLocked(nodeID)
	if node == nil {
		a.mu.Unlock()
		return fmt.Errorf("node %s not found", nodeID)
	}
	if node.Status == "ready" || nodeUnavailable(node.Status) {
		log.Printf("[FURO-MASTER] provision skipped id=%s role=%s status=%s", node.ID, node.Role, node.Status)
		a.mu.Unlock()
		return nil
	}
	node.ProvisionAttempts++
	node.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	node.LastError = ""
	local := *node
	if err := a.saveStateLocked(); err != nil {
		a.mu.Unlock()
		return err
	}
	a.mu.Unlock()

	log.Printf("[FURO-MASTER] provision started id=%s role=%s order=%s status=%s attempt=%d ip=%s", local.ID, local.Role, local.OrderID, local.Status, local.ProvisionAttempts, local.IP)
	if local.IP == "" || local.Password == "" {
		log.Printf("[FURO-MASTER] waiting for provider node id=%s order=%s backend=%s reason=missing_ip_or_password ip_set=%t password_set=%t", local.ID, local.OrderID, a.cfg.ProviderBackend, local.IP != "", local.Password != "")
		info, err := a.caasify.waitForOrderReady(ctx, local.OrderID)
		if err != nil {
			a.markNodeProvisionError(local.ID, "create_wait_failed", err)
			return err
		}
		a.mu.Lock()
		node = a.findNodeLocked(local.ID)
		if node == nil {
			a.mu.Unlock()
			return fmt.Errorf("node %s disappeared while waiting for order", local.ID)
		}
		node.IP = info.IP
		node.Password = info.Password
		if info.ServiceID != "" {
			node.ServiceID = info.ServiceID
		}
		node.Status = "created"
		node.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		local = *node
		err = a.saveStateLocked()
		a.mu.Unlock()
		if err != nil {
			return err
		}
		log.Printf("[FURO-MASTER] provider node ready id=%s order=%s backend=%s ip=%s password_set=%t", local.ID, local.OrderID, a.cfg.ProviderBackend, local.IP, local.Password != "")
	}

	a.mu.Lock()
	node = a.findNodeLocked(local.ID)
	if node == nil {
		a.mu.Unlock()
		return fmt.Errorf("node %s disappeared before bootstrap", local.ID)
	}
	node.Status = "bootstrapping"
	node.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	local = *node
	err := a.saveStateLocked()
	a.mu.Unlock()
	if err != nil {
		return err
	}
	log.Printf("[FURO-MASTER] bootstrap started id=%s role=%s ip=%s script=%s", local.ID, local.Role, local.IP, a.cfg.BootstrapScriptPath)
	if err := a.bootstrapNodeFunc(ctx, local); err != nil {
		a.markNodeProvisionError(local.ID, "bootstrap_failed", err)
		return err
	}
	log.Printf("[FURO-MASTER] bootstrap finished id=%s role=%s ip=%s", local.ID, local.Role, local.IP)

	if local.Role == "standby" {
		a.mu.Lock()
		node = a.findNodeLocked(local.ID)
		if node == nil {
			a.mu.Unlock()
			return fmt.Errorf("node %s disappeared before relay verification", local.ID)
		}
		node.Status = "verifying"
		node.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		local = *node
		err = a.saveStateLocked()
		a.mu.Unlock()
		if err != nil {
			return err
		}
		log.Printf("[FURO-MASTER] standby relay health check started id=%s ip=%s target=%s:%d", local.ID, local.IP, a.cfg.RelayHealthHost, a.cfg.RelayHealthPort)
		if err := a.verifyNodeRelayAccessFunc(ctx, local); err != nil {
			a.discardStandbyNodeForReplacement(local.ID, "standby_relay_health_failed", err)
			return err
		}
		log.Printf("[FURO-MASTER] standby relay health check ok id=%s ip=%s", local.ID, local.IP)
	}

	a.mu.Lock()
	node = a.findNodeLocked(local.ID)
	if node == nil {
		a.mu.Unlock()
		return fmt.Errorf("node %s disappeared before ready", local.ID)
	}
	wasReady := node.Status == "ready"
	node.Status = "ready"
	node.LastError = ""
	node.ReadyAt = time.Now().UTC().Format(time.RFC3339)
	node.UpdatedAt = node.ReadyAt
	if node.Role == "active" && a.state.ActiveID == "" {
		a.state.ActiveID = node.ID
	}
	if !wasReady {
		a.state.Generation++
	}
	err = a.saveStateLocked()
	ready := *node
	generation := a.state.Generation
	a.mu.Unlock()
	if err != nil {
		return err
	}
	log.Printf("[FURO-MASTER] node ready id=%s role=%s ip=%s generation=%d", ready.ID, ready.Role, ready.IP, generation)
	return nil
}

func (a *masterApp) markNodeProvisionError(nodeID, status string, cause error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	node := a.findNodeLocked(nodeID)
	if node == nil {
		log.Printf("[FURO-MASTER] provision error for missing node id=%s status=%s err=%v", nodeID, status, cause)
		return
	}
	if nodeUnavailable(node.Status) {
		log.Printf("[FURO-MASTER] provision error ignored for unavailable node id=%s current_status=%s attempted_status=%s err=%v", nodeID, node.Status, status, cause)
		return
	}
	node.Status = status
	node.LastError = cause.Error()
	node.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	if err := a.saveStateLocked(); err != nil {
		log.Printf("[FURO-MASTER] save provision error failed id=%s status=%s err=%v save_err=%v", nodeID, status, cause, err)
		return
	}
	log.Printf("[FURO-MASTER] node provision failed id=%s role=%s order=%s ip=%s status=%s attempt=%d err=%v", node.ID, node.Role, node.OrderID, node.IP, node.Status, node.ProvisionAttempts, cause)
}

func (a *masterApp) discardStandbyNodeForReplacement(nodeID, reason string, cause error) {
	a.mu.Lock()
	node := a.findNodeLocked(nodeID)
	if node == nil {
		a.mu.Unlock()
		log.Printf("[FURO-MASTER] discard standby skipped id=%s reason=%s err=node_not_found cause=%v", nodeID, reason, cause)
		return
	}
	if node.Role != "standby" {
		a.mu.Unlock()
		log.Printf("[FURO-MASTER] discard standby skipped id=%s role=%s reason=%s cause=%v", nodeID, node.Role, reason, cause)
		return
	}
	now := time.Now().UTC().Format(time.RFC3339)
	node.Status = "deleting"
	node.LastError = fmt.Sprintf("%s: %v", reason, cause)
	node.UpdatedAt = now
	a.retireNodeIDLocked(node.ID)
	a.state.Generation++
	local := *node
	generation := a.state.Generation
	err := a.saveStateLocked()
	a.mu.Unlock()
	if err != nil {
		log.Printf("[FURO-MASTER] save standby discard state failed id=%s err=%v", nodeID, err)
		return
	}
	log.Printf("[FURO-MASTER] standby discarded id=%s order=%s ip=%s reason=%s generation=%d", local.ID, local.OrderID, local.IP, reason, generation)
	go a.deleteNodeOrder(context.Background(), local, reason)
}

func (a *masterApp) bootstrapNode(ctx context.Context, node masterNode) error {
	scriptPath := resolvePath(filepath.Dir(*masterConfigPath), a.cfg.BootstrapScriptPath)
	data, err := os.ReadFile(scriptPath)
	if err != nil {
		return err
	}
	rendered, err := a.renderBootstrapScript(string(data), node)
	if err != nil {
		return err
	}
	return runSSHScript(ctx, node.IP, node.Password, rendered)
}

func (a *masterApp) renderBootstrapScript(template string, node masterNode) (string, error) {
	staticEnabled := a.cfg.StaticEgress.Enabled && a.staticEgressAvailable && node.EgressEnabled && node.EgressTunnelIP != "" && node.WireGuardPrivateKey != ""
	masterHost, err := a.staticEgressEndpointHost()
	if staticEnabled && err != nil {
		return "", err
	}
	prefix, err := cidrPrefix(a.cfg.StaticEgress.Subnet)
	if err != nil {
		prefix = 24
	}
	replacements := map[string]string{
		"{{api_key}}":                 a.cfg.APIKey,
		"{{namespace}}":               a.cfg.Namespace,
		"{{node_id}}":                 node.ID,
		"{{node_role}}":               node.Role,
		"{{master_url}}":              a.cfg.PublicURL,
		"{{relay_health_host}}":       a.cfg.RelayHealthHost,
		"{{relay_health_port}}":       strconv.Itoa(a.cfg.RelayHealthPort),
		"{{check_interval_seconds}}":  strconv.Itoa(a.cfg.NodeCheckIntervalSeconds),
		"{{failure_threshold}}":       strconv.Itoa(a.cfg.NodeFailureThreshold),
		"{{server_agent_port}}":       strconv.Itoa(a.cfg.ServerAgentPort),
		"{{node_max_sessions}}":       strconv.Itoa(a.cfg.NodeMaxSessions),
		"{{static_egress_enabled}}":   strconv.FormatBool(staticEnabled),
		"{{wg_interface}}":            a.cfg.StaticEgress.Interface,
		"{{wg_node_private_key}}":     node.WireGuardPrivateKey,
		"{{wg_node_tunnel_ip}}":       node.EgressTunnelIP,
		"{{wg_node_tunnel_prefix}}":   strconv.Itoa(prefix),
		"{{wg_master_public_key}}":    a.state.StaticEgress.PublicKey,
		"{{wg_master_endpoint_host}}": masterHost,
		"{{wg_master_listen_port}}":   strconv.Itoa(a.cfg.StaticEgress.ListenPort),
		"{{wg_node_route_table}}":     strconv.Itoa(a.cfg.StaticEgress.NodeRouteTable),
		"{{wg_node_route_priority}}":  strconv.Itoa(a.cfg.StaticEgress.NodeRoutePriority),
	}
	rendered := template
	for old, newValue := range replacements {
		rendered = strings.ReplaceAll(rendered, old, newValue)
	}
	return rendered, nil
}

func (a *masterApp) renderServerConfigJSON(node masterNode) (string, error) {
	staticEnabled := a.cfg.StaticEgress.Enabled && a.staticEgressAvailable && node.EgressEnabled && node.EgressTunnelIP != "" && node.WireGuardPrivateKey != ""
	cfg := map[string]any{
		"api_key":           a.cfg.APIKey,
		"agent_listen":      fmt.Sprintf("0.0.0.0:%d", a.cfg.ServerAgentPort),
		"admin_listen":      "127.0.0.1:19081",
		"dial_timeout":      "10s",
		"keepalive":         "30s",
		"write_timeout":     "30s",
		"frame_min_size":    32768,
		"frame_mid_size":    65536,
		"frame_max_size":    131072,
		"max_pending_bytes": 4194304,
		"max_sessions":      a.cfg.NodeMaxSessions,
		"log_file":          "server.log",
		"egress": map[string]any{
			"enabled": staticEnabled,
			"bind_ip": node.EgressTunnelIP,
		},
		"node": map[string]any{
			"enabled":                true,
			"namespace":              a.cfg.Namespace,
			"id":                     node.ID,
			"master_url":             a.cfg.PublicURL,
			"role":                   node.Role,
			"relay_health_host":      a.cfg.RelayHealthHost,
			"relay_health_port":      a.cfg.RelayHealthPort,
			"check_interval_seconds": a.cfg.NodeCheckIntervalSeconds,
			"failure_threshold":      a.cfg.NodeFailureThreshold,
		},
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return "", err
	}
	return string(append(data, '\n')), nil
}

func (a *masterApp) renderNodeSyncScript(node masterNode) (string, error) {
	serverConfig, err := a.renderServerConfigJSON(node)
	if err != nil {
		return "", err
	}
	staticEnabled := a.cfg.StaticEgress.Enabled && a.staticEgressAvailable && node.EgressEnabled && node.EgressTunnelIP != "" && node.WireGuardPrivateKey != ""
	masterHost, err := a.staticEgressEndpointHost()
	if staticEnabled && err != nil {
		return "", err
	}
	prefix, err := cidrPrefix(a.cfg.StaticEgress.Subnet)
	if err != nil {
		prefix = 24
	}
	var b strings.Builder
	fmt.Fprintf(&b, "set -euo pipefail\n")
	fmt.Fprintf(&b, "mkdir -p /root/furo\n")
	fmt.Fprintf(&b, "cat > /root/furo/config.server.json <<'FURO_CONFIG'\n%sFURO_CONFIG\n", serverConfig)
	if staticEnabled {
		fmt.Fprintf(&b, "if ! command -v ip >/dev/null 2>&1 || ! command -v iptables >/dev/null 2>&1 || ! command -v wg >/dev/null 2>&1 || ! command -v wg-quick >/dev/null 2>&1; then apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y wireguard-tools iproute2 iptables; fi\n")
		fmt.Fprintf(&b, "mkdir -p /etc/wireguard && chmod 700 /etc/wireguard\n")
		fmt.Fprintf(&b, "cat > /etc/wireguard/%s.conf <<'FURO_WG'\n", a.cfg.StaticEgress.Interface)
		fmt.Fprintf(&b, "[Interface]\n")
		fmt.Fprintf(&b, "PrivateKey = %s\n", node.WireGuardPrivateKey)
		fmt.Fprintf(&b, "Address = %s/%d\n", node.EgressTunnelIP, prefix)
		fmt.Fprintf(&b, "Table = off\n")
		fmt.Fprintf(&b, "PostUp = ip route replace default dev %%i table %d; ip rule add from %s/32 table %d priority %d 2>/dev/null || true\n", a.cfg.StaticEgress.NodeRouteTable, node.EgressTunnelIP, a.cfg.StaticEgress.NodeRouteTable, a.cfg.StaticEgress.NodeRoutePriority)
		fmt.Fprintf(&b, "PostDown = ip rule del from %s/32 table %d priority %d 2>/dev/null || true; ip route del default dev %%i table %d 2>/dev/null || true\n\n", node.EgressTunnelIP, a.cfg.StaticEgress.NodeRouteTable, a.cfg.StaticEgress.NodeRoutePriority, a.cfg.StaticEgress.NodeRouteTable)
		fmt.Fprintf(&b, "[Peer]\n")
		fmt.Fprintf(&b, "PublicKey = %s\n", a.state.StaticEgress.PublicKey)
		fmt.Fprintf(&b, "Endpoint = %s:%d\n", masterHost, a.cfg.StaticEgress.ListenPort)
		fmt.Fprintf(&b, "AllowedIPs = 0.0.0.0/0\n")
		fmt.Fprintf(&b, "PersistentKeepalive = 25\n")
		fmt.Fprintf(&b, "FURO_WG\n")
		fmt.Fprintf(&b, "chmod 600 /etc/wireguard/%s.conf\n", a.cfg.StaticEgress.Interface)
		fmt.Fprintf(&b, "systemctl enable wg-quick@%s || true\n", a.cfg.StaticEgress.Interface)
		fmt.Fprintf(&b, "systemctl restart wg-quick@%s\n", a.cfg.StaticEgress.Interface)
	} else if a.cfg.StaticEgress.Interface != "" {
		fmt.Fprintf(&b, "systemctl disable --now wg-quick@%s >/dev/null 2>&1 || true\n", a.cfg.StaticEgress.Interface)
	}
	fmt.Fprintf(&b, "cd /root/furo\n")
	fmt.Fprintf(&b, "if [ -x ./service.sh ]; then FURO_SERVICE_NONINTERACTIVE=1 ./service.sh server restart || FURO_SERVICE_NONINTERACTIVE=1 ./service.sh restart; else systemctl restart furo-server; fi\n")
	return b.String(), nil
}

func (a *masterApp) ensureExistingNodeStaticEgress(ctx context.Context) error {
	if !a.cfg.StaticEgress.Enabled {
		a.staticEgressAvailable = false
		return nil
	}
	a.mu.Lock()
	changed := false
	for idx := range a.state.Nodes {
		node := &a.state.Nodes[idx]
		if node.Namespace != a.cfg.Namespace || nodeUnavailable(node.Status) {
			continue
		}
		before := *node
		if err := a.ensureNodeStaticEgressLocked(node); err != nil {
			a.mu.Unlock()
			return err
		}
		if before.EgressTunnelIP != node.EgressTunnelIP || before.WireGuardPrivateKey != node.WireGuardPrivateKey || before.WireGuardPublicKey != node.WireGuardPublicKey || before.EgressEnabled != node.EgressEnabled {
			changed = true
		}
	}
	if changed {
		if err := a.saveStateLocked(); err != nil {
			a.mu.Unlock()
			return err
		}
	}
	a.mu.Unlock()
	return a.configureMasterStaticEgress(ctx)
}

func (a *masterApp) syncNodesReport(ctx context.Context) masterActionReport {
	report := newMasterActionReport("sync-nodes")
	if a.cfg.StaticEgress.Enabled {
		if err := a.ensureExistingNodeStaticEgress(ctx); err != nil {
			report.OK = false
			report.Message = "static egress setup failed before syncing nodes: " + err.Error()
			report.add("%s", report.Message)
			return report
		}
	}
	a.mu.Lock()
	nodes := make([]masterNode, 0, len(a.state.Nodes))
	for _, node := range a.state.Nodes {
		if node.Namespace != a.cfg.Namespace || nodeUnavailable(node.Status) || node.IP == "" || node.Password == "" {
			continue
		}
		if node.Role != "active" && node.Role != "standby" {
			continue
		}
		nodes = append(nodes, node)
	}
	a.mu.Unlock()
	report.Data["nodes"] = len(nodes)
	successes := 0
	for _, node := range nodes {
		script, err := a.renderNodeSyncScript(node)
		if err != nil {
			report.OK = false
			report.add("node=%s render failed: %v", node.ID, err)
			continue
		}
		nodeCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		output, err := runSSHCommand(nodeCtx, node.IP, node.Password, "bash -s", strings.NewReader(script))
		cancel()
		if err != nil {
			report.OK = false
			report.add("node=%s ip=%s sync failed: %v output=%s", node.ID, node.IP, err, strings.TrimSpace(output))
			continue
		}
		successes++
		report.add("node=%s role=%s ip=%s synced and restarted", node.ID, node.Role, node.IP)
	}
	report.Data["successes"] = successes
	if len(nodes) == 0 {
		report.Message = "no active/standby nodes with ssh credentials to sync"
		return report
	}
	if report.OK {
		report.Message = fmt.Sprintf("synced %d node(s)", successes)
	} else {
		report.Message = fmt.Sprintf("synced %d/%d node(s); see details", successes, len(nodes))
	}
	return report
}

func (a *masterApp) staticEgressEndpointHost() (string, error) {
	parsed, err := url.Parse(a.cfg.PublicURL)
	if err != nil || parsed.Hostname() == "" {
		return "", fmt.Errorf("static egress requires public_url with a hostname, got %q", a.cfg.PublicURL)
	}
	host := parsed.Hostname()
	if host == "0.0.0.0" || host == "::" {
		return "", fmt.Errorf("static egress public_url must not use wildcard host %q", host)
	}
	return host, nil
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

func (a *masterApp) configureMasterStaticEgress(ctx context.Context) error {
	if !a.cfg.StaticEgress.Enabled {
		a.staticEgressAvailable = false
		return nil
	}
	a.mu.Lock()
	if err := a.ensureMasterStaticEgressStateLocked(); err != nil {
		a.mu.Unlock()
		return err
	}
	if err := a.saveStateLocked(); err != nil {
		a.mu.Unlock()
		return err
	}
	state := a.state
	a.mu.Unlock()

	if err := a.ensureStaticEgressPackages(ctx); err != nil {
		a.staticEgressAvailable = false
		return err
	}
	conf, err := a.renderMasterWireGuardConfig(state)
	if err != nil {
		a.staticEgressAvailable = false
		return err
	}
	confDir := "/etc/wireguard"
	if err := os.MkdirAll(confDir, 0700); err != nil {
		a.staticEgressAvailable = false
		return err
	}
	confPath := filepath.Join(confDir, a.cfg.StaticEgress.Interface+".conf")
	if err := os.WriteFile(confPath, []byte(conf), 0600); err != nil {
		a.staticEgressAvailable = false
		return err
	}
	if err := runMasterCommand(ctx, "sysctl", "-w", "net.ipv4.ip_forward=1"); err != nil {
		a.staticEgressAvailable = false
		return err
	}
	unit := "wg-quick@" + a.cfg.StaticEgress.Interface
	if _, err := exec.LookPath("systemctl"); err == nil {
		if err := runMasterCommand(ctx, "systemctl", "enable", unit); err != nil {
			log.Printf("[FURO-MASTER] static egress systemctl enable failed iface=%s err=%v", a.cfg.StaticEgress.Interface, err)
		}
		if err := runMasterCommand(ctx, "systemctl", "restart", unit); err != nil {
			a.staticEgressAvailable = false
			return err
		}
	} else {
		_ = runMasterCommand(ctx, "wg-quick", "down", a.cfg.StaticEgress.Interface)
		if err := runMasterCommand(ctx, "wg-quick", "up", a.cfg.StaticEgress.Interface); err != nil {
			a.staticEgressAvailable = false
			return err
		}
	}
	a.staticEgressAvailable = true
	log.Printf("[FURO-MASTER] static egress configured iface=%s listen_port=%d peers=%d", a.cfg.StaticEgress.Interface, a.cfg.StaticEgress.ListenPort, wireGuardPeerCount(state))
	return nil
}

func (a *masterApp) ensureStaticEgressPackages(ctx context.Context) error {
	missing := missingCommands("wg", "wg-quick", "ip", "iptables")
	if len(missing) == 0 {
		return nil
	}
	if !a.cfg.StaticEgress.AutoInstallPackages {
		return fmt.Errorf("static egress missing commands: %s", strings.Join(missing, ", "))
	}
	log.Printf("[FURO-MASTER] static egress installing packages reason=missing_commands commands=%s", strings.Join(missing, ","))
	if err := runMasterCommand(ctx, "apt-get", "update"); err != nil {
		return err
	}
	return runMasterCommand(ctx, "apt-get", "install", "-y", "wireguard-tools", "iproute2", "iptables")
}

func missingCommands(names ...string) []string {
	var missing []string
	for _, name := range names {
		if _, err := exec.LookPath(name); err != nil {
			missing = append(missing, name)
		}
	}
	return missing
}

func runMasterCommand(ctx context.Context, name string, args ...string) error {
	log.Printf("[FURO-MASTER] command started cmd=%s args=%s", name, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w output=%s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	if trimmed := strings.TrimSpace(string(output)); trimmed != "" {
		log.Printf("[FURO-MASTER] command output cmd=%s output=%s", name, trimmed)
	}
	return nil
}

func (a *masterApp) renderMasterWireGuardConfig(state masterState) (string, error) {
	cfg := a.cfg.StaticEgress
	prefix, err := cidrPrefix(cfg.Subnet)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "[Interface]\n")
	fmt.Fprintf(&b, "Address = %s/%d\n", cfg.MasterTunnelIP, prefix)
	fmt.Fprintf(&b, "ListenPort = %d\n", cfg.ListenPort)
	fmt.Fprintf(&b, "PrivateKey = %s\n", state.StaticEgress.PrivateKey)
	fmt.Fprintf(&b, "PostUp = iptables -t nat -A POSTROUTING -s %s -j MASQUERADE\n", cfg.Subnet)
	fmt.Fprintf(&b, "PostUp = iptables -I FORWARD 1 -i %%i -j ACCEPT\n")
	fmt.Fprintf(&b, "PostUp = iptables -I FORWARD 1 -o %%i -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT\n")
	fmt.Fprintf(&b, "PostDown = iptables -t nat -D POSTROUTING -s %s -j MASQUERADE\n", cfg.Subnet)
	fmt.Fprintf(&b, "PostDown = iptables -D FORWARD -i %%i -j ACCEPT\n")
	fmt.Fprintf(&b, "PostDown = iptables -D FORWARD -o %%i -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT\n")
	for _, node := range state.Nodes {
		if !node.EgressEnabled || node.WireGuardPublicKey == "" || node.EgressTunnelIP == "" || nodeUnavailable(node.Status) {
			continue
		}
		fmt.Fprintf(&b, "\n[Peer]\n")
		fmt.Fprintf(&b, "PublicKey = %s\n", node.WireGuardPublicKey)
		fmt.Fprintf(&b, "AllowedIPs = %s/32\n", node.EgressTunnelIP)
	}
	return b.String(), nil
}

func cidrPrefix(raw string) (int, error) {
	_, ipNet, err := net.ParseCIDR(raw)
	if err != nil {
		return 0, err
	}
	ones, _ := ipNet.Mask.Size()
	return ones, nil
}

func wireGuardPeerCount(state masterState) int {
	count := 0
	for _, node := range state.Nodes {
		if node.EgressEnabled && node.WireGuardPublicKey != "" && node.EgressTunnelIP != "" && !nodeUnavailable(node.Status) {
			count++
		}
	}
	return count
}

func runSSHScript(ctx context.Context, host, password, script string) error {
	output, err := runSSHCommand(ctx, host, password, "bash -s", strings.NewReader(script))
	if err != nil {
		return fmt.Errorf("bootstrap failed: %w output=%s", err, strings.TrimSpace(output))
	}
	return nil
}

type lineLogBuffer struct {
	mu      sync.Mutex
	prefix  string
	pending string
	output  strings.Builder
}

func newLineLogBuffer(prefix string) *lineLogBuffer {
	return &lineLogBuffer{prefix: prefix}
}

func (b *lineLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	text := string(p)
	b.output.WriteString(text)
	b.pending += text
	for {
		idx := strings.IndexByte(b.pending, '\n')
		if idx < 0 {
			break
		}
		line := strings.TrimRight(b.pending[:idx], "\r")
		b.pending = b.pending[idx+1:]
		if strings.TrimSpace(line) != "" {
			log.Printf("%s %s", b.prefix, line)
		}
	}
	return len(p), nil
}

func (b *lineLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.output.String()
}

func (b *lineLogBuffer) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	line := strings.TrimSpace(b.pending)
	if line != "" {
		log.Printf("%s %s", b.prefix, line)
	}
	b.pending = ""
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
		output := newLineLogBuffer(fmt.Sprintf("[FURO-MASTER] ssh host=%s command=%q", host, command))
		session.Stdout = output
		session.Stderr = output
		startedAt := time.Now()
		log.Printf("[FURO-MASTER] ssh connected host=%s command=%q", host, command)
		if err := session.Start(command); err != nil {
			return output.String(), err
		}
		log.Printf("[FURO-MASTER] ssh command started host=%s command=%q", host, command)
		done := make(chan error, 1)
		go func() {
			done <- session.Wait()
		}()
		heartbeat := time.NewTicker(30 * time.Second)
		defer heartbeat.Stop()
		for {
			select {
			case err := <-done:
				output.Flush()
				log.Printf("[FURO-MASTER] ssh command finished host=%s command=%q duration=%s err=%v", host, command, time.Since(startedAt).Round(time.Second), err)
				return output.String(), err
			case <-heartbeat.C:
				log.Printf("[FURO-MASTER] ssh command still running host=%s command=%q duration=%s", host, command, time.Since(startedAt).Round(time.Second))
			case <-ctx.Done():
				_ = session.Close()
				output.Flush()
				log.Printf("[FURO-MASTER] ssh command canceled host=%s command=%q duration=%s err=%v", host, command, time.Since(startedAt).Round(time.Second), ctx.Err())
				return output.String(), ctx.Err()
			}
		}
	}
	return "", fmt.Errorf("ssh not ready: %w", lastErr)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func (a *masterApp) handleDeadNode(ctx context.Context, nodeID, reason string) error {
	var oldNode masterNode
	var promotedForRename masterNode
	renamePromoted := false
	a.mu.Lock()
	dead := a.findNodeLocked(nodeID)
	if dead == nil {
		a.mu.Unlock()
		log.Printf("[FURO-MASTER] dead report ignored id=%s reason=node_not_found report_reason=%s", nodeID, reason)
		return nil
	}
	log.Printf("[FURO-MASTER] dead report received id=%s role=%s order=%s ip=%s reason=%s", dead.ID, dead.Role, dead.OrderID, dead.IP, reason)
	dead.Status = "dead"
	dead.LastReportState = reason
	dead.LastReportAt = time.Now().UTC().Format(time.RFC3339)
	dead.UpdatedAt = dead.LastReportAt
	oldNode = *dead
	if dead.Role == "active" && a.state.ActiveID == nodeID {
		var promoted *masterNode
		for idx := range a.state.Nodes {
			if a.state.Nodes[idx].Role == "standby" && a.state.Nodes[idx].Status == "ready" {
				promoted = &a.state.Nodes[idx]
				break
			}
		}
		if promoted != nil {
			promoted.Role = "active"
			promoted.Status = "ready"
			promoted.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
			a.state.ActiveID = promoted.ID
			promotedForRename = *promoted
			renamePromoted = true
			a.retireNodeIDLocked(nodeID)
			a.state.Generation++
			log.Printf("[FURO-MASTER] promoted standby id=%s old_active=%s generation=%d", promoted.ID, nodeID, a.state.Generation)
		} else {
			a.state.ActiveID = ""
			a.retireNodeIDLocked(nodeID)
			a.state.Generation++
			log.Printf("[FURO-MASTER] no standby available after active death old_active=%s generation=%d", nodeID, a.state.Generation)
		}
	} else if dead.Role == "standby" {
		a.retireNodeIDLocked(nodeID)
		a.state.Generation++
		log.Printf("[FURO-MASTER] retired dead standby id=%s generation=%d reason=%s", nodeID, a.state.Generation, reason)
	}
	err := a.saveStateLocked()
	a.mu.Unlock()
	if err != nil {
		return err
	}
	if err := a.publishRouteMap(ctx); err != nil {
		log.Printf("[FURO-MASTER] publish after dead failed: %v", err)
	}
	if renamePromoted {
		go a.renamePromotedProviderNode(context.Background(), promotedForRename)
	}
	if oldNode.OrderID != "" {
		go a.deleteNodeOrder(context.Background(), oldNode, "dead")
	}
	return a.ensureFleet(ctx)
}

func providerActiveNameForNode(nodeID string) string {
	if strings.Contains(nodeID, "-standby-") {
		return strings.Replace(nodeID, "-standby-", "-active-", 1)
	}
	if strings.Contains(nodeID, "-active-") {
		return nodeID
	}
	return nodeID + "-active"
}

func (a *masterApp) renamePromotedProviderNode(ctx context.Context, node masterNode) {
	newName := providerActiveNameForNode(node.ID)
	if newName == "" || newName == node.ID {
		return
	}
	renameCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	log.Printf("[FURO-MASTER] provider rename started id=%s order=%s service_id=%s new_name=%s reason=promoted_to_active", node.ID, node.OrderID, node.ServiceID, newName)
	serviceID, err := a.caasify.renameOrder(renameCtx, node, newName)
	if err != nil {
		log.Printf("[FURO-MASTER] provider rename failed id=%s order=%s service_id=%s new_name=%s err=%v", node.ID, node.OrderID, node.ServiceID, newName, err)
		return
	}
	if serviceID != "" {
		a.mu.Lock()
		if current := a.findNodeLocked(node.ID); current != nil && current.ServiceID == "" {
			current.ServiceID = serviceID
			current.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
			if saveErr := a.saveStateLocked(); saveErr != nil {
				log.Printf("[FURO-MASTER] save provider rename service id failed id=%s err=%v", node.ID, saveErr)
			}
		}
		a.mu.Unlock()
	}
	log.Printf("[FURO-MASTER] provider rename ok id=%s order=%s service_id=%s new_name=%s", node.ID, node.OrderID, firstNonEmpty(serviceID, node.ServiceID), newName)
}

type relayRouteMap struct {
	Namespace     string           `json:"namespace"`
	FleetID       string           `json:"fleet_id"`
	Generation    int64            `json:"generation"`
	Active        *relayRouteSpec  `json:"active"`
	Standby       []relayRouteSpec `json:"standby"`
	ActiveRoutes  []relayRouteSpec `json:"active_routes,omitempty"`
	StandbyRoutes []relayRouteSpec `json:"standby_routes,omitempty"`
	Retired       []string         `json:"retired"`
	UpdatedAt     string           `json:"updated_at"`
}

type relayRouteSpec struct {
	ID           string `json:"id"`
	NodeID       string `json:"node_id,omitempty"`
	RelayID      string `json:"relay_id,omitempty"`
	RelayURL     string `json:"relay_url"`
	ServerHost   string `json:"server_host"`
	ServerPort   int    `json:"server_port"`
	SessionCount int    `json:"session_count"`
}

func relayRouteID(nodeID, relayID string) string {
	if relayID == "" || relayID == "primary" {
		return nodeID
	}
	return sanitizeMasterID(nodeID + "__" + relayID)
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
	relays := a.cfg.Relays
	if len(relays) == 0 {
		relays = normalizeMasterRelays(a.cfg.RelayURL, nil)
	}
	for _, node := range a.state.Nodes {
		if node.Namespace != a.cfg.Namespace {
			continue
		}
		if node.Status != "ready" || node.IP == "" {
			continue
		}
		for idx, relay := range relays {
			spec := relayRouteSpec{
				ID:           relayRouteID(node.ID, relay.ID),
				NodeID:       node.ID,
				RelayID:      relay.ID,
				RelayURL:     relay.URL,
				ServerHost:   node.IP,
				ServerPort:   a.cfg.ServerAgentPort,
				SessionCount: a.cfg.RouteSessionCount,
			}
			if node.ID == a.state.ActiveID && node.Role == "active" {
				routeMap.ActiveRoutes = append(routeMap.ActiveRoutes, spec)
				if idx == 0 {
					routeMap.Active = &spec
				}
				continue
			}
			if node.Role == "standby" {
				routeMap.StandbyRoutes = append(routeMap.StandbyRoutes, spec)
				if idx == 0 {
					routeMap.Standby = append(routeMap.Standby, spec)
				}
			}
		}
	}
	return routeMap
}

func (a *masterApp) publishRouteMap(ctx context.Context) error {
	report := a.publishRouteMapReport(ctx)
	if !report.OK {
		return errors.New(report.Message)
	}
	return nil
}

type masterActionReport struct {
	OK      bool           `json:"ok"`
	Action  string         `json:"action"`
	Message string         `json:"message"`
	Details []string       `json:"details,omitempty"`
	Data    map[string]any `json:"data,omitempty"`
	When    string         `json:"when"`
}

func newMasterActionReport(action string) masterActionReport {
	return masterActionReport{OK: true, Action: action, Data: map[string]any{}, When: time.Now().UTC().Format(time.RFC3339)}
}

func (r *masterActionReport) add(format string, args ...any) {
	r.Details = append(r.Details, fmt.Sprintf(format, args...))
}

func (a *masterApp) publishRouteMapReport(ctx context.Context) masterActionReport {
	report := newMasterActionReport("publish")
	a.mu.Lock()
	routeMap := a.routeMapLocked()
	relays := append([]masterRelayConfig(nil), a.cfg.Relays...)
	a.mu.Unlock()
	data, err := json.Marshal(routeMap)
	if err != nil {
		report.OK = false
		report.Message = err.Error()
		return report
	}
	activeID := ""
	if routeMap.Active != nil {
		activeID = routeMap.Active.ID
	}
	if len(relays) == 0 {
		relays = normalizeMasterRelays(a.cfg.RelayURL, nil)
	}
	var failures []string
	successes := 0
	for _, relay := range relays {
		endpoint, err := relayRouteMapURL(relay.URL, a.cfg.Namespace)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", relay.ID, err))
			report.add("relay=%s invalid url: %v", relay.ID, err)
			continue
		}
		log.Printf("[FURO-MASTER] publishing route-map namespace=%s fleet_id=%s generation=%d active=%s active_routes=%d standby=%d standby_routes=%d retired=%d relay=%s endpoint=%s", routeMap.Namespace, routeMap.FleetID, routeMap.Generation, activeID, len(routeMap.ActiveRoutes), len(routeMap.Standby), len(routeMap.StandbyRoutes), len(routeMap.Retired), relay.ID, endpoint)
		if err := a.publishRouteMapToRelay(ctx, endpoint, data); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", relay.ID, err))
			log.Printf("[FURO-MASTER] route-map publish failed relay=%s err=%v", relay.ID, err)
			report.add("relay=%s failed: %v", relay.ID, err)
			continue
		}
		successes++
		log.Printf("[FURO-MASTER] route-map publish ok namespace=%s generation=%d relay=%s", routeMap.Namespace, routeMap.Generation, relay.ID)
		report.add("relay=%s ok endpoint=%s", relay.ID, endpoint)
	}
	report.Data["namespace"] = routeMap.Namespace
	report.Data["fleet_id"] = routeMap.FleetID
	report.Data["generation"] = routeMap.Generation
	report.Data["active"] = activeID
	report.Data["active_routes"] = len(routeMap.ActiveRoutes)
	report.Data["standby_routes"] = len(routeMap.StandbyRoutes)
	report.Data["successes"] = successes
	report.Data["failures"] = len(failures)
	if successes == 0 {
		report.OK = false
		report.Message = fmt.Sprintf("route-map publish failed for all relays: %s", strings.Join(failures, "; "))
		return report
	}
	if len(failures) > 0 {
		log.Printf("[FURO-MASTER] route-map publish partial failures=%s", strings.Join(failures, "; "))
		report.Message = fmt.Sprintf("published to %d relay(s), %d failed", successes, len(failures))
		return report
	}
	report.Message = fmt.Sprintf("published generation %d to %d relay(s)", routeMap.Generation, successes)
	return report
}

func (a *masterApp) publishRouteMapToRelay(ctx context.Context, endpoint string, data []byte) error {
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

func (a *masterApp) recordNodeReport(report nodeReport) string {
	role := report.Role
	now := time.Now().UTC().Format(time.RFC3339)

	a.mu.Lock()
	defer a.mu.Unlock()
	node := a.findNodeLocked(report.NodeID)
	if node == nil {
		log.Printf("[FURO-MASTER] node report from unknown id=%s role=%s status=%s namespace=%s", report.NodeID, report.Role, report.Status, report.Namespace)
		return role
	}

	previousStatus := node.Status
	node.LastReportAt = now
	node.LastReportState = report.Status
	node.UpdatedAt = now
	role = node.Role
	if report.Status == "ready" && node.Status != "ready" {
		if nodeUnavailable(node.Status) {
			if err := a.saveStateLocked(); err != nil {
				log.Printf("[FURO-MASTER] save node report failed id=%s status=%s err=%v", node.ID, report.Status, err)
			}
			log.Printf("[FURO-MASTER] node ready report ignored id=%s role=%s previous_status=%s reason=node_unavailable", node.ID, node.Role, previousStatus)
			return role
		}
		node.Status = "ready"
		node.LastError = ""
		if node.ReadyAt == "" {
			node.ReadyAt = now
		}
		a.state.Generation++
		log.Printf("[FURO-MASTER] node report promoted state id=%s role=%s previous_status=%s new_status=ready generation=%d", node.ID, node.Role, previousStatus, a.state.Generation)
	}
	if err := a.saveStateLocked(); err != nil {
		log.Printf("[FURO-MASTER] save node report failed id=%s status=%s err=%v", node.ID, report.Status, err)
	}
	log.Printf("[FURO-MASTER] node report id=%s role=%s status=%s ip=%s state_status=%s reason=%s", node.ID, node.Role, report.Status, node.IP, node.Status, report.Reason)
	return role
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func (a *masterApp) listNodesReport() masterActionReport {
	report := newMasterActionReport("list-nodes")
	a.mu.Lock()
	nodes := append([]masterNode(nil), a.state.Nodes...)
	state := a.state
	a.mu.Unlock()
	report.Data["namespace"] = state.Namespace
	report.Data["fleet_id"] = state.FleetID
	report.Data["generation"] = state.Generation
	report.Data["nodes"] = len(nodes)
	report.Message = fmt.Sprintf("%d node(s)", len(nodes))
	for idx, node := range nodes {
		report.add("%d\t%s\t%s\t%s\t%s\t%s", idx+1, node.ID, node.Role, node.Status, node.IP, node.Password)
	}
	return report
}

func (a *masterApp) printNodesTable(w io.Writer) {
	a.mu.Lock()
	nodes := append([]masterNode(nil), a.state.Nodes...)
	a.mu.Unlock()
	fmt.Fprintf(w, "%-5s %-48s %-8s %-16s %-15s %-24s\n", "INDEX", "HOSTNAME", "ROLE", "STATUS", "IP", "SSH_PASSWORD")
	for idx, node := range nodes {
		password := node.Password
		if password == "" {
			password = "-"
		}
		ip := node.IP
		if ip == "" {
			ip = "-"
		}
		fmt.Fprintf(w, "%-5d %-48s %-8s %-16s %-15s %-24s\n", idx+1, node.ID, node.Role, node.Status, ip, password)
	}
}

func (a *masterApp) resetFleetReport(ctx context.Context) masterActionReport {
	report := newMasterActionReport("reset")
	a.mu.Lock()
	nodes := append([]masterNode(nil), a.state.Nodes...)
	a.state.Nodes = []masterNode{}
	a.state.ActiveID = ""
	a.state.Retired = []string{}
	a.state.Generation++
	if err := a.saveStateLocked(); err != nil {
		a.mu.Unlock()
		report.OK = false
		report.Message = "failed to save cleared state: " + err.Error()
		return report
	}
	a.mu.Unlock()

	deleted := 0
	for _, node := range nodes {
		if node.OrderID == "" || nodeUnavailable(node.Status) {
			continue
		}
		deleteCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		err := a.caasify.deleteOrder(deleteCtx, node.OrderID)
		cancel()
		if err != nil {
			report.OK = false
			report.add("delete node=%s order=%s failed: %v", node.ID, node.OrderID, err)
			continue
		}
		deleted++
		report.add("delete node=%s order=%s accepted", node.ID, node.OrderID)
	}
	if err := os.Remove(a.statePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		report.OK = false
		report.add("state file remove failed path=%s err=%v", a.statePath, err)
	}
	a.mu.Lock()
	a.state = defaultMasterState(a.cfg.Namespace)
	if err := a.saveStateLocked(); err != nil {
		a.mu.Unlock()
		report.OK = false
		report.Message = "failed to save fresh state: " + err.Error()
		return report
	}
	a.mu.Unlock()
	active, err := a.createAndBootstrapNode(ctx, "active")
	if err != nil {
		report.OK = false
		report.add("fresh active create failed: %v", err)
	} else {
		report.add("fresh active created id=%s ip=%s status=%s", active.ID, active.IP, active.Status)
		for i := 0; i < a.cfg.BackupCount; i++ {
			standby, err := a.createAndBootstrapNode(ctx, "standby")
			if err != nil {
				report.OK = false
				report.add("fresh standby create failed index=%d: %v", i+1, err)
				break
			}
			report.add("fresh standby created id=%s ip=%s status=%s", standby.ID, standby.IP, standby.Status)
		}
	}
	publish := a.publishRouteMapReport(ctx)
	report.Data["deleted"] = deleted
	report.Data["publish"] = publish.Data
	report.Details = append(report.Details, publish.Details...)
	if !publish.OK {
		report.OK = false
		report.add("publish failed: %s", publish.Message)
	}
	if report.OK {
		report.Message = fmt.Sprintf("reset complete; deleted %d old node(s) and started fresh reconcile", deleted)
	} else if report.Message == "" {
		report.Message = fmt.Sprintf("reset finished with errors; deleted %d old node(s)", deleted)
	}
	return report
}

type masterAdminView struct {
	Config           masterConfigFile
	State            masterState
	RelaysJSON       string
	ProviderPoolJSON string
	StaticEgressJSON string
	Action           masterActionReport
	HasAction        bool
	Uptime           string
	Version          string
	Commit           string
}

func prettyJSON(value any) string {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "null"
	}
	return string(data)
}

func (a *masterApp) adminView(action *masterActionReport) masterAdminView {
	a.mu.Lock()
	state := a.state
	a.mu.Unlock()
	cfg := a.cfg
	view := masterAdminView{
		Config:           cfg,
		State:            state,
		RelaysJSON:       prettyJSON(cfg.Relays),
		ProviderPoolJSON: prettyJSON(cfg.ProviderPool),
		StaticEgressJSON: prettyJSON(cfg.StaticEgress),
		Uptime:           time.Since(a.startedAt).Round(time.Second).String(),
		Version:          appVersion,
		Commit:           appCommit,
	}
	if action != nil {
		view.Action = *action
		view.HasAction = true
	}
	return view
}

func boolFormValue(r *http.Request, key string) bool {
	return r.FormValue(key) == "on" || r.FormValue(key) == "true" || r.FormValue(key) == "1"
}

func intFormValue(r *http.Request, key string, current int) (int, error) {
	value := strings.TrimSpace(r.FormValue(key))
	if value == "" {
		return current, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return current, fmt.Errorf("%s must be a number", key)
	}
	return parsed, nil
}

func stringFormValue(r *http.Request, key, current string) string {
	value := strings.TrimSpace(r.FormValue(key))
	return value
}

func masterNodeConfigChanged(oldCfg, newCfg masterConfigFile) bool {
	oldStatic, _ := json.Marshal(oldCfg.StaticEgress)
	newStatic, _ := json.Marshal(newCfg.StaticEgress)
	return oldCfg.APIKey != newCfg.APIKey ||
		oldCfg.Namespace != newCfg.Namespace ||
		oldCfg.PublicURL != newCfg.PublicURL ||
		oldCfg.RelayHealthHost != newCfg.RelayHealthHost ||
		oldCfg.RelayHealthPort != newCfg.RelayHealthPort ||
		oldCfg.ServerAgentPort != newCfg.ServerAgentPort ||
		oldCfg.NodeMaxSessions != newCfg.NodeMaxSessions ||
		oldCfg.NodeCheckIntervalSeconds != newCfg.NodeCheckIntervalSeconds ||
		oldCfg.NodeFailureThreshold != newCfg.NodeFailureThreshold ||
		string(oldStatic) != string(newStatic)
}

func masterRoutePublishConfigChanged(oldCfg, newCfg masterConfigFile) bool {
	oldRelays, _ := json.Marshal(oldCfg.Relays)
	newRelays, _ := json.Marshal(newCfg.Relays)
	return oldCfg.RelayURL != newCfg.RelayURL ||
		oldCfg.RouteSessionCount != newCfg.RouteSessionCount ||
		oldCfg.ServerAgentPort != newCfg.ServerAgentPort ||
		string(oldRelays) != string(newRelays)
}

func masterFleetConfigChanged(oldCfg, newCfg masterConfigFile) bool {
	oldPool, _ := json.Marshal(oldCfg.ProviderPool)
	newPool, _ := json.Marshal(newCfg.ProviderPool)
	return oldCfg.ProviderBackend != newCfg.ProviderBackend ||
		oldCfg.CaasifyToken != newCfg.CaasifyToken ||
		oldCfg.DopraxAPIKey != newCfg.DopraxAPIKey ||
		oldCfg.DopraxUsername != newCfg.DopraxUsername ||
		oldCfg.DopraxPassword != newCfg.DopraxPassword ||
		oldCfg.DopraxBaseURL != newCfg.DopraxBaseURL ||
		oldCfg.DopraxProductVersionID != newCfg.DopraxProductVersionID ||
		oldCfg.DopraxLocationOptionID != newCfg.DopraxLocationOptionID ||
		oldCfg.DopraxOSOptionID != newCfg.DopraxOSOptionID ||
		oldCfg.DopraxAccessMethod != newCfg.DopraxAccessMethod ||
		oldCfg.BackupCount != newCfg.BackupCount ||
		oldCfg.ProductID != newCfg.ProductID ||
		oldCfg.Template != newCfg.Template ||
		oldCfg.NotePrefix != newCfg.NotePrefix ||
		oldCfg.IPv4 != newCfg.IPv4 ||
		oldCfg.IPv6 != newCfg.IPv6 ||
		oldCfg.BootstrapScriptPath != newCfg.BootstrapScriptPath ||
		string(oldPool) != string(newPool)
}

func (a *masterApp) parseConfigForm(r *http.Request) (masterConfigFile, bool, bool, bool, error) {
	if err := r.ParseForm(); err != nil {
		return a.cfg, false, false, false, err
	}
	oldCfg := a.cfg
	cfg := oldCfg
	cfg.Namespace = sanitizeMasterID(stringFormValue(r, "namespace", cfg.Namespace))
	cfg.APIKey = stringFormValue(r, "api_key", cfg.APIKey)
	cfg.Listen = stringFormValue(r, "listen", cfg.Listen)
	cfg.PublicURL = stringFormValue(r, "public_url", cfg.PublicURL)
	cfg.AdminListen = stringFormValue(r, "admin_listen", cfg.AdminListen)
	cfg.ProviderBackend = strings.TrimSpace(r.FormValue("provider_backend"))
	cfg.CaasifyToken = stringFormValue(r, "caasify_token", cfg.CaasifyToken)
	cfg.DopraxAPIKey = stringFormValue(r, "doprax_api_key", cfg.DopraxAPIKey)
	cfg.DopraxUsername = stringFormValue(r, "doprax_username", cfg.DopraxUsername)
	cfg.DopraxPassword = stringFormValue(r, "doprax_password", cfg.DopraxPassword)
	cfg.DopraxBaseURL = stringFormValue(r, "doprax_base_url", cfg.DopraxBaseURL)
	cfg.DopraxProductVersionID = stringFormValue(r, "doprax_product_version_id", cfg.DopraxProductVersionID)
	cfg.DopraxLocationOptionID = stringFormValue(r, "doprax_location_option_id", cfg.DopraxLocationOptionID)
	cfg.DopraxOSOptionID = stringFormValue(r, "doprax_os_option_id", cfg.DopraxOSOptionID)
	cfg.DopraxAccessMethod = stringFormValue(r, "doprax_access_method", cfg.DopraxAccessMethod)
	cfg.StateFile = stringFormValue(r, "state_file", cfg.StateFile)
	cfg.LogFile = stringFormValue(r, "log_file", cfg.LogFile)
	cfg.RelayURL = stringFormValue(r, "relay_url", cfg.RelayURL)
	cfg.RelayHealthHost = stringFormValue(r, "relay_health_host", cfg.RelayHealthHost)
	cfg.Template = stringFormValue(r, "template", cfg.Template)
	cfg.NotePrefix = sanitizeMasterID(stringFormValue(r, "note_prefix", cfg.NotePrefix))
	cfg.BootstrapScriptPath = stringFormValue(r, "bootstrap_script_path", cfg.BootstrapScriptPath)

	var err error
	intFields := []struct {
		key string
		dst *int
	}{
		{"doprax_login_retry_attempts", &cfg.DopraxLoginRetryAttempts},
		{"doprax_login_retry_delay_seconds", &cfg.DopraxLoginRetryDelaySec},
		{"relay_health_port", &cfg.RelayHealthPort},
		{"server_agent_port", &cfg.ServerAgentPort},
		{"node_max_sessions", &cfg.NodeMaxSessions},
		{"route_session_count", &cfg.RouteSessionCount},
		{"backup_count", &cfg.BackupCount},
		{"node_check_interval_seconds", &cfg.NodeCheckIntervalSeconds},
		{"node_failure_threshold", &cfg.NodeFailureThreshold},
		{"publish_interval_seconds", &cfg.PublishIntervalSeconds},
		{"product_id", &cfg.ProductID},
		{"ipv4", &cfg.IPv4},
		{"ipv6", &cfg.IPv6},
	}
	for _, field := range intFields {
		*field.dst, err = intFormValue(r, field.key, *field.dst)
		if err != nil {
			return oldCfg, false, false, false, err
		}
	}
	if raw := strings.TrimSpace(r.FormValue("relays_json")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &cfg.Relays); err != nil {
			return oldCfg, false, false, false, fmt.Errorf("relays JSON: %w", err)
		}
	} else {
		cfg.Relays = nil
	}
	if raw := strings.TrimSpace(r.FormValue("provider_pool_json")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &cfg.ProviderPool); err != nil {
			return oldCfg, false, false, false, fmt.Errorf("provider_pool JSON: %w", err)
		}
	} else {
		cfg.ProviderPool = nil
	}
	if raw := strings.TrimSpace(r.FormValue("static_egress_json")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &cfg.StaticEgress); err != nil {
			return oldCfg, false, false, false, fmt.Errorf("static_egress JSON: %w", err)
		}
	}
	cfg.StaticEgress.Enabled = boolFormValue(r, "static_egress_enabled")
	cfg.StaticEgress.AutoInstallPackages = boolFormValue(r, "static_egress_auto_install_packages")
	cfg.StaticEgress.Interface = stringFormValue(r, "static_egress_interface", cfg.StaticEgress.Interface)
	cfg.StaticEgress.Subnet = stringFormValue(r, "static_egress_subnet", cfg.StaticEgress.Subnet)
	cfg.StaticEgress.MasterTunnelIP = stringFormValue(r, "static_egress_master_tunnel_ip", cfg.StaticEgress.MasterTunnelIP)
	cfg.StaticEgress.ListenPort, err = intFormValue(r, "static_egress_listen_port", cfg.StaticEgress.ListenPort)
	if err != nil {
		return oldCfg, false, false, false, err
	}
	cfg.StaticEgress.NodeRouteTable, err = intFormValue(r, "static_egress_node_route_table", cfg.StaticEgress.NodeRouteTable)
	if err != nil {
		return oldCfg, false, false, false, err
	}
	cfg.StaticEgress.NodeRoutePriority, err = intFormValue(r, "static_egress_node_route_priority", cfg.StaticEgress.NodeRoutePriority)
	if err != nil {
		return oldCfg, false, false, false, err
	}
	cfg.Relays = normalizeMasterRelays(cfg.RelayURL, cfg.Relays)
	if cfg.RelayURL == "" && len(cfg.Relays) > 0 {
		cfg.RelayURL = cfg.Relays[0].URL
	}
	if cfg.APIKey == "" || cfg.Listen == "" || cfg.PublicURL == "" || cfg.RelayURL == "" {
		return oldCfg, false, false, false, errors.New("api_key, listen, public_url, and relay_url/relays are required")
	}
	if cfg.ProviderBackend == "" {
		cfg.ProviderBackend = resolvedMasterProviderBackend(cfg)
	}
	if cfg.ProviderBackend != "doprax" && cfg.ProviderBackend != "caasify" {
		return oldCfg, false, false, false, errors.New("provider_backend must be doprax or caasify")
	}
	if cfg.BackupCount < 0 || cfg.NodeMaxSessions < 1 || cfg.RouteSessionCount < 1 {
		return oldCfg, false, false, false, errors.New("backup_count must be >= 0 and session counts must be >= 1")
	}
	cfg.StaticEgress = normalizeMasterStaticEgressConfig(cfg.StaticEgress, masterStaticEgressConfig{})
	if cfg.StaticEgress.Enabled {
		if err := validateMasterStaticEgressConfig(cfg.StaticEgress); err != nil {
			return oldCfg, false, false, false, err
		}
	}
	return cfg, masterNodeConfigChanged(oldCfg, cfg), masterRoutePublishConfigChanged(oldCfg, cfg), masterFleetConfigChanged(oldCfg, cfg), nil
}

func (a *masterApp) saveConfigFromRequest(ctx context.Context, r *http.Request) masterActionReport {
	report := newMasterActionReport("save-config")
	cfg, nodeChanged, publishChanged, fleetChanged, err := a.parseConfigForm(r)
	if err != nil {
		report.OK = false
		report.Message = err.Error()
		return report
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		report.OK = false
		report.Message = err.Error()
		return report
	}
	data = append(data, '\n')
	cfgPath := resolvePath(".", *masterConfigPath)
	tmp := cfgPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		report.OK = false
		report.Message = err.Error()
		return report
	}
	if err := os.Rename(tmp, cfgPath); err != nil {
		report.OK = false
		report.Message = err.Error()
		return report
	}
	a.cfg = cfg
	a.caasify = masterProviderFromConfig(cfg)
	a.statePath = resolvePath(filepath.Dir(*masterConfigPath), cfg.StateFile)
	report.Message = "configuration saved"
	report.add("wrote %s", cfgPath)
	if fleetChanged {
		if err := a.ensureFleet(ctx); err != nil {
			report.OK = false
			report.add("reconcile after save failed: %v", err)
		} else {
			report.add("reconcile after save completed")
		}
	}
	if nodeChanged {
		syncReport := a.syncNodesReport(ctx)
		report.Details = append(report.Details, syncReport.Details...)
		if !syncReport.OK {
			report.OK = false
			report.add("node sync after save failed: %s", syncReport.Message)
		} else {
			report.add("node sync after save: %s", syncReport.Message)
		}
	}
	if publishChanged || fleetChanged {
		pub := a.publishRouteMapReport(ctx)
		report.Details = append(report.Details, pub.Details...)
		if !pub.OK {
			report.OK = false
			report.add("publish after save failed: %s", pub.Message)
		} else {
			report.add("publish after save: %s", pub.Message)
		}
	}
	if !report.OK {
		report.Message = "configuration saved, but follow-up action failed"
	}
	return report
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
		role := a.recordNodeReport(report)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "role": role})
	})
	log.Printf("[FURO-MASTER] control listener on %s", a.cfg.Listen)
	return http.ListenAndServe(a.cfg.Listen, mux)
}

var masterAdminTemplate = template.Must(template.New("master-admin").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FURO Master</title>
<style>
:root{color-scheme:light dark;--bg:#f6f7f9;--panel:#fff;--text:#18202a;--muted:#687385;--line:#d9dee7;--ok:#0f7b45;--bad:#b42318;--warn:#b86400;--active:#0b61a4;--standby:#6f4bb8}
@media (prefers-color-scheme:dark){:root{--bg:#0f1216;--panel:#171b21;--text:#e8edf5;--muted:#9aa6b8;--line:#303844}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif}header{position:sticky;top:0;z-index:2;background:var(--panel);border-bottom:1px solid var(--line);padding:14px 22px;display:flex;justify-content:space-between;gap:16px;align-items:center}h1{font-size:20px;margin:0}main{padding:20px;max-width:1480px;margin:auto}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}.card,.section{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:14px}.section{margin-top:16px}h2{font-size:15px;margin:0 0 12px}.metric{font-size:24px;font-weight:650}.muted{color:var(--muted)}.actions{display:flex;flex-wrap:wrap;gap:8px}button,.btn{border:1px solid var(--line);background:var(--text);color:var(--panel);border-radius:6px;padding:8px 11px;font-weight:600;cursor:pointer}.secondary{background:transparent;color:var(--text)}.danger{background:var(--bad);color:#fff}.ok{color:var(--ok)}.bad{color:var(--bad)}.warn{color:var(--warn)}form.config{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}.field{display:flex;flex-direction:column;gap:5px}.field.full{grid-column:1/-1}label{font-size:12px;color:var(--muted);font-weight:650}input,select,textarea{width:100%;border:1px solid var(--line);border-radius:6px;background:transparent;color:var(--text);padding:8px;font:13px ui-monospace,SFMono-Regular,Menlo,monospace}textarea{min-height:120px;resize:vertical}.check{flex-direction:row;align-items:center}.check input{width:auto}table{width:100%;border-collapse:collapse}th,td{text-align:left;border-bottom:1px solid var(--line);padding:8px;vertical-align:top}th{font-size:12px;color:var(--muted)}.badge{display:inline-block;border-radius:999px;padding:2px 8px;font-size:12px;font-weight:700}.active{background:#dff0ff;color:var(--active)}.standby{background:#eee6ff;color:var(--standby)}.ready{background:#ddf7e8;color:var(--ok)}.dead,.deleted,.deleting,.verify_failed{background:#ffe3df;color:var(--bad)}pre{white-space:pre-wrap;background:rgba(127,127,127,.08);border:1px solid var(--line);border-radius:6px;padding:10px;overflow:auto}.two{display:grid;grid-template-columns:1fr 1fr;gap:12px}@media(max-width:900px){.grid,form.config,.two{grid-template-columns:1fr}header{align-items:flex-start;flex-direction:column}}
</style>
</head>
<body>
<header>
  <div><h1>FURO Server Master</h1><div class="muted">{{.Config.Namespace}} · {{.State.FleetID}} · {{.Version}} {{.Commit}} · uptime {{.Uptime}}</div></div>
  <div class="actions">
    <form method="post" action="/action"><button name="action" value="publish">Publish</button></form>
    <form method="post" action="/action"><button name="action" value="sync-nodes">Sync Nodes</button></form>
    <form method="post" action="/action"><button name="action" value="reconcile" class="secondary">Reconcile</button></form>
  </div>
</header>
<main>
{{if .HasAction}}<section class="section"><h2>Last Action</h2><div class="{{if .Action.OK}}ok{{else}}bad{{end}}"><strong>{{.Action.Action}}</strong>: {{.Action.Message}}</div>{{if .Action.Details}}<pre>{{range .Action.Details}}{{.}}
{{end}}</pre>{{end}}</section>{{end}}
<section class="grid">
  <div class="card"><div class="muted">Generation</div><div class="metric">{{.State.Generation}}</div></div>
  <div class="card"><div class="muted">Active</div><div class="metric">{{.State.ActiveID}}</div></div>
  <div class="card"><div class="muted">Nodes</div><div class="metric">{{len .State.Nodes}}</div></div>
  <div class="card"><div class="muted">Backups Wanted</div><div class="metric">{{.Config.BackupCount}}</div></div>
</section>
<section class="section">
<h2>Nodes</h2>
<table><thead><tr><th>#</th><th>Hostname</th><th>Role</th><th>Status</th><th>IP</th><th>SSH Password</th><th>Last Report</th><th>Error</th></tr></thead><tbody>
{{range $i,$n := .State.Nodes}}<tr><td>{{$i}}</td><td>{{$n.ID}}</td><td><span class="badge {{$n.Role}}">{{$n.Role}}</span></td><td><span class="badge {{$n.Status}}">{{$n.Status}}</span></td><td>{{$n.IP}}</td><td>{{$n.Password}}</td><td>{{$n.LastReportAt}} {{$n.LastReportState}}</td><td>{{$n.LastError}}</td></tr>{{else}}<tr><td colspan="8" class="muted">No nodes in state.</td></tr>{{end}}
</tbody></table>
</section>
<section class="section">
<h2>Configuration</h2>
<form class="config" method="post" action="/config">
<div class="field"><label>namespace</label><input name="namespace" value="{{.Config.Namespace}}"></div>
<div class="field"><label>api_key</label><input name="api_key" value="{{.Config.APIKey}}"></div>
<div class="field"><label>provider_backend</label><select name="provider_backend"><option value="doprax" {{if eq .Config.ProviderBackend "doprax"}}selected{{end}}>doprax</option><option value="caasify" {{if eq .Config.ProviderBackend "caasify"}}selected{{end}}>caasify</option></select></div>
<div class="field"><label>listen</label><input name="listen" value="{{.Config.Listen}}"></div>
<div class="field"><label>public_url</label><input name="public_url" value="{{.Config.PublicURL}}"></div>
<div class="field"><label>admin_listen</label><input name="admin_listen" value="{{.Config.AdminListen}}"></div>
<div class="field"><label>state_file</label><input name="state_file" value="{{.Config.StateFile}}"></div>
<div class="field"><label>log_file</label><input name="log_file" value="{{.Config.LogFile}}"></div>
<div class="field"><label>bootstrap_script_path</label><input name="bootstrap_script_path" value="{{.Config.BootstrapScriptPath}}"></div>
<div class="field"><label>relay_url</label><input name="relay_url" value="{{.Config.RelayURL}}"></div>
<div class="field"><label>relay_health_host</label><input name="relay_health_host" value="{{.Config.RelayHealthHost}}"></div>
<div class="field"><label>relay_health_port</label><input name="relay_health_port" value="{{.Config.RelayHealthPort}}"></div>
<div class="field"><label>server_agent_port</label><input name="server_agent_port" value="{{.Config.ServerAgentPort}}"></div>
<div class="field"><label>node_max_sessions</label><input name="node_max_sessions" value="{{.Config.NodeMaxSessions}}"></div>
<div class="field"><label>route_session_count per relay</label><input name="route_session_count" value="{{.Config.RouteSessionCount}}"></div>
<div class="field"><label>backup_count</label><input name="backup_count" value="{{.Config.BackupCount}}"></div>
<div class="field"><label>node_check_interval_seconds</label><input name="node_check_interval_seconds" value="{{.Config.NodeCheckIntervalSeconds}}"></div>
<div class="field"><label>node_failure_threshold</label><input name="node_failure_threshold" value="{{.Config.NodeFailureThreshold}}"></div>
<div class="field"><label>publish_interval_seconds</label><input name="publish_interval_seconds" value="{{.Config.PublishIntervalSeconds}}"></div>
<div class="field"><label>doprax_api_key</label><input name="doprax_api_key" value="{{.Config.DopraxAPIKey}}"></div>
<div class="field"><label>doprax_username</label><input name="doprax_username" value="{{.Config.DopraxUsername}}"></div>
<div class="field"><label>doprax_password</label><input type="password" name="doprax_password" value="{{.Config.DopraxPassword}}"></div>
<div class="field"><label>doprax_base_url</label><input name="doprax_base_url" value="{{.Config.DopraxBaseURL}}"></div>
<div class="field"><label>doprax_product_version_id</label><input name="doprax_product_version_id" value="{{.Config.DopraxProductVersionID}}"></div>
<div class="field"><label>doprax_location_option_id</label><input name="doprax_location_option_id" value="{{.Config.DopraxLocationOptionID}}"></div>
<div class="field"><label>doprax_os_option_id</label><input name="doprax_os_option_id" value="{{.Config.DopraxOSOptionID}}"></div>
<div class="field"><label>doprax_access_method</label><input name="doprax_access_method" value="{{.Config.DopraxAccessMethod}}"></div>
<div class="field"><label>doprax_login_retry_attempts</label><input name="doprax_login_retry_attempts" value="{{.Config.DopraxLoginRetryAttempts}}"></div>
<div class="field"><label>doprax_login_retry_delay_seconds</label><input name="doprax_login_retry_delay_seconds" value="{{.Config.DopraxLoginRetryDelaySec}}"></div>
<div class="field"><label>caasify_token</label><input name="caasify_token" value="{{.Config.CaasifyToken}}"></div>
<div class="field"><label>product_id</label><input name="product_id" value="{{.Config.ProductID}}"></div>
<div class="field"><label>template</label><input name="template" value="{{.Config.Template}}"></div>
<div class="field"><label>note_prefix</label><input name="note_prefix" value="{{.Config.NotePrefix}}"></div>
<div class="field"><label>ipv4</label><input name="ipv4" value="{{.Config.IPv4}}"></div>
<div class="field"><label>ipv6</label><input name="ipv6" value="{{.Config.IPv6}}"></div>
<div class="field check"><input type="checkbox" name="static_egress_enabled" {{if .Config.StaticEgress.Enabled}}checked{{end}}><label>static egress enabled</label></div>
<div class="field"><label>wg interface</label><input name="static_egress_interface" value="{{.Config.StaticEgress.Interface}}"></div>
<div class="field"><label>wg listen_port</label><input name="static_egress_listen_port" value="{{.Config.StaticEgress.ListenPort}}"></div>
<div class="field"><label>wg subnet</label><input name="static_egress_subnet" value="{{.Config.StaticEgress.Subnet}}"></div>
<div class="field"><label>wg master_tunnel_ip</label><input name="static_egress_master_tunnel_ip" value="{{.Config.StaticEgress.MasterTunnelIP}}"></div>
<div class="field"><label>wg node_route_table</label><input name="static_egress_node_route_table" value="{{.Config.StaticEgress.NodeRouteTable}}"></div>
<div class="field"><label>wg node_route_priority</label><input name="static_egress_node_route_priority" value="{{.Config.StaticEgress.NodeRoutePriority}}"></div>
<div class="field check"><input type="checkbox" name="static_egress_auto_install_packages" {{if .Config.StaticEgress.AutoInstallPackages}}checked{{end}}><label>auto install WireGuard packages</label></div>
<div class="field full"><label>relays JSON</label><textarea name="relays_json">{{.RelaysJSON}}</textarea></div>
<div class="field full"><label>provider_pool JSON</label><textarea name="provider_pool_json">{{.ProviderPoolJSON}}</textarea></div>
<div class="field full"><label>static_egress raw JSON</label><textarea name="static_egress_json">{{.StaticEgressJSON}}</textarea></div>
<div class="field full actions"><button name="save" value="1">Save Config</button><span class="muted">Node-related changes automatically sync and restart all active/standby nodes.</span></div>
</form>
</section>
<section class="section">
<h2>Danger Zone</h2>
<div class="two"><div><form method="post" action="/action"><input type="hidden" name="action" value="reset"><label><input type="checkbox" name="confirm" value="yes"> I understand reset deletes current nodes and state</label><br><br><button class="danger">Reset Fleet</button></form></div><pre>Manual CLI:
./furo-server-master --publish
./furo-server-master --sync-nodes
./furo-server-master --list-nodes
./furo-server-master --reset</pre></div>
</section>
</main>
</body></html>`))

func (a *masterApp) renderAdmin(w http.ResponseWriter, action *masterActionReport) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := masterAdminTemplate.Execute(w, a.adminView(action)); err != nil {
		log.Printf("[FURO-MASTER] admin render failed err=%v", err)
	}
}

func (a *masterApp) runAdminServer() error {
	if a.cfg.AdminListen == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		a.renderAdmin(w, nil)
	})
	mux.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Minute)
		defer cancel()
		report := a.saveConfigFromRequest(ctx, r)
		a.renderAdmin(w, &report)
	})
	mux.HandleFunc("/action", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
		defer cancel()
		var report masterActionReport
		switch r.FormValue("action") {
		case "publish":
			report = a.publishRouteMapReport(ctx)
		case "sync-nodes":
			report = a.syncNodesReport(ctx)
		case "reconcile":
			report = newMasterActionReport("reconcile")
			if err := a.ensureFleet(ctx); err != nil {
				report.OK = false
				report.Message = err.Error()
			} else {
				report.Message = "reconcile completed"
			}
		case "reset":
			if r.FormValue("confirm") != "yes" {
				report = newMasterActionReport("reset")
				report.OK = false
				report.Message = "reset requires confirmation checkbox"
				break
			}
			report = a.resetFleetReport(ctx)
		default:
			report = newMasterActionReport("unknown")
			report.OK = false
			report.Message = "unknown action"
		}
		a.renderAdmin(w, &report)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		state := a.state
		a.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"service":    "furo-server-master",
			"version":    appVersion,
			"commit":     appCommit,
			"uptime_sec": int64(time.Since(a.startedAt).Seconds()),
			"config":     a.cfg,
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
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	if err := a.publishRouteMap(ctx); err != nil {
		log.Printf("[FURO-MASTER] initial route-map publish failed: %v", err)
	}
	cancel()
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

func (a *masterApp) runReconcileLoop() {
	interval := time.Duration(a.cfg.PublishIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		if err := a.ensureFleet(ctx); err != nil {
			log.Printf("[FURO-MASTER] periodic reconcile failed: %v", err)
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

type caasifyAPI interface {
	createVPS(context.Context, caasifyCreateRequest) (caasifyCreateResult, error)
	waitForOrderReady(context.Context, string) (caasifyOrderInfo, error)
	deleteOrder(context.Context, string) error
	listOrders(context.Context) ([]caasifyListedOrder, error)
	renameOrder(context.Context, masterNode, string) (string, error)
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
	OrderID   string
	ServiceID string
	IP        string
	Password  string
}

type caasifyOrderInfo struct {
	OrderID   string
	ServiceID string
	IP        string
	Password  string
}

type caasifyListedOrder struct {
	OrderID   string
	ServiceID string
	Note      string
	IP        string
	Status    string
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

func (c *caasifyClient) listOrders(ctx context.Context) ([]caasifyListedOrder, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.caasify.com/api/orders", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("list orders status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil, err
	}
	rawOrders, _ := decoded["data"].([]any)
	orders := make([]caasifyListedOrder, 0, len(rawOrders))
	for _, raw := range rawOrders {
		item, _ := raw.(map[string]any)
		if item == nil {
			continue
		}
		order := caasifyListedOrder{
			OrderID: jsonString(item, "id"),
			Note:    jsonString(item, "note"),
			IP:      firstIPv4(item),
			Status:  jsonString(item, "status"),
		}
		if order.OrderID != "" {
			orders = append(orders, order)
		}
	}
	return orders, nil
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

func (c *caasifyClient) renameOrder(ctx context.Context, node masterNode, newName string) (string, error) {
	log.Printf("[FURO-MASTER] provider rename skipped backend=caasify id=%s order=%s new_name=%s reason=unsupported", node.ID, node.OrderID, newName)
	return "", nil
}

type dopraxClient struct {
	apiKey             string
	username           string
	password           string
	baseURL            string
	productVersionID   string
	locationOptionID   string
	osOptionID         string
	accessMethod       string
	loginRetryAttempts int
	loginRetryDelay    time.Duration
	client             *http.Client
	mu                 sync.Mutex
	bearerToken        string
}

func newDopraxClient(cfg masterConfigFile) *dopraxClient {
	return &dopraxClient{
		apiKey:             cfg.DopraxAPIKey,
		username:           cfg.DopraxUsername,
		password:           cfg.DopraxPassword,
		baseURL:            strings.TrimRight(cfg.DopraxBaseURL, "/"),
		productVersionID:   cfg.DopraxProductVersionID,
		locationOptionID:   cfg.DopraxLocationOptionID,
		osOptionID:         cfg.DopraxOSOptionID,
		accessMethod:       cfg.DopraxAccessMethod,
		loginRetryAttempts: cfg.DopraxLoginRetryAttempts,
		loginRetryDelay:    time.Duration(cfg.DopraxLoginRetryDelaySec) * time.Second,
		client:             &http.Client{Timeout: 45 * time.Second},
	}
}

func (c *dopraxClient) createVPS(ctx context.Context, input caasifyCreateRequest) (caasifyCreateResult, error) {
	body := map[string]any{
		"product_version_id": c.productVersionID,
		"idempotency_key":    randomUUID(),
		"name":               input.Note,
		"metadata": map[string]any{
			"access_method": c.accessMethod,
		},
		"selections": map[string]any{
			"location": map[string]any{
				"optionId": c.locationOptionID,
			},
			"operating_system": map[string]any{
				"optionId": c.osOptionID,
			},
		},
	}
	var decoded map[string]any
	if err := c.doV2JSON(ctx, http.MethodPost, "/api/v2/services/instances/", body, &decoded); err != nil {
		return caasifyCreateResult{}, err
	}
	data, _ := decoded["data"].(map[string]any)
	serviceID := jsonString(data, "service_id")
	if serviceID == "" {
		return caasifyCreateResult{}, fmt.Errorf("doprax create response missing service_id: %#v", decoded)
	}
	log.Printf("[FURO-MASTER] doprax create accepted service_id=%s name=%s", serviceID, input.Note)
	info, err := c.waitForServiceReady(ctx, serviceID)
	if err != nil {
		return caasifyCreateResult{}, err
	}
	return caasifyCreateResult{OrderID: info.OrderID, ServiceID: serviceID, IP: info.IP, Password: info.Password}, nil
}

func (c *dopraxClient) waitForServiceReady(ctx context.Context, serviceID string) (caasifyOrderInfo, error) {
	var last caasifyOrderInfo
	lastStatus := ""
	for i := 0; i < 90; i++ {
		info, status, err := c.showService(ctx, serviceID)
		if err == nil {
			last = info
			lastStatus = status
			if info.OrderID != "" && info.IP != "" && info.Password != "" && strings.EqualFold(status, "running") {
				info.ServiceID = serviceID
				return info, nil
			}
		} else {
			log.Printf("[FURO-MASTER] doprax service poll failed service_id=%s err=%v", serviceID, err)
		}
		log.Printf("[FURO-MASTER] doprax service waiting service_id=%s status=%s vm_code=%s ip=%s password_set=%t", serviceID, lastStatus, last.OrderID, last.IP, last.Password != "")
		select {
		case <-ctx.Done():
			return caasifyOrderInfo{}, ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}
	return caasifyOrderInfo{}, fmt.Errorf("doprax service not ready service_id=%s status=%s vm_code=%q ip=%q password_set=%t", serviceID, lastStatus, last.OrderID, last.IP, last.Password != "")
}

func (c *dopraxClient) showService(ctx context.Context, serviceID string) (caasifyOrderInfo, string, error) {
	var decoded map[string]any
	if err := c.doV2JSON(ctx, http.MethodGet, "/api/v2/services/instances/"+serviceID+"/", nil, &decoded); err != nil {
		return caasifyOrderInfo{}, "", err
	}
	data, _ := decoded["data"].(map[string]any)
	service, _ := data["service"].(map[string]any)
	access, _ := data["access"].(map[string]any)
	links, _ := data["links"].(map[string]any)
	vm, _ := data["vm"].(map[string]any)
	metadata, _ := service["metadata"].(map[string]any)
	status := jsonString(service, "status")
	vmCode := firstNonEmpty(
		jsonString(links, "vm_code"),
		jsonString(metadata, "vm_code"),
		jsonString(vm, "vm_code"),
	)
	ip := firstNonEmpty(
		jsonString(access, "public_ipv4"),
		jsonString(vm, "ipv4"),
	)
	password := ""
	if vmCode != "" {
		password = c.fetchV2Password(ctx, vmCode)
		if password == "" {
			if info, err := c.showOrder(ctx, vmCode); err == nil {
				password = info.Password
				if ip == "" {
					ip = info.IP
				}
			}
		}
	}
	return caasifyOrderInfo{OrderID: vmCode, ServiceID: serviceID, IP: ip, Password: password}, status, nil
}

func (c *dopraxClient) waitForOrderReady(ctx context.Context, orderID string) (caasifyOrderInfo, error) {
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
	return caasifyOrderInfo{}, fmt.Errorf("doprax vm not ready vm_code=%s ip=%q password_set=%t", orderID, last.IP, last.Password != "")
}

func (c *dopraxClient) showOrder(ctx context.Context, vmCode string) (caasifyOrderInfo, error) {
	var detail map[string]any
	if err := c.doV1JSON(ctx, http.MethodGet, "/api/v1/vms/"+vmCode+"/", nil, &detail); err != nil {
		return caasifyOrderInfo{}, err
	}
	data, _ := detail["data"].(map[string]any)
	vm, _ := data["vm"].(map[string]any)
	access, _ := data["access"].(map[string]any)
	ip := firstNonEmpty(
		jsonString(data, "ipv4"),
		jsonString(data, "public_ipv4"),
		jsonString(vm, "ipv4"),
		jsonString(access, "public_ipv4"),
	)

	var pass map[string]any
	if err := c.doV1JSON(ctx, http.MethodGet, "/api/v1/vms/"+vmCode+"/password/", nil, &pass); err != nil {
		return caasifyOrderInfo{OrderID: vmCode, IP: ip}, err
	}
	passData, _ := pass["data"].(map[string]any)
	password := firstNonEmpty(
		jsonString(passData, "tempPass"),
		jsonString(passData, "password"),
		jsonString(passData, "root_password"),
		jsonString(passData, "temp_pass"),
	)
	return caasifyOrderInfo{OrderID: vmCode, IP: ip, Password: password}, nil
}

func (c *dopraxClient) deleteOrder(ctx context.Context, orderID string) error {
	var decoded map[string]any
	if err := c.doV1JSON(ctx, http.MethodDelete, "/api/v1/vms/"+orderID+"/", nil, &decoded); err != nil {
		return err
	}
	if ok, _ := decoded["success"].(bool); !ok {
		return fmt.Errorf("doprax delete not accepted: %#v", decoded)
	}
	return nil
}

func (c *dopraxClient) listOrders(ctx context.Context) ([]caasifyListedOrder, error) {
	var decoded map[string]any
	if err := c.doV1JSON(ctx, http.MethodGet, "/api/v1/vms/", nil, &decoded); err != nil {
		return nil, err
	}
	rawOrders, _ := decoded["data"].([]any)
	orders := make([]caasifyListedOrder, 0, len(rawOrders))
	for _, raw := range rawOrders {
		item, _ := raw.(map[string]any)
		if item == nil {
			continue
		}
		order := caasifyListedOrder{
			OrderID:   firstNonEmpty(jsonString(item, "vmCode"), jsonString(item, "vm_code")),
			ServiceID: firstNonEmpty(jsonString(item, "service_id"), jsonString(item, "serviceId")),
			Note:      firstNonEmpty(jsonString(item, "name"), jsonString(item, "sysName"), jsonString(item, "sys_name")),
			IP:        firstNonEmpty(jsonString(item, "ipv4"), jsonString(item, "public_ipv4")),
			Status:    jsonString(item, "status"),
		}
		if order.OrderID != "" {
			orders = append(orders, order)
		}
	}
	return orders, nil
}

func (c *dopraxClient) renameOrder(ctx context.Context, node masterNode, newName string) (string, error) {
	serviceID := strings.TrimSpace(node.ServiceID)
	if serviceID == "" {
		var err error
		serviceID, err = c.findServiceIDByVMCode(ctx, node.OrderID)
		if err != nil {
			return "", err
		}
	}
	if serviceID == "" {
		return "", fmt.Errorf("doprax service_id not found for vm_code=%s", node.OrderID)
	}
	body := map[string]any{
		"name":        newName,
		"description": nil,
	}
	var decoded map[string]any
	if err := c.doV2JSON(ctx, http.MethodPut, "/api/v2/services/instances/"+url.PathEscape(serviceID)+"/", body, &decoded); err != nil {
		return serviceID, err
	}
	return serviceID, nil
}

func (c *dopraxClient) findServiceIDByVMCode(ctx context.Context, vmCode string) (string, error) {
	if strings.TrimSpace(vmCode) == "" {
		return "", fmt.Errorf("empty vm_code")
	}
	for page := 1; page <= 10; page++ {
		var decoded map[string]any
		path := fmt.Sprintf("/api/v2/services/instances/?service_type=vm&page=%d&page_size=100", page)
		if err := c.doV2JSON(ctx, http.MethodGet, path, nil, &decoded); err != nil {
			return "", err
		}
		rawItems, _ := decoded["data"].([]any)
		for _, raw := range rawItems {
			item, _ := raw.(map[string]any)
			if item == nil {
				continue
			}
			metadata, _ := item["metadata"].(map[string]any)
			links, _ := item["links"].(map[string]any)
			vm, _ := item["vm"].(map[string]any)
			candidateVMCode := firstNonEmpty(
				jsonString(item, "vmCode"),
				jsonString(item, "vm_code"),
				jsonString(metadata, "vm_code"),
				jsonString(links, "vm_code"),
				jsonString(vm, "vm_code"),
			)
			if candidateVMCode != vmCode {
				continue
			}
			serviceID := firstNonEmpty(jsonString(item, "service_id"), jsonString(item, "serviceId"))
			if serviceID != "" {
				return serviceID, nil
			}
		}
		meta, _ := decoded["meta"].(map[string]any)
		if hasNext, ok := meta["has_next"].(bool); ok && hasNext {
			continue
		}
		break
	}
	return "", fmt.Errorf("doprax service_id not found for vm_code=%s", vmCode)
}

func (c *dopraxClient) login(ctx context.Context) (string, error) {
	c.mu.Lock()
	if c.bearerToken != "" {
		token := c.bearerToken
		c.mu.Unlock()
		return token, nil
	}
	c.mu.Unlock()

	var lastErr error
	attempts := c.loginRetryAttempts
	if attempts <= 0 {
		attempts = 3
	}
	delay := c.loginRetryDelay
	if delay <= 0 {
		delay = 5 * time.Second
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		token, err := c.loginOnce(ctx)
		if err == nil {
			c.mu.Lock()
			c.bearerToken = token
			c.mu.Unlock()
			return token, nil
		}
		lastErr = err
		log.Printf("[FURO-MASTER] doprax login failed attempt=%d/%d err=%v", attempt, attempts, err)
		if attempt == attempts {
			break
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(delay):
		}
	}
	return "", lastErr
}

func (c *dopraxClient) clearBearerToken() {
	c.mu.Lock()
	c.bearerToken = ""
	c.mu.Unlock()
}

func (c *dopraxClient) loginOnce(ctx context.Context) (string, error) {
	body := map[string]string{"user_email": c.username, "user_pass": c.password}
	data, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/account/login-doprax-123321/", bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("accept", "application/json, text/plain, */*")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("origin", c.baseURL)
	req.Header.Set("referer", c.baseURL+"/v/signin/")
	req.Header.Set("user-agent", dopraxUserAgent)
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respData, _ := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("doprax login status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respData)))
	}
	var decoded map[string]any
	if err := json.Unmarshal(respData, &decoded); err != nil {
		return "", err
	}
	token := jsonString(decoded, "cca")
	if data, _ := decoded["data"].(map[string]any); token == "" {
		token = jsonString(data, "cca")
	}
	if token == "" {
		return "", fmt.Errorf("doprax login response missing cca: %s", strings.TrimSpace(string(respData)))
	}
	return token, nil
}

const dopraxUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"

type dopraxHTTPError struct {
	method string
	path   string
	status int
	body   string
}

func (e dopraxHTTPError) Error() string {
	return fmt.Sprintf("%s %s status=%d body=%s", e.method, e.path, e.status, e.body)
}

func isDopraxAuthError(err error) bool {
	if err == nil {
		return false
	}
	var httpErr dopraxHTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	if httpErr.status == http.StatusUnauthorized || httpErr.status == http.StatusForbidden {
		return true
	}
	body := strings.ToLower(httpErr.body)
	return strings.Contains(body, "unauthorized") ||
		strings.Contains(body, "forbidden") ||
		strings.Contains(body, "invalid token") ||
		strings.Contains(body, "token") && strings.Contains(body, "expired") ||
		strings.Contains(body, "authentication")
}

func (c *dopraxClient) doV1JSON(ctx context.Context, method, path string, body any, out any) error {
	return c.doJSON(ctx, method, path, "", body, out)
}

func (c *dopraxClient) doV2JSON(ctx context.Context, method, path string, body any, out any) error {
	token, err := c.login(ctx)
	if err != nil {
		return err
	}
	err = c.doJSON(ctx, method, path, token, body, out)
	if !isDopraxAuthError(err) {
		return err
	}
	log.Printf("[FURO-MASTER] doprax auth failed for %s %s; refreshing token and retrying once", method, path)
	c.clearBearerToken()
	token, loginErr := c.login(ctx)
	if loginErr != nil {
		return loginErr
	}
	return c.doJSON(ctx, method, path, token, body, out)
}

func (c *dopraxClient) doJSON(ctx context.Context, method, path, bearerToken string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("accept", "application/json, text/plain, */*")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("user-agent", dopraxUserAgent)
	if bearerToken != "" {
		req.Header.Set("authorization", "Bearer "+bearerToken)
		req.Header.Set("cookie", "cca="+bearerToken)
		req.Header.Set("origin", c.baseURL)
		req.Header.Set("referer", c.baseURL+"/v/virtual-machines/new")
		req.Header.Set("sec-fetch-dest", "empty")
		req.Header.Set("sec-fetch-mode", "cors")
		req.Header.Set("sec-fetch-site", "same-origin")
	} else {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return dopraxHTTPError{method: method, path: path, status: resp.StatusCode, body: strings.TrimSpace(string(data))}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return err
	}
	if decoded, ok := out.(*map[string]any); ok {
		if success, exists := (*decoded)["success"].(bool); exists && !success {
			return dopraxHTTPError{method: method, path: path, status: resp.StatusCode, body: strings.TrimSpace(string(data))}
		}
	}
	return nil
}

func (c *dopraxClient) fetchV2Password(ctx context.Context, vmCode string) string {
	var decoded map[string]any
	if err := c.doV2JSON(ctx, http.MethodGet, "/api/v2/vms/"+vmCode+"/actions/access/", nil, &decoded); err != nil {
		log.Printf("[FURO-MASTER] doprax password fetch failed vm_code=%s err=%v", vmCode, err)
		return ""
	}
	data, _ := decoded["data"].(map[string]any)
	return firstNonEmpty(jsonString(data, "tempPass"), jsonString(data, "password"), jsonString(data, "temp_pass"))
}

func randomUUID() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func generateWireGuardKeyPair() (string, string, error) {
	var raw [32]byte
	if _, err := crand.Read(raw[:]); err != nil {
		return "", "", err
	}
	curve := ecdh.X25519()
	private, err := curve.NewPrivateKey(raw[:])
	if err != nil {
		return "", "", err
	}
	return base64.StdEncoding.EncodeToString(raw[:]), base64.StdEncoding.EncodeToString(private.PublicKey().Bytes()), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
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

func printActionReport(w io.Writer, report masterActionReport) {
	status := "OK"
	if !report.OK {
		status = "FAILED"
	}
	fmt.Fprintf(w, "%s %s: %s\n", status, report.Action, report.Message)
	for _, detail := range report.Details {
		fmt.Fprintf(w, "- %s\n", detail)
	}
	if len(report.Data) > 0 {
		data, _ := json.MarshalIndent(report.Data, "", "  ")
		fmt.Fprintf(w, "%s\n", data)
	}
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
	if *listNodesFlag {
		app.printNodesTable(os.Stdout)
		return
	}
	if *publishFlag {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		report := app.publishRouteMapReport(ctx)
		cancel()
		printActionReport(os.Stdout, report)
		if !report.OK {
			os.Exit(1)
		}
		return
	}
	if *syncNodesFlag {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		report := app.syncNodesReport(ctx)
		cancel()
		printActionReport(os.Stdout, report)
		if !report.OK {
			os.Exit(1)
		}
		return
	}
	if *resetFlag {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
		report := app.resetFleetReport(ctx)
		cancel()
		printActionReport(os.Stdout, report)
		if !report.OK {
			os.Exit(1)
		}
		return
	}
	if cfg.StaticEgress.Enabled {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		if err := app.configureMasterStaticEgress(ctx); err != nil {
			log.Printf("[FURO-MASTER] static egress unavailable; nodes will bootstrap without static egress: %v", err)
		}
		cancel()
	}
	log.Printf("[FURO-MASTER] starting namespace=%s fleet_id=%s provider_backend=%s listen=%s admin_listen=%s public_url=%s relay_url=%s backup_count=%d state_file=%s", cfg.Namespace, app.state.FleetID, cfg.ProviderBackend, cfg.Listen, cfg.AdminListen, cfg.PublicURL, cfg.RelayURL, cfg.BackupCount, app.statePath)
	if cfg.AdminListen != "" {
		go func() {
			if err := app.runAdminServer(); err != nil {
				log.Fatalf("admin server failed: %v", err)
			}
		}()
	}
	go func() {
		if err := app.runControlServer(); err != nil {
			log.Fatalf("control server failed: %v", err)
		}
	}()
	go app.runPublishLoop()
	go app.runReconcileLoop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	if err := app.ensureFleet(ctx); err != nil {
		log.Printf("[FURO-MASTER] initial reconcile failed; master stays running and will retry: %v", err)
	}
	cancel()
	select {}
}
