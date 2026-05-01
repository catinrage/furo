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
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	airsBaseURL              = "https://napi.arvancloud.ir/ecc/v1"
	airsDefaultConfigPath    = "config.client.json"
	airsDefaultCheckInterval = 10
	airsDefaultIPType        = "ipv4"
	airsRequestTimeout       = 60 * time.Second
	airsRenewTimeout         = 8 * time.Minute
	airsPollInterval         = 5 * time.Second
)

var (
	airsConfigPath = flag.String("c", airsDefaultConfigPath, "Path to shared client config JSON")
	airsOnce       = flag.Bool("once", false, "Run one renewal and exit")
	airsCheckOnce  = flag.Bool("check-once", false, "Run one inspect check and exit")
	airsVerbose    = flag.Bool("verbose", false, "Print AIRS logs to stdout")
)

type airsClientConfig struct {
	PublicHost string            `json:"public_host"`
	Routes     []airsRouteConfig `json:"routes"`
	AIRS       airsConfig        `json:"airs"`
}

type airsRouteConfig struct {
	ID         string `json:"id"`
	PublicHost string `json:"public_host,omitempty"`
}

type airsConfig struct {
	ArvanAPIKey               string `json:"arvan_api_key"`
	ArvanRegion               string `json:"arvan_region"`
	ArvanServerID             string `json:"arvan_server_id"`
	FixedPublicIP             string `json:"fixed_public_ip,omitempty"`
	AutoRenewIntervalSeconds  int    `json:"auto_renew_interval_seconds"`
	CheckIntervalSeconds      int    `json:"check_interval_seconds"`
	LogFile                   string `json:"log_file"`
	SwitchScript              string `json:"switch_script,omitempty"`
	InspectBinary             string `json:"inspect_binary,omitempty"`
	ServiceScript             string `json:"service_script,omitempty"`
	ClientServiceRole         string `json:"client_service_role,omitempty"`
	OutboundCheckURL          string `json:"outbound_check_url,omitempty"`
	OutboundCheckRetrySeconds int    `json:"outbound_check_retry_seconds,omitempty"`
	PostAddWaitSeconds        int    `json:"post_add_wait_seconds,omitempty"`
	PostDetachWaitSeconds     int    `json:"post_detach_wait_seconds,omitempty"`
}

type airsRoute struct {
	ID         string
	PublicHost string
}

type airsRuntimeConfig struct {
	ConfigPath                string
	WorkDir                   string
	PublicHost                string
	Routes                    []airsRoute
	ArvanAPIKey               string
	ArvanRegion               string
	ArvanServerID             string
	FixedPublicIP             string
	AutoRenewInterval         time.Duration
	CheckInterval             time.Duration
	LogFile                   string
	SwitchScript              string
	InspectBinary             string
	ServiceScript             string
	ClientServiceRole         string
	OutboundCheckURL          string
	OutboundCheckRetryTimeout time.Duration
	PostAddWait               time.Duration
	PostDetachWait            time.Duration
}

type arvanServer struct {
	ID             string               `json:"id"`
	Name           string               `json:"name"`
	Addresses      map[string][]arvanIP `json:"addresses"`
	IPs            []arvanIPInfo        `json:"ips"`
	SecurityGroups []arvanSecurityGroup `json:"security_groups"`
}

type arvanIP struct {
	Addr     string `json:"addr"`
	Type     string `json:"type"`
	IsPublic bool   `json:"is_public"`
}

type arvanIPInfo struct {
	IP         string `json:"ip"`
	PortID     string `json:"port_id"`
	Public     bool   `json:"public"`
	SubnetID   string `json:"subnet_id"`
	SubnetName string `json:"subnet_name"`
	Version    string `json:"version"`
}

type arvanSecurityGroup struct {
	ID string `json:"id"`
}

type arvanServerIPInfo struct {
	ID     string             `json:"id"`
	IPData []arvanFloatIPData `json:"ip_data"`
}

type arvanFloatIPData struct {
	Address  string `json:"address"`
	PortID   string `json:"port_id"`
	SubnetID string `json:"subnet_id"`
	Type     string `json:"type"`
}

type arvanEnvelope[T any] struct {
	Data T `json:"data"`
}

type airsManager struct {
	cfg    airsRuntimeConfig
	client *http.Client
	logger *log.Logger
	logOut io.Closer
	mu     sync.Mutex
}

func main() {
	flag.Parse()

	cfg, err := loadAIRSConfig(*airsConfigPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "airs config: %v\n", err)
		os.Exit(1)
	}

	manager, err := newAIRSManager(cfg, *airsVerbose || *airsOnce || *airsCheckOnce)
	if err != nil {
		fmt.Fprintf(os.Stderr, "airs init: %v\n", err)
		os.Exit(1)
	}
	defer manager.close()

	switch {
	case *airsOnce:
		if err := manager.renew(context.Background(), "manual once"); err != nil {
			manager.logf("renew failed: %v", err)
			os.Exit(1)
		}
	case *airsCheckOnce:
		if err := manager.inspect(context.Background()); err != nil {
			manager.logf("inspect failed: %v", err)
			os.Exit(1)
		}
		manager.logf("inspect succeeded")
	default:
		manager.run(context.Background())
	}
}

func loadAIRSConfig(path string) (airsRuntimeConfig, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return airsRuntimeConfig{}, err
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		return airsRuntimeConfig{}, err
	}

	var fileCfg airsClientConfig
	if err := json.Unmarshal(data, &fileCfg); err != nil {
		return airsRuntimeConfig{}, err
	}

	workDir := filepath.Dir(absPath)
	cfg := fileCfg.AIRS
	if cfg.CheckIntervalSeconds <= 0 {
		cfg.CheckIntervalSeconds = airsDefaultCheckInterval
	}
	if cfg.SwitchScript == "" {
		cfg.SwitchScript = "switch-outbound-ip.sh"
	}
	if cfg.InspectBinary == "" {
		cfg.InspectBinary = "inspect"
	}
	if cfg.ServiceScript == "" {
		cfg.ServiceScript = "service.sh"
	}
	if cfg.ClientServiceRole == "" {
		cfg.ClientServiceRole = "client"
	}
	if cfg.OutboundCheckURL == "" {
		cfg.OutboundCheckURL = "https://chabokan.net/ip/"
	}
	if cfg.OutboundCheckRetrySeconds <= 0 {
		cfg.OutboundCheckRetrySeconds = int(airsRenewTimeout.Seconds())
	}
	if cfg.PostAddWaitSeconds < 0 {
		cfg.PostAddWaitSeconds = 0
	}
	if cfg.PostDetachWaitSeconds < 0 {
		cfg.PostDetachWaitSeconds = 0
	}
	cfg.FixedPublicIP = strings.TrimSpace(cfg.FixedPublicIP)
	if cfg.FixedPublicIP != "" && net.ParseIP(cfg.FixedPublicIP) == nil {
		return airsRuntimeConfig{}, fmt.Errorf("airs.fixed_public_ip is not a valid IP address: %q", cfg.FixedPublicIP)
	}

	switch {
	case cfg.ArvanAPIKey == "":
		return airsRuntimeConfig{}, errors.New("airs.arvan_api_key is required")
	case cfg.ArvanRegion == "":
		return airsRuntimeConfig{}, errors.New("airs.arvan_region is required")
	case cfg.ArvanServerID == "":
		return airsRuntimeConfig{}, errors.New("airs.arvan_server_id is required")
	}

	routes := make([]airsRoute, 0, len(fileCfg.Routes))
	for _, route := range fileCfg.Routes {
		routes = append(routes, airsRoute{ID: route.ID, PublicHost: route.PublicHost})
	}

	return airsRuntimeConfig{
		ConfigPath:                absPath,
		WorkDir:                   workDir,
		PublicHost:                fileCfg.PublicHost,
		Routes:                    routes,
		ArvanAPIKey:               cfg.ArvanAPIKey,
		ArvanRegion:               cfg.ArvanRegion,
		ArvanServerID:             cfg.ArvanServerID,
		FixedPublicIP:             cfg.FixedPublicIP,
		AutoRenewInterval:         time.Duration(cfg.AutoRenewIntervalSeconds) * time.Second,
		CheckInterval:             time.Duration(cfg.CheckIntervalSeconds) * time.Second,
		LogFile:                   resolveOptionalAIRSPath(workDir, cfg.LogFile),
		SwitchScript:              resolveAIRSPath(workDir, cfg.SwitchScript),
		InspectBinary:             resolveAIRSPath(workDir, cfg.InspectBinary),
		ServiceScript:             resolveAIRSPath(workDir, cfg.ServiceScript),
		ClientServiceRole:         cfg.ClientServiceRole,
		OutboundCheckURL:          cfg.OutboundCheckURL,
		OutboundCheckRetryTimeout: time.Duration(cfg.OutboundCheckRetrySeconds) * time.Second,
		PostAddWait:               time.Duration(cfg.PostAddWaitSeconds) * time.Second,
		PostDetachWait:            time.Duration(cfg.PostDetachWaitSeconds) * time.Second,
	}, nil
}

func resolveAIRSPath(workDir, value string) string {
	if filepath.IsAbs(value) {
		return value
	}
	return filepath.Join(workDir, value)
}

func resolveOptionalAIRSPath(workDir, value string) string {
	if value == "" {
		return ""
	}
	return resolveAIRSPath(workDir, value)
}

func newAIRSManager(cfg airsRuntimeConfig, console bool) (*airsManager, error) {
	out := io.Writer(io.Discard)
	var closer io.Closer
	if cfg.LogFile != "" {
		file, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		out = file
		closer = file
	}
	if console {
		if cfg.LogFile != "" {
			out = io.MultiWriter(out, os.Stdout)
		} else {
			out = os.Stdout
		}
	}
	return &airsManager{
		cfg:    cfg,
		client: &http.Client{Timeout: airsRequestTimeout},
		logger: log.New(out, "AIRS ", log.LstdFlags|log.Lmicroseconds),
		logOut: closer,
	}, nil
}

func (m *airsManager) close() {
	if m.logOut != nil {
		_ = m.logOut.Close()
	}
}

func (m *airsManager) run(ctx context.Context) {
	m.logf("started check_interval=%s auto_renew_interval=%s", m.cfg.CheckInterval, m.cfg.AutoRenewInterval)

	checkTicker := time.NewTicker(m.cfg.CheckInterval)
	defer checkTicker.Stop()

	var renewTicker *time.Ticker
	var renewC <-chan time.Time
	if m.cfg.AutoRenewInterval > 0 {
		renewTicker = time.NewTicker(m.cfg.AutoRenewInterval)
		defer renewTicker.Stop()
		renewC = renewTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			m.logf("stopped: %v", ctx.Err())
			return
		case <-checkTicker.C:
			if err := m.inspect(ctx); err != nil {
				m.logf("inspect failed: %v", err)
				if err := m.renew(ctx, "inspect failure"); err != nil {
					m.logf("renew after inspect failure failed: %v", err)
				}
			} else {
				m.logf("inspect succeeded")
			}
		case <-renewC:
			if err := m.renew(ctx, "scheduled"); err != nil {
				m.logf("scheduled renew failed: %v", err)
			}
		}
	}
}

func (m *airsManager) inspect(ctx context.Context) error {
	start := time.Now()
	cmd := exec.CommandContext(ctx, m.cfg.InspectBinary, "-c", m.cfg.ConfigPath)
	cmd.Dir = m.cfg.WorkDir
	output, err := cmd.CombinedOutput()
	duration := time.Since(start)
	if err != nil {
		return fmt.Errorf("inspect duration=%s err=%w output=%s", duration.Round(time.Millisecond), err, strings.TrimSpace(string(output)))
	}
	m.logf("inspect duration=%s result=success", duration.Round(time.Millisecond))
	return nil
}

func (m *airsManager) renew(ctx context.Context, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logf("renew started reason=%q", reason)
	currentCfg, err := loadAIRSConfig(m.cfg.ConfigPath)
	if err != nil {
		return err
	}
	m.cfg = currentCfg

	oldIP := strings.TrimSpace(currentCfg.PublicHost)
	before, err := m.listServerIPs(ctx)
	if err != nil {
		return err
	}
	m.logf("current public_host=%s fixed_public_ip=%s server_ips=%s", oldIP, m.cfg.FixedPublicIP, formatAIRSIPs(before))
	if m.cfg.FixedPublicIP != "" && !containsAIRSIP(before, m.cfg.FixedPublicIP) {
		m.logf("warning: fixed_public_ip=%s is not currently attached to server %s", m.cfg.FixedPublicIP, m.cfg.ArvanServerID)
	}

	m.logf("requesting new Arvan public IP for server=%s region=%s", m.cfg.ArvanServerID, m.cfg.ArvanRegion)
	if err := m.addPublicIP(ctx); err != nil {
		return fmt.Errorf("add public ip: %w", err)
	}
	if m.cfg.PostAddWait > 0 {
		m.logf("waiting after add duration=%s", m.cfg.PostAddWait)
		time.Sleep(m.cfg.PostAddWait)
	}

	m.logf("waiting for new public IP to appear")
	after, newIP, err := m.waitForNewIP(ctx, before)
	if err != nil {
		return err
	}
	m.logf("new ip acquired ip=%s all_ips=%s", newIP.Address, formatAIRSIPs(after))

	switch {
	case oldIP == "":
		m.logf("no old public_host configured; skipping detach")
	case oldIP == newIP.Address:
		m.logf("old public_host already equals new IP; skipping detach ip=%s", oldIP)
	case m.isFixedPublicIP(oldIP):
		m.logf("skipping detach for fixed_public_ip=%s", oldIP)
	default:
		if err := m.detachIP(ctx, oldIP, after); err != nil {
			return fmt.Errorf("detach old ip %s: %w", oldIP, err)
		}
		if m.cfg.PostDetachWait > 0 {
			m.logf("waiting after detach duration=%s", m.cfg.PostDetachWait)
			time.Sleep(m.cfg.PostDetachWait)
		}
	}

	m.logf("switching outbound IP to %s", newIP.Address)
	if err := m.switchOutbound(ctx, newIP.Address); err != nil {
		return err
	}
	m.logf("checking outbound IP until it becomes %s", newIP.Address)
	if err := m.waitOutbound(ctx, newIP.Address); err != nil {
		return err
	}
	m.logf("updating client config public_host to %s", newIP.Address)
	if err := updateClientPublicHost(m.cfg.ConfigPath, oldIP, newIP.Address); err != nil {
		return err
	}
	m.logf("config public_host updated old=%s new=%s", oldIP, newIP.Address)

	m.logf("running post-renew inspect")
	if err := m.inspect(ctx); err != nil {
		return fmt.Errorf("post-renew inspect: %w", err)
	}
	m.logf("restarting client service role=%s", m.cfg.ClientServiceRole)
	if err := m.restartClient(ctx); err != nil {
		return err
	}
	m.logf("renew completed new_ip=%s", newIP.Address)
	return nil
}

type airsIPAttachment struct {
	Address  string
	PortID   string
	SubnetID string
	Type     string
}

func (m *airsManager) listServerIPs(ctx context.Context) ([]airsIPAttachment, error) {
	payload, err := arvanDo[arvanEnvelope[[]arvanServerIPInfo]](ctx, m, http.MethodGet, fmt.Sprintf("regions/%s/float-ips/ips", m.cfg.ArvanRegion), nil)
	if err != nil {
		return nil, err
	}
	var out []airsIPAttachment
	for _, server := range payload.Data {
		if server.ID != m.cfg.ArvanServerID {
			continue
		}
		for _, ip := range server.IPData {
			if ip.Type != "" && ip.Type != "public" {
				continue
			}
			out = append(out, airsIPAttachment{
				Address:  ip.Address,
				PortID:   ip.PortID,
				SubnetID: ip.SubnetID,
				Type:     ip.Type,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Address < out[j].Address })
	return out, nil
}

func (m *airsManager) getServer(ctx context.Context) (arvanServer, error) {
	payload, err := arvanDo[arvanEnvelope[arvanServer]](ctx, m, http.MethodGet, fmt.Sprintf("regions/%s/servers/%s", m.cfg.ArvanRegion, m.cfg.ArvanServerID), nil)
	if err == nil && payload.Data.ID != "" {
		return payload.Data, nil
	}

	listPayload, listErr := arvanDo[arvanEnvelope[[]arvanServer]](ctx, m, http.MethodGet, fmt.Sprintf("regions/%s/servers", m.cfg.ArvanRegion), nil)
	if listErr != nil {
		if err != nil {
			return arvanServer{}, err
		}
		return arvanServer{}, listErr
	}
	for _, server := range listPayload.Data {
		if server.ID == m.cfg.ArvanServerID {
			return server, nil
		}
	}
	return arvanServer{}, fmt.Errorf("server %s not found", m.cfg.ArvanServerID)
}

func (m *airsManager) addPublicIP(ctx context.Context) error {
	server, err := m.getServer(ctx)
	if err != nil {
		return err
	}
	securityGroups := uniqueSecurityGroupIDs(server.SecurityGroups)
	body := map[string]any{
		"type": airsDefaultIPType,
	}
	if len(securityGroups) > 0 {
		body["security_groups"] = securityGroups
	}
	_, err = arvanDo[map[string]any](ctx, m, http.MethodPost, fmt.Sprintf("regions/%s/servers/%s/add-public-ip", m.cfg.ArvanRegion, m.cfg.ArvanServerID), body)
	return err
}

func uniqueSecurityGroupIDs(groups []arvanSecurityGroup) []string {
	seen := make(map[string]struct{}, len(groups))
	var out []string
	for _, group := range groups {
		if group.ID == "" {
			continue
		}
		if _, ok := seen[group.ID]; ok {
			continue
		}
		seen[group.ID] = struct{}{}
		out = append(out, group.ID)
	}
	return out
}

func (m *airsManager) waitForNewIP(ctx context.Context, before []airsIPAttachment) ([]airsIPAttachment, airsIPAttachment, error) {
	beforeSet := make(map[string]struct{}, len(before))
	for _, ip := range before {
		beforeSet[ip.Address] = struct{}{}
	}
	deadline := time.Now().Add(airsRenewTimeout)
	for time.Now().Before(deadline) {
		current, err := m.listServerIPs(ctx)
		if err != nil {
			m.logf("poll new ip failed: %v", err)
		} else {
			for _, ip := range current {
				if _, exists := beforeSet[ip.Address]; !exists && ip.Address != "" && ip.PortID != "" {
					return current, ip, nil
				}
			}
		}
		if err := sleepContext(ctx, airsPollInterval); err != nil {
			return nil, airsIPAttachment{}, err
		}
	}
	return nil, airsIPAttachment{}, errors.New("timed out waiting for new public ip")
}

func (m *airsManager) detachIP(ctx context.Context, address string, ips []airsIPAttachment) error {
	if m.isFixedPublicIP(address) {
		return fmt.Errorf("refusing to detach fixed_public_ip %s", address)
	}
	for _, ip := range ips {
		if ip.Address != address {
			continue
		}
		if ip.PortID == "" {
			return fmt.Errorf("ip %s has no port_id", address)
		}
		m.logf("detaching ip=%s port_id=%s", ip.Address, ip.PortID)
		body := map[string]any{"server_id": m.cfg.ArvanServerID}
		_, err := arvanDo[map[string]any](ctx, m, http.MethodPatch, fmt.Sprintf("regions/%s/networks/%s/detach", m.cfg.ArvanRegion, ip.PortID), body)
		return err
	}
	return fmt.Errorf("ip %s not found in server ip list", address)
}

func (m *airsManager) isFixedPublicIP(address string) bool {
	return m.cfg.FixedPublicIP != "" && strings.TrimSpace(address) == m.cfg.FixedPublicIP
}

func containsAIRSIP(ips []airsIPAttachment, address string) bool {
	for _, ip := range ips {
		if ip.Address == address {
			return true
		}
	}
	return false
}

func (m *airsManager) switchOutbound(ctx context.Context, ip string) error {
	deadline := time.Now().Add(m.cfg.OutboundCheckRetryTimeout)
	for time.Now().Before(deadline) {
		cmd := exec.CommandContext(ctx, m.cfg.SwitchScript, "--quiet", ip)
		cmd.Dir = m.cfg.WorkDir
		output, err := cmd.CombinedOutput()
		if err == nil {
			m.logf("switch outbound script succeeded ip=%s", ip)
			return nil
		}
		m.logf("switch outbound script failed ip=%s err=%v output=%s", ip, err, strings.TrimSpace(string(output)))
		if err := sleepContext(ctx, airsPollInterval); err != nil {
			return err
		}
	}
	return fmt.Errorf("timed out switching outbound ip to %s", ip)
}

func (m *airsManager) waitOutbound(ctx context.Context, expected string) error {
	deadline := time.Now().Add(m.cfg.OutboundCheckRetryTimeout)
	for time.Now().Before(deadline) {
		got, err := m.currentOutboundIP(ctx)
		if err != nil {
			m.logf("outbound ip check failed: %v", err)
		} else {
			m.logf("outbound ip check expected=%s got=%s", expected, got)
			if got == expected {
				return nil
			}
		}
		if err := m.switchOutbound(ctx, expected); err != nil {
			m.logf("retry switch outbound failed: %v", err)
		}
		if err := sleepContext(ctx, airsPollInterval); err != nil {
			return err
		}
	}
	return fmt.Errorf("outbound ip did not become %s", expected)
}

func (m *airsManager) currentOutboundIP(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.cfg.OutboundCheckURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	value := strings.TrimSpace(string(body))
	ip := parseOutboundIPResponse(value)
	if ip == nil {
		return "", fmt.Errorf("invalid outbound ip response %q", value)
	}
	return ip.String(), nil
}

func parseOutboundIPResponse(value string) net.IP {
	if ip := net.ParseIP(strings.TrimSpace(value)); ip != nil {
		return ip
	}

	var obj map[string]any
	if err := json.Unmarshal([]byte(value), &obj); err != nil {
		return nil
	}
	for _, key := range []string{"ip", "origin", "query", "address"} {
		raw, ok := obj[key].(string)
		if !ok {
			continue
		}
		if ip := net.ParseIP(strings.TrimSpace(raw)); ip != nil {
			return ip
		}
	}
	return nil
}

func updateClientPublicHost(path, oldIP, newIP string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return err
	}
	doc["public_host"] = newIP
	if routes, ok := doc["routes"].([]any); ok {
		for _, rawRoute := range routes {
			route, ok := rawRoute.(map[string]any)
			if !ok {
				continue
			}
			current, _ := route["public_host"].(string)
			if current == "" || current == oldIP {
				route["public_host"] = newIP
			}
		}
	}
	encoded, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, encoded, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (m *airsManager) restartClient(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, m.cfg.ServiceScript, m.cfg.ClientServiceRole, "restart")
	cmd.Dir = m.cfg.WorkDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("restart client service: %w output=%s", err, strings.TrimSpace(string(output)))
	}
	m.logf("client service restarted role=%s", m.cfg.ClientServiceRole)
	return nil
}

func arvanDo[T any](ctx context.Context, m *airsManager, method, path string, body any) (T, error) {
	var zero T
	var requestBody io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return zero, err
		}
		requestBody = bytes.NewReader(encoded)
	}
	url := strings.TrimRight(airsBaseURL, "/") + "/" + strings.TrimLeft(path, "/")
	req, err := http.NewRequestWithContext(ctx, method, url, requestBody)
	if err != nil {
		return zero, err
	}
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "fa")
	req.Header.Set("Authorization", normalizeArvanAuthorization(m.cfg.ArvanAPIKey))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return zero, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		return zero, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return zero, fmt.Errorf("%s %s failed with %d: %s", method, url, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	if len(respBody) == 0 {
		return zero, nil
	}
	if err := json.Unmarshal(respBody, &zero); err != nil {
		return zero, fmt.Errorf("decode %s %s: %w: %s", method, url, err, strings.TrimSpace(string(respBody)))
	}
	return zero, nil
}

func normalizeArvanAuthorization(value string) string {
	trimmed := strings.TrimSpace(value)
	if strings.HasPrefix(strings.ToLower(trimmed), "apikey ") || strings.HasPrefix(strings.ToLower(trimmed), "bearer ") {
		return trimmed
	}
	return "Apikey " + trimmed
}

func formatAIRSIPs(ips []airsIPAttachment) string {
	parts := make([]string, 0, len(ips))
	for _, ip := range ips {
		parts = append(parts, fmt.Sprintf("%s(port=%s)", ip.Address, ip.PortID))
	}
	return strings.Join(parts, ",")
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (m *airsManager) logf(format string, args ...any) {
	m.logger.Printf(format, args...)
}
