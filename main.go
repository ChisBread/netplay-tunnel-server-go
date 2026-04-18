// RetroArch - A frontend for libretro.
// Copyright (C) 2021-2022 - Libretro team & Chisbread
//
// RetroArch is free software: you can redistribute it and/or modify it under the terms
// of the GNU General Public License as published by the Free Software Found-
// ation, either version 3 of the License, or (at your option) any later version.
//
// RetroArch is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
// without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
// PURPOSE.  See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with RetroArch.
// If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/ini.v1"
)

// ─── Constants ───────────────────────────────────────────────────────────────

const (
	magicSize  = 4
	uniqueSize = 12
)

var (
	magicSession = [magicSize]byte{'R', 'A', 'T', 'S'}
	magicLink    = [magicSize]byte{'R', 'A', 'T', 'L'}
	magicAddress = [magicSize]byte{'R', 'A', 'T', 'A'}
	magicPing    = [magicSize]byte{'R', 'A', 'T', 'P'}
	magicRANP    = [magicSize]byte{'R', 'A', 'N', 'P'}

	invalidUnique [uniqueSize]byte // all zeros
)

// ─── Log Level ───────────────────────────────────────────────────────────────

type LogLevel int

const (
	LogNone  LogLevel = 0
	LogError LogLevel = 1
	LogWarn  LogLevel = 2
	LogInfo  LogLevel = 3
)

func parseLogLevel(s string) (LogLevel, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "NONE":
		return LogNone, nil
	case "ERROR":
		return LogError, nil
	case "WARN":
		return LogWarn, nil
	case "INFO":
		return LogInfo, nil
	default:
		return LogNone, fmt.Errorf("invalid log level: %s", s)
	}
}

func (l LogLevel) String() string {
	switch l {
	case LogError:
		return "ERROR"
	case LogWarn:
		return "WARN"
	case LogInfo:
		return "INFO"
	default:
		return "NONE"
	}
}

// ─── Logger ──────────────────────────────────────────────────────────────────

type Logger struct {
	mu    sync.Mutex
	path  string
	level LogLevel
}

func NewLogger(path string, level LogLevel) *Logger {
	return &Logger{path: path, level: level}
}

func (l *Logger) Log(level LogLevel, format string, args ...any) {
	if level == LogNone || l.level < level {
		return
	}
	msg := fmt.Sprintf(format, args...)
	now := time.Now()
	line := fmt.Sprintf("(%02d/%02d/%04d %02d:%02d:%02d) [%s] %s\n",
		now.Day(), now.Month(), now.Year(),
		now.Hour(), now.Minute(), now.Second(),
		level, msg)

	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	f.WriteString(line)
}

func (l *Logger) Info(format string, args ...any)  { l.Log(LogInfo, format, args...) }
func (l *Logger) Warn(format string, args ...any)  { l.Log(LogWarn, format, args...) }
func (l *Logger) Error(format string, args ...any) { l.Log(LogError, format, args...) }

// ─── Config ──────────────────────────────────────────────────────────────────

type Config struct {
	Port              int
	Timeout           time.Duration
	MaxSessions       int
	MaxClientsPerSess int
	LogPath           string
	LogLevel          LogLevel
}

func LoadConfig(path string) (*Config, error) {
	cfg, err := ini.Load(path)
	if err != nil {
		return nil, fmt.Errorf("configuration not found: %w", err)
	}

	server := cfg.Section("Server")
	port, err := server.Key("Port").Int()
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid server port")
	}
	timeoutSec, err := server.Key("Timeout").Float64()
	if err != nil || timeoutSec <= 0 {
		return nil, fmt.Errorf("invalid server timeout")
	}

	session := cfg.Section("Session")
	maxSess, err := session.Key("Max").Int()
	if err != nil || maxSess < 0 {
		return nil, fmt.Errorf("invalid session max")
	}
	maxClients, err := session.Key("Clients").Int()
	if err != nil || maxClients < 0 {
		return nil, fmt.Errorf("invalid session clients")
	}

	logSec := cfg.Section("Log")
	logPath := logSec.Key("Path").String()
	if logPath == "" {
		return nil, fmt.Errorf("log path not found")
	}
	logLevel, err := parseLogLevel(logSec.Key("Level").String())
	if err != nil {
		return nil, err
	}

	return &Config{
		Port:              port,
		Timeout:           time.Duration(timeoutSec * float64(time.Second)),
		MaxSessions:       maxSess,
		MaxClientsPerSess: maxClients,
		LogPath:           logPath,
		LogLevel:          logLevel,
	}, nil
}

// ─── Unique ID helpers ───────────────────────────────────────────────────────

type UniqueID [uniqueSize]byte

func (u UniqueID) IsInvalid() bool {
	return u == invalidUnique
}

func (u UniqueID) String() string {
	return fmt.Sprintf("%x", [uniqueSize]byte(u))
}

func randomUnique() UniqueID {
	var u UniqueID
	rand.Read(u[:])
	return u
}

// ─── readExact / writeAll helpers ────────────────────────────────────────────

func readExact(conn net.Conn, n int, timeout time.Duration) ([]byte, error) {
	buf := make([]byte, n)
	conn.SetReadDeadline(time.Now().Add(timeout))
	_, err := io.ReadFull(conn, buf)
	return buf, err
}

func writeAll(conn net.Conn, data []byte) error {
	_, err := conn.Write(data)
	return err
}

// ─── TunnelServer ────────────────────────────────────────────────────────────

type TunnelServer struct {
	config *Config
	logger *Logger

	mu       sync.Mutex
	clients  map[UniqueID]interface{} // *SessionOwner | *SessionUserClient | *SessionUserHost
	sessions int
}

func NewTunnelServer(config *Config, logger *Logger) *TunnelServer {
	return &TunnelServer{
		config:  config,
		logger:  logger,
		clients: make(map[UniqueID]interface{}),
	}
}

func (s *TunnelServer) requestSession() bool {
	if s.config.MaxSessions == 0 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions >= s.config.MaxSessions {
		return false
	}
	s.sessions++
	return true
}

func (s *TunnelServer) freeSession() {
	if s.config.MaxSessions == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions > 0 {
		s.sessions--
	}
}

func (s *TunnelServer) addClient(unique UniqueID, client interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[unique] = client
}

func (s *TunnelServer) removeClient(unique UniqueID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, unique)
}

func (s *TunnelServer) generateUnique() UniqueID {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		u := randomUnique()
		if u.IsInvalid() {
			continue
		}
		if _, exists := s.clients[u]; !exists {
			return u
		}
	}
}

func (s *TunnelServer) getClient(id UniqueID) (interface{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.clients[id]
	return c, ok
}

// ─── SessionOwner ────────────────────────────────────────────────────────────

type SessionOwner struct {
	config *Config
	logger *Logger
	conn   net.Conn
	unique UniqueID

	mu        sync.Mutex
	connected map[UniqueID]connInfo // host included as invalidUnique
	ready     bool
}

type connInfo struct {
	address string
}

func NewSessionOwner(config *Config, logger *Logger, conn net.Conn, unique UniqueID) *SessionOwner {
	addr := conn.RemoteAddr().(*net.TCPAddr).IP.String()
	o := &SessionOwner{
		config:    config,
		logger:    logger,
		conn:      conn,
		unique:    unique,
		connected: make(map[UniqueID]connInfo),
	}
	o.connected[invalidUnique] = connInfo{address: addr} // host counts as one
	return o
}

func (o *SessionOwner) AddrPort() string {
	a := o.conn.RemoteAddr().(*net.TCPAddr)
	return fmt.Sprintf("%s|%d", a.IP, a.Port)
}

func (o *SessionOwner) sendAddress(requestedUnique UniqueID) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	var rawAddr [16]byte
	outUnique := requestedUnique

	ci, found := o.connected[requestedUnique]
	if !found {
		outUnique = invalidUnique
		// rawAddr stays all zeros = "::" packed
	} else {
		ip := net.ParseIP(ci.address)
		if ip == nil {
			outUnique = invalidUnique
		} else {
			ip4 := ip.To4()
			if ip4 != nil {
				// IPv4-mapped IPv6: ::ffff:x.x.x.x
				rawAddr[10] = 0xff
				rawAddr[11] = 0xff
				copy(rawAddr[12:], ip4)
			} else {
				copy(rawAddr[:], ip.To16())
			}
		}
	}

	var buf [magicSize + uniqueSize + 16]byte
	copy(buf[:magicSize], magicAddress[:])
	copy(buf[magicSize:magicSize+uniqueSize], outUnique[:])
	copy(buf[magicSize+uniqueSize:], rawAddr[:])

	return writeAll(o.conn, buf[:]) == nil
}

func (o *SessionOwner) requestLink(clientUnique UniqueID, clientAddr string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	if !o.ready {
		return false
	}
	if o.config.MaxClientsPerSess > 0 && len(o.connected) >= o.config.MaxClientsPerSess {
		return false
	}
	if _, exists := o.connected[clientUnique]; exists {
		return false
	}

	// Tell the host to establish a new link connection: RATL + unique
	var buf [magicSize + uniqueSize]byte
	copy(buf[:magicSize], magicLink[:])
	copy(buf[magicSize:], clientUnique[:])
	if writeAll(o.conn, buf[:]) != nil {
		return false
	}
	o.connected[clientUnique] = connInfo{address: clientAddr}
	return true
}

func (o *SessionOwner) requestUnlink(clientUnique UniqueID) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.ready {
		return false
	}
	if clientUnique.IsInvalid() {
		return false
	}
	if _, exists := o.connected[clientUnique]; !exists {
		return false
	}
	delete(o.connected, clientUnique)
	return true
}

func (o *SessionOwner) Run() {
	ap := o.AddrPort()
	defer func() {
		o.mu.Lock()
		o.ready = false
		o.mu.Unlock()
		o.logger.Info("Tunnel session closed for: %s", ap)
	}()

	// Send the host its session id: RATS + unique
	var hdr [magicSize + uniqueSize]byte
	copy(hdr[:magicSize], magicSession[:])
	copy(hdr[magicSize:], o.unique[:])

	o.mu.Lock()
	err := writeAll(o.conn, hdr[:])
	if err != nil {
		o.mu.Unlock()
		return
	}
	o.ready = true
	o.mu.Unlock()

	pings := 0
	magicBuf := make([]byte, magicSize)

	for {
		o.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, err := io.ReadFull(o.conn, magicBuf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if pings < 3 {
					o.mu.Lock()
					err2 := writeAll(o.conn, magicPing[:])
					o.mu.Unlock()
					if err2 != nil {
						return
					}
					pings++
					continue
				}
				o.logger.Error("Tunnel session timeout for: %s", ap)
			}
			return
		}

		var magic [magicSize]byte
		copy(magic[:], magicBuf)

		switch magic {
		case magicAddress:
			uniqueBuf, err := readExact(o.conn, uniqueSize, 30*time.Second)
			if err != nil {
				return
			}
			var reqUnique UniqueID
			copy(reqUnique[:], uniqueBuf)

			if !o.sendAddress(reqUnique) {
				o.logger.Error("Failed to send requested address for: %s", ap)
				return
			}

		case magicPing:
			pings = 0

		default:
			o.logger.Error("Tunnel session received unknown data for: %s", ap)
			return
		}
	}
}

// ─── SessionUser (shared between Client and Host) ────────────────────────────

type SessionUser struct {
	config *Config
	logger *Logger
	conn   net.Conn
	unique UniqueID

	mu     sync.Mutex
	owner  *SessionOwner
	link   *SessionUser
	linked chan struct{} // closed when linked
}

func newSessionUser(config *Config, logger *Logger, conn net.Conn, unique UniqueID) *SessionUser {
	return &SessionUser{
		config: config,
		logger: logger,
		conn:   conn,
		unique: unique,
		linked: make(chan struct{}),
	}
}

func (u *SessionUser) AddrPort() string {
	a := u.conn.RemoteAddr().(*net.TCPAddr)
	return fmt.Sprintf("%s|%d", a.IP, a.Port)
}

func (u *SessionUser) Address() string {
	return u.conn.RemoteAddr().(*net.TCPAddr).IP.String()
}

func (u *SessionUser) isLinked() bool {
	select {
	case <-u.linked:
		return true
	default:
		return false
	}
}

func (u *SessionUser) forward(data []byte) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return writeAll(u.conn, data) == nil
}

func (u *SessionUser) tryLink(link *SessionUser) bool {
	u.mu.Lock()
	defer u.mu.Unlock()

	link.mu.Lock()
	defer link.mu.Unlock()

	if u.owner == nil {
		return false
	}

	if u.link != nil || link.link != nil {
		return false
	}
	if u.isLinked() || link.isLinked() {
		return false
	}

	u.link = link
	link.link = u

	close(u.linked)
	close(link.linked)
	return true
}

func (u *SessionUser) tryUnlink() bool {
	u.mu.Lock()
	link := u.link
	if link == nil {
		u.mu.Unlock()
		return false
	}
	link.mu.Lock()

	u.link = nil
	link.link = nil

	link.mu.Unlock()
	u.mu.Unlock()
	return true
}

func (u *SessionUser) Run() {
	ap := u.AddrPort()
	timeout := u.config.Timeout
	defer func() {
		u.logger.Info("Tunnel closed for: %s", ap)
	}()

	// Wait until linked or timeout.
	select {
	case <-u.linked:
		// good
	case <-time.After(timeout):
		u.logger.Error("Timeout while awaiting link for: %s", ap)
		return
	}

	// Forward data.
	buf := make([]byte, 8192)
	for {
		u.conn.SetReadDeadline(time.Now().Add(timeout))
		n, err := u.conn.Read(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				u.logger.Error("Tunnel link timeout for: %s", ap)
			}
			return
		}
		if n == 0 {
			return
		}

		u.mu.Lock()
		link := u.link
		u.mu.Unlock()

		if link == nil {
			return
		}
		if !link.forward(buf[:n]) {
			u.logger.Error("Failed to forward data from: %s", ap)
			return
		}
	}
}

// ─── Client types (just tags for type assertion clarity) ─────────────────────

type SessionUserClient struct{ *SessionUser }
type SessionUserHost struct{ *SessionUser }

// ─── Server connection handler ───────────────────────────────────────────────

func (s *TunnelServer) handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		s.logger.Info("Connection closed for: %s|%d",
			conn.RemoteAddr().(*net.TCPAddr).IP,
			conn.RemoteAddr().(*net.TCPAddr).Port)
	}()

	addr := conn.RemoteAddr().(*net.TCPAddr)
	ap := fmt.Sprintf("%s|%d", addr.IP, addr.Port)

	s.logger.Info("Received connection from: %s", ap)

	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}

	// Read magic (30s timeout).
	magicBuf, err := readExact(conn, magicSize, 30*time.Second)
	if err != nil {
		s.logger.Error("Failed to receive tunnel magic from: %s", ap)
		return
	}

	var magic [magicSize]byte
	copy(magic[:], magicBuf)

	if magic == magicRANP {
		// Unsupported client – send fake header.
		var resp [magicSize + 20]byte
		copy(resp[:magicSize], magicRANP[:])
		writeAll(conn, resp[:])
		s.logger.Error("Unsupported client from: %s", ap)
		return
	}

	if magic != magicSession && magic != magicLink {
		s.logger.Error("Unknown tunnel magic from: %s", ap)
		return
	}

	// Read unique id (30s timeout).
	uniqueBuf, err := readExact(conn, uniqueSize, 30*time.Second)
	if err != nil {
		s.logger.Error("Failed to receive tunnel unique id from: %s", ap)
		return
	}
	var recvUnique UniqueID
	copy(recvUnique[:], uniqueBuf)

	if magic == magicSession {
		if recvUnique.IsInvalid() {
			// New session request.
			if !s.requestSession() {
				s.logger.Error("Refused to create tunnel session for: %s", ap)
				return
			}

			unique := s.generateUnique()
			owner := NewSessionOwner(s.config, s.logger, conn, unique)
			s.addClient(unique, owner)
			s.logger.Info("Tunnel session created for: %s", ap)

			owner.Run()

			s.removeClient(unique)
			s.freeSession()
		} else {
			// Client wants to link to existing session.
			unique := s.generateUnique()
			client := &SessionUserClient{newSessionUser(s.config, s.logger, conn, unique)}
			s.addClient(unique, client)

			// Find the session owner.
			ownerIface, ok := s.getClient(recvUnique)
			if !ok {
				s.removeClient(unique)
				s.logger.Error("Failed to find session for: %s", ap)
				return
			}
			owner, ok := ownerIface.(*SessionOwner)
			if !ok {
				s.removeClient(unique)
				s.logger.Error("Invalid session id from: %s", ap)
				return
			}

			client.owner = owner
			if !owner.requestLink(unique, client.Address()) {
				s.removeClient(unique)
				s.logger.Error("Failed to establish tunnel link for: %s", ap)
				return
			}

			s.logger.Info("Pending tunnel linking for: %s", ap)
			client.Run()

			s.removeClient(unique)
			client.tryUnlink()
			owner.requestUnlink(unique)
		}
	} else {
		// RATL: host opening a link connection to a user client.
		unique := s.generateUnique()
		hostUser := &SessionUserHost{newSessionUser(s.config, s.logger, conn, unique)}
		s.addClient(unique, hostUser)

		// Find the peer (SessionUserClient).
		peerIface, ok := s.getClient(recvUnique)
		if !ok {
			s.removeClient(unique)
			s.logger.Error("Failed to find peer for: %s", ap)
			return
		}
		peer, ok := peerIface.(*SessionUserClient)
		if !ok {
			s.removeClient(unique)
			s.logger.Error("Invalid peer id from: %s", ap)
			return
		}

		peer.mu.Lock()
		peerOwner := peer.owner
		peer.mu.Unlock()

		if peerOwner == nil {
			s.removeClient(unique)
			s.logger.Error("Invalid peer session owner for: %s", ap)
			return
		}

		hostUser.mu.Lock()
		hostUser.owner = peerOwner
		hostUser.mu.Unlock()

		if !peer.tryLink(hostUser.SessionUser) {
			s.removeClient(unique)
			s.logger.Error("Failed to establish tunnel link for: %s", ap)
			return
		}

		s.logger.Info("Tunnel linking completed for: %s <-> %s", peer.AddrPort(), ap)
		hostUser.Run()

		s.removeClient(unique)
		hostUser.tryUnlink()
	}
}

func (s *TunnelServer) ListenAndServe() error {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(s.config.Port))
	if err != nil {
		return fmt.Errorf("failed to create tunnel server: %w", err)
	}
	defer ln.Close()

	addr := ln.Addr().(*net.TCPAddr)
	msg := fmt.Sprintf("%s|%d", addr.IP, addr.Port)
	fmt.Printf("[*] Tunnel server listening on: %s\n", msg)
	s.logger.Info("Tunnel server listening on: %s", msg)

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.logger.Error("Accept error: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

// ─── main ────────────────────────────────────────────────────────────────────

func main() {
	configPath := ""
	if len(os.Args) > 1 {
		configPath = strings.TrimSpace(os.Args[1])
	}
	if configPath == "" {
		exe, _ := os.Executable()
		ext := filepath.Ext(exe)
		if ext != "" {
			configPath = strings.TrimSuffix(exe, ext) + ".ini"
		} else {
			configPath = exe + ".ini"
		}
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Config error: %v", err)
	}

	logger := NewLogger(cfg.LogPath, cfg.LogLevel)
	server := NewTunnelServer(cfg, logger)

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
