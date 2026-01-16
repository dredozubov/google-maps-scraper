package webrunner

import (
	"math/rand"
	"strings"
	"sync"
	"time"
)

type proxyManager struct {
	mu             sync.Mutex
	states         map[string]*proxyState
	rrIndex        int
	rng            *rand.Rand
	strategy       string
	validationRate float64
	cooldownBase   time.Duration
	cooldownMax    time.Duration
}

type proxyState struct {
	consecutiveFailures int
	cooldownUntil       time.Time
	lastFailure         time.Time
}

func newProxyManager(cfg proxyManagerConfig) *proxyManager {
	return &proxyManager{
		states:         make(map[string]*proxyState),
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
		strategy:       strings.ToLower(cfg.selectionStrategy),
		validationRate: cfg.validationRate,
		cooldownBase:   cfg.cooldownBase,
		cooldownMax:    cfg.cooldownMax,
	}
}

type proxyManagerConfig struct {
	selectionStrategy string
	validationRate    float64
	cooldownBase      time.Duration
	cooldownMax       time.Duration
}

func (pm *proxyManager) UpdateProxies(proxies []string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if len(pm.states) == 0 || len(proxies) == 0 {
		return
	}

	set := make(map[string]struct{}, len(proxies))
	for _, proxy := range proxies {
		set[proxy] = struct{}{}
	}

	for proxy := range pm.states {
		if _, ok := set[proxy]; !ok {
			delete(pm.states, proxy)
		}
	}
}

func (pm *proxyManager) Select(proxies []string) string {
	if len(proxies) == 0 {
		return ""
	}

	if len(proxies) == 1 {
		return proxies[0]
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	now := time.Now()

	switch pm.strategy {
	case "round_robin":
		return pm.selectRoundRobinLocked(proxies, now)
	case "least_failed":
		return pm.selectLeastFailedLocked(proxies, now)
	default:
		return pm.selectWeightedRandomLocked(proxies, now)
	}
}

func (pm *proxyManager) ShouldValidate() bool {
	if pm.validationRate <= 0 {
		return false
	}

	if pm.validationRate >= 1 {
		return true
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.rng.Float64() < pm.validationRate
}

func (pm *proxyManager) MarkFailure(proxy string) time.Duration {
	if proxy == "" {
		return 0
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	state := pm.ensureStateLocked(proxy)
	state.consecutiveFailures++
	state.lastFailure = time.Now()

	cooldown := pm.cooldownBase
	if pm.cooldownBase > 0 {
		cooldown = pm.cooldownBase * time.Duration(1<<minInt(state.consecutiveFailures-1, 16))
	}

	if pm.cooldownMax > 0 && cooldown > pm.cooldownMax {
		cooldown = pm.cooldownMax
	}

	if cooldown > 0 {
		state.cooldownUntil = time.Now().Add(cooldown)
	}

	return cooldown
}

func (pm *proxyManager) MarkSuccess(proxy string) {
	if proxy == "" {
		return
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	state := pm.ensureStateLocked(proxy)
	state.consecutiveFailures = 0
	state.cooldownUntil = time.Time{}
}

func (pm *proxyManager) ensureStateLocked(proxy string) *proxyState {
	state, ok := pm.states[proxy]
	if !ok {
		state = &proxyState{}
		pm.states[proxy] = state
	}

	return state
}

func (pm *proxyManager) selectRoundRobinLocked(proxies []string, now time.Time) string {
	start := pm.rrIndex % len(proxies)

	for i := 0; i < len(proxies); i++ {
		idx := (start + i) % len(proxies)
		proxy := proxies[idx]
		state := pm.ensureStateLocked(proxy)
		if state.cooldownUntil.After(now) {
			continue
		}

		pm.rrIndex = idx + 1
		return proxy
	}

	pm.rrIndex = start + 1
	return proxies[start]
}

func (pm *proxyManager) selectLeastFailedLocked(proxies []string, now time.Time) string {
	var (
		selected     string
		minFailures  int
		hasCandidate bool
	)

	for _, proxy := range proxies {
		state := pm.ensureStateLocked(proxy)
		if state.cooldownUntil.After(now) {
			continue
		}

		if !hasCandidate || state.consecutiveFailures < minFailures {
			minFailures = state.consecutiveFailures
			selected = proxy
			hasCandidate = true
		}
	}

	if hasCandidate {
		return selected
	}

	selected = proxies[0]
	minFailures = pm.ensureStateLocked(selected).consecutiveFailures
	for _, proxy := range proxies[1:] {
		state := pm.ensureStateLocked(proxy)
		if state.consecutiveFailures < minFailures {
			minFailures = state.consecutiveFailures
			selected = proxy
		}
	}

	return selected
}

func (pm *proxyManager) selectWeightedRandomLocked(proxies []string, now time.Time) string {
	weights := make([]float64, 0, len(proxies))
	candidates := make([]string, 0, len(proxies))

	for _, proxy := range proxies {
		state := pm.ensureStateLocked(proxy)
		if state.cooldownUntil.After(now) {
			continue
		}

		weight := 1.0 / float64(1+state.consecutiveFailures)
		weights = append(weights, weight)
		candidates = append(candidates, proxy)
	}

	if len(candidates) == 0 {
		candidates = proxies
		weights = weights[:0]
		for _, proxy := range proxies {
			state := pm.ensureStateLocked(proxy)
			weight := 1.0 / float64(1+state.consecutiveFailures)
			weights = append(weights, weight)
		}
	}

	total := 0.0
	for _, weight := range weights {
		total += weight
	}

	if total <= 0 {
		return candidates[0]
	}

	target := pm.rng.Float64() * total
	for i, weight := range weights {
		target -= weight
		if target <= 0 {
			return candidates[i]
		}
	}

	return candidates[len(candidates)-1]
}

func minInt(a, b int) int {
	if a < b {
		return a
	}

	return b
}
