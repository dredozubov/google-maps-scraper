package webrunner

import (
	"math/rand"
	"testing"
	"time"
)

func TestProxyManagerSelectRoundRobinSkipsCooldown(t *testing.T) {
	pm := newProxyManager(proxyManagerConfig{
		selectionStrategy: "round_robin",
		cooldownBase:      time.Hour,
		cooldownMax:       time.Hour,
	})
	pm.rng = rand.New(rand.NewSource(1))

	proxies := []string{"proxy-a", "proxy-b"}
	pm.MarkFailure("proxy-a")

	selected := pm.Select(proxies)
	if selected != "proxy-b" {
		t.Fatalf("expected proxy-b, got %s", selected)
	}
}

func TestProxyManagerSelectLeastFailed(t *testing.T) {
	pm := newProxyManager(proxyManagerConfig{
		selectionStrategy: "least_failed",
		cooldownBase:      time.Second,
		cooldownMax:       time.Minute,
	})
	pm.rng = rand.New(rand.NewSource(1))

	proxies := []string{"proxy-a", "proxy-b"}
	pm.MarkFailure("proxy-a")
	pm.MarkFailure("proxy-a")

	selected := pm.Select(proxies)
	if selected != "proxy-b" {
		t.Fatalf("expected proxy-b, got %s", selected)
	}
}

func TestProxyManagerShouldValidate(t *testing.T) {
	pm := newProxyManager(proxyManagerConfig{
		selectionStrategy: "random",
		validationRate:    1,
	})
	pm.rng = rand.New(rand.NewSource(1))

	if !pm.ShouldValidate() {
		t.Fatal("expected validation at rate 1")
	}

	pm.validationRate = 0
	if pm.ShouldValidate() {
		t.Fatal("expected no validation at rate 0")
	}
}
