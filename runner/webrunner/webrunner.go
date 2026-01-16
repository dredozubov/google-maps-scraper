package webrunner

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gosom/google-maps-scraper/deduper"
	"github.com/gosom/google-maps-scraper/exiter"
	"github.com/gosom/google-maps-scraper/runner"
	"github.com/gosom/google-maps-scraper/tlmt"
	"github.com/gosom/google-maps-scraper/web"
	"github.com/gosom/google-maps-scraper/web/sqlite"
	"github.com/gosom/scrapemate"
	"github.com/gosom/scrapemate/adapters/writers/csvwriter"
	"github.com/gosom/scrapemate/scrapemateapp"
	"golang.org/x/sync/errgroup"
)

type webrunner struct {
	srv *web.Server
	svc *web.Service
	cfg *runner.Config
	mu  sync.RWMutex

	proxies                 []string
	proxySourceURL          string
	refreshInterval         time.Duration
	proxyManager            *proxyManager
	proxyFailureCount       int
	lastProxyRefreshAttempt time.Time
}

func New(cfg *runner.Config) (runner.Runner, error) {
	if cfg.DataFolder == "" {
		return nil, fmt.Errorf("data folder is required")
	}

	if err := os.MkdirAll(cfg.DataFolder, os.ModePerm); err != nil {
		return nil, err
	}

	const dbfname = "jobs.db"

	dbpath := filepath.Join(cfg.DataFolder, dbfname)

	repo, err := sqlite.New(dbpath)
	if err != nil {
		return nil, err
	}

	svc := web.NewService(repo, cfg.DataFolder)

	srv, err := web.New(svc, cfg.Addr)
	if err != nil {
		return nil, err
	}

	ans := webrunner{
		srv: srv,
		svc: svc,
		cfg: cfg,
		proxies: func() []string {
			if len(cfg.Proxies) == 0 {
				return nil
			}

			return append([]string(nil), cfg.Proxies...)
		}(),
		proxySourceURL:  cfg.ScrapoxyProxyURL,
		refreshInterval: cfg.ProxyRefreshInterval,
		proxyManager: newProxyManager(proxyManagerConfig{
			selectionStrategy: cfg.ProxySelectionStrategy,
			validationRate:    cfg.ProxyValidationRate,
			cooldownBase:      cfg.ProxyCooldownBase,
			cooldownMax:       cfg.ProxyCooldownMax,
		}),
	}

	return &ans, nil
}

func (w *webrunner) Run(ctx context.Context) error {
	egroup, ctx := errgroup.WithContext(ctx)

	if w.refreshInterval > 0 && w.proxySourceURL != "" {
		egroup.Go(func() error {
			return w.refreshProxyLoop(ctx)
		})
	}

	egroup.Go(func() error {
		return w.work(ctx)
	})

	egroup.Go(func() error {
		return w.srv.Start(ctx)
	})

	return egroup.Wait()
}

func (w *webrunner) Close(context.Context) error {
	return nil
}

func (w *webrunner) work(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			jobs, err := w.svc.SelectPending(ctx)
			if err != nil {
				return err
			}

			for i := range jobs {
				select {
				case <-ctx.Done():
					return nil
				default:
					t0 := time.Now().UTC()
					if err := w.scrapeJob(ctx, &jobs[i]); err != nil {
						params := map[string]any{
							"job_count": len(jobs[i].Data.Keywords),
							"duration":  time.Now().UTC().Sub(t0).String(),
							"error":     err.Error(),
						}

						evt := tlmt.NewEvent("web_runner", params)

						_ = runner.Telemetry().Send(ctx, evt)

						log.Printf("error scraping job %s: %v", jobs[i].ID, err)
					} else {
						params := map[string]any{
							"job_count": len(jobs[i].Data.Keywords),
							"duration":  time.Now().UTC().Sub(t0).String(),
						}

						_ = runner.Telemetry().Send(ctx, tlmt.NewEvent("web_runner", params))

						log.Printf("job %s scraped successfully", jobs[i].ID)
					}
				}
			}
		}
	}
}

func (w *webrunner) scrapeJob(ctx context.Context, job *web.Job) error {
	job.Status = web.StatusWorking

	err := w.svc.Update(ctx, job)
	if err != nil {
		return err
	}

	if len(job.Data.Keywords) == 0 {
		job.Status = web.StatusFailed

		return w.svc.Update(ctx, job)
	}

	outpath := filepath.Join(w.cfg.DataFolder, job.ID+".csv")

	maxRetries := w.cfg.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}

	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("retrying job %s (attempt %d/%d) after error: %v", job.ID, attempt+1, maxRetries+1, lastErr)

			if w.cfg.RetryDelay > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(w.cfg.RetryDelay):
				}
			}
		}

		proxyUsed, err := w.runJobAttempt(ctx, job, outpath)
		if err == nil {
			w.proxyManager.MarkSuccess(proxyUsed)
			job.Status = web.StatusOK
			return w.svc.Update(ctx, job)
		}

		lastErr = err

		if isProxyRetryable(err, w.cfg.ProxyErrorsRetry) {
			w.proxyManager.MarkFailure(proxyUsed)
			w.maybeRefreshOnFailure(ctx)
		} else {
			break
		}
	}

	job.Status = web.StatusFailed

	err2 := w.svc.Update(ctx, job)
	if err2 != nil {
		log.Printf("failed to update job status: %v", err2)
	}

	return lastErr
}

func (w *webrunner) setupMate(ctx context.Context, writer io.Writer, job *web.Job) (*scrapemateapp.ScrapemateApp, string, error) {
	opts := []func(*scrapemateapp.Config) error{
		scrapemateapp.WithConcurrency(w.cfg.Concurrency),
		scrapemateapp.WithExitOnInactivity(time.Minute * 3),
	}

	if !job.Data.FastMode {
		opts = append(opts,
			scrapemateapp.WithJS(scrapemateapp.DisableImages()),
		)
	} else {
		opts = append(opts,
			scrapemateapp.WithStealth("firefox"),
		)
	}

	selectedProxy, err := w.pickProxyForJob(ctx, job)
	if err != nil {
		return nil, "", err
	}

	hasProxy := false
	if selectedProxy != "" {
		opts = append(opts, scrapemateapp.WithProxies([]string{selectedProxy}))
		hasProxy = true
	}

	if !w.cfg.DisablePageReuse {
		opts = append(opts,
			scrapemateapp.WithPageReuseLimit(2),
			scrapemateapp.WithPageReuseLimit(200),
		)
	}

	log.Printf("job %s has proxy: %v", job.ID, hasProxy)

	csvWriter := csvwriter.NewCsvWriter(csv.NewWriter(writer))

	writers := []scrapemate.ResultWriter{csvWriter}

	matecfg, err := scrapemateapp.NewConfig(
		writers,
		opts...,
	)
	if err != nil {
		return nil, "", err
	}

	mate, err := scrapemateapp.NewScrapeMateApp(matecfg)
	if err != nil {
		return nil, "", err
	}

	return mate, selectedProxy, nil
}

func (w *webrunner) refreshProxyLoop(ctx context.Context) error {
	if err := w.refreshProxies(ctx); err != nil {
		log.Printf("proxy refresh failed: %v", err)
	}

	ticker := time.NewTicker(w.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := w.refreshProxies(ctx); err != nil {
				log.Printf("proxy refresh failed: %v", err)
			}
		}
	}
}

func (w *webrunner) refreshProxies(ctx context.Context) error {
	proxies, err := fetchProxyList(ctx, w.proxySourceURL)
	if err != nil {
		return err
	}

	if len(proxies) == 0 {
		return fmt.Errorf("proxy list is empty")
	}

	w.mu.Lock()
	w.proxies = proxies
	w.proxyFailureCount = 0
	w.lastProxyRefreshAttempt = time.Now()
	w.mu.Unlock()

	w.proxyManager.UpdateProxies(proxies)

	log.Printf("refreshed proxy list: %d entries", len(proxies))

	return nil
}

func (w *webrunner) currentProxies(job *web.Job) []string {
	w.mu.RLock()
	globalProxies := append([]string(nil), w.proxies...)
	w.mu.RUnlock()

	if len(globalProxies) > 0 {
		return globalProxies
	}

	if len(job.Data.Proxies) > 0 {
		return job.Data.Proxies
	}

	return nil
}

func (w *webrunner) pickProxyForJob(ctx context.Context, job *web.Job) (string, error) {
	proxies := w.currentProxies(job)
	if len(proxies) == 0 {
		return "", nil
	}

	maxReselect := w.cfg.ProxyReselectAttempts
	if !w.cfg.ProxyReselectOnValidationFailure {
		maxReselect = 0
	}

	var lastErr error

	for attempt := 0; attempt <= maxReselect; attempt++ {
		selected := w.proxyManager.Select(proxies)
		if selected == "" {
			break
		}

		if !w.proxyManager.ShouldValidate() {
			return selected, nil
		}

		if err := validateProxy(ctx, selected); err == nil {
			return selected, nil
		} else {
			lastErr = err
			w.proxyManager.MarkFailure(selected)
			w.maybeRefreshOnFailure(ctx)
		}

		if attempt < maxReselect {
			proxies = w.currentProxies(job)
		}
	}

	if lastErr == nil {
		return "", fmt.Errorf("%w: no proxy candidates available", errProxyValidationFailed)
	}

	return "", fmt.Errorf("%w: %v", errProxyValidationFailed, lastErr)
}

func (w *webrunner) maybeRefreshOnFailure(ctx context.Context) {
	if !w.cfg.ProxyRefreshOnFailure || w.cfg.ProxyRefreshFailureThreshold <= 0 {
		return
	}

	const proxyRefreshMinInterval = 30 * time.Second

	shouldRefresh := false
	now := time.Now()

	w.mu.Lock()
	w.proxyFailureCount++
	lastAttempt := w.lastProxyRefreshAttempt
	if lastAttempt.IsZero() {
		lastAttempt = now.Add(-proxyRefreshMinInterval)
	}

	if w.proxyFailureCount >= w.cfg.ProxyRefreshFailureThreshold && now.Sub(lastAttempt) >= proxyRefreshMinInterval {
		w.proxyFailureCount = 0
		w.lastProxyRefreshAttempt = now
		shouldRefresh = true
	}
	w.mu.Unlock()

	if shouldRefresh {
		if err := w.refreshProxies(ctx); err != nil {
			log.Printf("proxy refresh failed: %v", err)
		}
	}
}

func fetchProxyList(ctx context.Context, sourceURL string) ([]string, error) {
	if sourceURL == "" {
		return nil, fmt.Errorf("proxy source URL is empty")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, http.NoBody)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Timeout: 15 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("proxy list returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	proxies, err := parseProxyList(body)
	if err != nil {
		return nil, err
	}

	return proxies, nil
}

func parseProxyList(body []byte) ([]string, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("proxy list response empty")
	}

	var parsed []string
	if err := json.Unmarshal(body, &parsed); err == nil {
		return normalizeProxyList(parsed), nil
	}

	text := string(body)
	proxies := normalizeProxyList(strings.FieldsFunc(text, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r'
	}))

	if len(proxies) == 0 {
		return nil, fmt.Errorf("proxy list response format unsupported")
	}

	return proxies, nil
}

func normalizeProxyList(proxies []string) []string {
	if len(proxies) == 0 {
		return nil
	}

	out := make([]string, 0, len(proxies))

	for _, proxy := range proxies {
		proxy = strings.TrimSpace(proxy)

		if proxy == "" {
			continue
		}

		out = append(out, proxy)
	}

	return out
}

func (w *webrunner) runJobAttempt(ctx context.Context, job *web.Job, outpath string) (string, error) {
	outfile, err := os.Create(outpath)
	if err != nil {
		return "", err
	}

	defer func() {
		_ = outfile.Close()
	}()

	mate, proxyUsed, err := w.setupMate(ctx, outfile, job)
	if err != nil {
		return proxyUsed, err
	}
	defer mate.Close()

	var coords string
	if job.Data.Lat != "" && job.Data.Lon != "" {
		coords = job.Data.Lat + "," + job.Data.Lon
	}

	dedup := deduper.New()
	exitMonitor := exiter.New()

	seedJobs, err := runner.CreateSeedJobs(
		job.Data.FastMode,
		job.Data.Lang,
		strings.NewReader(strings.Join(job.Data.Keywords, "\n")),
		job.Data.Depth,
		job.Data.Email,
		coords,
		job.Data.Zoom,
		func() float64 {
			if job.Data.Radius <= 0 {
				return 10000 // 10 km
			}

			return float64(job.Data.Radius)
		}(),
		dedup,
		exitMonitor,
		w.cfg.ExtraReviews,
		w.cfg.ExtraPhotos,
	)
	if err != nil {
		return proxyUsed, err
	}

	if len(seedJobs) == 0 {
		return proxyUsed, nil
	}

	exitMonitor.SetSeedCount(len(seedJobs))

	allowedSeconds := max(60, len(seedJobs)*10*job.Data.Depth/50+120)

	if job.Data.MaxTime > 0 {
		if job.Data.MaxTime.Seconds() < 180 {
			allowedSeconds = 180
		} else {
			allowedSeconds = int(job.Data.MaxTime.Seconds())
		}
	}

	log.Printf("running job %s with %d seed jobs and %d allowed seconds", job.ID, len(seedJobs), allowedSeconds)

	mateCtx, cancel := context.WithTimeout(ctx, time.Duration(allowedSeconds)*time.Second)
	defer cancel()

	exitMonitor.SetCancelFunc(cancel)

	go exitMonitor.Run(mateCtx)

	err = mate.Start(mateCtx, seedJobs...)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return proxyUsed, err
	}

	return proxyUsed, nil
}

var errProxyValidationFailed = errors.New("proxy validation failed")

func isProxyRetryable(err error, patterns []string) bool {
	if err == nil || len(patterns) == 0 {
		return errors.Is(err, errProxyValidationFailed)
	}

	if errors.Is(err, errProxyValidationFailed) {
		return true
	}

	msg := strings.ToLower(err.Error())

	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}

		if strings.Contains(msg, pattern) {
			return true
		}
	}

	return false
}

func validateProxy(ctx context.Context, proxyURL string) error {
	u, err := url.Parse(proxyURL)
	if err != nil {
		return err
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(u),
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   20 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", "http://clients3.google.com/generate_204", http.NoBody)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 500 {
		return fmt.Errorf("proxy returned status %d", resp.StatusCode)
	}

	return nil
}
