package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tonkeeper/tongo/config"
	_ "github.com/tonkeeper/tongo/lib"
	"github.com/tonkeeper/tongo/liteapi"
	"github.com/tonkeeper/tongo/liteapi/pool"
	"github.com/tonkeeper/tongo/ton"
)

type Mode string

const (
	ModeBlocks   Mode = "blocks"
	ModeAccounts Mode = "accounts"
	ModeBoth     Mode = "both"
)

type Result struct {
	Config      string        `json:"config"`
	Targets     string        `json:"targets"`
	Mode        string        `json:"mode"`
	Concurrency int           `json:"concurrency"`
	Total       int           `json:"total"`
	Success     int           `json:"success"`
	Errors      int           `json:"errors"`
	Duration    time.Duration `json:"duration"`
	RPS         float64       `json:"rps"`
	AvgMs       float64       `json:"avg_ms"`
	P50Ms       float64       `json:"p50_ms"`
	P90Ms       float64       `json:"p90_ms"`
	P95Ms       float64       `json:"p95_ms"`
	P99Ms       float64       `json:"p99_ms"`
	MaxMs       float64       `json:"max_ms"`
	SeriesSec   []int         `json:"series_sec,omitempty"`
	SeriesRPS   []float64     `json:"series_rps,omitempty"`
	SeriesErr   []float64     `json:"series_err,omitempty"`
	SeriesP50   []float64     `json:"series_p50,omitempty"`
	SeriesP90   []float64     `json:"series_p90,omitempty"`
	SeriesP95   []float64     `json:"series_p95,omitempty"`
	SeriesP99   []float64     `json:"series_p99,omitempty"`
	SeriesStart int64         `json:"series_start_ms,omitempty"`
}

func main() {
	loadDotEnv(envOr("LS_LOAD_ENV", ".env"))

	var (
		configsStr         = flag.String("configs", envOr("LS_LOAD_CONFIGS", "config.json"), "Comma-separated config paths or globs (optional alias: name=path)")
		concurrency        = flag.String("concurrency", envOr("LS_LOAD_CONCURRENCY", "5,10,20,50"), "Comma-separated concurrency levels")
		stepsStr           = flag.String("steps", envOr("LS_LOAD_STEPS", ""), "Comma-separated step concurrency levels (overrides --concurrency)")
		stepDurStr         = flag.String("step-duration", envOr("LS_LOAD_STEP_DURATION", ""), "Duration per step (e.g. 5m)")
		blocksSpec         = flag.String("blocks", envOr("LS_LOAD_BLOCKS", "last:200"), "Block range: last:N or range:FROM-TO (masterchain seqno)")
		blocksRand         = flag.Bool("blocks-random", envOrBool("LS_LOAD_BLOCKS_RANDOM", false), "Randomize block selection per request (reduces caching)")
		blocksRefStr       = flag.String("blocks-refresh", envOr("LS_LOAD_BLOCKS_REFRESH", "5s"), "Refresh interval for last:N when blocks-random is on")
		accountsFile       = flag.String("accounts", envOr("LS_LOAD_ACCOUNTS", ""), "Path to file with account addresses (one per line)")
		accountsN          = flag.Int("accounts-count", envOrInt("LS_LOAD_ACCOUNTS_COUNT", 10000), "Number of random accounts when --accounts not set")
		accountsWarm       = flag.Bool("accounts-warmup", true, "Warm up accounts by scanning recent shard blocks")
		accountsWarmBlocks = flag.Int("accounts-warmup-blocks", envOrInt("LS_LOAD_ACCOUNTS_WARMUP_BLOCKS", 8), "Masterchain blocks to scan during warmup")
		accountsShuf       = flag.Bool("accounts-shuffle", envOrBool("LS_LOAD_ACCOUNTS_SHUFFLE", false), "Shuffle account list on load")
		outDir             = flag.String("out", envOr("LS_LOAD_OUT", "results"), "Output directory")
		timeoutStr         = flag.String("timeout", envOr("LS_LOAD_TIMEOUT", "10s"), "Per-request timeout")
		durationStr        = flag.String("duration", envOr("LS_LOAD_DURATION", ""), "Test duration per scenario (e.g. 10s). Empty = fixed dataset run")
		reportFrom         = flag.String("report-from", envOr("LS_LOAD_REPORT_FROM", ""), "Regenerate report.html from existing results dir (reads summary.json and requests.jsonl)")
		reportMaxPts       = flag.Int("report-max-points", envOrInt("LS_LOAD_REPORT_MAX_POINTS", 240), "Max points per series in HTML report (downsample; 0 = no downsample)")
		reqLogStr          = flag.String("request-log", envOr("LS_LOAD_REQUEST_LOG", "auto"), "Per-request JSONL log path (use 'auto' to write in results dir, 'off' to disable)")
		retries            = flag.Int("retries", envOrInt("LS_LOAD_RETRIES", 0), "LiteServer retry attempts (0 = auto) ")
		maxConns           = flag.Int("max-connections", envOrInt("LS_LOAD_MAX_CONNECTIONS", 0), "Max connections to liteservers (0 = auto)")
		workers            = flag.Int("workers-per-conn", envOrInt("LS_LOAD_WORKERS_PER_CONN", 0), "Workers per connection (0 = default)")
		poolStr            = flag.String("pool-strategy", envOr("LS_LOAD_POOL_STRATEGY", ""), "Pool strategy: best-ping|first-working")
		proofStr           = flag.String("proof", envOr("LS_LOAD_PROOF", "fast"), "proof check policy: unsafe|fast|secure")
	)
	flag.Parse()

	if strings.TrimSpace(*reportFrom) != "" {
		if err := regenerateReport(*reportFrom, *reqLogStr, *reportMaxPts); err != nil {
			exitf("failed to regenerate report: %v", err)
		}
		return
	}

	concurrencyLevels, err := parseIntList(*concurrency)
	if err != nil || len(concurrencyLevels) == 0 {
		exitf("invalid concurrency list: %s", *concurrency)
	}

	configs, err := resolveConfigPaths(*configsStr)
	if err != nil || len(configs) == 0 {
		exitf("no configs found: %s", *configsStr)
	}

	proofPolicy, err := parseProofPolicy(*proofStr)
	if err != nil {
		exitf("invalid proof policy: %s", *proofStr)
	}
	proofPolicy = liteapi.ProofPolicyUnsafe

	timeout, err := time.ParseDuration(*timeoutStr)
	if err != nil {
		exitf("invalid timeout: %s", *timeoutStr)
	}

	duration, err := parseDurationOptional(*durationStr)
	if err != nil {
		exitf("invalid duration: %s", *durationStr)
	}
	stepDuration, err := parseDurationOptional(*stepDurStr)
	if err != nil {
		exitf("invalid step-duration: %s", *stepDurStr)
	}

	var blocksRefresh time.Duration
	if *blocksRand {
		if strings.TrimSpace(*blocksRefStr) != "" {
			blocksRefresh, err = parseDurationOptional(*blocksRefStr)
			if err != nil {
				exitf("invalid blocks-refresh: %s", *blocksRefStr)
			}
		}
		if blocksRefresh == 0 {
			blocksRefresh = 5 * time.Second
		}
	}

	if strings.TrimSpace(*stepsStr) != "" {
		stepsLevels, err := parseIntList(*stepsStr)
		if err != nil || len(stepsLevels) == 0 {
			exitf("invalid steps: %s", *stepsStr)
		}
		concurrencyLevels = stepsLevels
		if stepDuration == 0 && duration == 0 {
			exitf("steps require --step-duration or --duration")
		}
		if stepDuration > 0 {
			duration = stepDuration
		}
	}

	rng := newLockedRand()

	var accounts []ton.AccountID
	var accountsBase []ton.AccountID
	var accountsFromFile bool
	if *accountsFile == "" {
		if !*accountsWarm {
			accountsBase, err = generateRandomAccounts(*accountsN)
			if err != nil {
				exitf("failed to generate accounts: %v", err)
			}
		}
	} else {
		accountsBase, err = loadAccounts(*accountsFile)
		if err != nil {
			exitf("failed to load accounts: %v", err)
		}
		if len(accountsBase) == 0 {
			exitf("no accounts loaded from %s", *accountsFile)
		}
		accountsFromFile = true
	}
	if *accountsShuf && len(accountsBase) > 1 {
		shuffleAccounts(accountsBase, rng)
	}

	br := blockRange{}
	br, err = parseBlockRange(*blocksSpec)
	if err != nil {
		exitf("invalid block range: %v", err)
	}

	stamp := time.Now().Format("20060102-150405")
	outRoot := filepath.Join(*outDir, stamp)
	if err := os.MkdirAll(outRoot, 0o755); err != nil {
		exitf("failed to create output dir: %v", err)
	}

	var logger *reqLogger
	reqLogPath := ""
	if strings.TrimSpace(*reqLogStr) != "" {
		logPath := strings.TrimSpace(*reqLogStr)
		switch strings.ToLower(logPath) {
		case "off", "none", "false", "0":
			logPath = ""
		}
		if logPath == "" {
			// logging disabled
		} else {
			if strings.EqualFold(logPath, "auto") {
				logPath = filepath.Join(outRoot, "requests.jsonl")
			}
			var err error
			logger, err = newReqLogger(logPath)
			if err != nil {
				exitf("failed to init request log: %v", err)
			}
			reqLogPath = logPath
			fmt.Printf("Per-request log: %s\n", logPath)
		}
	}

	var allResults []Result
	var methodData map[methodKey]methodSeries
	var errorSummary []errorSummaryEntry
	var errorSeriesData map[errorSeriesKey]errorSeries

	for _, cfgItem := range configs {
		cfgName := cfgItem.Name
		fmt.Printf("\n== Config: %s (%s) ==\n", cfgName, cfgItem.Path)

		cfg, err := config.ParseConfigFile(cfgItem.Path)
		if err != nil {
			fmt.Printf("config read failed (%s): %v\n", cfgItem.Path, err)
			continue
		}
		targets := formatTargets(cfg)

		opts := []liteapi.Option{
			liteapi.WithConfigurationFile(*cfg),
			liteapi.WithProofPolicy(proofPolicy),
		}
		if timeout > 0 {
			opts = append(opts, liteapi.WithTimeout(timeout))
		}
		if len(cfg.LiteServers) > 0 {
			opts = append(opts, liteapi.WithMaxConnectionsNumber(len(cfg.LiteServers)))
		}
		if *poolStr != "" {
			switch strings.ToLower(strings.TrimSpace(*poolStr)) {
			case "best-ping":
				opts = append(opts, liteapi.WithPoolStrategy(pool.BestPingStrategy))
			case "first-working":
				opts = append(opts, liteapi.WithPoolStrategy(pool.FirstWorkingConnection))
			default:
				exitf("invalid pool-strategy: %s", *poolStr)
			}
		}

		maxConnections := *maxConns
		if maxConnections <= 0 {
			maxConnections = len(cfg.LiteServers)
		}
		if *maxConns == 0 {
			maxConnections = len(cfg.LiteServers)
		}
		if maxConnections > 0 {
			opts = append(opts, liteapi.WithMaxConnectionsNumber(maxConnections))
		}

		workersPerConn := *workers
		if workersPerConn == 0 {
			workersPerConn = 8
		}
		if workersPerConn > 0 {
			opts = append(opts, liteapi.WithWorkersPerConnection(workersPerConn))
		}

		_ = *retries

		api, err := liteapi.NewClient(opts...)
		if err != nil {
			fmt.Printf("connection failed: %v\n", err)
			continue
		}

		blockSeqs, err2 := buildBlockSeqs(api, br)
		if err2 != nil {
			fmt.Printf("block range build failed: %v\n", err2)
		} else {
			for _, conc := range concurrencyLevels {
				res := runBlockTest(api, cfgName, targets, blockSeqs, conc, timeout, duration, logger, *blocksRand, blocksRefresh, rng, br)
				res.Config = cfgName
				res.Targets = targets
				allResults = append(allResults, res)
				printResult(res)
			}
		}

		accounts = accountsBase
		if !accountsFromFile && *accountsWarm {
			fmt.Printf("warming up accounts from recent blocks (target=%d, mc_blocks=%d)\n", *accountsN, *accountsWarmBlocks)
			accounts, err = warmupAccounts(api, timeout, *accountsN, *accountsWarmBlocks, rng)
			if err != nil {
				fmt.Printf("warmup failed: %v\n", err)
				continue
			}
			if *accountsShuf && len(accounts) > 1 {
				shuffleAccounts(accounts, rng)
			}
		}
		if len(accounts) == 0 {
			fmt.Printf("no accounts available for test\n")
			continue
		}
		for _, conc := range concurrencyLevels {
			res := runAccountTest(api, cfgName, targets, accounts, conc, timeout, duration, logger, true, rng)
			res.Config = cfgName
			res.Targets = targets
			allResults = append(allResults, res)
			printResult(res)
		}

		// liteapi client has no explicit Close; connections will close on process exit
	}

	if len(allResults) == 0 {
		exitf("no results collected")
	}

	if logger != nil {
		logger.Close()
		logger = nil
	}

	if reqLogPath != "" {
		if _, err := os.Stat(reqLogPath); err == nil {
			md, err := buildMethodSeriesFromLog(reqLogPath)
			if err == nil {
				methodData = md
			} else {
				fmt.Printf("failed to parse request log: %v\n", err)
			}
			ed, err := buildErrorSummaryFromLog(reqLogPath)
			if err == nil {
				errorSummary = ed
			} else {
				fmt.Printf("failed to build error summary: %v\n", err)
			}
			es, err := buildErrorSeriesFromLog(reqLogPath)
			if err == nil {
				errorSeriesData = es
			} else {
				fmt.Printf("failed to build error series: %v\n", err)
			}
		}
	}

	if err := writeCSV(filepath.Join(outRoot, "summary.csv"), allResults); err != nil {
		fmt.Printf("failed to write CSV: %v\n", err)
	}

	if err := writeJSON(filepath.Join(outRoot, "summary.json"), allResults); err != nil {
		fmt.Printf("failed to write JSON: %v\n", err)
	}

	if len(errorSummary) > 0 {
		if err := writeJSON(filepath.Join(outRoot, "errors.json"), errorSummary); err != nil {
			fmt.Printf("failed to write errors JSON: %v\n", err)
		}
		if err := writeErrorsCSV(filepath.Join(outRoot, "errors.csv"), errorSummary); err != nil {
			fmt.Printf("failed to write errors CSV: %v\n", err)
		}
	}

	if err := writeHTMLReport(filepath.Join(outRoot, "report.html"), allResults, methodData, errorSummary, errorSeriesData, *reportMaxPts); err != nil {
		fmt.Printf("failed to write HTML report: %v\n", err)
	}

	fmt.Printf("\nReport written to: %s\n", filepath.Join(outRoot, "report.html"))
}

func exitf(format string, args ...interface{}) {
	fmt.Printf(format+"\n", args...)
	os.Exit(1)
}

func printResult(r Result) {
	if r.Targets != "" {
		fmt.Printf("  mode=%s conc=%d ok=%d err=%d rps=%.2f p95=%.1fms targets=%s\n",
			r.Mode, r.Concurrency, r.Success, r.Errors, r.RPS, r.P95Ms, r.Targets)
		return
	}
	fmt.Printf("  mode=%s conc=%d ok=%d err=%d rps=%.2f p95=%.1fms\n",
		r.Mode, r.Concurrency, r.Success, r.Errors, r.RPS, r.P95Ms)
}
