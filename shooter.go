package main

import (
	"bufio"
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	mathrand "math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tonkeeper/tongo/liteapi"
	"github.com/tonkeeper/tongo/ton"
)

type logEntry struct {
	Ts          string `json:"ts"`
	Config      string `json:"config"`
	Targets     string `json:"targets,omitempty"`
	Mode        string `json:"mode"`
	Concurrency int    `json:"concurrency"`
	Request     string `json:"request"`
	RespBytes   int    `json:"resp_bytes,omitempty"`
	OK          bool   `json:"ok"`
	LatencyMs   int64  `json:"latency_ms"`
	Error       string `json:"error,omitempty"`
}

type reqLogger struct {
	ch chan logEntry
	wg sync.WaitGroup
	w  *bufio.Writer
	f  *os.File
}

type lockedRand struct {
	mu sync.Mutex
	r  *mathrand.Rand
}

type blockRange struct {
	from  int32
	to    int32
	count int32
	mode  string
}

type blockPicker struct {
	api         *liteapi.Client
	mode        string
	from        int32
	to          int32
	window      int32
	refresh     time.Duration
	lastRefresh time.Time
	latest      int32
	mu          sync.Mutex
	rng         *lockedRand
}

type jobRun struct {
	result      Result
	durations   []int64
	seriesSec   []int
	seriesRPS   []float64
	seriesErr   []float64
	seriesP50   []float64
	seriesP90   []float64
	seriesP95   []float64
	seriesP99   []float64
	seriesStart int64
}

func newLockedRand() *lockedRand {
	var seed int64
	var buf [8]byte
	if _, err := cryptorand.Read(buf[:]); err == nil {
		seed = int64(binaryBigEndian(buf[:]))
	} else {
		seed = time.Now().UnixNano()
	}
	return &lockedRand{r: mathrand.New(mathrand.NewSource(seed))}
}

func (lr *lockedRand) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	lr.mu.Lock()
	v := lr.r.Intn(n)
	lr.mu.Unlock()
	return v
}

func binaryBigEndian(b []byte) uint64 {
	var v uint64
	for _, c := range b {
		v = (v << 8) | uint64(c)
	}
	return v
}

func newBlockPicker(api *liteapi.Client, br blockRange, refresh time.Duration, rng *lockedRand) *blockPicker {
	return &blockPicker{
		api:     api,
		mode:    br.mode,
		from:    br.from,
		to:      br.to,
		window:  br.count,
		refresh: refresh,
		rng:     rng,
	}
}

func (p *blockPicker) pick(ctx context.Context) (int32, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mode == "range" {
		span := int(p.to - p.from + 1)
		if span <= 1 {
			return p.from, nil
		}
		seq := p.from + int32(p.rng.Intn(span))
		return seq, nil
	}
	now := time.Now()
	if p.latest == 0 || (p.refresh > 0 && now.Sub(p.lastRefresh) >= p.refresh) {
		info, err := p.api.GetMasterchainInfo(ctx)
		if err != nil {
			return 0, err
		}
		p.latest = int32(info.Last.Seqno)
		p.lastRefresh = now
	}
	start := p.latest - p.window + 1
	if start < 1 {
		start = 1
	}
	span := int(p.latest - start + 1)
	if span <= 1 {
		return p.latest, nil
	}
	seq := start + int32(p.rng.Intn(span))
	return seq, nil
}

func generateRandomAccounts(n int) ([]ton.AccountID, error) {
	if n <= 0 {
		return nil, fmt.Errorf("invalid accounts count")
	}
	out := make([]ton.AccountID, 0, n)
	for i := 0; i < n; i++ {
		var addr [32]byte
		if _, err := cryptorand.Read(addr[:]); err != nil {
			return nil, err
		}
		out = append(out, ton.AccountID{Workchain: 0, Address: addr})
	}
	return out, nil
}

func shuffleAccounts(accounts []ton.AccountID, rng *lockedRand) {
	for i := len(accounts) - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		accounts[i], accounts[j] = accounts[j], accounts[i]
	}
}

func loadAccounts(path string) ([]ton.AccountID, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []ton.AccountID
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		addr, err := ton.ParseAccountID(line)
		if err != nil {
			return nil, fmt.Errorf("invalid address '%s': %w", line, err)
		}
		out = append(out, addr)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func warmupAccounts(api *liteapi.Client, timeout time.Duration, want int, mcBlocks int, rng *lockedRand) ([]ton.AccountID, error) {
	if want <= 0 {
		return nil, fmt.Errorf("invalid accounts count")
	}
	if mcBlocks <= 0 {
		mcBlocks = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	info, err := api.GetMasterchainInfo(ctx)
	cancel()
	if err != nil {
		return nil, err
	}
	last := int32(info.Last.Seqno)
	start := last - int32(mcBlocks) + 1
	if start < 1 {
		start = 1
	}

	seqs := make([]int32, 0, last-start+1)
	for s := start; s <= last; s++ {
		seqs = append(seqs, s)
	}
	for i := len(seqs) - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		seqs[i], seqs[j] = seqs[j], seqs[i]
	}

	type accountKey struct {
		wc   int32
		addr [32]byte
	}
	seen := make(map[accountKey]ton.AccountID, want)
	for _, seq := range seqs {
		if len(seen) >= want {
			break
		}
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		mcBlock, err := api.WaitMasterchainBlock(ctx, uint32(seq), 15*time.Second)
		cancel()
		if err != nil {
			continue
		}
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		shards, err := api.GetAllShardsInfo(ctx, mcBlock)
		cancel()
		if err != nil {
			continue
		}
		shards = append(shards, mcBlock)
		for _, shard := range shards {
			if len(seen) >= want {
				break
			}
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			block, err := api.GetBlock(ctx, shard)
			cancel()
			if err != nil {
				continue
			}
			wc := block.Info.Shard.WorkchainID
			for _, key := range block.Extra.AccountBlocks.Keys() {
				if len(seen) >= want {
					break
				}
				var addr [32]byte
				copy(addr[:], key[:])
				k := accountKey{wc: wc, addr: addr}
				if _, ok := seen[k]; ok {
					continue
				}
				seen[k] = ton.AccountID{Workchain: wc, Address: addr}
			}
		}
	}

	if len(seen) == 0 {
		return nil, fmt.Errorf("warmup collected zero accounts")
	}
	out := make([]ton.AccountID, 0, len(seen))
	for _, v := range seen {
		out = append(out, v)
	}
	if len(out) > want {
		out = out[:want]
	}
	return out, nil
}

func buildBlockSeqs(api *liteapi.Client, br blockRange) ([]int32, error) {
	if br.mode == "range" {
		var seqs []int32
		for s := br.from; s <= br.to; s++ {
			seqs = append(seqs, s)
		}
		return seqs, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	info, err := api.GetMasterchainInfo(ctx)
	if err != nil {
		return nil, err
	}
	last := int32(info.Last.Seqno)
	start := last - br.count + 1
	if start < 1 {
		start = 1
	}
	var seqs []int32
	for s := start; s <= last; s++ {
		seqs = append(seqs, s)
	}
	return seqs, nil
}

func runBlockTest(api *liteapi.Client, cfgName, targets string, seqs []int32, conc int, timeout time.Duration, duration time.Duration, logger *reqLogger, randomBlocks bool, blocksRefresh time.Duration, rng *lockedRand, br blockRange) Result {
	fmt.Printf("blocks: concurrency=%d, total=%d\n", conc, len(seqs))
	start := time.Now()
	var picker *blockPicker
	if randomBlocks {
		picker = newBlockPicker(api, br, blocksRefresh, rng)
	}
	work := func(i int) error {
		seq := seqs[i]
		if picker != nil {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			ps, err := picker.pick(ctx)
			cancel()
			if err != nil {
				return err
			}
			seq = ps
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		t0 := time.Now()
		block, err := api.WaitMasterchainBlock(ctx, uint32(seq), 15*time.Second)
		logRequest(logger, cfgName, targets, string(ModeBlocks), conc, "WaitMasterchainBlock", t0, 0, err)
		if err != nil {
			return err
		}
		t1 := time.Now()
		raw, err := api.GetBlockRaw(ctx, block)
		respBytes := 0
		if err == nil {
			respBytes = len(raw.Data)
		}
		logRequest(logger, cfgName, targets, string(ModeBlocks), conc, "GetBlockRaw", t1, respBytes, err)
		return err
	}

	var jr jobRun
	if duration > 0 {
		jr = runTimedJobs(len(seqs), conc, duration, work)
	} else {
		jr = runJobs(len(seqs), conc, work)
	}
	res := jr.result
	res.Mode = string(ModeBlocks)
	res.Concurrency = conc
	if duration > 0 {
		res.Total = res.Success + res.Errors
		res.SeriesSec = jr.seriesSec
		res.SeriesRPS = jr.seriesRPS
		res.SeriesErr = jr.seriesErr
		res.SeriesP50 = jr.seriesP50
		res.SeriesP90 = jr.seriesP90
		res.SeriesP95 = jr.seriesP95
		res.SeriesP99 = jr.seriesP99
		res.SeriesStart = jr.seriesStart
	} else {
		res.Total = len(seqs)
	}
	res.Duration = time.Since(start)
	applyMetrics(&res, jr.durations)
	return res
}

func runAccountTest(api *liteapi.Client, cfgName, targets string, accounts []ton.AccountID, conc int, timeout time.Duration, duration time.Duration, logger *reqLogger, randomPick bool, rng *lockedRand) Result {
	fmt.Printf("accounts: concurrency=%d, total=%d\n", conc, len(accounts))
	start := time.Now()
	var master ton.BlockIDExt
	var masterErr error
	{
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		t0 := time.Now()
		info, err := api.GetMasterchainInfo(ctx)
		if err != nil {
			masterErr = err
		} else {
			master = info.Last.ToBlockIdExt()
		}
		logRequest(logger, cfgName, targets, string(ModeAccounts), conc, "GetMasterchainInfo", t0, 0, masterErr)
		cancel()
	}
	targetClient := api
	if masterErr == nil {
		targetClient = api.WithBlock(master)
	}
	work := func(i int) error {
		idx := i
		if randomPick {
			idx = rng.Intn(len(accounts))
		}
		addr := accounts[idx]
		if masterErr != nil {
			return masterErr
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		t0 := time.Now()
		raw, err := targetClient.GetAccountStateRaw(ctx, addr)
		respBytes := 0
		if err == nil {
			respBytes = len(raw.State) + len(raw.Proof) + len(raw.ShardProof)
		}
		logRequest(logger, cfgName, targets, string(ModeAccounts), conc, "GetAccountStateRaw", t0, respBytes, err)
		return err
	}

	var jr jobRun
	if duration > 0 {
		jr = runTimedJobs(len(accounts), conc, duration, work)
	} else {
		jr = runJobs(len(accounts), conc, work)
	}
	res := jr.result
	res.Mode = string(ModeAccounts)
	res.Concurrency = conc
	if duration > 0 {
		res.Total = res.Success + res.Errors
		res.SeriesSec = jr.seriesSec
		res.SeriesRPS = jr.seriesRPS
		res.SeriesErr = jr.seriesErr
		res.SeriesP50 = jr.seriesP50
		res.SeriesP90 = jr.seriesP90
		res.SeriesP95 = jr.seriesP95
		res.SeriesP99 = jr.seriesP99
		res.SeriesStart = jr.seriesStart
	} else {
		res.Total = len(accounts)
	}
	res.Duration = time.Since(start)
	applyMetrics(&res, jr.durations)
	return res
}

func runJobs(total, conc int, fn func(i int) error) jobRun {
	if conc > total {
		conc = total
	}
	if conc <= 0 {
		conc = 1
	}

	durations := make([]int64, total)
	var successes int64
	var errors int64

	jobs := make(chan int, conc)
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				t0 := time.Now()
				err := fn(idx)
				d := time.Since(t0).Milliseconds()
				if err != nil {
					durations[idx] = -1
					atomic.AddInt64(&errors, 1)
					continue
				}
				durations[idx] = d
				atomic.AddInt64(&successes, 1)
			}
		}()
	}

	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	return jobRun{
		result: Result{
			Success: int(successes),
			Errors:  int(errors),
		},
		durations: durations,
	}
}

func runTimedJobs(itemCount, conc int, duration time.Duration, fn func(i int) error) jobRun {
	if itemCount <= 0 {
		return jobRun{result: Result{Errors: 1}}
	}
	if conc <= 0 {
		conc = 1
	}

	var successes int64
	var errors int64
	var idx uint64
	start := time.Now()
	deadline := start.Add(duration)
	buckets := int(math.Ceil(duration.Seconds()))
	if buckets < 1 {
		buckets = 1
	}
	var mu sync.Mutex
	perSec := make([][]int64, buckets)
	perSecMu := make([]sync.Mutex, buckets)
	durations := make([]int64, 0, conc*10)
	okCounts := make([]int64, buckets)
	errCounts := make([]int64, buckets)
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if time.Now().After(deadline) {
					return
				}
				i := int(atomic.AddUint64(&idx, 1)-1) % itemCount
				t0 := time.Now()
				err := fn(i)
				d := time.Since(t0).Milliseconds()

				sec := int(time.Since(start).Seconds())
				if sec >= 0 && sec < buckets {
					if err != nil {
						atomic.AddInt64(&errCounts[sec], 1)
					} else {
						atomic.AddInt64(&okCounts[sec], 1)
						perSecMu[sec].Lock()
						perSec[sec] = append(perSec[sec], d)
						perSecMu[sec].Unlock()
					}
				}

				mu.Lock()
				if err != nil {
					durations = append(durations, -1)
					atomic.AddInt64(&errors, 1)
				} else {
					durations = append(durations, d)
					atomic.AddInt64(&successes, 1)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	seriesSec := make([]int, buckets)
	seriesRPS := countsToFloat64(okCounts)
	seriesErr := countsToFloat64(errCounts)
	seriesP50, seriesP90, seriesP95, seriesP99 := percentileSeries(perSec)
	for i := 0; i < buckets; i++ {
		seriesSec[i] = i + 1
	}

	return jobRun{
		result: Result{
			Success: int(successes),
			Errors:  int(errors),
		},
		durations:   durations,
		seriesSec:   seriesSec,
		seriesRPS:   seriesRPS,
		seriesErr:   seriesErr,
		seriesP50:   seriesP50,
		seriesP90:   seriesP90,
		seriesP95:   seriesP95,
		seriesP99:   seriesP99,
		seriesStart: start.UTC().UnixMilli(),
	}
}

// metrics helpers moved to metrics.go

func newReqLogger(path string) (*reqLogger, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	l := &reqLogger{
		ch: make(chan logEntry, 10000),
		w:  bufio.NewWriterSize(f, 1<<20),
		f:  f,
	}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		enc := json.NewEncoder(l.w)
		for entry := range l.ch {
			_ = enc.Encode(entry)
		}
		_ = l.w.Flush()
		_ = l.f.Close()
	}()
	return l, nil
}

func (l *reqLogger) Close() {
	if l == nil {
		return
	}
	close(l.ch)
	l.wg.Wait()
}

func logRequest(l *reqLogger, cfg, targets, mode string, conc int, req string, start time.Time, respBytes int, err error) {
	if l == nil {
		return
	}
	entry := logEntry{
		Ts:          start.UTC().Format(time.RFC3339Nano),
		Config:      cfg,
		Targets:     targets,
		Mode:        mode,
		Concurrency: conc,
		Request:     req,
		RespBytes:   respBytes,
		OK:          err == nil,
		LatencyMs:   time.Since(start).Milliseconds(),
	}
	if err != nil {
		entry.Error = err.Error()
	}
	l.ch <- entry
}
