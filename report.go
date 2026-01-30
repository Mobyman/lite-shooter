package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "embed"
)

//go:embed report_template.html
var reportTemplate string

type methodKey struct {
	Config      string
	Mode        string
	Concurrency int
	Method      string
}

type methodSeries struct {
	Sec   []int
	P50   []float64
	P90   []float64
	P95   []float64
	P99   []float64
	OK    []float64
	Err   []float64
	Total int
	Start int64
}

type methodSeriesEntry struct {
	Config      string    `json:"config"`
	Mode        string    `json:"mode"`
	Concurrency int       `json:"concurrency"`
	Method      string    `json:"method"`
	Sec         []int     `json:"sec"`
	P50         []float64 `json:"p50"`
	P90         []float64 `json:"p90"`
	P95         []float64 `json:"p95"`
	P99         []float64 `json:"p99"`
	OK          []float64 `json:"ok"`
	Err         []float64 `json:"err"`
	Total       int       `json:"total"`
	StartMs     int64     `json:"start_ms,omitempty"`
}

type errorKey struct {
	Config      string
	Mode        string
	Concurrency int
	Request     string
	Code        string
	Error       string
}

type errorSummaryEntry struct {
	Config      string `json:"config"`
	Mode        string `json:"mode"`
	Concurrency int    `json:"concurrency"`
	Request     string `json:"request"`
	Code        string `json:"code"`
	Error       string `json:"error"`
	Count       int    `json:"count"`
}

type errorSeriesKey struct {
	Config      string
	Mode        string
	Concurrency int
	Code        string
}

type errorSeries struct {
	Start int64
	Sec   []int
	Cnt   []float64
}

type errorSeriesEntry struct {
	Config      string    `json:"config"`
	Mode        string    `json:"mode"`
	Concurrency int       `json:"concurrency"`
	Code        string    `json:"code"`
	StartMs     int64     `json:"start_ms"`
	Sec         []int     `json:"sec"`
	Cnt         []float64 `json:"cnt"`
}

func writeCSV(path string, results []Result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{"config", "targets", "mode", "concurrency", "total", "success", "errors", "duration_ms", "rps", "avg_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms"}
	if err := w.Write(header); err != nil {
		return err
	}
	for _, r := range results {
		row := []string{
			r.Config,
			r.Targets,
			r.Mode,
			strconv.Itoa(r.Concurrency),
			strconv.Itoa(r.Total),
			strconv.Itoa(r.Success),
			strconv.Itoa(r.Errors),
			fmt.Sprintf("%d", r.Duration.Milliseconds()),
			fmt.Sprintf("%.4f", r.RPS),
			fmt.Sprintf("%.4f", r.AvgMs),
			fmt.Sprintf("%.4f", r.P50Ms),
			fmt.Sprintf("%.4f", r.P90Ms),
			fmt.Sprintf("%.4f", r.P95Ms),
			fmt.Sprintf("%.4f", r.P99Ms),
			fmt.Sprintf("%.4f", r.MaxMs),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func readResultsJSON(path string) ([]Result, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var results []Result
	if err := json.Unmarshal(b, &results); err != nil {
		return nil, err
	}
	return results, nil
}

func regenerateReport(reportFrom, reqLogSpec string, maxPoints int) error {
	reportDir := strings.TrimSpace(reportFrom)
	if reportDir == "" {
		return fmt.Errorf("report-from is empty")
	}
	info, err := os.Stat(reportDir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		reportDir = filepath.Dir(reportDir)
	}
	summaryPath := filepath.Join(reportDir, "summary.json")
	results, err := readResultsJSON(summaryPath)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", summaryPath, err)
	}

	reqLogSpec = strings.TrimSpace(reqLogSpec)
	reqLogPath := ""
	switch strings.ToLower(reqLogSpec) {
	case "", "off":
		reqLogPath = ""
	case "auto":
		reqLogPath = filepath.Join(reportDir, "requests.jsonl")
	default:
		reqLogPath = reqLogSpec
	}

	var methodData map[methodKey]methodSeries
	var errorSummary []errorSummaryEntry
	var errorSeriesData map[errorSeriesKey]errorSeries
	if reqLogPath != "" {
		if _, err := os.Stat(reqLogPath); err != nil {
			return fmt.Errorf("request log not found: %s", reqLogPath)
		}
		md, err := buildMethodSeriesFromLog(reqLogPath)
		if err != nil {
			return fmt.Errorf("failed to parse request log: %w", err)
		}
		methodData = md
		ed, err := buildErrorSummaryFromLog(reqLogPath)
		if err != nil {
			return fmt.Errorf("failed to build error summary: %w", err)
		}
		errorSummary = ed
		es, err := buildErrorSeriesFromLog(reqLogPath)
		if err != nil {
			return fmt.Errorf("failed to build error series: %w", err)
		}
		errorSeriesData = es
	}

	reportPath := filepath.Join(reportDir, "report.html")
	if err := writeHTMLReport(reportPath, results, methodData, errorSummary, errorSeriesData, maxPoints); err != nil {
		return err
	}
	fmt.Printf("\nReport written to: %s\n", reportPath)
	return nil
}

func writeHTMLReport(path string, results []Result, methods map[methodKey]methodSeries, errorsSummary []errorSummaryEntry, errorSeries map[errorSeriesKey]errorSeries, maxPoints int) error {
	if maxPoints < 0 {
		maxPoints = 0
	}
	results = downsampleResults(results, maxPoints)
	methods = downsampleMethodSeries(methods, maxPoints)
	errorSeries = downsampleErrorSeries(errorSeries, maxPoints)
	configs := uniqueConfigs(results)
	sanity := buildSanityBlock(results)
	origMethods := methods
	summarySection := buildSummarySection(results, configs, origMethods)
	errorsSection := buildErrorsSection(errorsSummary, configs)
	chartsSection := buildChartsSection(configs)
	methodEntries := flattenMethodSeries(methods)
	errorEntries := flattenErrorSeries(errorSeries)
	reportJSON, _ := json.Marshal(struct {
		Results []Result            `json:"results"`
		Methods []methodSeriesEntry `json:"methods"`
		Errors  []errorSeriesEntry  `json:"errors"`
	}{
		Results: results,
		Methods: methodEntries,
		Errors:  errorEntries,
	})
	tmpl := strings.TrimSpace(reportTemplate)
	if tmpl == "" {
		return fmt.Errorf("report template is empty")
	}

	body := strings.ReplaceAll(tmpl, "{{TIME}}", time.Now().Format(time.RFC3339))
	body = strings.ReplaceAll(body, "{{SANITY}}", sanity)
	body = strings.ReplaceAll(body, "{{SUMMARY_SECTION}}", summarySection)
	body = strings.ReplaceAll(body, "{{ERRORS_SECTION}}", errorsSection)
	body = strings.ReplaceAll(body, "{{CHARTS_SECTION}}", chartsSection)
	body = strings.ReplaceAll(body, "{{MAX_POINTS}}", strconv.Itoa(maxPoints))
	body = strings.ReplaceAll(body, "{{REPORT_JSON}}", string(reportJSON))

	return os.WriteFile(path, []byte(body), 0o644)
}

func writeErrorsCSV(path string, entries []errorSummaryEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{"config", "mode", "concurrency", "request", "code", "count", "error"}); err != nil {
		return err
	}
	for _, e := range entries {
		row := []string{
			e.Config,
			e.Mode,
			strconv.Itoa(e.Concurrency),
			e.Request,
			e.Code,
			strconv.Itoa(e.Count),
			e.Error,
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return w.Error()
}

func buildHTMLTable(results []Result) string {
	if len(results) == 0 {
		return ""
	}
	byConfig := map[string][]Result{}
	var configs []string
	for _, r := range results {
		if _, ok := byConfig[r.Config]; !ok {
			configs = append(configs, r.Config)
		}
		byConfig[r.Config] = append(byConfig[r.Config], r)
	}
	sort.Strings(configs)

	headers := []string{"Mode", "Conc", "Total", "OK", "Err", "Duration ms", "RPS", "Avg ms", "P50", "P90", "P95", "P99", "Max"}
	var b strings.Builder
	b.WriteString("<div class=\"summary-grid\">")
	for _, cfg := range configs {
		list := byConfig[cfg]
		sort.Slice(list, func(i, j int) bool {
			if list[i].Mode != list[j].Mode {
				return list[i].Mode < list[j].Mode
			}
			return list[i].Concurrency < list[j].Concurrency
		})
		title := htmlEsc(cfg)
		if list[0].Targets != "" {
			title += " — " + htmlEsc(list[0].Targets)
		}
		b.WriteString("<div>")
		b.WriteString("<div class=\"summary-title\">" + title + "</div>")
		b.WriteString("<table class=\"table\">\n")
		b.WriteString("<thead><tr>")
		for _, h := range headers {
			b.WriteString("<th>")
			b.WriteString(h)
			b.WriteString("</th>")
		}
		b.WriteString("</tr></thead><tbody>\n")
		for _, r := range list {
			b.WriteString("<tr class=\"item\">")
			b.WriteString("<td>" + htmlEsc(r.Mode) + "</td>")
			b.WriteString("<td>" + strconv.Itoa(r.Concurrency) + "</td>")
			b.WriteString("<td>" + strconv.Itoa(r.Total) + "</td>")
			b.WriteString("<td>" + strconv.Itoa(r.Success) + "</td>")
			b.WriteString("<td>" + strconv.Itoa(r.Errors) + "</td>")
			b.WriteString("<td>" + strconv.FormatInt(r.Duration.Milliseconds(), 10) + "</td>")
			b.WriteString("<td>" + fmt.Sprintf("%.2f", r.RPS) + "</td>")
			b.WriteString("<td>" + fmt.Sprintf("%.1f", r.AvgMs) + "</td>")
			b.WriteString("<td>" + fmt.Sprintf("%.1f", r.P50Ms) + "</td>")
			b.WriteString("<td>" + fmt.Sprintf("%.1f", r.P90Ms) + "</td>")
			b.WriteString("<td>" + fmt.Sprintf("%.1f", r.P95Ms) + "</td>")
			b.WriteString("<td>" + fmt.Sprintf("%.1f", r.P99Ms) + "</td>")
			b.WriteString("<td>" + fmt.Sprintf("%.1f", r.MaxMs) + "</td>")
			b.WriteString("</tr>\n")
		}
		b.WriteString("</tbody></table>")
		b.WriteString("</div>")
	}
	b.WriteString("</div>")
	return b.String()
}

func uniqueConfigs(results []Result) []string {
	if len(results) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var configs []string
	for _, r := range results {
		if !seen[r.Config] {
			seen[r.Config] = true
			configs = append(configs, r.Config)
		}
	}
	sort.Strings(configs)
	return configs
}

func buildSummarySection(results []Result, configs []string, methods map[methodKey]methodSeries) string {
	if len(results) == 0 || len(configs) == 0 {
		return ""
	}
	byConfig := map[string][]Result{}
	for _, r := range results {
		byConfig[r.Config] = append(byConfig[r.Config], r)
	}

	var b strings.Builder
	b.WriteString("<section class=\"section\">")
	b.WriteString("<h2>Summary</h2>")
	b.WriteString("<div class=\"config-grid\">")
	for _, cfg := range configs {
		list := byConfig[cfg]
		if len(list) == 0 {
			continue
		}
		sort.Slice(list, func(i, j int) bool {
			if list[i].Mode != list[j].Mode {
				return list[i].Mode < list[j].Mode
			}
			return list[i].Concurrency < list[j].Concurrency
		})
		title := htmlEsc(cfg)
		if list[0].Targets != "" {
			title += " — " + htmlEsc(list[0].Targets)
		}
		b.WriteString("<div class=\"card\">")
		b.WriteString("<div class=\"summary-title\">" + title + "</div>")
		b.WriteString(buildSummaryTable(list))
		if mt := buildMethodSummaryTable(cfg, methods); mt != "" {
			b.WriteString("<div class=\"summary-title\">Methods</div>")
			b.WriteString(mt)
		}
		b.WriteString("</div>")
	}
	b.WriteString("</div>")
	b.WriteString("</section>")
	return b.String()
}

func buildErrorsSection(errorsSummary []errorSummaryEntry, configs []string) string {
	if len(configs) == 0 {
		return ""
	}
	byConfig := map[string][]errorSummaryEntry{}
	for _, e := range errorsSummary {
		byConfig[e.Config] = append(byConfig[e.Config], e)
	}
	if len(errorsSummary) == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("<section class=\"section\">")
	b.WriteString("<h2>Errors</h2>")
	b.WriteString("<div class=\"config-grid\">")
	for _, cfg := range configs {
		cfgErrors := byConfig[cfg]
		b.WriteString("<div class=\"card\">")
		b.WriteString("<div class=\"summary-title\">" + htmlEsc(cfg) + "</div>")
		if len(cfgErrors) == 0 {
			b.WriteString("<div class=\"chart-title\">no errors</div>")
		} else {
			b.WriteString(buildErrorTable(cfgErrors))
		}
		b.WriteString("</div>")
	}
	b.WriteString("</div>")
	b.WriteString("</section>")
	return b.String()
}

func buildChartsSection(configs []string) string {
	if len(configs) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("<section class=\"section\">")
	b.WriteString("<h2>Charts</h2>")
	b.WriteString("<div class=\"config-grid\">")
	for _, cfg := range configs {
		b.WriteString("<div class=\"card\">")
		b.WriteString("<div class=\"summary-title\">" + htmlEsc(cfg) + "</div>")
		b.WriteString("<div class=\"chart\"><div class=\"charts-root\" data-config=\"" + htmlEsc(cfg) + "\"></div></div>")
		b.WriteString("</div>")
	}
	b.WriteString("</div>")
	b.WriteString("</section>")
	return b.String()
}

func buildSanityBlock(results []Result) string {
	if len(results) < 2 {
		return ""
	}
	type key struct {
		Mode string
		Conc int
	}
	group := map[key][]Result{}
	for _, r := range results {
		group[key{Mode: r.Mode, Conc: r.Concurrency}] = append(group[key{Mode: r.Mode, Conc: r.Concurrency}], r)
	}
	var keys []key
	for k := range group {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Mode != keys[j].Mode {
			return keys[i].Mode < keys[j].Mode
		}
		return keys[i].Conc < keys[j].Conc
	})

	var b strings.Builder
	b.WriteString("<section class=\"section sanity\">")
	b.WriteString("<h2>Sanity check</h2>")
	b.WriteString("<div class=\"hint\">Compare RPS and latency across configs for identical mode/concurrency.</div>")
	for _, k := range keys {
		list := group[k]
		if len(list) < 2 {
			continue
		}
		sort.Slice(list, func(i, j int) bool { return list[i].Config < list[j].Config })

		bestRPS := list[0].RPS
		bestAvg := list[0].AvgMs
		for _, r := range list[1:] {
			if r.RPS > bestRPS {
				bestRPS = r.RPS
			}
			if r.AvgMs < bestAvg {
				bestAvg = r.AvgMs
			}
		}

		b.WriteString("<div class=\"card\">")
		b.WriteString("<div class=\"summary-title\">" + htmlEsc(k.Mode) + " · concurrency " + strconv.Itoa(k.Conc) + "</div>")
		b.WriteString("<table class=\"table\"><thead><tr>")
		headers := []string{"Config", "RPS", "Avg ms", "P50", "ΔRPS vs best", "ΔAvg vs best"}
		for _, h := range headers {
			b.WriteString("<th>" + h + "</th>")
		}
		b.WriteString("</tr></thead><tbody>")
		for _, r := range list {
			dRPS := percentDelta(r.RPS, bestRPS)
			dAvg := percentDelta(r.AvgMs, bestAvg)
			b.WriteString("<tr class=\"item\">")
			b.WriteString("<td>" + htmlEsc(r.Config) + "</td>")
			b.WriteString("<td>" + fmt.Sprintf("%.2f", r.RPS) + "</td>")
			b.WriteString("<td>" + fmt.Sprintf("%.1f", r.AvgMs) + "</td>")
			b.WriteString("<td>" + fmt.Sprintf("%.1f", r.P50Ms) + "</td>")
			b.WriteString("<td>" + deltaBadge(dRPS) + "</td>")
			b.WriteString("<td>" + deltaBadge(dAvg) + "</td>")
			b.WriteString("</tr>")
		}
		b.WriteString("</tbody></table></div>")
	}
	b.WriteString("</section>")
	return b.String()
}

func percentDelta(v, best float64) float64 {
	if best == 0 {
		return 0
	}
	return (v/best - 1.0) * 100.0
}

func deltaBadge(v float64) string {
	class := "delta"
	av := math.Abs(v)
	switch {
	case av >= 40:
		class += " bad"
	case av >= 20:
		class += " warn"
	}
	return "<span class=\"" + class + "\">" + fmt.Sprintf("%+.0f%%", v) + "</span>"
}

func buildSummaryTable(results []Result) string {
	headers := []string{"Mode", "Conc", "Total", "OK", "Err", "Duration ms", "RPS", "Avg ms", "P50", "P90", "P95", "P99", "Max"}
	var b strings.Builder
	b.WriteString("<table class=\"table\">\n")
	b.WriteString("<thead><tr>")
	for _, h := range headers {
		b.WriteString("<th>")
		b.WriteString(h)
		b.WriteString("</th>")
	}
	b.WriteString("</tr></thead><tbody>\n")
	for _, r := range results {
		b.WriteString("<tr class=\"item\">")
		b.WriteString("<td>" + htmlEsc(r.Mode) + "</td>")
		b.WriteString("<td>" + strconv.Itoa(r.Concurrency) + "</td>")
		b.WriteString("<td>" + strconv.Itoa(r.Total) + "</td>")
		b.WriteString("<td>" + strconv.Itoa(r.Success) + "</td>")
		b.WriteString("<td>" + strconv.Itoa(r.Errors) + "</td>")
		b.WriteString("<td>" + strconv.FormatInt(r.Duration.Milliseconds(), 10) + "</td>")
		b.WriteString("<td>" + fmt.Sprintf("%.2f", r.RPS) + "</td>")
		b.WriteString("<td>" + fmt.Sprintf("%.1f", r.AvgMs) + "</td>")
		b.WriteString("<td>" + fmt.Sprintf("%.1f", r.P50Ms) + "</td>")
		b.WriteString("<td>" + fmt.Sprintf("%.1f", r.P90Ms) + "</td>")
		b.WriteString("<td>" + fmt.Sprintf("%.1f", r.P95Ms) + "</td>")
		b.WriteString("<td>" + fmt.Sprintf("%.1f", r.P99Ms) + "</td>")
		b.WriteString("<td>" + fmt.Sprintf("%.1f", r.MaxMs) + "</td>")
		b.WriteString("</tr>\n")
	}
	b.WriteString("</tbody></table>")
	return b.String()
}

func buildMethodSummaryTable(cfg string, methods map[methodKey]methodSeries) string {
	if len(methods) == 0 {
		return ""
	}
	type row struct {
		Mode   string
		Conc   int
		Method string
		Total  int
		OK     int
		Err    int
		RPS    float64
	}
	var rows []row
	for k, v := range methods {
		if k.Config != cfg {
			continue
		}
		okSum := 0
		errSum := 0
		for _, n := range v.OK {
			okSum += int(n)
		}
		for _, n := range v.Err {
			errSum += int(n)
		}
		total := okSum + errSum
		if v.Total > 0 {
			total = v.Total
		}
		secs := len(v.OK)
		if secs == 0 {
			secs = len(v.Sec)
		}
		if secs == 0 {
			secs = 1
		}
		rows = append(rows, row{
			Mode:   k.Mode,
			Conc:   k.Concurrency,
			Method: k.Method,
			Total:  total,
			OK:     okSum,
			Err:    errSum,
			RPS:    float64(total) / float64(secs),
		})
	}
	if len(rows) == 0 {
		return ""
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Mode != rows[j].Mode {
			return rows[i].Mode < rows[j].Mode
		}
		if rows[i].Conc != rows[j].Conc {
			return rows[i].Conc < rows[j].Conc
		}
		return rows[i].Method < rows[j].Method
	})

	headers := []string{"Mode", "Conc", "Method", "Total", "OK", "Err", "Avg RPS"}
	var b strings.Builder
	b.WriteString("<table class=\"table method-table\">\n")
	b.WriteString("<thead><tr>")
	for _, h := range headers {
		b.WriteString("<th>" + h + "</th>")
	}
	b.WriteString("</tr></thead><tbody>")
	for _, r := range rows {
		b.WriteString("<tr class=\"item\">")
		b.WriteString("<td>" + htmlEsc(r.Mode) + "</td>")
		b.WriteString("<td>" + strconv.Itoa(r.Conc) + "</td>")
		b.WriteString("<td>" + htmlEsc(r.Method) + "</td>")
		b.WriteString("<td>" + strconv.Itoa(r.Total) + "</td>")
		b.WriteString("<td>" + strconv.Itoa(r.OK) + "</td>")
		b.WriteString("<td>" + strconv.Itoa(r.Err) + "</td>")
		b.WriteString("<td>" + fmt.Sprintf("%.2f", r.RPS) + "</td>")
		b.WriteString("</tr>")
	}
	b.WriteString("</tbody></table>")
	return b.String()
}

func downsampleResults(results []Result, maxPoints int) []Result {
	if maxPoints <= 0 || len(results) == 0 {
		return results
	}
	out := make([]Result, len(results))
	for i, r := range results {
		out[i] = r
		if len(r.SeriesSec) == 0 || len(r.SeriesSec) <= maxPoints {
			continue
		}
		idxs := sampleIndices(len(r.SeriesSec), maxPoints)
		out[i].SeriesSec = sampleInts(r.SeriesSec, idxs)
		out[i].SeriesRPS = sampleFloats(r.SeriesRPS, idxs)
		out[i].SeriesErr = sampleFloats(r.SeriesErr, idxs)
		out[i].SeriesP50 = sampleFloats(r.SeriesP50, idxs)
		out[i].SeriesP90 = sampleFloats(r.SeriesP90, idxs)
		out[i].SeriesP95 = sampleFloats(r.SeriesP95, idxs)
		out[i].SeriesP99 = sampleFloats(r.SeriesP99, idxs)
	}
	return out
}

func downsampleMethodSeries(methods map[methodKey]methodSeries, maxPoints int) map[methodKey]methodSeries {
	if maxPoints <= 0 || len(methods) == 0 {
		return methods
	}
	out := make(map[methodKey]methodSeries, len(methods))
	for k, v := range methods {
		if len(v.Sec) == 0 || len(v.Sec) <= maxPoints {
			out[k] = v
			continue
		}
		idxs := sampleIndices(len(v.Sec), maxPoints)
		v.Sec = sampleInts(v.Sec, idxs)
		v.P50 = sampleFloats(v.P50, idxs)
		v.P90 = sampleFloats(v.P90, idxs)
		v.P95 = sampleFloats(v.P95, idxs)
		v.P99 = sampleFloats(v.P99, idxs)
		v.OK = sampleFloats(v.OK, idxs)
		v.Err = sampleFloats(v.Err, idxs)
		out[k] = v
	}
	return out
}

func downsampleErrorSeries(errors map[errorSeriesKey]errorSeries, maxPoints int) map[errorSeriesKey]errorSeries {
	if maxPoints <= 0 || len(errors) == 0 {
		return errors
	}
	out := make(map[errorSeriesKey]errorSeries, len(errors))
	for k, v := range errors {
		if len(v.Sec) == 0 || len(v.Sec) <= maxPoints {
			out[k] = v
			continue
		}
		idxs := sampleIndices(len(v.Sec), maxPoints)
		v.Sec = sampleInts(v.Sec, idxs)
		v.Cnt = sampleFloats(v.Cnt, idxs)
		out[k] = v
	}
	return out
}

func sampleIndices(n, maxPoints int) []int {
	if n <= 0 {
		return nil
	}
	if maxPoints <= 0 || n <= maxPoints {
		idxs := make([]int, n)
		for i := range idxs {
			idxs[i] = i
		}
		return idxs
	}
	step := int(math.Ceil(float64(n) / float64(maxPoints)))
	if step < 1 {
		step = 1
	}
	idxs := make([]int, 0, maxPoints+1)
	for i := 0; i < n; i += step {
		idxs = append(idxs, i)
	}
	if idxs[len(idxs)-1] != n-1 {
		idxs = append(idxs, n-1)
	}
	return idxs
}

func sampleInts(src []int, idxs []int) []int {
	if len(src) == 0 || len(idxs) == 0 {
		return nil
	}
	out := make([]int, 0, len(idxs))
	for _, idx := range idxs {
		if idx >= 0 && idx < len(src) {
			out = append(out, src[idx])
		}
	}
	return out
}

func sampleFloats(src []float64, idxs []int) []float64 {
	if len(src) == 0 || len(idxs) == 0 {
		return nil
	}
	out := make([]float64, 0, len(idxs))
	for _, idx := range idxs {
		if idx >= 0 && idx < len(src) {
			out = append(out, src[idx])
		}
	}
	return out
}

func buildErrorTable(errorsSummary []errorSummaryEntry) string {
	if len(errorsSummary) == 0 {
		return ""
	}
	sort.Slice(errorsSummary, func(i, j int) bool {
		if errorsSummary[i].Request != errorsSummary[j].Request {
			return errorsSummary[i].Request < errorsSummary[j].Request
		}
		if errorsSummary[i].Config != errorsSummary[j].Config {
			return errorsSummary[i].Config < errorsSummary[j].Config
		}
		if errorsSummary[i].Mode != errorsSummary[j].Mode {
			return errorsSummary[i].Mode < errorsSummary[j].Mode
		}
		if errorsSummary[i].Concurrency != errorsSummary[j].Concurrency {
			return errorsSummary[i].Concurrency < errorsSummary[j].Concurrency
		}
		if errorsSummary[i].Code != errorsSummary[j].Code {
			return errorsSummary[i].Code < errorsSummary[j].Code
		}
		return errorsSummary[i].Count > errorsSummary[j].Count
	})
	limit := 200
	if len(errorsSummary) < limit {
		limit = len(errorsSummary)
	}
	var b strings.Builder
	b.WriteString("<table class=\"table\">\n")
	b.WriteString("<thead><tr>")
	headers := []string{"Config", "Mode", "Conc", "Request", "Code", "Count", "Error"}
	for _, h := range headers {
		b.WriteString("<th>" + h + "</th>")
	}
	b.WriteString("</tr></thead>\n")
	b.WriteString("<tbody>\n")
	lastReq := ""
	for i := 0; i < limit; i++ {
		e := errorsSummary[i]
		b.WriteString("<tr class=\"item\">")
		b.WriteString("<td>" + htmlEsc(e.Config) + "</td>")
		b.WriteString("<td>" + htmlEsc(e.Mode) + "</td>")
		b.WriteString("<td>" + strconv.Itoa(e.Concurrency) + "</td>")
		reqCell := ""
		if e.Request != lastReq {
			reqCell = htmlEsc(e.Request)
			lastReq = e.Request
		}
		b.WriteString("<td>" + reqCell + "</td>")
		b.WriteString("<td><span class=\"badge\">" + htmlEsc(e.Code) + "</span></td>")
		b.WriteString("<td>" + strconv.Itoa(e.Count) + "</td>")
		b.WriteString("<td>" + htmlEsc(e.Error) + "</td>")
		b.WriteString("</tr>\n")
	}
	b.WriteString("</tbody></table>\n")
	return b.String()
}

func htmlEsc(s string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		"\"", "&quot;",
		"'", "&#39;",
	)
	return replacer.Replace(s)
}

func flattenMethodSeries(methods map[methodKey]methodSeries) []methodSeriesEntry {
	if len(methods) == 0 {
		return nil
	}
	out := make([]methodSeriesEntry, 0, len(methods))
	for k, v := range methods {
		out = append(out, methodSeriesEntry{
			Config:      k.Config,
			Mode:        k.Mode,
			Concurrency: k.Concurrency,
			Method:      k.Method,
			Sec:         v.Sec,
			P50:         v.P50,
			P90:         v.P90,
			P95:         v.P95,
			P99:         v.P99,
			OK:          v.OK,
			Err:         v.Err,
			Total:       v.Total,
			StartMs:     v.Start,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Mode != out[j].Mode {
			return out[i].Mode < out[j].Mode
		}
		if out[i].Config != out[j].Config {
			return out[i].Config < out[j].Config
		}
		if out[i].Concurrency != out[j].Concurrency {
			return out[i].Concurrency < out[j].Concurrency
		}
		return out[i].Method < out[j].Method
	})
	return out
}

func classifyError(s string) string {
	v := strings.ToLower(strings.TrimSpace(s))
	switch {
	case v == "":
		return "unknown"
	case strings.Contains(v, "context deadline exceeded") || strings.Contains(v, "timeout"):
		return "timeout"
	case strings.Contains(v, "unknown query"):
		return "unknown_query"
	case strings.Contains(v, "connection reset"):
		return "conn_reset"
	case strings.Contains(v, "broken pipe"):
		return "broken_pipe"
	case strings.Contains(v, "eof"):
		return "eof"
	case strings.Contains(v, "canceled"):
		return "canceled"
	case strings.Contains(v, "not found"):
		return "not_found"
	default:
		return "other"
	}
}

func buildErrorSummaryFromLog(path string) ([]errorSummaryEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	counts := map[errorKey]int{}
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)
	for scanner.Scan() {
		var e logEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue
		}
		if e.OK {
			continue
		}
		code := classifyError(e.Error)
		key := errorKey{
			Config:      e.Config,
			Mode:        e.Mode,
			Concurrency: e.Concurrency,
			Request:     e.Request,
			Code:        code,
			Error:       e.Error,
		}
		counts[key]++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	out := make([]errorSummaryEntry, 0, len(counts))
	for k, v := range counts {
		out = append(out, errorSummaryEntry{
			Config:      k.Config,
			Mode:        k.Mode,
			Concurrency: k.Concurrency,
			Request:     k.Request,
			Code:        k.Code,
			Error:       k.Error,
			Count:       v,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		if out[i].Config != out[j].Config {
			return out[i].Config < out[j].Config
		}
		if out[i].Mode != out[j].Mode {
			return out[i].Mode < out[j].Mode
		}
		if out[i].Concurrency != out[j].Concurrency {
			return out[i].Concurrency < out[j].Concurrency
		}
		return out[i].Request < out[j].Request
	})
	return out, nil
}

func flattenErrorSeries(errors map[errorSeriesKey]errorSeries) []errorSeriesEntry {
	if len(errors) == 0 {
		return nil
	}
	out := make([]errorSeriesEntry, 0, len(errors))
	for k, v := range errors {
		out = append(out, errorSeriesEntry{
			Config:      k.Config,
			Mode:        k.Mode,
			Concurrency: k.Concurrency,
			Code:        k.Code,
			StartMs:     v.Start,
			Sec:         v.Sec,
			Cnt:         v.Cnt,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Mode != out[j].Mode {
			return out[i].Mode < out[j].Mode
		}
		if out[i].Config != out[j].Config {
			return out[i].Config < out[j].Config
		}
		if out[i].Concurrency != out[j].Concurrency {
			return out[i].Concurrency < out[j].Concurrency
		}
		return out[i].Code < out[j].Code
	})
	return out
}

func buildErrorSeriesFromLog(path string) (map[errorSeriesKey]errorSeries, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	type bounds struct {
		min time.Time
		max time.Time
	}
	boundsMap := map[errorSeriesKey]bounds{}

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)
	for scanner.Scan() {
		var e logEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue
		}
		if e.OK {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, e.Ts)
		if err != nil {
			continue
		}
		code := classifyError(e.Error)
		key := errorSeriesKey{Config: e.Config, Mode: e.Mode, Concurrency: e.Concurrency, Code: code}
		if b, ok := boundsMap[key]; ok {
			if t.Before(b.min) {
				b.min = t
			}
			if t.After(b.max) {
				b.max = t
			}
			boundsMap[key] = b
		} else {
			boundsMap[key] = bounds{min: t, max: t}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	_, _ = f.Seek(0, 0)
	type agg struct {
		start time.Time
		cnt   []int64
	}
	aggs := map[errorSeriesKey]*agg{}
	for k, b := range boundsMap {
		secs := int(b.max.Sub(b.min).Seconds()) + 1
		if secs < 1 {
			secs = 1
		}
		aggs[k] = &agg{
			start: b.min,
			cnt:   make([]int64, secs),
		}
	}

	scanner = bufio.NewScanner(f)
	scanner.Buffer(buf, 10*1024*1024)
	for scanner.Scan() {
		var e logEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue
		}
		if e.OK {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, e.Ts)
		if err != nil {
			continue
		}
		code := classifyError(e.Error)
		key := errorSeriesKey{Config: e.Config, Mode: e.Mode, Concurrency: e.Concurrency, Code: code}
		a := aggs[key]
		if a == nil {
			continue
		}
		sec := int(t.Sub(a.start).Seconds())
		if sec < 0 {
			sec = 0
		}
		if sec >= len(a.cnt) {
			sec = len(a.cnt) - 1
		}
		a.cnt[sec]++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	out := map[errorSeriesKey]errorSeries{}
	for k, a := range aggs {
		series := errorSeries{
			Start: a.start.UTC().UnixMilli(),
			Sec:   make([]int, len(a.cnt)),
			Cnt:   make([]float64, len(a.cnt)),
		}
		for i := 0; i < len(a.cnt); i++ {
			series.Sec[i] = i + 1
			series.Cnt[i] = float64(a.cnt[i])
		}
		out[k] = series
	}
	return out, nil
}

func buildMethodSeriesFromLog(path string) (map[methodKey]methodSeries, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	type bounds struct {
		min time.Time
		max time.Time
	}
	boundsMap := map[methodKey]bounds{}

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)
	for scanner.Scan() {
		var e logEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, e.Ts)
		if err != nil {
			continue
		}
		key := methodKey{Config: e.Config, Mode: e.Mode, Concurrency: e.Concurrency, Method: e.Request}
		if b, ok := boundsMap[key]; ok {
			if t.Before(b.min) {
				b.min = t
			}
			if t.After(b.max) {
				b.max = t
			}
			boundsMap[key] = b
		} else {
			boundsMap[key] = bounds{min: t, max: t}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	_, _ = f.Seek(0, 0)
	type agg struct {
		start time.Time
		per   [][]int64
		ok    []int64
		err   []int64
		total int
	}
	aggs := map[methodKey]*agg{}

	for k, b := range boundsMap {
		secs := int(b.max.Sub(b.min).Seconds()) + 1
		if secs < 1 {
			secs = 1
		}
		aggs[k] = &agg{
			start: b.min,
			per:   make([][]int64, secs),
			ok:    make([]int64, secs),
			err:   make([]int64, secs),
		}
	}

	scanner = bufio.NewScanner(f)
	scanner.Buffer(buf, 10*1024*1024)
	for scanner.Scan() {
		var e logEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, e.Ts)
		if err != nil {
			continue
		}
		key := methodKey{Config: e.Config, Mode: e.Mode, Concurrency: e.Concurrency, Method: e.Request}
		a := aggs[key]
		if a == nil {
			continue
		}
		sec := int(t.Sub(a.start).Seconds())
		if sec < 0 {
			sec = 0
		}
		if sec >= len(a.ok) {
			sec = len(a.ok) - 1
		}
		a.total++
		if e.OK {
			a.ok[sec]++
			a.per[sec] = append(a.per[sec], e.LatencyMs)
		} else {
			a.err[sec]++
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	out := map[methodKey]methodSeries{}
	for k, a := range aggs {
		p50, p90, p95, p99 := percentileSeries(a.per)
		series := methodSeries{
			Sec:   make([]int, len(a.ok)),
			P50:   p50,
			P90:   p90,
			P95:   p95,
			P99:   p99,
			OK:    make([]float64, len(a.ok)),
			Err:   make([]float64, len(a.ok)),
			Total: a.total,
			Start: a.start.UTC().UnixMilli(),
		}
		for i := 0; i < len(a.ok); i++ {
			series.Sec[i] = i + 1
			series.OK[i] = float64(a.ok[i])
			series.Err[i] = float64(a.err[i])
		}
		out[k] = series
	}
	return out, nil
}
