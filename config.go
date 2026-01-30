package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tonkeeper/tongo/config"
	"github.com/tonkeeper/tongo/liteapi"
)

type configItem struct {
	Path string
	Name string
}

func parseIntList(v string) ([]int, error) {
	parts := strings.Split(v, ",")
	var out []int
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		i, err := strconv.Atoi(p)
		if err != nil || i <= 0 {
			return nil, fmt.Errorf("invalid int: %s", p)
		}
		out = append(out, i)
	}
	return out, nil
}

func resolveConfigPaths(spec string) ([]configItem, error) {
	parts := strings.Split(spec, ",")
	seen := map[string]bool{}
	var out []configItem
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		name := ""
		pathSpec := p
		if eq := strings.Index(p, "="); eq > 0 {
			name = strings.TrimSpace(p[:eq])
			pathSpec = strings.TrimSpace(p[eq+1:])
		}

		matches, err := filepath.Glob(pathSpec)
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			matches = []string{pathSpec}
		}
		for _, m := range matches {
			if seen[m] {
				continue
			}
			seen[m] = true

			cfgName := filepath.Base(m)
			if name != "" {
				if len(matches) == 1 {
					cfgName = name
				} else {
					cfgName = name + ":" + filepath.Base(m)
				}
			}
			out = append(out, configItem{Path: m, Name: cfgName})
		}
	}
	return out, nil
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envOrInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func envOrBool(key string, def bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		switch strings.ToLower(v) {
		case "1", "true", "yes", "y", "on":
			return true
		case "0", "false", "no", "n", "off":
			return false
		}
	}
	return def
}

func loadDotEnv(path string) {
	if path == "" {
		return
	}
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if key == "" {
			continue
		}
		if (strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"")) ||
			(strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'")) {
			val = strings.TrimSuffix(strings.TrimPrefix(val, val[:1]), val[:1])
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, val)
	}
}

func parseProofPolicy(v string) (liteapi.ProofPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "unsafe":
		return liteapi.ProofPolicyUnsafe, nil
	case "fast", "secure":
		return liteapi.ProofPolicyFast, nil
	default:
		return liteapi.ProofPolicyFast, fmt.Errorf("unknown policy")
	}
}

func parseBlockRange(spec string) (blockRange, error) {
	spec = strings.ToLower(strings.TrimSpace(spec))
	if spec == "" {
		return blockRange{}, errors.New("empty")
	}
	if strings.HasPrefix(spec, "last:") {
		nStr := strings.TrimPrefix(spec, "last:")
		n, err := strconv.Atoi(nStr)
		if err != nil || n <= 0 {
			return blockRange{}, fmt.Errorf("invalid last:N")
		}
		return blockRange{count: int32(n), mode: "last"}, nil
	}
	if strings.HasPrefix(spec, "range:") {
		pair := strings.TrimPrefix(spec, "range:")
		parts := strings.Split(pair, "-")
		if len(parts) != 2 {
			return blockRange{}, fmt.Errorf("invalid range")
		}
		from, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
		to, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err1 != nil || err2 != nil || from <= 0 || to <= 0 || from > to {
			return blockRange{}, fmt.Errorf("invalid range values")
		}
		return blockRange{from: int32(from), to: int32(to), mode: "range"}, nil
	}
	return blockRange{}, fmt.Errorf("unknown spec")
}

func parseDurationOptional(spec string) (time.Duration, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" || spec == "0" || strings.EqualFold(spec, "off") {
		return 0, nil
	}
	return time.ParseDuration(spec)
}

func formatTargets(cfg *config.GlobalConfigurationFile) string {
	if cfg == nil || len(cfg.LiteServers) == 0 {
		return ""
	}
	parts := make([]string, 0, len(cfg.LiteServers))
	for _, ls := range cfg.LiteServers {
		parts = append(parts, ls.Host)
	}
	return strings.Join(parts, ", ")
}
