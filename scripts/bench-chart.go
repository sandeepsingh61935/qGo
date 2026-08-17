// Regenerates assets/bench-throughput.svg from bench-results.json.
//
//	go run ./cmd/bench -jobs 5000 -json bench-results.json
//	go run ./scripts/bench-chart.go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type latency struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

type scenario struct {
	Name          string   `json:"name"`
	ThroughputOps float64  `json:"throughput_ops_per_sec"`
	Latency       latency  `json:"latency_ms"`
	Process       *latency `json:"process_latency_ms"`
}

type report struct {
	GeneratedAt time.Time `json:"generated_at"`
	GoVersion   string    `json:"go_version"`
	NumCPU      int       `json:"num_cpu"`
	Config      struct {
		Jobs         int `json:"jobs"`
		Producers    int `json:"producers"`
		Consumers    int `json:"consumers"`
		PayloadBytes int `json:"payload_bytes"`
	} `json:"config"`
	Scenarios []scenario `json:"scenarios"`
}

type barMeta struct {
	short string
	title string
	desc  string
	color string
}

var meta = map[string]barMeta{
	"enqueue_sequential": {
		short: "Enqueue\nseq",
		title: "Enqueue (1 worker)",
		desc:  "SETNX + RPUSH, single goroutine — baseline RTT",
		color: "#3d8bfd",
	},
	"enqueue_parallel": {
		short: "Enqueue\nparallel",
		title: "Enqueue (N producers)",
		desc:  "Same path, concurrent producers — scales until Redis contends",
		color: "#3dd68c",
	},
	"dequeue_parallel": {
		short: "Dequeue\nparallel",
		title: "Dequeue (N consumers)",
		desc:  "RPOPLPUSH + visibility stamp on a pre-filled queue",
		color: "#f5a524",
	},
	"e2e_produce_consume": {
		short: "E2E\nthroughput",
		title: "E2E produce→consume",
		desc:  "Parallel enqueue + dequeue + complete (no-op handler)",
		color: "#f31260",
	},
}

func main() {
	in := "bench-results.json"
	out := filepath.Join("assets", "bench-throughput.svg")
	if len(os.Args) > 1 {
		in = os.Args[1]
	}
	if len(os.Args) > 2 {
		out = os.Args[2]
	}

	raw, err := os.ReadFile(in)
	if err != nil {
		fail(err)
	}
	var r report
	if err := json.Unmarshal(raw, &r); err != nil {
		fail(err)
	}
	if len(r.Scenarios) == 0 {
		fail(fmt.Errorf("no scenarios in %s", in))
	}

	type bar struct {
		m     barMeta
		ops   float64
		name  string
		p50   float64
		proc  *latency
	}
	bars := make([]bar, 0, len(r.Scenarios))
	maxOps := 0.0
	var e2e *bar
	for _, s := range r.Scenarios {
		m, ok := meta[s.Name]
		if !ok {
			m = barMeta{
				short: s.Name,
				title: s.Name,
				desc:  s.Name,
				color: "#a78bfa",
			}
		}
		b := bar{m: m, ops: s.ThroughputOps, name: s.Name, p50: s.Latency.P50, proc: s.Process}
		bars = append(bars, b)
		if s.ThroughputOps > maxOps {
			maxOps = s.ThroughputOps
		}
		if s.Name == "e2e_produce_consume" {
			cp := b
			e2e = &cp
		}
	}

	ceiling := 5000.0
	for ceiling < maxOps*1.15 {
		ceiling += 5000
	}

	// Layout: header + plot + legend + footer
	const (
		width = 780.0
		// plot
		baseY = 300.0
		topY  = 100.0
		plotH = baseY - topY
		x0    = 200.0
		bw    = 78.0
		gap   = 36.0
		// legend starts
		legY = 340.0
	)
	height := 520.0
	if e2e != nil && e2e.proc != nil {
		height = 560.0
	}

	var b strings.Builder
	font := `font-family="ui-sans-serif,system-ui,sans-serif"`

	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" viewBox="0 0 %.0f %.0f" role="img" aria-label="qGo lab throughput: what each scenario measures">`+"\n",
		width, height, width, height)
	fmt.Fprintf(&b, `  <rect width="%.0f" height="%.0f" fill="#0f1419"/>`+"\n", width, height)

	// Header
	fmt.Fprintf(&b, `  <text x="36" y="36" fill="#e7ecf1" %s font-size="18" font-weight="700">qGo — lab throughput by scenario</text>`+"\n", font)
	fmt.Fprintf(&b, `  <text x="36" y="58" fill="#a8b6c5" %s font-size="12">Jobs/sec through Redis list primitives (not multi-AZ SLOs). Higher bar = more ops/s on this machine.</text>`+"\n", font)
	fmt.Fprintf(&b, `  <text x="36" y="78" fill="#8b9aab" %s font-size="11">Setup: Redis local · %d jobs · %d producers · %d consumers · %dB payload · %d CPUs · %s</text>`+"\n",
		font, r.Config.Jobs, r.Config.Producers, r.Config.Consumers, r.Config.PayloadBytes, r.NumCPU, strings.TrimPrefix(r.GoVersion, "go"))

	// Grid
	b.WriteString(`  <g stroke="#243040" stroke-width="1">` + "\n")
	for i := 0; i < 5; i++ {
		y := baseY - plotH*float64(i)/4
		fmt.Fprintf(&b, `    <line x1="170" y1="%.0f" x2="720" y2="%.0f"/>`+"\n", y, y)
	}
	b.WriteString(`  </g>` + "\n")

	// Y labels
	fmt.Fprintf(&b, `  <g fill="#8b9aab" %s font-size="11" text-anchor="end">`+"\n", font)
	for i := 0; i < 5; i++ {
		y := baseY - plotH*float64(i)/4
		val := ceiling * float64(i) / 4
		fmt.Fprintf(&b, `    <text x="162" y="%.0f">%s</text>`+"\n", y+4, fmtK(val))
	}
	b.WriteString(`  </g>` + "\n")
	fmt.Fprintf(&b, `  <text x="48" y="200" fill="#5c6b7a" %s font-size="11" transform="rotate(-90 48 200)">throughput (ops/s)</text>`+"\n", font)

	// Bars
	for i, bar := range bars {
		h := bar.ops / ceiling * plotH
		if h < 2 {
			h = 2
		}
		x := x0 + float64(i)*(bw+gap)
		y := baseY - h
		fmt.Fprintf(&b, `  <rect x="%.0f" y="%.0f" width="%.0f" height="%.0f" fill="%s" rx="5"/>`+"\n", x, y, bw, h, bar.m.color)
		fmt.Fprintf(&b, `  <text x="%.0f" y="%.0f" fill="#e7ecf1" %s font-size="13" font-weight="600" text-anchor="middle">%s</text>`+"\n",
			x+bw/2, y-18, font, fmtK(bar.ops))
		pLabel := fmt.Sprintf("p50 %.2fms", bar.p50)
		if bar.name == "e2e_produce_consume" {
			// E2E latency sample is create→done (queue wait), not single-op RTT.
			pLabel = fmt.Sprintf("sojourn p50 %.0fms", bar.p50)
		}
		fmt.Fprintf(&b, `  <text x="%.0f" y="%.0f" fill="#8b9aab" %s font-size="10" text-anchor="middle">%s</text>`+"\n",
			x+bw/2, y-4, font, pLabel)

		// multi-line label under bar
		lines := strings.Split(bar.m.short, "\n")
		for li, line := range lines {
			fmt.Fprintf(&b, `  <text x="%.0f" y="%.0f" fill="#c5d0db" %s font-size="11" font-weight="600" text-anchor="middle">%s</text>`+"\n",
				x+bw/2, baseY+18+float64(li)*14, font, line)
		}
	}

	// Legend / what each bar means
	fmt.Fprintf(&b, `  <text x="36" y="%.0f" fill="#e7ecf1" %s font-size="13" font-weight="600">What each bar measures</text>`+"\n", legY, font)
	for i, bar := range bars {
		yy := legY + 22 + float64(i)*22
		fmt.Fprintf(&b, `  <rect x="36" y="%.0f" width="12" height="12" rx="2" fill="%s"/>`+"\n", yy-10, bar.m.color)
		fmt.Fprintf(&b, `  <text x="56" y="%.0f" fill="#c5d0db" %s font-size="12"><tspan fill="#e7ecf1" font-weight="600">%s</tspan>  —  %s</text>`+"\n",
			yy, font, bar.m.title, bar.m.desc)
	}

	// Callout: sojourn vs process
	callY := legY + 22 + float64(len(bars))*22 + 18
	if e2e != nil {
		msg := "E2E latency note: bar height is completed jobs/sec under a producer burst."
		if e2e.proc != nil {
			msg = fmt.Sprintf(
				"E2E latency note: sojourn p50 ≈ %.0fms (includes queue wait when producers outrun consumers). Process-only p50 ≈ %.2fms (dequeue→complete, no-op handler).",
				e2e.p50, e2e.proc.P50,
			)
		}
		fmt.Fprintf(&b, `  <rect x="36" y="%.0f" width="708" height="44" rx="6" fill="#1a2330" stroke="#2c3a4d"/>`+"\n", callY-16)
		fmt.Fprintf(&b, `  <text x="48" y="%.0f" fill="#f5a524" %s font-size="11" font-weight="600">Read this in an interview</text>`+"\n", callY, font)
		// wrap roughly
		wrap := wrapText(msg, 96)
		for i, line := range wrap {
			fmt.Fprintf(&b, `  <text x="48" y="%.0f" fill="#a8b6c5" %s font-size="11">%s</text>`+"\n", callY+16+float64(i)*14, font, xmlEscape(line))
		}
		callY += 44 + float64(len(wrap))*4
	}

	fmt.Fprintf(&b, `  <text x="36" y="%.0f" fill="#5c6b7a" %s font-size="10">Single-host lab result · regenerate: go run ./cmd/bench -json bench-results.json &amp;&amp; go run ./scripts/bench-chart.go</text>`+"\n",
		height-16, font)

	b.WriteString(`</svg>` + "\n")

	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		fail(err)
	}
	if err := os.WriteFile(out, []byte(b.String()), 0o644); err != nil {
		fail(err)
	}
	fmt.Printf("wrote %s (%.0fx%.0f)\n", out, width, height)
}

func fmtK(v float64) string {
	if v >= 1000 {
		return fmt.Sprintf("%.1fk", v/1000)
	}
	return fmt.Sprintf("%.0f", v)
}

func wrapText(s string, max int) []string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return nil
	}
	var lines []string
	cur := words[0]
	for _, w := range words[1:] {
		if len(cur)+1+len(w) > max {
			lines = append(lines, cur)
			cur = w
			continue
		}
		cur += " " + w
	}
	lines = append(lines, cur)
	return lines
}

func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
