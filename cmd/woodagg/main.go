package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"woodagg/internal/bench"
	"woodagg/internal/check"
	"woodagg/internal/data"
	"woodagg/internal/engine"
	"woodagg/internal/server"
)

func main() {
	if len(os.Args) == 1 {
		serveCmd([]string{"-addr", ":8090"})
		return
	}
	switch os.Args[1] {
	case "serve":
		serveCmd(os.Args[2:])
	case "verify":
		verifyCmd(os.Args[2:])
	case "bench":
		benchCmd(os.Args[2:])
	case "-h", "--help", "help":
		printHelp()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(2)
	}
}

func printHelp() {
	fmt.Println(`woodagg commands:
  serve   [-addr :8090]
  verify  [--scenario dozens|all|ID] [--modes adaptive,always_pre,never_pre]
          [--groups N] [--rows N] [--threads N] [--local N] [--partitions N] [--json]
  bench   [--scenario dozens] [--threads N] [--local N] [--partitions N] [--json]`)
}

func serveCmd(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", ":8090", "HTTP listen address")
	_ = fs.Parse(args)
	srv := server.New()
	listenAddr := *addr
	if strings.HasPrefix(listenAddr, ":") {
		listenAddr = "localhost" + listenAddr
	}
	fmt.Printf("woodagg dashboard: http://%s\n", listenAddr)
	if err := http.ListenAndServe(*addr, srv.Handler()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func verifyCmd(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	scenarioID := fs.String("scenario", "dozens", "scenario ID or 'all'")
	modesFlag := fs.String("modes", "adaptive,always_pre,never_pre", "comma-separated modes")
	groups := fs.Int("groups", 0, "override target groups")
	rows := fs.Int("rows", 0, "override row count")
	threads := fs.Int("threads", 8, "worker threads")
	localMax := fs.Int("local", engine.DefaultConfig().LocalTableMax, "local table max entries")
	partitions := fs.Int("partitions", engine.DefaultConfig().Partitions, "partition count")
	asJSON := fs.Bool("json", false, "emit JSON")
	_ = fs.Parse(args)

	modes := parseModes(*modesFlag)
	scenarios := selectScenarios(*scenarioID)
	reports := make([]check.Report, 0)
	allOK := true

	for _, base := range scenarios {
		sc := base
		if *groups > 0 {
			sc.Groups = *groups
		}
		if *rows > 0 {
			sc.Rows = *rows
		}
		cfg := engine.DefaultConfig()
		cfg.Threads = *threads
		cfg.LocalTableMax = *localMax
		cfg.Partitions = *partitions
		rt := engine.NewRuntimeConfig()
		rt.Set(cfg)

		fmt.Fprintf(os.Stderr, "→ naive materialize %s groups≈%d rows=%d\n", sc.ID, sc.Groups, sc.Rows)
		naiveStart := time.Now()
		naive := check.RunNaive(sc, nil)
		fmt.Fprintf(os.Stderr, "  naive ready: %d groups in %s\n", len(naive.Groups), time.Since(naiveStart).Round(time.Millisecond))
		naive.ReleaseMaterializedRows()

		for _, mode := range modes {
			res, err := engine.Run(context.Background(), engine.Options{
				Scenario: sc, Mode: mode, Config: rt,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "engine error %s/%s: %v\n", sc.ID, mode, err)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "  engine done %s/%s: %d groups in %dms; comparing...\n",
				sc.ID, mode, res.Report.Groups, res.Report.DurationMS)
			rep := check.Compare(sc, res, naive)
			reports = append(reports, rep)
			allOK = allOK && rep.OK
			if *asJSON {
				continue
			}
			status := "PASS"
			if !rep.OK {
				status = "FAIL"
			}
			fmt.Printf("[%s] %s / %s engine=%d naive=%d checked=%d mismatch=%d maxFloatErr=%.3e dur=%dms decisions=%v\n",
				status, sc.ID, mode, rep.EngineGroups, rep.NaiveGroups, rep.CheckedGroups,
				rep.MismatchCount, rep.MaxFloatRelErr, res.Report.DurationMS, res.Report.Decisions)
			for _, m := range rep.Mismatches {
				fmt.Println("  " + m)
			}
			res = nil
			runtime.GC()
		}
		naive = nil
		runtime.GC()
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(reports)
	}
	if !allOK {
		os.Exit(1)
	}
}

func benchCmd(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	scenarioID := fs.String("scenario", "dozens", "scenario ID")
	threads := fs.Int("threads", engine.DefaultConfig().Threads, "worker threads")
	localMax := fs.Int("local", engine.DefaultConfig().LocalTableMax, "local table max entries")
	partitions := fs.Int("partitions", engine.DefaultConfig().Partitions, "base partition count")
	asJSON := fs.Bool("json", false, "emit JSON")
	_ = fs.Parse(args)

	sc, ok := data.FindScenario(*scenarioID)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", *scenarioID)
		os.Exit(2)
	}
	cfg := engine.DefaultConfig()
	cfg.Threads = *threads
	cfg.LocalTableMax = *localMax
	cfg.Partitions = *partitions
	rt := engine.NewRuntimeConfig()
	rt.Set(cfg)
	mon := engine.NewMonitor(rt)
	rep, err := bench.Run(context.Background(), sc, rt, mon)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
		return
	}
	fmt.Printf("Scenario %s: %s\n", rep.ScenarioID, rep.ScenarioName)
	for _, r := range rep.Results {
		fmt.Printf("%-12s dur=%8dms rows/s=%12.0f groups=%10d peakHeap=%8s allocDelta=%8s gcHeap=%8s parts=%d decisions=%v\n",
			r.Mode, r.DurationMS, r.RowsPerSecond, r.Groups,
			humanBytes(r.PeakHeap), humanBytes(r.TotalAllocDelta), humanBytes(r.HeapAfterGC),
			r.EffectivePartitions, r.Decisions)
	}
}

func selectScenarios(id string) []data.Scenario {
	all := data.DefaultScenarios()
	if id == "all" {
		return all
	}
	sc, ok := data.FindScenario(id)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", id)
		os.Exit(2)
	}
	return []data.Scenario{sc}
}

func parseModes(s string) []engine.Mode {
	var out []engine.Mode
	for _, p := range strings.Split(s, ",") {
		switch strings.TrimSpace(p) {
		case "adaptive":
			out = append(out, engine.ModeAdaptive)
		case "always_pre", "always":
			out = append(out, engine.ModeAlwaysPre)
		case "never_pre", "never":
			out = append(out, engine.ModeNeverPre)
		}
	}
	if len(out) == 0 {
		out = []engine.Mode{engine.ModeAdaptive}
	}
	return out
}

func humanBytes(v uint64) string {
	const unit = 1024
	if v < unit {
		return fmt.Sprintf("%dB", v)
	}
	div, exp := uint64(unit), 0
	for n := v / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(v)/float64(div), "KMGT"[exp])
}
