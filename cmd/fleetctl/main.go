// Command fleetctl is a thin CLI over the fleetd REST API.
//
//	fleetctl create kong edge --replicas 2 --route echo=/echo=http://echo.default.svc:8080
//	fleetctl create postgres orders --replicas 2 --set work_mem=8MB --ext pg_stat_statements
//	fleetctl list
//	fleetctl get <id|name>
//	fleetctl scale <id|name> 3
//	fleetctl upgrade <id|name> 3.10
//	fleetctl events <id|name>
//	fleetctl failover <id|name>
//	fleetctl delete <id|name>
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

var server = envOr("FLEET_API", "http://localhost:8080")

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "create":
		err = create(args)
	case "list", "ls":
		err = list()
	case "get":
		err = withID(args, 1, func(id string) error { return printRaw("GET", "/v1/services/"+id, nil) })
	case "scale":
		err = withID(args, 2, func(id string) error {
			var n int
			if _, err := fmt.Sscan(args[1], &n); err != nil {
				return fmt.Errorf("replicas must be a number")
			}
			return printRaw("PATCH", "/v1/services/"+id, map[string]any{"replicas": n})
		})
	case "upgrade":
		err = withID(args, 2, func(id string) error {
			return printRaw("PATCH", "/v1/services/"+id, map[string]any{"version": args[1]})
		})
	case "events":
		err = withID(args, 1, events)
	case "failover":
		err = withID(args, 1, func(id string) error { return printRaw("POST", "/v1/services/"+id+"/failover", nil) })
	case "delete", "rm":
		err = withID(args, 1, func(id string) error { return printRaw("DELETE", "/v1/services/"+id, nil) })
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: fleetctl <command> [args]

  create kong <name> [--version 3.9] [--replicas N] [--route name=/path=http://upstream]... [--plugin rate-limiting]
  create postgres <name> [--version 17-alpine] [--replicas N] [--set key=value]... [--ext name]...
  list | get <svc> | scale <svc> <N> | upgrade <svc> <version>
  events <svc> | failover <svc> | delete <svc>

<svc> is an id or a name. FLEET_API sets the server (default http://localhost:8080).`)
	os.Exit(2)
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

func create(args []string) error {
	if len(args) < 2 {
		usage()
	}
	engine, name := args[0], args[1]
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	version := fs.String("version", "", "engine version (image tag)")
	replicas := fs.Int("replicas", 0, "replicas")
	var routes, plugins, sets, exts multi
	fs.Var(&routes, "route", "kong route name=/path=http://upstream (repeatable)")
	fs.Var(&plugins, "plugin", "kong global plugin name (repeatable)")
	fs.Var(&sets, "set", "postgres setting key=value (repeatable)")
	fs.Var(&exts, "ext", "postgres extension (repeatable)")
	_ = fs.Parse(args[2:])

	body := map[string]any{"name": name, "engine": engine}
	if *version != "" {
		body["version"] = *version
	}
	if *replicas > 0 {
		body["replicas"] = *replicas
	}
	switch engine {
	case "kong":
		rs := []map[string]string{}
		for _, r := range routes {
			p := strings.SplitN(r, "=", 3)
			if len(p) != 3 {
				return fmt.Errorf("--route wants name=/path=http://upstream, got %q", r)
			}
			rs = append(rs, map[string]string{"name": p[0], "path": p[1], "upstream": p[2]})
		}
		ps := []map[string]any{}
		for _, p := range plugins {
			ps = append(ps, map[string]any{"name": p})
		}
		body["kong"] = map[string]any{"routes": rs, "plugins": ps}
	case "postgres":
		settings := map[string]string{}
		for _, s := range sets {
			k, v, ok := strings.Cut(s, "=")
			if !ok {
				return fmt.Errorf("--set wants key=value, got %q", s)
			}
			settings[k] = v
		}
		body["postgres"] = map[string]any{"settings": settings, "extensions": []string(exts)}
	}
	return printRaw("POST", "/v1/services", body)
}

type service struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Engine   string `json:"engine"`
	Deleting bool   `json:"deleting"`
	Spec     struct {
		Version  string `json:"version"`
		Replicas int    `json:"replicas"`
	} `json:"spec"`
	Status struct {
		Phase         string `json:"phase"`
		ReadyReplicas int    `json:"readyReplicas"`
		Endpoint      string `json:"endpoint"`
		Message       string `json:"message"`
	} `json:"status"`
}

func list() error {
	var out struct{ Items []service }
	if err := call("GET", "/v1/services", nil, &out); err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tENGINE\tVERSION\tREADY\tPHASE\tENDPOINT")
	for _, s := range out.Items {
		phase := s.Status.Phase
		if s.Deleting {
			phase = "Deleting"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d/%d\t%s\t%s\n", s.ID, s.Name, s.Engine, s.Spec.Version, s.Status.ReadyReplicas, s.Spec.Replicas, phase, s.Status.Endpoint)
	}
	return w.Flush()
}

func events(id string) error {
	var out struct {
		Items []struct {
			Time    time.Time `json:"time"`
			Type    string    `json:"type"`
			Reason  string    `json:"reason"`
			Message string    `json:"message"`
		}
	}
	if err := call("GET", "/v1/services/"+id+"/events", nil, &out); err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "TIME\tTYPE\tREASON\tMESSAGE")
	for _, e := range out.Items {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", e.Time.Local().Format("15:04:05"), e.Type, e.Reason, e.Message)
	}
	return w.Flush()
}

// withID resolves a name to an id so users can type either.
func withID(args []string, want int, fn func(id string) error) error {
	if len(args) < want {
		usage()
	}
	ref := args[0]
	if strings.HasPrefix(ref, "svc-") {
		return fn(ref)
	}
	var out struct{ Items []service }
	if err := call("GET", "/v1/services", nil, &out); err != nil {
		return err
	}
	for _, s := range out.Items {
		if s.Name == ref {
			return fn(s.ID)
		}
	}
	return fmt.Errorf("no service named %q", ref)
}

func printRaw(method, path string, body any) error {
	var out json.RawMessage
	if err := call(method, path, body, &out); err != nil {
		return err
	}
	var buf bytes.Buffer
	_ = json.Indent(&buf, out, "", "  ")
	fmt.Println(buf.String())
	return nil
}

func call(method, path string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, server+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		var e struct{ Error string }
		if json.Unmarshal(data, &e) == nil && e.Error != "" {
			return fmt.Errorf("%s (HTTP %d)", e.Error, resp.StatusCode)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, data)
	}
	return json.Unmarshal(data, out)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
