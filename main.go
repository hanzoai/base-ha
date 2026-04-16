// base-ha launches a Hanzo Base cluster with writer/replica replication.
//
// Writer-pin uses a Quasar-derived heartbeat-lease protocol — faster and
// lighter than RAFT. All nodes are equal validators; the lowest-sorted alive
// NodeID holds the SQLite write lock. Heartbeats travel over ZAP (sub-ms
// binary protocol) with HTTP fallback.
//
// All configuration lives under the BASE_* env namespace.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hanzoai/base"
	"github.com/hanzoai/base/apis"
	"github.com/hanzoai/base/core"
	"github.com/hanzoai/base/plugins/zap"
	"github.com/hanzoai/dbx"
	"github.com/litesql/go-ha"
	sqliteha "github.com/litesql/go-sqlite-ha"
	"modernc.org/sqlite"
)

var (
	bootstrap   = make(chan struct{})
	interceptor = new(ChangeSetInterceptor)
	quasarWriter *QuasarWriterProvider // set when BASE_CONSENSUS=quasar is active
)

// envBool parses a BASE_* env var as a bool, defaulting to def on missing/invalid.
func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		log.Fatalf("invalid %s: %v", key, err)
	}
	return b
}

// envInt parses a BASE_* env var as an int, returning 0 on missing.
func envInt(key string) int {
	v := os.Getenv(key)
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid %s: %v", key, err)
	}
	return n
}

func init() {
	drv := sqliteha.Driver{
		ConnectionHook: func(conn sqlite.ExecQuerierContext, dsn string) error {
			_, err := conn.ExecContext(context.Background(), `
				PRAGMA busy_timeout       = 10000;
				PRAGMA journal_mode       = WAL;
				PRAGMA journal_size_limit = 200000000;
				PRAGMA synchronous        = NORMAL;
				PRAGMA foreign_keys       = ON;
				PRAGMA temp_store         = MEMORY;
				PRAGMA cache_size         = -16000;
			`, nil)
			return err
		},
		Options: []ha.Option{
			ha.WithName(os.Getenv("BASE_NODE_ID")),
			ha.WithReplicationURL(os.Getenv("BASE_PUBSUB_URL")),
			ha.WithWaitFor(bootstrap),
			ha.WithChangeSetInterceptor(interceptor),
		},
	}

	if envBool("BASE_ASYNC_PUBLISHER", false) {
		drv.Options = append(drv.Options,
			ha.WithAsyncPublisher(),
			ha.WithAsyncPublisherOutboxDir(os.Getenv("BASE_ASYNC_PUBLISHER_DIR")),
		)
	}

	stream := os.Getenv("BASE_REPLICATION_STREAM")
	if stream == "" {
		stream = "base"
	}
	drv.Options = append(drv.Options, ha.WithReplicationStream(stream))

	// Embedded Hanzo pubsub (NATS-compatible fork) is optional.
	// Use it when BASE_PUBSUB_EMBEDDED=true or a config file / port is set.
	var embedded *ha.EmbeddedNatsConfig
	if cfgFile := os.Getenv("BASE_PUBSUB_CONFIG"); cfgFile != "" {
		embedded = &ha.EmbeddedNatsConfig{File: cfgFile}
	} else if port := envInt("BASE_PUBSUB_PORT"); port != 0 {
		embedded = &ha.EmbeddedNatsConfig{
			Port:     port,
			StoreDir: os.Getenv("BASE_PUBSUB_STORE_DIR"),
		}
	}
	if replicas := envInt("BASE_REPLICAS"); replicas > 0 {
		drv.Options = append(drv.Options, ha.WithReplicas(replicas))
	}
	drv.Options = append(drv.Options, ha.WithEmbeddedNatsConfig(embedded))

	if rid := os.Getenv("BASE_ROW_IDENTIFY"); rid != "" {
		switch rid {
		case string(ha.PK):
			drv.Options = append(drv.Options, ha.WithRowIdentify(ha.PK))
		case string(ha.Rowid):
			drv.Options = append(drv.Options, ha.WithRowIdentify(ha.Rowid))
		case string(ha.Full):
			drv.Options = append(drv.Options, ha.WithRowIdentify(ha.Full))
		default:
			log.Fatalf("invalid BASE_ROW_IDENTIFY: %s", rid)
		}
	}

	// Leader election — pick exactly one strategy.
	// Precedence: static > quasar (default for dynamic) > pubsub-based dynamic fallback
	switch {
	case os.Getenv("BASE_STATIC_WRITER") != "":
		drv.Options = append(drv.Options,
			ha.WithLeaderProvider(&ha.StaticLeader{Target: os.Getenv("BASE_STATIC_WRITER")}),
		)
	case os.Getenv("BASE_LOCAL_TARGET") != "":
		local := os.Getenv("BASE_LOCAL_TARGET")
		mode := strings.ToLower(os.Getenv("BASE_CONSENSUS"))
		if mode == "" {
			mode = "quasar" // default: quasar heartbeat-lease — faster than RAFT
		}
		switch mode {
		case "quasar":
			provider, err := NewQuasarWriterProvider(QuasarWriterConfig{
				NodeID:      os.Getenv("BASE_NODE_ID"),
				LocalTarget: local,
				Peers:       splitCSV(os.Getenv("BASE_PEERS")),
			})
			if err != nil {
				log.Fatalf("quasar writer init: %v", err)
			}
			quasarWriter = provider
			drv.Options = append(drv.Options, ha.WithLeaderProvider(provider))
		case "pubsub":
			// Fallback: go-ha's built-in graft-over-pubsub dynamic leader.
			drv.Options = append(drv.Options, ha.WithLeaderElectionLocalTarget(local))
		default:
			log.Fatalf("invalid BASE_CONSENSUS: %s (use quasar or pubsub)", mode)
		}
	}

	sql.Register("base_ha", &drv)
	dbx.BuilderFuncMap["base_ha"] = dbx.BuilderFuncMap["sqlite"]
}

func main() {
	app := base.NewWithConfig(base.Config{
		DBConnect: func(dbPath string) (*dbx.DB, error) {
			return dbx.Open("base_ha", dbPath)
		},
	})

	// Register ZAP transport — sub-ms binary protocol for heartbeats + data.
	zap.MustRegister(app)

	app.OnServe().BindFunc(func(se *core.ServeEvent) error {
		close(bootstrap)

		var dataDSN string
		for _, dsn := range ha.ListDSN() {
			if strings.HasSuffix(dsn, "data.db") {
				dataDSN = dsn
				break
			}
		}

		connector, ok := ha.LookupConnector(dataDSN)
		if !ok {
			return fmt.Errorf("ha connector not found for %q", dataDSN)
		}
		slog.Info("waiting for writer election")
		<-connector.LeaderProvider().Ready()

		if connector.LeaderProvider().IsLeader() {
			// force sync token definition on the writer only
			_, err := app.ConcurrentDB().Update("_collections",
				dbx.Params{"updated": time.Now().Format("2006-01-02 15:04:05.000Z")},
				dbx.In("name", "_superusers", "users")).Execute()
			if err != nil {
				return fmt.Errorf("failed to sync token definition: %w", err)
			}
		}

		if email, pass := os.Getenv("BASE_SUPERUSER_EMAIL"), os.Getenv("BASE_SUPERUSER_PASS"); email != "" && pass != "" {
			col, err := app.FindCachedCollectionByNameOrId(core.CollectionNameSuperusers)
			if err != nil {
				return fmt.Errorf("fetch %q: %w", core.CollectionNameSuperusers, err)
			}
			su, err := app.FindAuthRecordByEmail(col, email)
			if err != nil {
				su = core.NewRecord(col)
			}
			su.SetEmail(email)
			su.SetPassword(pass)
			if err := app.Save(su); err != nil {
				return fmt.Errorf("set superuser: %w", err)
			}
		}

		timeout := 10 * time.Second
		se.Router.BindFunc(apis.WrapStdMiddleware(connector.ForwardToLeader(timeout, "POST", "PUT", "PATCH", "DELETE")))
		se.Router.BindFunc(apis.WrapStdMiddleware(connector.ConsistentReader(timeout, "GET")))
		if quasarWriter != nil {
			se.Router.POST("/_ha/heartbeat", func(e *core.RequestEvent) error {
				quasarWriter.HandleHeartbeat(e.Response, e.Request)
				return nil
			})
		}
		return se.Next()
	})

	app.OnTerminate().BindFunc(func(e *core.TerminateEvent) error {
		ha.Shutdown()
		return e.Next()
	})

	interceptor.app = app
	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}
