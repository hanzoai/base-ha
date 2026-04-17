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
	"encoding/json"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hanzoai/base"
	"github.com/hanzoai/base/core"
	"github.com/hanzoai/base/plugins/zap"
	"github.com/hanzoai/dbx"
	"github.com/litesql/go-ha"
	sqliteha "github.com/litesql/go-sqlite-ha"
	"modernc.org/sqlite"
)

var (
	bootstrap          = make(chan struct{})
	interceptor        = new(ChangeSetInterceptor)
	quasarWriter       *QuasarWriterProvider  // set when BASE_CONSENSUS=quasar
	consensusPublisher *ConsensusPublisher    // set when replication=consensus (default)
	consensusSubscriber *ConsensusSubscriber  // set on replicas when replication=consensus
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
	peers := splitCSV(os.Getenv("BASE_PEERS"))
	replicationMode := strings.ToLower(os.Getenv("BASE_REPLICATION"))
	if replicationMode == "" {
		if os.Getenv("BASE_PUBSUB_URL") != "" || os.Getenv("BASE_PUBSUB_PORT") != "" {
			replicationMode = "pubsub" // explicit broker config → pubsub
		} else {
			replicationMode = "consensus" // default: direct consensus replication
		}
	}

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
			ha.WithWaitFor(bootstrap),
			ha.WithChangeSetInterceptor(interceptor),
		},
	}

	switch replicationMode {
	case "consensus":
		// Quasar consensus replication — no broker, direct peer delivery.
		nodeID := os.Getenv("BASE_NODE_ID")
		writerURL := os.Getenv("BASE_LOCAL_TARGET")

		consensusPublisher = NewConsensusPublisher(nodeID, peers)
		consensusSubscriber = NewConsensusSubscriber(ConsensusSubscriberConfig{
			Node:        nodeID,
			WriterURL:   writerURL,
			Interceptor: interceptor,
		})
		drv.Options = append(drv.Options,
			ha.WithReplicationPublisher(consensusPublisher),
			ha.WithReplicationSubscriber(consensusSubscriber),
		)
		slog.Info("replication: consensus mode (no broker)")

	case "pubsub":
		// Hanzo pubsub / NATS fallback.
		drv.Options = append(drv.Options,
			ha.WithReplicationURL(os.Getenv("BASE_PUBSUB_URL")),
		)
		stream := os.Getenv("BASE_REPLICATION_STREAM")
		if stream == "" {
			stream = "base"
		}
		drv.Options = append(drv.Options, ha.WithReplicationStream(stream))

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

		if envBool("BASE_ASYNC_PUBLISHER", false) {
			drv.Options = append(drv.Options,
				ha.WithAsyncPublisher(),
				ha.WithAsyncPublisherOutboxDir(os.Getenv("BASE_ASYNC_PUBLISHER_DIR")),
			)
		}
		slog.Info("replication: pubsub mode", "url", os.Getenv("BASE_PUBSUB_URL"))
	}

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
			slog.Error("ha connector not found", "dsn", dataDSN)
			return se.Next()
		}
		slog.Info("waiting for writer election")
		<-connector.LeaderProvider().Ready()

		timeout := 10 * time.Second

		// Write-forwarding: replicas reverse-proxy mutating requests to the writer.
		se.Router.BindFunc(func(e *core.RequestEvent) error {
			m := e.Request.Method
			if (m == "POST" || m == "PUT" || m == "PATCH" || m == "DELETE") && !connector.LeaderProvider().IsLeader() {
				target := connector.LeaderProvider().RedirectTarget()
				if target == "" {
					http.Error(e.Response, "no writer available", http.StatusServiceUnavailable)
					return nil
				}
				connector.ForwardToLeaderFunc(
					func(w http.ResponseWriter, r *http.Request) {},
					timeout, m,
				)(e.Response, e.Request)
				return nil
			}
			return e.Next()
		})
		if quasarWriter != nil {
			se.Router.POST("/_ha/heartbeat", func(e *core.RequestEvent) error {
				quasarWriter.HandleHeartbeat(e.Response, e.Request)
				return nil
			})
		}
		// Consensus replication endpoints
		if consensusPublisher != nil {
			se.Router.GET("/_ha/replicate/log", func(e *core.RequestEvent) error {
				consensusPublisher.HandleLog(e.Response, e.Request)
				return nil
			})
		}
		if consensusSubscriber != nil {
			se.Router.POST("/_ha/replicate", func(e *core.RequestEvent) error {
				var env replicateEnvelope
				if err := json.NewDecoder(e.Request.Body).Decode(&env); err != nil {
					return e.BadRequestError("invalid envelope", err)
				}
				consensusSubscriber.Receive(env)
				e.Response.WriteHeader(http.StatusAccepted)
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
