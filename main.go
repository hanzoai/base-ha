package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"strconv"

	"github.com/litesql/go-ha"
	sqliteha "github.com/litesql/go-sqlite-ha"
	"github.com/litesql/sqlite"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"
)

var (
	bootstrap   = make(chan struct{})
	interceptor = new(ChangeSetInterceptor)
)

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
			ha.WithName(os.Getenv("PB_NAME")),
			ha.WithReplicationURL(os.Getenv("PB_REPLICATION_URL")),
			ha.WithWaitFor(bootstrap),
			ha.WithChangeSetInterceptor(interceptor),
		},
	}

	stream := os.Getenv("PB_REPLICATION_STREAM")
	if stream == "" {
		stream = "pb"
	}
	drv.Options = append(drv.Options, ha.WithReplicationStream(stream))

	var embeddedNatsConfig *ha.EmbeddedNatsConfig
	if natsConfigFile := os.Getenv("PB_NATS_CONFIG"); natsConfigFile != "" {
		embeddedNatsConfig = &ha.EmbeddedNatsConfig{
			File: natsConfigFile,
		}
	} else if natsPort := os.Getenv("PB_NATS_PORT"); natsPort != "" {
		port, err := strconv.Atoi(natsPort)
		if err != nil {
			panic("invalid PB_NATS_PORT value:" + err.Error())
		}
		embeddedNatsConfig = &ha.EmbeddedNatsConfig{
			Port:     port,
			StoreDir: os.Getenv("PB_NATS_STORE_DIR"),
		}
	}
	if replicas := os.Getenv("PB_REPLICAS"); replicas != "" {
		replicasInt, err := strconv.Atoi(replicas)
		if err != nil {
			panic("invalid PB_REPLICAS value:" + err.Error())
		}
		drv.Options = append(drv.Options, ha.WithReplicas(replicasInt))
	}
	drv.Options = append(drv.Options, ha.WithEmbeddedNatsConfig(embeddedNatsConfig))
	sql.Register("pb_ha", &drv)

	dbx.BuilderFuncMap["pb_ha"] = dbx.BuilderFuncMap["sqlite"]
}

func main() {
	app := pocketbase.NewWithConfig(pocketbase.Config{
		DBConnect: func(dbPath string) (*dbx.DB, error) {
			return dbx.Open("pb_ha", dbPath)
		},
	})
	interceptor.app = app

	app.OnServe().BindFunc(func(e *core.ServeEvent) error {
		close(bootstrap)
		return e.Next()
	})

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
}

type ChangeSetInterceptor struct {
	app core.App
}

func (i *ChangeSetInterceptor) BeforeApply(cs *ha.ChangeSet, _ *sql.Conn) (skip bool, err error) {
	for _, change := range cs.Changes {
		if change.Table == "_authOrigins" {
			return true, nil
		}
	}
	return false, nil
}

func (i *ChangeSetInterceptor) AfterApply(cs *ha.ChangeSet, _ *sql.Conn, err error) error {
	if err == nil {
		var reloadCollections, reloadSettings bool
		for _, change := range cs.Changes {
			if change.Table == "_collections" {
				reloadCollections = true
			}
			if change.Table == "_params" {
				reloadSettings = true
			}
		}
		if reloadCollections {
			i.app.ReloadCachedCollections()
		}
		if reloadSettings {
			i.app.ReloadSettings()
		}
	}
	return err
}
