package main

import (
	"context"
	"database/sql"

	"github.com/hanzoai/base/core"
	"github.com/litesql/go-ha"
)

// ChangeSetInterceptor bridges go-ha change-set events into Base's model
// event hooks so followers fire the same post-write events as the leader.
type ChangeSetInterceptor struct {
	app core.App
}

func (i *ChangeSetInterceptor) BeforeApply(cs *ha.ChangeSet, _ *sql.Conn) (skip bool, err error) {
	return false, nil
}

func (i *ChangeSetInterceptor) AfterApply(cs *ha.ChangeSet, _ *sql.Conn, err error) error {
	var reloadCollections, reloadSettings bool
	for _, change := range cs.Changes {
		switch change.Table {
		case "_collections":
			reloadCollections = true
		case "_params":
			reloadSettings = true
		}
		if m := modelFromChange(change, err); m != nil {
			m.triggerAfterEvent(i.app)
		}
	}
	if err == nil {
		if reloadCollections {
			i.app.ReloadCachedCollections()
		}
		if reloadSettings {
			i.app.ReloadSettings()
		}
	}
	return err
}

var _ core.Model = (*replicatedModel)(nil)

// replicatedModel is a minimal core.Model built from a replicated change,
// used only to fire the appropriate after-event hooks on followers.
type replicatedModel struct {
	tableName string
	pk        any
	oldPk     any
	new       bool
	eventType string
	err       error
}

func modelFromChange(c ha.Change, err error) *replicatedModel {
	m := replicatedModel{tableName: c.Table, err: err}
	switch c.Operation {
	case "INSERT":
		m.new = true
		m.eventType = core.ModelEventTypeCreate
	case "UPDATE":
		m.oldPk = c.PKOldValues()[0]
		m.eventType = core.ModelEventTypeUpdate
	case "DELETE":
		m.oldPk = c.PKOldValues()[0]
		m.eventType = core.ModelEventTypeDelete
	default:
		return nil
	}
	m.pk = c.PKNewValues()[0]
	return &m
}

func (m *replicatedModel) TableName() string  { return m.tableName }
func (m *replicatedModel) PK() any             { return m.pk }
func (m *replicatedModel) LastSavedPK() any    { return m.oldPk }
func (m *replicatedModel) IsNew() bool         { return m.new }
func (m *replicatedModel) MarkAsNew()          { m.oldPk = nil; m.new = true }
func (m *replicatedModel) MarkAsNotNew()       { m.oldPk = m.pk; m.new = false }

func (m *replicatedModel) triggerAfterEvent(app core.App) {
	event := &core.ModelEvent{
		App:     app,
		Context: context.Background(),
		Type:    m.eventType,
	}
	event.Model = m
	switch m.eventType {
	case core.ModelEventTypeCreate:
		if m.err != nil {
			app.OnModelAfterCreateError().Trigger(&core.ModelErrorEvent{ModelEvent: *event, Error: m.err})
			return
		}
		app.OnModelAfterCreateSuccess().Trigger(event)
	case core.ModelEventTypeUpdate:
		if m.err != nil {
			app.OnModelAfterUpdateError().Trigger(&core.ModelErrorEvent{ModelEvent: *event, Error: m.err})
			return
		}
		app.OnModelAfterUpdateSuccess().Trigger(event)
	case core.ModelEventTypeDelete:
		if m.err != nil {
			app.OnModelAfterDeleteError().Trigger(&core.ModelErrorEvent{ModelEvent: *event, Error: m.err})
			return
		}
		app.OnModelAfterDeleteSuccess().Trigger(event)
	}
}
