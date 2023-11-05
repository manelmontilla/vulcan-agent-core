// Copyright 2023 Adevinta

// Package checks provides the functionality needed to run checks.
package checks

import (
	"encoding/json"
	"sync"
	"time"
)

const (
	StatusCreated      = "CREATED"
	StatusQueued       = "QUEUED"
	StatusAssigned     = "ASSIGNED"
	StatusRunning      = "RUNNING"
	StatusTimeout      = "TIMEOUT"
	StatusAborted      = "ABORTED"
	StatusPurging      = "PURGING"
	StatusKilled       = "KILLED"
	StatusFailed       = "FAILED"
	StatusFinished     = "FINISHED"
	StatusMalformed    = "MALFORMED"
	StatusInconclusive = "INCONCLUSIVE"
)

// Check stores the information necessary to run a check.
type Check struct {
	CheckID      string            `json:"check_id"`      // Required
	StartTime    time.Time         `json:"start_time"`    // Required
	Image        string            `json:"image"`         // Required
	Target       string            `json:"target"`        // Required
	Timeout      int               `json:"timeout"`       // Required
	AssetType    string            `json:"assettype"`     // Optional
	Options      string            `json:"options"`       // Optional
	RequiredVars []string          `json:"required_vars"` // Optional
	Metadata     map[string]string `json:"metadata"`      // Optional
	RunTime      int64
}

// func (j *Check) logTrace(msg, action string) string {
// 	if j.RunTime == 0 {
// 		j.RunTime = time.Now().Unix()
// 	}
// 	return fmt.Sprintf(
// 		"event=checkTrace checkID=%s target=%s assetType=%s checkImage=%s queuedTime=%d runningTime=%d action=%s msg=\"%s\"",
// 		j.CheckID,
// 		j.Target,
// 		j.AssetType,
// 		j.Image,
// 		j.RunTime-j.StartTime.Unix(),
// 		time.Now().Unix()-j.RunTime,
// 		action,
// 		msg,
// 	)
// }

// TerminalStatuses contains all the possible statuses of a check that are
// terminal.
var TerminalStatuses = map[string]struct{}{
	StatusFailed:       {},
	StatusFinished:     {},
	StatusInconclusive: {},
	StatusKilled:       {},
	StatusMalformed:    {},
	StatusTimeout:      {},
}

// State defines the all the possible fields of the states
// sent to the check state queue.
type State struct {
	ID       string   `json:"id" validate:"required"`
	Status   *string  `json:"status,omitempty"`
	AgentID  *string  `json:"agent_id,omitempty"`
	Report   *string  `json:"report,omitempty"`
	Raw      *string  `json:"raw,omitempty"`
	Progress *float32 `json:"progress,omitempty"`
}

// Merge overrides the fields of the receiver with the value of the non nil
// fields of the provided CheckState.
func (cs *State) Merge(s State) {
	if s.Status != nil {
		cs.Status = s.Status
	}
	if s.Raw != nil {
		cs.Raw = s.Raw
	}
	if s.AgentID != nil {
		cs.AgentID = s.AgentID
	}
	if s.Progress != nil {
		cs.Progress = s.Progress
	}
	if s.Report != nil {
		cs.Report = s.Report
	}
}

// QueueWriter defines the queue services used by an
// updater to send updates.
type QueueWriter interface {
	Write(body string) error
}

// Updater takes a CheckState an send its to a queue using the defined queue
// writer.
type Updater struct {
	qw             QueueWriter
	terminalChecks sync.Map
}

// New creates a new updater using the provided queue writer.
func NewStateUpdater(qw QueueWriter) *Updater {
	return &Updater{qw, sync.Map{}}
}

// UpdateState updates the state of tha check into the underlying queue.
// If the state is terminal it keeps the state in memory locally. If the state
// is not terminal it sends the state to queue.
func (u *Updater) UpdateState(s State) error {
	status := ""
	if s.Status != nil {
		status = *s.Status
	} else {
		storedCheckStatus, ok := u.terminalChecks.Load(s.ID)
		if ok {
			status = *(storedCheckStatus.(State)).Status
		}
	}
	if _, ok := TerminalStatuses[status]; ok {
		u.UpdateCheckStatusTerminal(s)
		return nil
	}

	// We continue with non-terminal states.
	body, err := json.Marshal(s)
	if err != nil {
		return err
	}
	err = u.qw.Write(string(body))
	if err != nil {
		return err
	}
	return nil
}

// CheckStatusTerminal returns true if a check with the given ID has
// sent so far a state update including a status in a terminal state.
func (u *Updater) CheckStatusTerminal(ID string) bool {
	_, ok := u.terminalChecks.Load(ID)
	return ok
}

// FlushCheckStatus deletes the information about a check that the
// Updater is storing. Before deleting the check from the "list" of finished
// checks, it writes the state of the check to the queue.
func (u *Updater) FlushCheckStatus(ID string) error {
	checkStatus, ok := u.terminalChecks.Load(ID)
	if ok {
		// Write the terminal status in the queue
		body, err := json.Marshal(checkStatus)
		if err != nil {
			return err
		}
		err = u.qw.Write(string(body))
		if err != nil {
			return err
		}
	}
	u.terminalChecks.Delete(ID)
	return nil
}

// UpdateCheckStatusTerminal update and keep the information about a check in a
// status terminal.
func (u *Updater) UpdateCheckStatusTerminal(s State) {
	checkState, ok := u.terminalChecks.Load(s.ID)

	if !ok {
		u.terminalChecks.Store(s.ID, s)
		return
	}
	cs := checkState.(State)

	// We update the existing CheckState.
	cs.Merge(s)

	u.terminalChecks.Store(s.ID, cs)
}
