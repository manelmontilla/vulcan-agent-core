// Copyright 2023 Adevinta

// Package backend allows to run checks using a concrete backend,
// for instance Docker.
package backend

import "context"

// Backend runs checks using a specific container technology. The backend is
// responsible for pulling the container image of the check, setting its
// environment variables, making available in the proper network the REST API
// needed by the check to update its state, and running the container.
type Backend interface {
	Run(ctx context.Context, params RunParams) (<-chan RunResult, error)
}

// RunParams defines the parameters needed by the [Backend.Run] function to run
// a check.
type RunParams struct {
	CheckID          string
	CheckTypeName    string
	ChecktypeVersion string
	Image            string
	Target           string
	AssetType        string
	Options          string
	RequiredVars     []string
	Metadata         map[string]string
}

// RunResult defines the info returned by the [Backend.Run] function.
type RunResult struct {
	Output []byte
	Error  error
}
