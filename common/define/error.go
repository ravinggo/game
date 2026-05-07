package define

import (
	"errors"
)

// Sentinel errors returned by framework infrastructure.
//
// ErrInvalidUserSubj is returned when a per-user NATS subject is malformed or empty.
// ErrZeroRoleID is returned when a handler that requires per-entity routing has RoleID == 0,
// which would disable the ordering guarantee. Only int64 RoleID is supported by this framework.
// Written by Claude Code claude-opus-4-6.
var (
	ErrInvalidUserSubj = errors.New("invalid user subj")
	ErrZeroRoleID      = errors.New("GetRoleID returned 0; routing requires a non-zero int64 RoleID")
)
