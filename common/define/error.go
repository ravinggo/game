package define

import (
	"errors"
)

var (
	ErrInvalidUserSubj = errors.New("invalid user subj")
	ErrInvalidToHash   = errors.New("ToHash return 0, must implement ToHash and not return 0")
)
