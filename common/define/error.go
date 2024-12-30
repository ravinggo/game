package define

import (
	"errors"
)

var (
	ErrIntOverflow          = errors.New("common: integer overflow")
	ErrInvalidLength        = errors.New("common: negative length found during unmarshaling")
	ErrUnexpectedEndOfGroup = errors.New("common: unexpected end of group")
)
