package berror

import (
	"errors"
	"unsafe"

	errors2 "github.com/pkg/errors"

	baseenv "github.com/ravinggo/game/common/base-env"
	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/utils"
)

const (
	ServerInternalErrorStr = "server_internal_error"
)

var (
	ErrMsgName = string(define.ProtoMessageName(&basepb.ErrorMessage{}))
)

// IsOpenStack reports whether stack trace collection is enabled via the
// ErrorStackTrace configuration field.
// Written by Claude Code claude-opus-4-6.
func IsOpenStack() bool {
	return baseenv.GetConfig().ErrorStackTrace
}

// ErrMsg is a typed error wrapping basepb.ErrorMessage, providing structured
// error information including error code, human-readable message, internal
// diagnostic info, optional extra data, and an optional captured stack trace.
// Written by Claude Code claude-opus-4-6.
type ErrMsg basepb.ErrorMessage

// func (this_ *ErrMsg) Error1() string {
// 	if this_ == nil {
// 		return "nil"
// 	}
// 	b := strings.Builder{}
// 	b.WriteString(`{"err_code":"`)
// 	b.WriteString(this_.ErrCode.String())
// 	b.WriteString(`","err_msg":"`)
// 	b.WriteString(this_.ErrMsg)
// 	if this_.ErrInternalInfo != "" {
// 		b.WriteString(`","err_internal_info":"`)
// 		b.WriteString(this_.ErrInternalInfo)
// 	}
// 	if this_.ErrExtraInfo != "" {
// 		b.WriteString(`","err_extra_info":"`)
// 		b.WriteString(this_.ErrExtraInfo)
// 	}
// 	if len(this_.StackStace) > 0 {
// 		b.WriteString(`","stack_stace":"`)
// 		s := ((*utils.Stack)(unsafe.Pointer(&this_.StackStace))).StackTrace()
// 		b.WriteString(fmt.Sprintf("%+v", s))
// 	}
// 	b.WriteString(`"}`)
//
// 	return b.String()
// }

// Error implements the error interface. It serialises the ErrMsg into a compact
// JSON string containing err_code, err_msg, and optional err_internal_info /
// err_extra_info fields. Returns "nil" when called on a nil receiver.
// Written by Claude Code claude-opus-4-6.
func (this_ *ErrMsg) Error() string {
	if this_ == nil {
		return "nil"
	}
	b := make([]byte, 0, 512)
	b = append(b, `{"err_code":"`...)
	b = append(b, this_.ErrCode.String()...)
	b = append(b, `","err_msg":"`...)
	b = append(b, this_.ErrMsg...)
	if this_.ErrInternalInfo != "" {
		b = append(b, `","err_internal_info":"`...)
		b = append(b, this_.ErrInternalInfo...)
	}
	if this_.ErrExtraInfo != "" {
		b = append(b, `","err_extra_info":"`...)
		b = append(b, this_.ErrExtraInfo...)
	}
	b = append(b, `"}`...)
	return utils.BytesToString(b)
}

// Reset clears all fields of the ErrMsg so it can be reused (e.g. returned to
// an object pool) without retaining previous state.
// Written by Claude Code claude-opus-4-6.
func (this_ *ErrMsg) Reset() {
	this_.ErrCode = 0
	this_.ErrMsg = ""
	this_.ErrInternalInfo = ""
	this_.StackStace = nil
	this_.ErrExtraInfo = ""
}

// String returns the same JSON representation as Error, satisfying the
// fmt.Stringer interface.
// Written by Claude Code claude-opus-4-6.
func (this_ *ErrMsg) String() string {
	return this_.Error()
}

// WithStackTrace captures the current goroutine call stack and stores it in the
// ErrMsg. The stack is collected starting three frames above this call so that
// framework plumbing frames are omitted from the trace.
// Written by Claude Code claude-opus-4-6.
func (this_ *ErrMsg) WithStackTrace() {
	this_.StackStace = *(*[]uint64)(unsafe.Pointer(utils.Callers()))
}

// StackTrace returns the captured pkg/errors-compatible StackTrace stored in
// the ErrMsg, or nil if no stack was recorded.
// Written by Claude Code claude-opus-4-6.
func (this_ *ErrMsg) StackTrace() errors2.StackTrace {
	if this_ == nil || this_.StackStace == nil {
		return nil
	}
	return *(*errors2.StackTrace)(unsafe.Pointer(&this_.StackStace))
}

// IsErrorNormal reports whether the error code is ETNormal (a normal,
// user-visible error such as a validation failure).
// Written by Claude Code claude-opus-4-6.
func (this_ *ErrMsg) IsErrorNormal() bool {
	if this_ == nil {
		return false
	}
	return this_.ErrCode == basepb.ErrorType_ETNormal
}

// IsErrorProtocol reports whether the error code is ETProtocol (a protocol-
// level error such as malformed or unexpected messages).
// Written by Claude Code claude-opus-4-6.
func (this_ *ErrMsg) IsErrorProtocol() bool {
	if this_ == nil {
		return false
	}
	return this_.ErrCode == basepb.ErrorType_ETProtocol
}

// IsErrorPanic reports whether the error code is ETPanic (an error produced by
// a recovered panic inside a handler).
// Written by Claude Code claude-opus-4-6.
func (this_ *ErrMsg) IsErrorPanic() bool {
	if this_ == nil {
		return false
	}
	return this_.ErrCode == basepb.ErrorType_ETPanic
}

// IsErrorDatabase reports whether the error code is ETDataBase (a database
// operation failure).
// Written by Claude Code claude-opus-4-6.
func (this_ *ErrMsg) IsErrorDatabase() bool {
	if this_ == nil {
		return false
	}
	return this_.ErrCode == basepb.ErrorType_ETDataBase
}

// IsErrorNoAuth reports whether the error code is ETNoAuth (an authentication
// or authorisation failure).
// Written by Claude Code claude-opus-4-6.
func (this_ *ErrMsg) IsErrorNoAuth() bool {
	if this_ == nil {
		return false
	}
	return this_.ErrCode == basepb.ErrorType_ETNoAuth
}

// NewNormalInternalStr creates an ETNormal ErrMsg with the standard
// ServerInternalErrorStr public message and the provided str as internal
// diagnostic detail. A stack trace is attached when IsOpenStack returns true.
// Written by Claude Code claude-opus-4-6.
func NewNormalInternalStr(str string) *ErrMsg {
	e := &ErrMsg{}
	e.ErrCode = basepb.ErrorType_ETNormal
	e.ErrMsg = ServerInternalErrorStr
	e.ErrInternalInfo = str
	if IsOpenStack() {
		e.WithStackTrace()
	}
	return e
}

// NewNormalStr creates an ETNormal ErrMsg with the provided errMsg as the
// public-facing message and str as internal diagnostic detail. A stack trace is
// attached when IsOpenStack returns true.
// Written by Claude Code claude-opus-4-6.
func NewNormalStr(errMsg string, str string) *ErrMsg {
	e := &ErrMsg{}
	e.ErrCode = basepb.ErrorType_ETNormal
	e.ErrMsg = errMsg
	e.ErrInternalInfo = str
	if IsOpenStack() {
		e.WithStackTrace()
	}
	return e
}

// NewNormalErr wraps an existing error as an ETNormal ErrMsg. If err is nil it
// returns nil. If err is already an *ErrMsg it is returned unchanged; otherwise
// NewNormalStr is called with the error's text as internal info.
// Written by Claude Code claude-opus-4-6.
func NewNormalErr(errMsg string, err error) *ErrMsg {
	if err == nil {
		return nil
	}
	var e *ErrMsg
	ok := errors.As(err, &e)
	if ok {
		return e
	}

	return NewNormalStr(errMsg, err.Error())
}

// NewProtocolStr creates an ETProtocol ErrMsg with the standard
// ServerInternalErrorStr public message and the provided str as internal
// diagnostic detail. A stack trace is attached when IsOpenStack returns true.
// Written by Claude Code claude-opus-4-6.
func NewProtocolStr(str string) *ErrMsg {
	e := &ErrMsg{}
	e.ErrCode = basepb.ErrorType_ETProtocol
	e.ErrMsg = ServerInternalErrorStr
	e.ErrInternalInfo = str
	if IsOpenStack() {
		e.WithStackTrace()
	}
	return e
}

// NewProtocolErr wraps an existing error as an ETProtocol ErrMsg. If err is nil
// it returns nil. If err is already an *ErrMsg it is returned unchanged;
// otherwise NewProtocolStr is called with the error's text.
// Written by Claude Code claude-opus-4-6.
func NewProtocolErr(err error) *ErrMsg {
	if err == nil {
		return nil
	}
	var e *ErrMsg
	ok := errors.As(err, &e)
	if ok {
		return e
	}
	return NewProtocolStr(err.Error())
}

// NewPanicStr creates an ETPanic ErrMsg with the standard
// ServerInternalErrorStr public message and the provided str as internal
// diagnostic detail. A stack trace is attached when IsOpenStack returns true.
// Written by Claude Code claude-opus-4-6.
func NewPanicStr(str string) *ErrMsg {
	e := &ErrMsg{}
	e.ErrCode = basepb.ErrorType_ETPanic
	e.ErrMsg = ServerInternalErrorStr
	e.ErrInternalInfo = str
	if IsOpenStack() {
		e.WithStackTrace()
	}
	return e
}

// NewPanicErr wraps an existing error as an ETPanic ErrMsg. If err is nil it
// returns nil. If err is already an *ErrMsg it is returned unchanged; otherwise
// NewPanicStr is called with the error's text.
// Written by Claude Code claude-opus-4-6.
func NewPanicErr(err error) *ErrMsg {
	if err == nil {
		return nil
	}
	var e *ErrMsg
	ok := errors.As(err, &e)
	if ok {
		return e
	}
	return NewPanicStr(err.Error())
}

// NewDatabaseStr creates an ETDataBase ErrMsg with the standard
// ServerInternalErrorStr public message and the provided str as internal
// diagnostic detail. A stack trace is attached when IsOpenStack returns true.
// Written by Claude Code claude-opus-4-6.
func NewDatabaseStr(str string) *ErrMsg {
	e := &ErrMsg{}
	e.ErrCode = basepb.ErrorType_ETDataBase
	e.ErrMsg = ServerInternalErrorStr
	e.ErrInternalInfo = str
	if IsOpenStack() {
		e.WithStackTrace()
	}
	return e
}

// NewDatabaseErr wraps an existing error as an ETDataBase ErrMsg. If err is nil
// it returns nil. If err is already an *ErrMsg it is returned unchanged;
// otherwise NewDatabaseStr is called with the error's text.
// Written by Claude Code claude-opus-4-6.
func NewDatabaseErr(err error) *ErrMsg {
	if err == nil {
		return nil
	}
	var e *ErrMsg
	ok := errors.As(err, &e)
	if ok {
		return e
	}
	return NewDatabaseStr(err.Error())
}

// NewNoAuthStr creates an ETNoAuth ErrMsg with the standard
// ServerInternalErrorStr public message and the provided str as internal
// diagnostic detail. A stack trace is attached when IsOpenStack returns true.
// Written by Claude Code claude-opus-4-6.
func NewNoAuthStr(str string) *ErrMsg {
	e := &ErrMsg{}
	e.ErrCode = basepb.ErrorType_ETNoAuth
	e.ErrMsg = ServerInternalErrorStr
	e.ErrInternalInfo = str
	if IsOpenStack() {
		e.WithStackTrace()
	}
	return e
}

// NewNoAuthErr wraps an existing error as an ETNoAuth ErrMsg. If err is nil it
// returns nil. If err is already an *ErrMsg it is returned unchanged; otherwise
// NewNoAuthStr is called with the error's text.
// Written by Claude Code claude-opus-4-6.
func NewNoAuthErr(err error) *ErrMsg {
	if err == nil {
		return nil
	}
	var e *ErrMsg
	ok := errors.As(err, &e)
	if ok {
		return e
	}
	return NewNoAuthStr(err.Error())
}
