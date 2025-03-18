package berror

import (
	"errors"
	"fmt"
	"unsafe"

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

func IsOpenStack() bool {
	return baseenv.GetConfig().ErrorStackTrace
}

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
	if len(this_.StackStace) > 0 {
		b = append(b, `","stack_stace":"`...)
		s := ((*utils.Stack)(unsafe.Pointer(&this_.StackStace))).StackTrace()
		b = append(b, fmt.Sprintf("%+v", s)...)
	}
	b = append(b, `"}`...)
	return utils.BytesToString(b)
}

func (this_ *ErrMsg) Reset() {
	this_.ErrCode = 0
	this_.ErrMsg = ""
	this_.ErrInternalInfo = ""
	this_.StackStace = nil
	this_.ErrExtraInfo = ""
}

func (this_ *ErrMsg) String() string {
	return this_.Error()
}

func (this_ *ErrMsg) WithStackTrace() {
	this_.StackStace = *(*[]uint64)(unsafe.Pointer(utils.Callers()))
}

func (this_ *ErrMsg) IsErrorNormal() bool {
	if this_ == nil {
		return false
	}
	return this_.ErrCode == basepb.ErrorType_ETNormal
}

func (this_ *ErrMsg) IsErrorProtocol() bool {
	if this_ == nil {
		return false
	}
	return this_.ErrCode == basepb.ErrorType_ETProtocol
}

func (this_ *ErrMsg) IsErrorPanic() bool {
	if this_ == nil {
		return false
	}
	return this_.ErrCode == basepb.ErrorType_ETPanic
}

func (this_ *ErrMsg) IsErrorDatabase() bool {
	if this_ == nil {
		return false
	}
	return this_.ErrCode == basepb.ErrorType_ETDataBase
}

func (this_ *ErrMsg) IsErrorNoAuth() bool {
	if this_ == nil {
		return false
	}
	return this_.ErrCode == basepb.ErrorType_ETNoAuth
}

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
