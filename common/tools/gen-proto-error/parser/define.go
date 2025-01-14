package parser

import (
	"regexp"
	"strings"

	"github.com/emicklei/proto"
	"github.com/huandu/xstrings"
)

type Protocol int64

const (
	Request  = 0
	Response = 1
)

type MessageBase struct {
	Id       string
	Type     Protocol
	Errors   []string
	Msg      *proto.Message
	Name     string
	Module   string
	FileName string
}

type ErrorInfo struct {
	TipName   string
	TipDesc   string
	ErrorCode string
	Enum      *proto.EnumField
	Name      string
	Integer   int
	FileName  string
	Module    string
}

type File struct {
	Mbs      []*MessageBase
	Ext      map[string]*proto.Message
	Eis      map[string]*ErrorInfo
	FileName string
}

func (this_ *File) Check(commonErrors map[string]bool) {
	for _, mb := range this_.Mbs {
		if mb.Module == "" {
			Errorf("message[%s][%s:%d] has no module name", mb.Msg.Name, mb.FileName, mb.Msg.Position.Line)
		}
		for _, er := range mb.Errors {
			if errorCode, ok := this_.Eis[er]; ok {
				if errorCode.Module == "" {
					errorCode.Module = mb.Module
				}
			} else {
				if _, okc := commonErrors[er]; !okc {
					Errorf("error [%s][%s:%d] not in file [%s]", er, mb.FileName, mb.Msg.Position.Line, mb.FileName)
				}
			}
		}
	}
}

const minOptionParamLen = 2

func TrimOption(str string) string {
	str = strings.TrimSpace(str)
	if len(str) < minOptionParamLen {
		return str
	}
	if str[0] == '(' {
		str = str[1:]
	}
	l := len(str) - 1
	if str[l] == ')' {
		str = str[:l]
	}
	return str
}

func (this_ *File) VisitMessage(m *proto.Message) {
	if m.IsExtend {
		if this_.Ext == nil {
			this_.Ext = map[string]*proto.Message{}
		}
		this_.Ext[m.Name] = m
	} else {
		for _, v := range m.Elements {
			switch e := v.(type) {
			case *proto.Option:
				optionName := TrimOption(e.Name)
				if optionName == "models.message_base" {
					mb := &MessageBase{
						Name: optionName,
					}
					for _, ac := range e.AggregatedConstants {
						switch strings.ToLower(ac.Name) {
						case "id":
							mb.Id = ac.Source
						case "type":
							switch strings.ToLower(ac.Source) {
							case "request":
								mb.Type = Request
							case "response":
								mb.Type = Response
							}
						case "errors":
							for _, err := range ac.Array {
								mb.Errors = append(mb.Errors, err.Source)
							}
						case "module":
							mb.Module = ac.Source
						}
					}
					mb.Msg = m
					mb.FileName = this_.FileName
					if mb.Id == "" {
						mb.Id = m.Name
					}
					if mb.Module == "" {
						Errorf("message [%s][%s:%d] has no module", m.Name, mb.FileName, m.Position.Line)
					}
					this_.Mbs = append(this_.Mbs, mb)
				}
			case *proto.Message:
				this_.VisitMessage(e)
			default:
			}
		}
	}
}

var reg = regexp.MustCompile(`id=[\d]{5}`)

const messageIDLen = 8

func getMsgID(comment *proto.Comment) string {
	for _, v := range comment.Lines {
		s := reg.FindString(v)
		if len(s) == messageIDLen {
			return strings.Split(s, "=")[1]
		}
	}
	return ""
}
func (this_ *File) VisitService(v *proto.Service) {
}
func (this_ *File) VisitSyntax(s *proto.Syntax) {
}
func (this_ *File) VisitPackage(pkg *proto.Package) {
}
func (this_ *File) VisitOption(m *proto.Option) {

}
func (this_ *File) VisitImport(i *proto.Import) {
}
func (this_ *File) VisitNormalField(i *proto.NormalField) {
}
func (this_ *File) VisitEnumField(i *proto.EnumField) {
	if this_.Eis == nil {
		this_.Eis = map[string]*ErrorInfo{}
	}
	if i.ValueOption != nil {
		o := i.ValueOption
		optionName := TrimOption(o.Name)
		if optionName == "basepb.error_info" {
			ei := &ErrorInfo{Name: optionName, ErrorCode: "1", Integer: i.Integer}

			for _, ac := range o.AggregatedConstants {
				switch strings.ToLower(ac.Name) {
				case "tip_name":
					ei.TipName = ac.Source
				case "tip_desc":
					ei.TipDesc = ac.Source
				case "error_code":
					ei.ErrorCode = ac.Source
				case "module":
					ei.Module = ac.Source
				}
			}

			ei.Enum = i
			if ei.TipName == "" {
				ei.TipName = xstrings.ToSnakeCase(ei.Enum.Name)
			}
			ei.FileName = this_.FileName
			this_.Eis[ei.Enum.Name] = ei
		}
	}
}
func (this_ *File) VisitEnum(e *proto.Enum) {
	for _, v := range e.Elements {
		v.Accept(this_)
	}
}
func (this_ *File) VisitComment(e *proto.Comment) {
}
func (this_ *File) VisitOneof(o *proto.Oneof) {
}
func (this_ *File) VisitOneofField(o *proto.OneOfField) {
}
func (this_ *File) VisitReserved(rs *proto.Reserved) {
}
func (this_ *File) VisitRPC(rpc *proto.RPC) {
}
func (this_ *File) VisitMapField(f *proto.MapField) {
}
func (this_ *File) VisitGroup(g *proto.Group) {
}
func (this_ *File) VisitExtensions(e *proto.Extensions) {
}
