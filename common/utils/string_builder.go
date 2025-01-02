package utils

import (
	"strconv"
)

type Builder struct {
	buf []byte
}

func NewStringBuilder(cap int64) *Builder {
	return &Builder{buf: make([]byte, 0, cap)}
}

func (b *Builder) WriteString(s string) {
	b.buf = append(b.buf, s...)
}

func (b *Builder) WriteByte(c byte) {
	b.buf = append(b.buf, c)
}

func (b *Builder) WriteBytes(bs []byte) {
	b.buf = append(b.buf, bs...)
}

func (b *Builder) WriteInt(i int64) {
	b.buf = strconv.AppendInt(b.buf, i, 10)
}

func (b *Builder) WriteUint(i uint64) {
	b.buf = strconv.AppendUint(b.buf, i, 10)
}

func (b *Builder) WriteFloat(f float64) {
	b.buf = strconv.AppendFloat(b.buf, f, 'f', -1, 64)
}

func (b *Builder) WriteBool(v bool) {
	if v {
		b.WriteString("true")
	} else {
		b.WriteString("false")
	}
}

func (b *Builder) String() string {
	return BytesToString(b.buf)
}
