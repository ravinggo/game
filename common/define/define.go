package define

type ServerType string

func (s ServerType) String() string {
	return string(s)
}
