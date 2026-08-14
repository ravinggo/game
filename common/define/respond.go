package define

type Responder interface {
	Respond([]byte) error
}
