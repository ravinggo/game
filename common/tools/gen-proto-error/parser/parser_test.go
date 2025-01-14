package parser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWalk(t *testing.T) {
	r := require.New(t)
	protos, err := Walk("../../../share/proto/proto/service")
	r.NoError(err)
	fmt.Println(protos)
}
