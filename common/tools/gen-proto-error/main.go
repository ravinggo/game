package main

import (
	"flag"
	"fmt"

	"github.com/ravinggo/game/common/tools/gen-proto-error/parser"
)

var parseDir = flag.String("parseDir", ".", "the dir of proto with error code")
var txtPath = flag.String("txtPath", "./error_code.txt", "output txt path")
var goPath = flag.String("goPath", "../errmsg", "output go code path")
var pkgName = flag.String("pkgName", "errmsg", "package name")

func main() {
	flag.Parse()
	p, err := parser.NewParser(*parseDir)
	if err != nil {
		panic(err)
	}
	err = p.Parse()
	if err != nil {
		panic(err)
	}
	p.Check()
	p.OutputErrorCodeTxt(*txtPath)
	p.OutputErrorCodeGoCode(*pkgName, *goPath)
	fmt.Println("success")
}
