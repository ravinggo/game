module github.com/ravinggo/game

go 1.23.3

require (
	github.com/emicklei/proto v1.14.0
	github.com/google/gofuzz v1.2.0
	github.com/huandu/xstrings v1.5.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/nats-io/nats.go v1.39.0
	github.com/petermattis/goid v0.0.0-20250121172306-05bcfb9a85dc
	github.com/ravinggo/objectpool v0.0.6
	github.com/ravinggo/zerolog v0.0.2
	github.com/spf13/pflag v1.0.6
	github.com/stretchr/testify v1.10.0
	google.golang.org/protobuf v1.36.5
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	k8s.io/apimachinery v0.32.1
	k8s.io/gengo/v2 v2.0.0-20250130153323-76c5745d3511
	k8s.io/klog/v2 v2.130.1
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/nats-io/nkeys v0.4.9 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/mod v0.21.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/tools v0.26.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace k8s.io/gengo/v2 v2.0.0-20250130153323-76c5745d3511 => github.com/ravinggo/gengo/v2 v2.0.1
