# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests
go test ./...

# Run tests with race detector (always use for concurrency changes)
go test -race ./...

# Run a single test
go test -run TestName ./common/package/...

# Run the test service
go run ./common/service/testservice/main.go

# Generate protobuf files (requires protoc-gen-gogogame from ravinggo/tools)
protoc --gogogame_out=. *.proto

# Generate zerolog methods for proto messages (requires zerolog-gen from ravinggo/tools)
zerolog-gen ./...
```

## Architecture

This is a distributed game server framework built on NATS messaging and Go generics. Module: `github.com/ravinggo/game`, Go 1.25.5.

### Request Flow

Every inbound message follows this path:

1. **NATS subscription** (`ClusterClient`) → raw `nats.Msg` arrives
2. **`BaseService.dealNatsMsg`** parses the wire format: `[traceSize:2B][trace:varlen][protobuf:varlen]`
3. **Handler lookup** by protobuf message name (derived from the NATS subject)
4. **`BaseCtx` acquired from pool**, trace + request unmarshalled into it
5. **Task routing** — one of four paths depending on handler flags and service config (see below)
6. **`Elem.Call(ctx)`** runs the middleware chain → user handler
7. **Response sent** via NATS reply if RPC; context + request returned to pool

### Four Execution Modes

The routing decision in step 5 is the core design. Flags on the `Elem` (set at registration time) and service-level config determine which engine handles the message:

| Condition | Engine | Use case |
|---|---|---|
| `elem.IsSingle()` | **EventLoop** (single goroutine) | Handlers that must not run concurrently |
| `roleID != 0 && FixedHashPoolMode` | **TaskGroup** (`taskGroupHash[abs(roleID) % poolMark]`) | Per-user ordering with bounded goroutines |
| `roleID != 0 && OneHashOneGo` | **TaskGroup** (dynamic, created on demand) | Per-user ordering, unbounded goroutines |
| `TaskPool` config | **TaskPool** (worker pool) | High throughput, no ordering guarantee |
| fallback | **`safego.Go`** | One goroutine per message |

`HashRunMode` (Fixed vs OneHashOneGo) and `TaskRunMode` (TaskPool vs OneTaskOneGo) are set via `NewBaseService` options. `IsSingle` and `IsForce` are per-handler flags set at registration.

### Generic Type Parameters

All core types are generic over `[TraceData any, TP ctx.TracePtr[TraceData]]`:
- `TraceData` — the user-defined struct stored in the context (e.g., `IntTrace` holding `RoleId`)
- `TP` — a pointer to `TraceData` implementing the `Trace` interface (`GetRoleID()`, `TraceMarshalFrom()`)

**This framework only supports `int64` RoleID.** The sole built-in trace type is `ctx.IntTrace`. `GetRoleID() int64` is the routing key — same value → same worker → no lock contention for per-entity data. `StringTrace` (string-based RoleID) was removed; use `IntTrace` exclusively.

### Handler Registration

Handlers are registered before `Start()` is called. The protobuf message type itself determines the NATS subject prefix — no string topic is explicitly passed. Registration variants:

- `RegisterRPC` / `RegisterRPCResp` — request+response, one subscriber receives
- `RegisterEvent` — fire-and-forget, one subscriber receives  
- `RegisterEventBroadcast` — all subscribers of the same server type receive
- `*Single` suffix — forces EventLoop execution
- `*Force` suffix — bypasses the "pool full" backpressure check (use only for critical ops like payments)

A panic is thrown at registration time if the same message is registered twice or if a subject is registered as both broadcast and non-broadcast.

### ServerUserService

`ServerUserService` wraps `BaseService` and adds per-user NATS subscriptions. User subjects are subscribed/unsubscribed dynamically via `UserSubscribeOne` / `UserUnsubscribe`. The user subject `GetRoleID()` takes precedence over the trace `GetRoleID()` for routing. Messages to user subjects can carry multiple protobuf messages in one NATS payload (batch), handled by `NatsUnmarshalResponseMany`. Only `ServerIntUserSubject` is supported (int64 RoleId); `ServerStringUserSubject` was removed.

### Object Pooling

Nearly everything on the hot path is pooled via `sync.Pool` (wrapped by `ravinggo/objectpool`):
- `BaseCtx` — acquired in `dealNatsMsg`, returned in `handleCtx`
- Request and response proto messages — pools keyed per type, stored in `Elem`
- `timeTask` / `TaskGroup` wrappers — pooled for dynamic hash goroutines
- `timer.Timer` — object-pooled

Do not hold references to pooled objects after returning them. After `PutCtxToPool`, the context is reset and reused.

### Local Event System

`localevent/caller-local-event` and `localevent/publisher-local-event` provide in-process event routing. `callerlocalevent.Call(ctx, req)` dispatches to all registered handlers for that proto message type synchronously. `Publish` defers until the request ends (via `MiddleLocalEvent` middleware). `Caller` supports `prevCalls` for dependency ordering between handlers.

### Wire Format

NATS message body: `[traceSize uint16 LE][trace bytes][protobuf bytes]`  
NATS subject: `{pkg}.{MsgName}.{serverId}.{qualifier}` — the last component after the final `.` is stripped to get the handler key.

### Key Infrastructure

- **`berror`** — structured errors with stack traces and i18n support; use `berror.NewProtocolStr` / `berror.NewProtocolErr`
- **`logger`** — zerolog with async file output; use `logger.Log` globally or `c.Info()` / `c.Error()` on context for trace-attached logging
- **`base-env`** — reads `ServerId` and `ServerType` from env vars at startup via `kelseyhightower/envconfig`
- **`closer`** — signal-based graceful shutdown
- **`define`** — proto serialization helpers and type constraints (`ProtoMessagePtr`, `ProtoUnmarshal`)
