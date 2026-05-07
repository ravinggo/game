# Architecture / 架构图

## Overall Architecture / 整体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Game Server Framework                              │
│                     github.com/ravinggo/game                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────── Service Layer ──────────────────────────┐    │
│  │                                                                     │    │
│  │  ┌─────────────────┐     ┌──────────────────────┐                  │    │
│  │  │  BaseService     │     │ ServerUserService     │                  │    │
│  │  │  [TraceData, TP] │     │ [T1, TraceData,TP,US] │                  │    │
│  │  │                  │     │                        │                  │    │
│  │  │ - dealNatsMsg()  │     │ - User subscription    │                  │    │
│  │  │ - handleCtx()    │     │ - Subject parsing      │                  │    │
│  │  │ - Task routing   │     │ - User hash routing    │                  │    │
│  │  └────────┬─────────┘     └───────────┬────────────┘                  │    │
│  │           │  inherits                  │                              │    │
│  │           └───────────┬────────────────┘                              │    │
│  └───────────────────────┼─────────────────────────────────────────────┘    │
│                          │                                                  │
│  ┌───────────────────────▼─────────────────────────────────────────────┐    │
│  │                      Handler / Middleware                           │    │
│  │                                                                     │    │
│  │  ┌──────────────────────────────────────────────────────────────┐   │    │
│  │  │  Handler[TraceData, TP]                                      │   │    │
│  │  │                                                              │   │    │
│  │  │  RegisterRPC() / RegisterEvent()                             │   │    │
│  │  │  RegisterRPCBroadcast() / RegisterEventBroadcast()           │   │    │
│  │  │  RegisterRPCSingle() / RegisterEventSingle()                 │   │    │
│  │  │  RegisterRPCForce() / RegisterEventForce()                   │   │    │
│  │  │                                                              │   │    │
│  │  │  Middleware Chain: mid1 → mid2 → ... → handler               │   │    │
│  │  └──────────────────────────────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌──────────────────── Execution Engine ───────────────────────────────┐    │
│  │                                                                     │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌───────────┐  ┌───────────┐ │    │
│  │  │ EventLoop    │  │ TaskGroup[T] │  │ TaskPool  │  │ safego.Go │ │    │
│  │  │ (Single      │  │ (Fixed Hash  │  │ (Worker   │  │ (One      │ │    │
│  │  │  Thread)     │  │  Pool Mode)  │  │  Pool)    │  │  Goroutine│ │    │
│  │  │              │  │              │  │           │  │  Per Task) │ │    │
│  │  │ DoubleBuff   │  │ DoubleBuff   │  │ Channel   │  │           │ │    │
│  │  │ Queue        │  │ Queue        │  │ based     │  │ Panic     │ │    │
│  │  │              │  │ + Goroutine  │  │           │  │ Recovery  │ │    │
│  │  └──────────────┘  └──────────────┘  └───────────┘  └───────────┘ │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌──────────────────── Messaging Layer (NATS) ────────────────────────┐    │
│  │                                                                     │    │
│  │  ┌────────────────────────┐     ┌─────────────────────────────┐    │    │
│  │  │  ClusterClient         │     │  NatsJetStream              │    │    │
│  │  │  (Multi-Node)          │     │  (Persistent Messaging)     │    │    │
│  │  │                        │     │                             │    │    │
│  │  │  getClient(ctx)        │     │  JetStream API wrapper     │    │    │
│  │  │  → hash routing        │     └─────────────────────────────┘    │    │
│  │  │                        │                                        │    │
│  │  │  ┌──────────┐ ┌──────────┐ ┌──────────┐                       │    │
│  │  │  │NatsClient│ │NatsClient│ │NatsClient│  ...                  │    │
│  │  │  │ (Node 1) │ │ (Node 2) │ │ (Node N) │                       │    │
│  │  │  └──────────┘ └──────────┘ └──────────┘                       │    │
│  │  └────────────────────────┘                                        │    │
│  │                                                                     │    │
│  │  Wire Format: [traceSize:2B][trace:varlen][protobuf:varlen]        │    │
│  │  Subject:     pkg.msgName.serverId.handler                         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌──────────────────── Context System ────────────────────────────────┐    │
│  │                                                                     │    │
│  │  IContext ◄── BaseCtx[TraceData, TP]                               │    │
│  │                 │                                                   │    │
│  │                 ├── Trace (IntTrace)                                 │    │
│  │                 │    └── ToHash() → consistent routing             │    │
│  │                 ├── Req / Resp (protobuf, pooled)                  │    │
│  │                 └── Logger (zerolog)                                │    │
│  │                                                                     │    │
│  │  sync.Pool: context objects reused to minimize GC                  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌──────────────────── Local Event System ────────────────────────────┐    │
│  │                                                                     │    │
│  │  ┌──────────────────────┐    ┌──────────────────────────┐          │    │
│  │  │ Publisher             │    │ Caller                    │          │    │
│  │  │                      │    │                            │          │    │
│  │  │ Register() → handler │    │ Register() → handler      │          │    │
│  │  │ Call()    → sync     │    │ Call()    → sync + deps   │          │    │
│  │  │ Publish() → deferred │    │ Publish() → deferred      │          │    │
│  │  │                      │    │ prevCalls → prerequisites │          │    │
│  │  └──────────────────────┘    └──────────────────────────┘          │    │
│  │                                                                     │    │
│  │  MiddleLocalEvent: flush queued events at request end              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌──────────────────── Infrastructure ────────────────────────────────┐    │
│  │                                                                     │    │
│  │  ┌────────┐ ┌────────┐ ┌──────┐ ┌───────┐ ┌─────┐ ┌───────────┐ │    │
│  │  │ logger │ │ berror │ │ cmap │ │ timer │ │ xid │ │ closer    │ │    │
│  │  │        │ │        │ │      │ │       │ │     │ │           │ │    │
│  │  │zerolog │ │ErrMsg  │ │32-   │ │Object │ │12B  │ │Signal     │ │    │
│  │  │+lumber │ │+stack  │ │shard │ │pooled │ │dist │ │wait +     │ │    │
│  │  │jack    │ │+types  │ │sync  │ │timers │ │IDs  │ │cleanup    │ │    │
│  │  │+async  │ │        │ │map   │ │       │ │     │ │           │ │    │
│  │  └────────┘ └────────┘ └──────┘ └───────┘ └─────┘ └───────────┘ │    │
│  │                                                                     │    │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────────┐                       │    │
│  │  │ base-env │ │ basepb   │ │ define       │                       │    │
│  │  │          │ │          │ │              │                       │    │
│  │  │ ENV vars │ │ Proto    │ │ Proto type   │                       │    │
│  │  │ config   │ │ messages │ │ constraints  │                       │    │
│  │  └──────────┘ └──────────┘ └──────────────┘                       │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Request Flow / 请求处理流程

```
 Client
   │
   ▼
 NATS Cluster
   │
   ▼
 ClusterClient.getClient(ctx)         ← hash-based routing to NATS node
   │
   ▼
 NatsClient.Subscribe()              ← receives nats.Msg
   │
   ▼
 BaseService.dealNatsMsg(msg)         ← entry point
   │
   ├── 1. Extract subject → msgName
   ├── 2. Handler lookup: GetHandler(msgName) → Elem
   ├── 3. Parse wire format: [traceSize:2B][trace][protobuf]
   ├── 4. GetCtxFromPool() → BaseCtx
   ├── 5. Unmarshal trace + request
   └── 6. Compute trace.ToHash() → routing key
          │
          ▼
   ┌──────────────────────────────────────────┐
   │         Task Routing Decision             │
   │                                          │
   │  IsSingle?  ──yes──▶  EventLoop          │
   │      │                (single thread)    │
   │      no                                  │
   │      │                                   │
   │  hash != 0 &&                            │
   │  FixedHashPool? ──yes──▶ TaskGroup       │
   │      │               [hash % poolSize]   │
   │      no                                  │
   │      │                                   │
   │  TaskPool? ──yes──▶ TaskPool.Put()       │
   │      │                                   │
   │      no                                  │
   │      │                                   │
   │      └──▶ safego.Go()                    │
   │           (new goroutine)                │
   └──────────────────────────────────────────┘
          │
          ▼
   Elem.Call(ctx)
          │
          ▼
   Middleware Chain
   mid1 → mid2 → ... → handler(ctx, req)
          │
          ├── May call localevent.Publish()   (deferred side effects)
          ├── May call nc.Request()           (RPC to other service)
          └── Returns response / *berror.ErrMsg
          │
          ▼
   ┌──────────────────────┐
   │  Response Handling    │
   │                      │
   │  RPC? → Reply via    │
   │         NATS msg     │
   │                      │
   │  Event? → No reply   │
   └──────────────────────┘
          │
          ▼
   Cleanup: Reset + Pool return (ctx, req, resp)
```

## Package Dependency Graph / 包依赖关系

```
                    service
                   /   |   \
                  /    |    \
                 /     |     \
           handler  natsclient  natsjetstream
              |      /    \
              |     /      \
              ▼    ▼        ▼
             ctx  define   basepb
              |     |
              ▼     ▼
           berror  base-env
              |
              ▼
            logger

   Independent utilities (no internal deps):
   ┌──────┬───────┬───────┬──────┬────────┬──────────┐
   │ cmap │ timer │ safego│ xid  │closer  │ utils    │
   └──────┴───────┴───────┴──────┴────────┴──────────┘

   localevent (publisher / caller):
       └── depends on: ctx, handler, define
```

## Execution Modes / 执行模式

```
┌─────────────────────────────────────────────────────────────────┐
│                    Execution Modes                               │
├─────────────────┬───────────────────────────────────────────────┤
│                 │                                               │
│  OneHashOneGo   │  Each unique hash → dedicated goroutine       │
│                 │  Pros: No contention for same-user requests   │
│                 │  Cons: Memory intensive                       │
│                 │                                               │
├─────────────────┼───────────────────────────────────────────────┤
│                 │                                               │
│  FixedHashPool  │  Fixed N TaskGroups, hash % N routing         │
│  Mode           │  Pros: Bounded memory, consistent routing     │
│                 │  Cons: Possible hash collision                 │
│                 │                                               │
├─────────────────┼───────────────────────────────────────────────┤
│                 │                                               │
│  TaskPool       │  Fixed worker pool, channel-based dispatch    │
│                 │  Pros: Bounded goroutines                     │
│                 │  Cons: No hash affinity                       │
│                 │                                               │
├─────────────────┼───────────────────────────────────────────────┤
│                 │                                               │
│  OneTaskOneGo   │  New goroutine per message                    │
│                 │  Pros: Simple, maximum parallelism            │
│                 │  Cons: GC pressure under high load            │
│                 │                                               │
└─────────────────┴───────────────────────────────────────────────┘
```

## Key Design Patterns / 核心设计模式

```
┌────────────────────────────────────────────────────────┐
│  1. Object Pooling (sync.Pool)                         │
│     Context, Request, Response, Timer objects           │
│     → Near-zero GC for hot paths                       │
├────────────────────────────────────────────────────────┤
│  2. Generics [TraceData, TP]                           │
│     Type-safe handlers without reflection              │
│     → Compile-time type checking                       │
├────────────────────────────────────────────────────────┤
│  3. Double-Buffered Queues                             │
│     EventLoop & TaskGroup use swap-buffer pattern      │
│     → Lock-free producer, batch consumer               │
├────────────────────────────────────────────────────────┤
│  4. Consistent Hashing (ToHash)                        │
│     Same user → same worker → no lock contention       │
│     → Enables full-memory model per user               │
├────────────────────────────────────────────────────────┤
│  5. Middleware Chain                                    │
│     Composable request processing pipeline             │
│     → Auth, logging, metrics, local events             │
├────────────────────────────────────────────────────────┤
│  6. Trace Propagation                                  │
│     Trace data serialized in message header             │
│     → Distributed tracing across services              │
├────────────────────────────────────────────────────────┤
│  7. Functional Options                                 │
│     Service configuration via WithXxx() functions      │
│     → Flexible, extensible configuration               │
└────────────────────────────────────────────────────────┘
```

## External Dependencies / 外部依赖

```
┌─────────────────────────────────────────────────┐
│              External Dependencies               │
├──────────────────────┬──────────────────────────┤
│ nats-io/nats.go      │ Message broker           │
│ gogo/protobuf        │ Serialization (legacy)   │
│ google.golang.org/   │ Serialization (modern)   │
│   protobuf           │                          │
│ ravinggo/zerolog      │ Structured logging       │
│ ravinggo/objectpool   │ Generic object pool      │
│ ravinggo/tools        │ Code generators          │
│   protoc-gen-gogogame │   Proto → Go             │
│   zerolog-gen         │   Auto zerolog methods   │
│ lumberjack.v2         │ Log file rotation        │
│ kelseyhightower/      │ Env var configuration    │
│   envconfig           │                          │
└──────────────────────┴──────────────────────────┘
```