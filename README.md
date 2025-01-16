# game
A server framework developed specifically for global servers and large-scale distributed games, with high performance and very fast development speed. <br>
The core code is very simple. based on NATS multi-cluster mode, easily achieve millions of qps,
Componentization, a large number of generic implementations, and mandatory constraints, use object pools to reduce GC consumption. <br>
Can be used for all game development except global server MMORPG games <br>
Of course, it can also be used as a server-divided mode, which can greatly save server resources <br>
Support actor thinking <br>
Support DDD mode development <br>
Support event-driven <br>
Support full memory model, support database model <br>
All logic can be directly modularized, which is very friendly to companies that need to build a middle platform <br>

专为全球同服、大型分布式游戏开发的服务器框架，性能高，开发速度非常快。<br>
核心代码非常简单，基于nats多集群模式，轻松实现百万qps,
组件化，大量泛型实现，以及强制约束，使用对象池减少GC消耗。<br>
能满足除全球同服mmorpg游戏以外的所有游戏开发<br>
当然也能用作分服的模式，可以极大的节省服务器资源<br>
支持actor思想<br>
支持DDD模式开发<br>
支持事件驱动<br>
支持全内存模型，支持数据库模型<br>
所有逻辑可直接模块化，对需要做中台的公司非常友好<br>

It can now be used for production environment game development<br>
目前已经能用作生产环境游戏开发了

But there is still a lot of work to be done<br>
However, implementation solutions are reserved to make it as compatible as possible<br>
For the functions that I don’t have time to complete for now, users can integrate them by themselves<br>
但还有大量的工作未完成<br>
不过都预留了实现方案，尽可能的兼容<br>
暂时没时间实现的功能都可以自己实现集成进去<br>

Welcome friends who are interested to participate in the development<br>
欢迎有兴趣的朋友一起参与开发<br>

### 游戏配置文件接入(Game configuration file access)
请暂时自己实现或者用(Please implement it yourself or use) [luban](https://luban.doc.code-philosophy.com/docs/intro)


### 各大主流服务器配置文件接入(Access to configuration files for major mainstream servers)
consul etcd3 .... <br>
file(json,yaml...)<br>
请使用(please use) [viper](https://github.com/spf13/viper)

### i18n 多国家语言(Multi-language)
use tools/gen-proto-error to generate errmsg <br>
使用tools/gen-proto-error 生成errmsg代码和文件可以解决多语言和错误堆栈等问题
详情可以看<br> [common/tools/gen-proto-error/README.md](./common/tools/gen-proto-error/README.md)

### 数据库集成(Database Integration)
- [ ] mongo
- [ ] mysql
- [ ] redis

### 内存模式开发对象管理(Memory Mode Development Object Management)
- [ ] user data manager
- [ ] global data manager

### [使用示例(Examples)](https://github.com/ravinggo/examples)
- [ ] base use(or global server)
- [ ] ddd (or global server)
- [ ] event driven (or global server)
- [ ] actor (or global server)
- [ ] fps (or global server)
- [ ] etc...

### 监控集成(Monitoring Integration)
- [ ] prometheus
- [ ] grafana
- [ ] etc...

### 日志收集(Log Collection)
- [ ] elk
- [ ] etc...

### 链路追踪(Tracing)
- [ ] jaeger
- [ ] etc...
