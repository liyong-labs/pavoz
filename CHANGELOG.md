# Changelog

本项目遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

_(空 — 见 [0.5.7] 及更早)_

## [0.5.7] — 2026-10-17

### ⚠️ 升级注意 (0.5.6 → 0.5.7)

下面几条会改变**已部署系统的可观察行为**, 逐条写明"升级后你会看到什么", 免得从
细节条目里自己拼。

| 变化 | 你会看到什么 | 怎么办 |
|------|-------------|--------|
| **并发 run 共用一个 `task_id`** | ⚠️ **本条只在配置了 `cancel_registry` 时成立** —— `Runtime()` 默认 `cancel_registry=None`, 而占用登记就发生在那条路径上。不带 registry 的 Runtime 连 `cancel()` 都不可用, 两个 run 仍可共用 task_id 并发跑完且**不报错** (`latest` 指针变成"最后写完的那个")。配了同一个 registry 时: 第二个 `run()` 抛 `TaskIdInUse` (需 `from pavoz import TaskIdInUse`)。此前是**静默撞车**: 硬杀不生效却报告成功、`killable=False` 被绕过、目标 run 被误伤 | 给并发 run 各自独立的 `task_id` (最省事); 或让相关 Runtime **共用同一个** `CancelRegistry` 实例 —— 两个各传各的等于没传 |
| **`asyncio.wait_for(rt.run(...), t)` 现在会抛 `TimeoutError`** | 此前即使 run 被外部超时取消, 也可能返回一个 `RunResult(status="cancelled")` 而**不抛异常** —— 你的超时保护静默失效 | 若你**依赖**"不抛异常、返回 cancelled RunResult"这个行为, 改判返回值; 正常依赖超时的代码无需改 |
| **取消日志措辞变了** | graceful / hard 两条日志都多了一句 `先 registry.reset(task_id)`, 旧的"resume 可续跑"被去掉 | 仅日志文本, 无行为影响 |
| **旧 checkpoint 不能续跑** | `CheckpointMismatchError`, 且 **resume / fork_run / replay 三条入口都会报** (都走 `workflow_hash` 校验)。`workflow_hash` 自本版起含 **dag 名**, 已在盘的旧 cp 指纹与新算法不同 | 该 task 重跑一次生成新 cp; 若只是**改了 dag 名**, 把名字改回去或换 task_id |
| **`state --key <K>` 缺 K 时** | 退出码从 `0` 变 `1`, 并点名缺失的 key + 列可用 key (原来输出 `null`、rc=0, 与"值就是 null"逐字节相同) | 依赖旧语义做 `X=$(pavoz state --key K)` 的脚本改判退出码 |
| **`EnginePolicy` 构造期校验** | 负值 / 限值类字段的 `0` 现在抛 `ValueError`。此前负 `stage_alive_interval` 会变忙循环 (实测 0.2s 的 stage 喷 58099 条 `stage_alive` 而 run 报 done) | 想"不限 / 关闭"请显式写 `None` |
| **task_id 为 `.` / `..` / 全点串 现在报 `ValueError`** | 此前是**合法** task_id。`Runtime(debug_dir=D).run(dag, "..")` 会正常跑完 (`status=done`), 而 debug 快照 `00_xx_done.json` 落在 **D 的外面** (D 的父目录); `"."` 则落在 D 根, 与各 task 子目录混在一起 | 换个名字。**只拒整段是 `.` / `..`**, `a..b` / `.hidden` / `v1.0-rc.1` 照旧合法 |
| **`default_timeout=0` 现在构造期报 `ValueError`** (两侧: `EnginePolicy(...)` 与 `Runtime(...)`) | 此前**静默失效**: run 级 deadline 的调用点是 `now + _to if _to else None` (真值判断), 0 被当成"没设" ⇒ **整个 run 的超时护栏消失**, `status=done` 无任何提示。实测一个跑 0.3s 的 stage 在 `default_timeout=0` 下跑满 0.30s 报 done, 同一个 dag 在 `0.1` 下报 `TimeoutError` | 若你想"不限", 显式写 `default_timeout=None`。**亚秒超时完全合法** (`0.2` / `0.05` 都照跑), 只拒 `0` |
| **非有限值不能进 state** | `stage` 返回 `NaN`/`±Inf` 或 `--set-file` 里写 `.nan` 现在报 `ValueError`。此前能跑通, 但落盘 JSON 含 `NaN` 字面量, **不是合法 RFC 8259**, 任何严格解析器 (其他语言 / jq / JSONB) 拒收整个文件 | 用 `None` / 字符串表达"无值" |
| **override 里 list 内的 dict 现在也校验 key** | `{"items": [{"__proto__": 1}]}` 或 `{"items": [{"": 1}]}` 现在报 `ValueError`。此前**完全放行** —— 校验只递归 dict 值, list 被整段跳过, 所以同一个坏 key 挂 dict 上拒、挂 list 里过 | 若你的 list 里**确实**有这些 key, 换个名字 |
| **override 里嵌套 dict 的点号 key 不再按"路径"判** | `{"cfg": {"a.b.c.d.e.f": 1}}` (6 段) 此前报"深度 6 > 5", **现在放行** —— 嵌套 key 不会被展开, 它就是一个字面 key, 会原样落进 state | 无需操作; 若你**依赖过**那个拒绝 (比如拿它当深度探针), 改成自己数点号 |
| **旧 cp 的 visit 对齐可能本来就有错位** | 修复前, `stage` 返回 `{}`/`None` 的那次执行只进 `done_stages` 不进 deltas ⇒ 老 cp 里两个列表已经错位, 升级后**照旧错** | 受影响的 cp 重跑一次; 新写的不会错 (见 Fixed 第一条) |
| **旧 cp 在**循环图**上的 fork 重建取错 visit** | 0.5.6 及更早写的 fork cp 没有 `fork_keep_counts`, 重建按顺序索引取 delta ⇒ 在环图上取到的是**首跑段**那条 (实测 `R4[F]` 取成 `R4[none]`)。**试过推断自愈, 不成立** —— 见下方"为什么老 cp 只能重跑" | 受影响的 cp 重跑一次 |
| **fork 的 cp 重建结果会变** | 带 `fork_keep_counts` 的旧 cp (0.5.6 写的 fork), 升级后 `cp.state` **变成正确值** (此前取到 fork 之前那条 delta) | 无需操作; 若你**依赖过**旧行为(把重建值当"上次真实产出"), 那是 bug 不是契约 |
| **可调用对象 route_fn 开始落盘** | `class Router: def __call__` 这种写法, 此前 `run` 报 `done` 但 **checkpoint 一个都存不下来** (save 被"失败不杀 run"吞掉), 升级后正常落盘 | 无需操作; 若你已经在别处绕开, 现在可以去掉 |
| **`async __call__` 的 route_fn 被拒** | 声明期 `TypeError`。此前 `inspect.iscoroutinefunction(实例)` 是 `False` 会漏网, 运行期才报一条**指错方向**的错 | 异步判定做成 judge-stage 写 state, 边只读 |
| **`diff_runs` 多一个键** | 每个 stage 条目新增 `visits: [a, b]` (两 run 各跑了多少轮)。**顶层**另加 `dag_names: [a, b]` (两个 run 各自的 dag 名)。其余键语义不变 | 纯新增; 按 key 取值的代码不受影响。若你的消费者按"顶层键个数"做断言, 需相应放宽 |
| **fork 重跑**失败**时的 `export-input`** | 该 checkpoint 下导出**本次 run 还没跑完**的 stage 时, 输出**多出** fork 注入的 key。修复前那些 key 静默消失 (rc=0) | 无需操作; 若你**依赖过**"导出里没有 override"这个行为 (靠缺省值二次加工), 那是 bug 不是契约 |

其余条目 (CLI 友好错误 / 守卫补齐 / 测试加固) 不改变已部署行为。

**为什么老 cp 只能重跑 (不是"懒得兼容", 是信息没了)**: 试过从旧 cp 反推
`fork_keep_counts` —— 候选公式 `kc[n] = done_stages 里 n 的出现次数` —— 实测 **0/6
形状与真值相符**。根因: `done_stages` 记的是 `keep + 重跑段` 的**全量**, 与
`stage_deltas_visits` 逐条 1:1 对齐, 从这两者**无法区分**哪几条是截断前的历史、
哪几条是重跑产物。缺的那个信息 (截断点位置) 在旧格式里根本没落盘。所以"读旧 cp
时保留旧语义"和"推断出正确的"之间**不存在取舍** —— 只有错和更错的区别, 而现状
(错) 已由上表两行明确告知"重跑一次"。别再试一遍推断。

**关于指纹**: 本轮只有 `dag 名` 入指纹这一处变化。上表那行之外, 我们把指纹实现
改前改后拿出来, 拿仓里 `tests/` + `examples/` 的 **886 个真实函数**逐个比对了
`stage_input_hash` 的取值 —— **完全一致**, 唯一不同的两类 (可调用对象 / 内置函数)
是改写前根本不支持的形态。故除 `dag 名` 外, 已有 checkpoint 的指纹不变。

### Added

- **`CallMeta` 补进顶层公开 API** (此前只有 `pavoz.runtime.__all__` 声明了它,
  顶层没导出)。caller 是**核心扩展点** (Runtime 的注入参数 / `Ctx.caller` 字段 /
  `TestPipe(caller=...)`), 而 `CallMeta` 是它的签名类型 —— 用户写自定义 caller 时
  无法标注 `Callable[[str, str, dict, CallMeta], Awaitable[dict]]`, 只能走内部路径
  `from pavoz.runtime import ...`。同族的 `CallResult` / `Ctx` 都导出了, 只有它没有。
  更实际的一层: **`CallMeta` 其实早就写在 `docs/cn/api.md` 和 `docs/en/api.md` 里** ——
  用户读到文档说能用, 却 `import` 不到。

- **公开面一致性门禁** (`test_api_docs_coverage.py` 新增两条)。原有那条只查"已在
  `pavoz.__all__` 里的名字有没有写进文档", 对**"某个公开名压根没被导出"完全无感** ——
  那种名字不在 `__all__` 里, 循环压根碰不到它, 门禁一路绿灯 (CallMeta 就是这样漏了)。
  新判据: 子模块 `__all__` 声明的每个名字, 必须"顶层可 import"**或**"文档里能找到"。
  这条门禁上线当场又逮到 3 个同类: `NodeFn` / `merge_state` / `new_id` —— 三个都是
  内部工具被写进了子模块 `__all__`, 既没进顶层也没进文档。已从各自的 `__all__` 移除
  (而不是导出到顶层: 它们确实不是公开面)。
  判据特意**不是**"必须在顶层" —— `key_sha256` 就是合法的模块级公开面
  (`docs/storage.md` 明确记了它的用法), 只查顶层会误报。所以门禁扫**整个 `docs/`**,
  而不只是 `api.md` 两份。

- **`pavoz trace` 输出多两个字段**: `is_latest` 与 `total_runs`。`trace` 只显示
  latest 那一个 run, 而输出里原先**没有任何迹象**表明这是个多 run 的 task ——
  跑过 5 次和只跑过 1 次的输出长得一模一样, 用户据此以为"这就是全部"。尤其在
  `diff_runs` 需要两个 run_id、而 CLI 层没有命令能列出它们的时候 (`prune
  --dry-run` 能, 但那是另一回事)。纯增字段, 原有键一个不改 (有用例钉住)。
  存储后端枚举不了 run 时 `total_runs` 退成 `null` 而不是让整条命令报错 ——
  `trace` 的本职是打印 latest checkpoint, 因一个附加字段失败是本末倒置。

- **文档写明 `max_visits` 熔断后的出路**: `resume` 无效 (计数**跨 resume 累计**,
  预算耗尽后一个 stage 都不跑就再 `MaxVisitsExceeded`), 调大 `max_visits` 会被
  `workflow_hash` 拦住 (`resume` / `fork_run` 双双 `CheckpointMismatchError`) ——
  出路是**修好退出条件 + `resume=False`** (新 run_id, 不读旧 cp)。附 6 个用例把
  这张表钉住, 免得它只是纸上说明。

  顺带记一笔排查过程, 因为我**差点改错方向**: 我试到第三条路 (调大 `max_visits`
  被拒) 就下了"死锁"的结论, 并准备把 `max_visits` 移出 `workflow_hash` 来"修复"
  —— 那会让**全体旧 checkpoint 再次失效** (继 dag 名那次之后), 解决的是一个根本
  不存在的问题。教训: **穷举出路再下"死锁"的判断**, 尤其是那种"看起来无解"的
  结论。已把四条路各自的可观察行为写成用例。

- **`diff_runs` 多一个 `dag_names` 字段** —— 让"纯改名"这个**最常见的假警报**
  能被单独排除。dag 名自 d21aab8 起是 `workflow_hash` 的一部分, 所以改名就会让
  `workflow_hash_changed` 变 `true`; 而改名恰恰是最该被判成"这不是真差异"的那种
  (它不改变任何 stage 的行为)。修复前的实测: 两个 run 唯一差别是 dag 名时,
  `stages` 里每一项都显示"无差异", 而旧的单个 `dag_name` 字段**只报 a 的名字**,
  b 叫什么完全不可见 —— 消费者只能得出"结构变了但行为没变"这个错误结论, 而那个
  结论会直接导致一次无谓的全量重跑。`dag_names` 与 `stages[*].status` / `visits`
  的 `[a, b]` 约定一致。**是加不是换**: `dag_name` 单值字段保留 (diff_runs 在
  0.5.4 已发布, 改成 list 会打断按字符串读的现有代码)。
  顺带在文档里把另一条边界写明: `workflow_hash_changed: true` **不说明差在哪**
  (指纹是整串, 拆不出"哪个 stage 的哪个属性"), 这是有意的 —— 该字段的作用是叫
  消费者"别拿这两份比", 而不是替它解释; 唯一的例外 (改名) 已由 `dag_names` 兜住。

- **明确"链式 fork 不继承祖先 `overrides`"这条契约** (中英文档 + 测试 + 代码注释)。
  这块此前是**空白**: `fork_run` 只带本次的 `overrides`、不合并 `cp.fork_overrides`,
  但没有任何一处写明这是有意的。空白地带的下场是被当 bug "顺手修好", 而直觉的
  修法 (合并祖先) 会让**结果依赖历史链**: 同一份 `(dag, from_stage, overrides)`,
  fork 之前的历史不同, 结果就不同 —— 因为 keep 段的 stage 不重跑, 祖先注入值
  没人覆盖得回来。实测: fork#1 from s_a 带 `a=A9` (s_a 重跑覆盖回 `'A'`), fork#2
  from s_c 合并祖先后 `a` 变成 `'A9'` —— 一个本次 run 里从未发生过的注入盖掉了
  真实产出。**行为零变化**, 只是把现状写成契约并钉住。踩坑记一笔: 我最初以为合并
  会破坏 `cp.state == 活跑` 的一致性不变式, 手工造 cp 对比确实"分叉"了 —— 那是假的,
  手工造的 cp 绕过了 runtime 侧, 真改代码后两侧同源、依然一致 (变异实测)。被破坏
  的是可复现性, 不是一致性。

- **未知异常的 `error_category` 透传** (ai@home id=2925 提出): `_run_stage` 的泛型
  `except Exception` 分支此前把 `error_category` 硬编码为 `None`, 于是 ——
  (a) 自定义异常即使带 `.category` 也拿不到分流信息, 消费方只能退回"错误消息里有
  没有触发词"这种字符串匹配判断, 触发词表一改行为就静默漂移;
  (b) `UnmappedRouteError` / `MaxVisitsExceeded` / `OrphanStagesError` 在
  `__init__` 已接受 `category` (它们继承 `PavozError`), 走泛型分支时同样丢失。
  现改为 `getattr(e, "category", None)`, 与已知类分支一致。
  **行为边界**: 没有任何调用方给上述三个异常传过 category, 故对现有调用点
  逐字节不变; 非 `StageError`/`FatalError`/`RetryableError`/`TimeoutError` 的异常
  仍按 **FatalError 语义**处理 (fail、不重试), 携带 category 不改变这一点。

### Fixed

- **`error_category` 的 9 类建议值里有 3 类是结构性拿不到的 —— 文档已写清**。
  `docs/{cn,en}/api.md` 推荐 9 个 category, 其中 `CHECKPOINT_LOAD_FAIL` /
  `STATE_VALIDATION` / `PROTOCOL_BREACH` 描述的是**引擎侧**失败。而引擎**不替 caller
  猜业务类别** (不猜是对的), 于是这 3 类永远到不了那个字段。实测矩阵:

  | 引擎侧失败 | 实际拿到 |
  |---|---|
  | `STATE_VALIDATION` (stage 返回非有限值) | failed / `ValueError` / `category=None` |
  | `PROTOCOL_BREACH` (router 返回未声明 key) | failed / `UnmappedRouteError` / `category=None` |
  | `CHECKPOINT_LOAD_FAIL` (resume 结构变了) | **抛 `CheckpointMismatchError`**, 没有 `RunResult` |

  文档接着写"caller 据此分流重试/通知策略" —— 只 dispatch `error_category` 的话, 引擎
  引发的失败**全落进 `None` 分支**, 而那恰恰是最需要分诊的一类。

  修文档而不是加行为: 让引擎给引擎失败打默认类别是**新功能**, 且会把
  `error_category` 从"caller 没分类就是 None"变成"有时引擎会填" —— 对已经在 dispatch
  `None` 的 caller 是行为变更。分流该走两条腿: caller 自打的看 `error_category`,
  引擎侧失败看 `error_class` (或捕获那个异常)。

  矩阵钉进 `tests/test_error_category.py` (含一条正向对照: caller 自带的 category 仍
  透传 —— 引擎侧为 `None` ≠ 透传坏了)。判别力: 让所有失败兜一个 `CODE_BUG` → 3 红。
  哪天真给引擎失败补默认类别, 这条会红, 那时该同步改文档。

- ★ **`dag.stages` 是"每次访问都建一份 dict 副本"的 property, 却被放在循环里**。
  接着前两条 perf 查下去: 它服务两个读者 (① 每 stage 全量落盘 = 崩溃可从任意点续跑,
  ② 每 stage 对全量 state 算输入指纹 = resume 能跳过未变 stage), **两个都是文档化的
  保证, 不是浪费**。但 profile 里还剩一处纯浪费: `dag.stages` 的访问。

  两处热位置各提到循环外 —— `_run_inner` 的 `_gated_by` 循环 (外层 N 个 stage × 内层
  祖先闭包 = O(N²) 次拷贝)、主执行循环 `stage = dag.stages[name]` (每 stage 一次),
  以及 `workflow_hash` 内那一次。`dag.validate()` 已把 dag 冻结 (run 期间结构不变),
  拷贝是**浅**的 (Stage 对象共享) ⇒ 语义完全不变, `TestPipe` 换 fn 的写法照样生效。

  实测线性 DAG (含落盘): 640 stage 2677ms → **2173ms** (17%), 320 stage 654 → 544。
  省掉的是**平方项**, 所以占比随规模继续涨。

  判据是**访问次数**而不是耗时: `dag.stages` 被访问几次是确定性事实, 而耗时断言在 CI
  上不可靠。640 stage 修前是 O(N²) 次 (40 万+), 修后 **O(1)** 次 (2 次) —— 判据把
  这个界钉住 (`<= 3`), 下一个人再把 property 挪回循环里就会红。

  ⚠️ 第二条测试的风险假设是**错的**, 被变异逮出来: 我以为"拷贝若改成深语义 /
  取早了, 就会静默跑旧实现", 于是把拷贝改成深拷贝做变异 —— **全绿**。因为
  `_all_stages` 是在 `run()` **内部**取的, 而 `TestPipe` 按设计一定在 `run()`
  **之前**换完 fn, 所以"取早了"根本不会发生。留着那条测试 (它守的是"这个优化没弄坏
  TestPipe 的换 fn 写法", 这是真的), 但把说明改成事实。

- ★ **不配 checkpoint store 时, 每个 stage 仍在算一个没人读的输入指纹 (95% 运行时)**。
  接着结构指纹那条换个变量查 (固定 30 stage × 100KB payload), profile:

  ```
  _run_inner          0.206s
    └ stage_input_hash 0.195s   ← 95%
  ```

  `stage_input_hash` (fn 源码 + 执行前全量 state 的指纹) 的读者只有两个: ①
  `Checkpoint.stage_input_hashes` (resume 的跳过判定 + `diff_runs` 的诊断) 与 ②
  run 循环内的"重放命中"判定 —— 而 ② 只在 `skip_replay` 为真时才走 (默认 False:
  取 `policy.skip_unchanged`, 没传 policy 就是 False)。**没有 store 时 ① 根本不存在**,
  `RunResult` 也不带这个字段 —— 也就是说纯内存跑法 (TestPipe 就是这么用的) 在为
  一个不会落盘的字段付 95% 的运行时。

  修法: 只在**有人会读**时才算 —— `checkpoint_store is not None or skip_replay`。
  实测同一场景 `_run_inner` 0.206s → **0.006s**。

  `or skip_replay` 那半边是**这个优化唯一可能踩空的地方**, 专门钉住: 少了它,
  环图重入时 `input_hashes.get(name)` 永远取不到值 ⇒ 本该跳过的 stage 全部重跑 ——
  行为变了, 而且是"变慢 + 结果可能不同"那种不易察觉的方向。

  判别力 (2 次变异, 两个方向各一次): 去掉整个守卫 → 1 红; 去掉 `or skip_replay` →
  1 红。

- ★ **结构指纹每个 stage 重算一次 → 整次 run 只算一次** (纯性能, 行为不变)。
  起因是 profile 不是猜测: 160 stage 的线性 DAG, `_save_cp` 占 **92%** 运行时,
  其中 `workflow_hash` 占 **31%** —— 而 `workflow_hash(dag)` 是 dag **结构**的纯函数,
  一次 run 里结构不变, 这 31% 全花在重算一个常量上。

  规模实测 (线性 DAG, 含落盘):

  | stages | 修前 | 修后 |
  |---|---|---|
  | 80 | 84ms | 54ms |
  | 160 | 368ms | 172ms |
  | 320 | 2169ms | **635ms** (3.4x) |

  每 stage 成本的相对增长从 14.2x **降到** 4.19x。关键是这个浪费**所有规模都按同比例存在**
  (n=10 时也是 30%), 不是"到几百个 stage 才出现的规模问题" —— 所以是纯浪费, 不是
  提前设计。

  为什么是"当参数传下去"而不是"缓存到 dag 对象上": `dag.validate()` 里就有
  `self._frozen = True`, 所以第一次算指纹之后 DAG 就冻结了, run 期间结构根本不可能
  变 (`stage()` / `add_conditional_edges()` 都会抛)。既然如此, 传参数比加一层带失效
  语义的缓存更简单, 也没有"陈旧指纹"这一类风险 —— 而那正是这个函数存在的理由
  (指纹对不上要报错)。resume 的比对分支复用同一个值。

  判别力 (两个方向都验): 退回每 stage 重算 → 1 红 (数调用次数, 不用耗时 —— 耗时
  断言在 CI 上不可靠); 传一个假的 `deadbeef` → **4 红** (验"优化不能变成陈旧值")。

  ⚠️ 我第一版写的两条测试前提是错的 ("加 stage 后 hash 要变"): 首次算 hash 就冻结
  了 DAG, 冻结后加 stage 会抛 —— 那两条钉的是不可能发生的事, 换成上面那两条。

- ★ **9 个 example 里 5 个照 README 跑必然失败, 而仓库里零条判据看着这件事**。
  两类死路, 都是"用户照文档做, 第一条命令就挂":

  1. **引用仓外路径** —— `01/02/04/05` 全部硬编码
     `backend/integration/research_pipeline_dag.py`, 那是**消费方 (ai-research) 仓库**
     的路径, pavoz 仓里没有。它们连同 `llm_config.research_writer.*` 这个 key、
     `prompts.compose_synthesize` 这个 schema、LongCat 模型名一起, 都是业务值漏进了
     开源仓 —— 既跑不通, 也不符合 "pvos 不绑业务" 的口径。
  2. **调了 CLI 上不存在的 flag** —— `03_compare_diff.py` 调
     `pavoz state --run-id <id>`, 而 `state` 只有 `--task-id/--key/--stats`; CLI
     **没有任何命令**能读指定 run 的 state (`state` 只读 latest)。argparse 直接
     `unrecognized arguments`, 对谁都一样。那句"轻量, 不依赖内部 API"在没有该 flag
     的前提下无法成立。

  修法: 新增 `examples/demo_dag.py` (自包含, 三层嵌套配置 —— 深合并演示要的就是
  这种形状), 5 个 example 全部指向它且**路径按脚本自身目录解析** (与 cwd 无关), key
  改成中性的 `llm_config.writer.*`, 模型名改占位值。`03` 改用**公开**的
  `CheckpointStore` API (CLI 读不了指定 run, 库能; 差在 docstring 里写明)。想用自己
  的 dag 就 `DAG=<path>` 覆盖。README 补上"先跑一次 demo dag"这一步。

  端到端复验: 5 个 fork-run 系列 + 4 个自包含的 (llm_pipeline / agent_loop /
  conditional_loop / resume_after_crash 两段) **全部 rc=0**, 03 的 diff 输出正好显示
  深合并性质 (model 变了而 temperature/prompt/reviewer 保留)。

  新增门禁 `tests/test_examples_runnable.py`: ① example 引用的每个 `.py` 路径必须
  **能在仓里找到** (认变量根路径, 但仓外路径一律拒); ② example 传给 `pavoz <cmd>`
  的每个 flag 必须真属于那个子命令 (flag 面从 `--help` 现取, 不写死)。这类错误与
  文档里的坏链接是同一类 —— 例子是用户抄的东西, 也是文档的一部分。

  ⚠️ 判据自己翻过两趟, 都记在这: ① 第一版不剥注释/docstring, 于是"⚠️ 这里原先坏在
  哪"的说明文字被当成活引用, 5 个假阳性; ② flag 正则只认 shell 写法
  `pavoz state ...`, **所有 python 例子一条都没扫到** —— 变异把 `state --run-id` 塞回
  02 仍然全绿, 判据是瞎的 (这正是写判据时该跑的那次变异)。修正后两次变异各 1 红。

- **`to_mermaid` 与 `to_graph_json` 现在被钉成彼此一致** (一致性本身没坏, 缺的是判据)。
  两个导出服务两类消费者 (README 里嵌 Mermaid / 前端 dagre-cytoscape 吃 JSON), 描述
  的**是同一张图**, 而仓库里没有任何一条判据说它们必须一致。哪天有人给 JSON 加了
  第三种边而忘了 Mermaid (或反过来), 用户就会拿到两张不同的图, 而且**两边都看着对**。

  钉住三类东西: 节点集合 / 静态边 / 条件边, 外加 `topo_order` 与 mermaid 行序一致
  (布局依赖顺序)。判别力: mermaid 少画静态边 → 1 红; mermaid 节点序改成
  definition order (与 topo_order 脱节) → 1 红。

  写这条时踩了一次**空集绿灯**: 提取 mermaid 的正则写成 `--` 而生成器发的是 `-->`,
  于是静态边一条没提出来, 而"两边都空"看起来正是"一致" —— 我差点据此报告"两个导出
  不一致"(它们其实一致)。所以提取器的每个断言都带非空检查: 提取器与生成器脱节要
  **响**, 不是悄悄变空。同一条纪律这周已经栽过三次 (`pvos`/`pvos` 前缀、形状正则
  漏空格、这次)。

- ★ **`replay --patch` 里用户代码抛出的异常, 甩出一整段 runpy/asyncio 栈**。
  `--patch` 加载的是**用户提供的数据** (与 `--input` 的 JSON、`--set-file` 同类),
  而 CLI 对这类输入的既有口径一直是"友好一行 + 退出码 1" (`--input` 的 JSON 解析、
  `fork-run` 的四个 override 来源、`prune --keep-last`、patch 文件不存在/缺
  `patch()`/`patch` 是 async)。唯独 `patcher(dag)` 这一行没有 try。

  2026-10-17 实测: 按最自然的写法用 `@dag.stage()` 重定义一个已存在的 stage,
  撞 `duplicate stage name: s_a`, 而栈指向 `runpy` / `cli.py:195` —— 用户拿到的
  是"引擎炸了"的错觉, 真因在他自己 patch 文件的第 3 行。

  修法: 收敛成一行 `patch 执行失败 (<类型>): <消息> — 文件 <路径>` + rc=1。
  带文件名是因为用户往往同时有好几个 patch 文件; 保留原始消息是因为那是他自己写的,
  真因在里面。

  连带一条文档缺口 (同一处踩到): 文档只说 patch 模块要暴露 `patch(dag) -> None`,
  **没给可用写法** —— 而 `@dag.stage()` 重定义必然撞 duplicate stage name, 能用的是
  直接换 `dag.stages["x"].fn` (测试里一直这么写, 从没写进文档)。现在 `api.md` 与
  `--patch` 的 `--help` 都给了可复制的示例, 并点明"patch 别的 stage 再 replay 它"
  不会生效 (只跑 `--stage` 指的那一个, 前序输入从 checkpoint 重建)。

  判别力: 去掉那个 try → 3 红。

- ★ **`TestPipe.calls()` 在 mock 抛异常时是空的 —— 而那正是需要它的时刻**。
  `_wrapped` 把 `self._recorded.append(...)` 写在 `out = _fn(ctx.state)` **之后**,
  mock 一抛异常那行就走不到。后果不是数字不准, 是**调试夹具在需要它的时候瞎掉**:

  · "我的 mock 到底被调了几次?" —— 抛异常时恒答 0 次, 哪怕真调了 1 次
  · 配合 `retries`: 恒失败的 mock 被重试 N 次, `calls()` 仍一条都没有
  · stage 有 mock 但 `calls()` 里查无此 stage, 第一反应是"它根本没被调用" ——
    于是去查 DAG 调度, 而真因在 mock 里

  修法: 记录前移到调用 mock 之前 (`state_keys` 那时也拿得到)。成功路径记录的内容
  与顺序完全一致, 只是失败路径从"无记录"变成"有记录"。

  顺带钉住一条引擎契约 (我第一版测试前提写错了): **只有 `RetryableError` /
  `TimeoutError` 会重试**, 抛 `ValueError` 走 FatalError 语义 attempts=1 ——
  要验"重试 N 次都记下了", mock 得抛 `RetryableError`。

  判别力: 把记录挪回调用之后 → 1 红。

  同轮另一条 (`docs/replay-params.md` 类型推断表) 在下方。

- **`docs/replay-params.md` 的类型推断表漏掉了所有会静默改写用户字符串的写法, 而且
  "无逃生门"这句是错的**。那张表原本只列了安全的一侧 (`42` / `3.14` / `true` /
  `null` / JSON / 日期), 于是用户只会照着它判断"我的值安不安全" —— 而真正危险的
  一个都没列。实测 `--set` 会**静默**改写:

  | `--set` 写的 | 实际落进 state 的 |
  |---|---|
  | `version=1.10` | `1.1` (尾零没了) |
  | `code=007` | `7` (前导零没了) |
  | `n=1_000` | `1000` |
  | `n=+5` / `n=" 5 "` | `5` |
  | `x=1e3` | `1000.0` (float 不是 int) |

  根因: 推断阶梯是 `int()` → `float()` → JSON → str, 而 **`int()` 接受的写法比 JSON
  多** (JSON 不允许前导零、下划线、正号), 所以这些才会被改写 —— 于是"看起来像 JSON
  的值"恰好是判不准的那一类。

  文档还写着"**无逃生门**, 类型不对时用 `--dry-run` 看解析结果再执行" ——
  两句都不对: ①**逃生门就是同一个命令的 `--set-file`**: JSON 里写了引号就是字符串,
  不过推断 (`{"version": "1.10"}` 原样保留, 已实测); ②`--dry-run` 只**显示**结果,
  不给你把类型改回去的办法, 它解决的是"确认要改什么"而不是"类型不对"。两种来源
  不能混用同一个 key (优先级 `--set > --set-file > --overrides > --input`), 要保类型
  就用 `--set-file` 一次写全。

  行为**没改** (推断规则是既有契约, 改它会让现有用户的值漂移), 改的是: 文档补齐
  静默清单 + 给出真出路; `--set` 的 `--help` 里也点了一句 (那是用户真正会看的地方)。

  表格的每一行都钉成用例 (`TestDocumentedTypeInferenceTable`): 将来谁改了推断逻辑,
  文档会被迫跟着更新 —— 文档与代码各写一遍却互不校验, 就等于没写。
  判别力 (2 次变异): 让 `--set-file` 也走推断 (抽掉逃生门) → 1 红;
  推断改成只认规范十进制 → **6 红**。

- **`EventRecorder` 的 sink 父目录不存在时: 每个事件一条 traceback, 而文件一个都
  没有**。`open(sink, "a")` 在父目录缺失时抛 `FileNotFoundError`, 而这个异常在
  `Runtime._emit` 的 observer 隔离层被记下 (ERROR + 完整 traceback) 后继续跑。实测
  一个**单 stage** 的 run: 5 条 traceback、`status=done`、`recorder.events` 5 条齐全、
  **磁盘上一个字节没有**。

  问题不在"失败了" (它确实响), 而在这个失败是**永久性**的却按事件重复报: 长 run
  能刷几千行同样的 traceback 把真正的错误淹掉, 而 run 报 done, 不看 stderr 的人只会
  以为"日志开着"。用户要的是日志文件, 报一万条 traceback 不等于给了他日志。

  口径统一到 `_dump_debug` (debug_dir 快照): 那是同一类事 (用户给路径, 我们往里写
  JSON), 它 `os.makedirs(..., exist_ok=True)` 把 task 子目录建了出来。两个功能对
  "父目录怎么办"给出**不一致**的答案才是真问题 —— 统一到 debug_dir 那条, 而不是
  让每个调用方自己记得 mkdir。

  懒建 (不在 `__init__`): 一个从不落盘的 recorder 不该留下目录副作用; 裸文件名
  (`ev.jsonl`) 的 dirname 是空串, 跳过不去建 "."。

  判别力 (2 次变异): 去掉 `_ensure_sink_dir` 调用 → 2 红; 改成 `__init__` 里就建
  → **10 红** (因为 `sink_path=None` 时 `os.path.dirname(None)` 直接炸)。

  注: 第一次写这条时被自己的变异工具挡了两下 —— 它的"写入确认"把"`new` 是 `old`
  的前缀扩展"误判成没写进去, 而那正是最常见的变异形状 (见工具仓那条 commit)。

- ★ **`fork-run --compare-with` 是 CLI 里唯一"先跑后验"的参数: 拼错 run_id 的代价
  是多跑一遍 + 多留一份 checkpoint**。目标 run 的存在性检查原先落在
  `runtime.fork_run(...)` **之后**。实测 `fork-run --compare-with nope` (run_id
  拼错): fork **已经跑完**、盘上从 1 个 run 变成 2 个, 然后才打印
  `compare-with: run nope 不存在` + rc=1 —— 而那句报错读起来像"参数错, 什么都没
  发生", 用户会以为环境干净。

  它是 CLI 里唯一的例外: `--input` 的 JSON 先 parse 再跑; fork-run 的四个
  override 来源先 merge 再跑 (错 → `参数错误: ...`, 什么都没发生); `prune` 的
  `--keep-last` 先校验再删。所以这条没有理由例外, 现在也在跑之前查。

  判据钉的是**盘上 run 数不变** (而不是"返回了错误码" —— rc 修复前后都是 1, 变的是
  有没有副作用)。判别力: 把检查挪回跑之后 → 3 红。

- **`to_mermaid` 把用户文本原样拼进语法位 → 一个换行让整张图渲染不出来**。
  用户文本进了两处: 节点标签 `id["<stage 名>"]` 与边标签 `id -.<key>.-> id`。
  stage 名来自 `fn.__name__` (天然是标识符), 但**条件边的 mapping key 完全不受限**
  —— 它就是 `route_fn` 的返回值, 而 `add_conditional_edges` 对 key 零校验。

  实测 key = `"line1\nline2"`:

  ```
  graph LR
      gate["gate"]
      tgt["tgt"]
      gate -.line1          ← 一条边被劈成两行, 这行成了悬空节点声明
  line2.-> tgt              ← 碎片
  ```

  后果不是"标签难看", 是 **Mermaid 解析到悬空节点与不存在的源, 整张图失败**。
  而看图的人恰恰是 debug 的人 (只有出事了才去看图), 偏偏这时候看不到。

  改法收敛两样, 且**只**收敛这两样:
  · 换行 / 回车 / 制表符 → 单空格。它们是唯一能改变"行结构"的字符, 也就是唯一
    能**证明**会破图的字符。顺带把 `direction` 参数也收了 —— 它同样原样拼进首行。
  · 节点标签位里的裸引号 → `#quot;` 实体 (会提前闭合标签, 后面全是垃圾), 显示
    不受影响。

  边标签位里的 `"` / `;` / `-->` **故意不处理**: 本地没有 Mermaid 解析器, 它们究竟
  被当字面文本还是当语法**验不了** —— 把验不了的假设写成"已处理"就是另一种形式的
  假绿 (它会一直绿, 而不是因为对)。`viz.py` 的保留字清单本来就是"尽力而为"的,
  这条边界与它一致。

  判据也不写"渲染得出来"这种本地验不了的断言, 而写**能验的那条**: 输出每一行都
  必须是一条完整语句 (`_MMD_STATEMENT`), 不能是碎片。

  判别力 (两半各验一次): 换行收敛失效 → 6 红; 节点标签不转义引号 → 1 红。

  ⚠️ 判据自己翻过两次车, 都记在这: ① 形状正则漏了 `->` 后的空格与 ID 与 `-.`
  之间的空格, 连**对照组** `gate -.ok.-> tgt` 都不匹配 —— 判据整体失明却还有 5 条
  "红"; ② 批量替换锚点没匹配还报了 AssertionError, 差点以为已改。**判据第一次
  变红时先确认它红得对不对**: 这次是"真 bug + 坏判据"叠在一起, 只看"变红了"
  会把坏判据当成好判据。

- ★ **"并发同 task_id 会抛 TaskIdInUse" 这句承诺漏了前提, 而默认配置下根本没有
  这层保护**。`Runtime.cancel_registry` 默认是 `None`, 占用登记
  (`CancelRegistry._register`) 就发生在那条路径上。实测 (两个默认 `Runtime`,
  共用一个 `FileStorage`):

  ```
  gather(rt_a.run(dag, "same"), rt_b.run(dag, "same"))
    → 两个都 status=done, run_id 各不相同, latest 指针指向最后写完的那个
  ```

  而 docs/{cn,en}/api.md 的 bullet 与本文件上方那条升级注意都读起来像是"默认就有
  保护"。危害不是数据损坏 (各 run 的 cp 文件独立), 是 **latest 指针与
  resume/trace 的指向变得不确定** —— 而那正是用户当初不敢并发跑同 task_id 的原因。

  为什么只改文档、不把守卫提到进程级: 守卫存在的**原始理由**是 `cancel(task_id)`
  在两个 run 共用时无法定义目标 (寻址歧义), 而**没有 registry 时 `cancel()` 压根
  不可用**, 不存在这个歧义; 剩下的只有 latest 指针竞争, 那是调用方自己的选择。
  为一个推断出来的需求引入进程级全局状态, 会让互不相干的 `Runtime` 实例互相
  干扰 (700 条测试都在一个进程里跑), 爆炸半径远大于问题本身。

  三种配置的实际行为**钉成判据** (`test_cancel_flag_hygiene.py` 末尾三条), 让文档
  那句话不再是一句没人验证的承诺:
  | 配置 | 行为 |
  |------|------|
  | 默认 (无 registry) | 两个都跑完, 不报错 |
  | **共用同一个 registry** | 第二个 `TaskIdInUse` ← 文档现在推荐这个 |
  | 各自各传各的 registry | 两个都跑完 (等于没传) |

  既有那条并发用例只覆盖了"一个 Runtime + 一个 registry", 而实际用法往往是一堆
  Runtime 实例 —— "跨实例共享同一个 registry 也拦得住"这个**被推荐的前提本身**
  此前没有判据。判别力: 关掉 `_register` 的守卫 → 4 红。

- ★ **`task_id=".."` 让 debug 快照落出 `debug_dir` 之外 → 入口处拒整段点号**。
  `_validate_task_id` 的字符集是 `[A-Za-z0-9_.-]`, `.` 在内 (合法名字确实要用:
  `v1.0-rc.1` / `a..b` / `.hidden`), 于是 `.` 与 `..` 都是合法 task_id。而
  `_dump_debug` 把它**直接当路径段**拼:

  ```python
  d = os.path.join(self.debug_dir, task_id)   # task_id=".." → debug_dir 的父目录
  os.makedirs(d, exist_ok=True)                # 父目录本来就存在 → 成功
  ```

  实测 (修复前): `Runtime(debug_dir=D).run(dag, "..")` → `status=done`, 而快照
  `00_s_a_done.json` 落在 **D 的外面**。`task_id="."` 则落在 D 根, 与各 task 子目录
  混在一起。

  为什么修校验而不是修 `_dump_debug`: `_validate_task_id` 已经是 task_id 的**唯一**
  入口校验, 而 `run()` / `fork_run()` / `run_stage()` 三个入口都在任何落盘之前
  调它 —— `_dump_debug` 在 run 循环深处, 拿到的必然是已校验的值。在那里补一份
  realpath 断言就是**第二份防护**, 迟早与第一份漂。

  为什么不只改 debug 侧: FileStorage 的 key 走哈希 + realpath 断言, 同一个输入
  在那条路上侥幸没出事 —— 但那是**巧合性的安全**, 不是设计。同一个"用户标识符 →
  文件路径"的类别, 两处表现不一致本身就是缺陷。

  判据是"**整段**等于 `.` / `..` (或整串全是点)", 不是"含 `..`": `a..b` / `..a` /
  `.hidden` 都是正常名字。这条我**第一版写错了** —— 把 `..a` / `a..` 放进了拒列表,
  而实现按"整段"判, 2 条测试当场变红。这与同一天上一条修复 (override key 校验)
  是同一个毛病的两种触发方式: **收紧校验时必须同时写"哪些必须仍合法"的反向用例**,
  否则实现和判据会一起往严的方向滑, 而且**绿得看不出问题**。

  判别力 (2 次变异, 方向相反的两侧都验):
  | 变异 | 结果 |
  |------|------|
  | 去掉整段点号的拒 (退回原行为) | **6 红** ✅ |
  | 判据换成"含 `..` 就拒" (过度收紧) | **6 红** ✅ |

  注: `test_storage_path_containment.py` 里那条端到端用例原先断言
  `run(dag, task_id="..")` **能跑完** —— 它编码的是旧契约 (在 storage 层观察防线)。
  改成断言入口拒绝, 并**另起一条**继续直接打 `FileStorage`: storage 侧的 realpath
  断言仍然存在, 防的是不经 Runtime 的 library 调用方。**少一层防护可以, 少一份
  覆盖不行。**

- ★ **override key 校验与它自己的 docstring 对不上, 两个方向都错**。
  `_validate_dict_keys` 自称"递归校验 override 源 dict 的**所有** key", 而实现:

  | 输入 | 此前 | 判定 |
  |------|------|------|
  | `{"a": {"": 1}}` | 拒 (非空字符串) | ✅ 对 |
  | `{"a": [{"": 1}]}` | **放行** | ❌ 洞 |
  | `{"a": {"__proto__": 1}}` | 拒 (禁词) | ✅ 对 |
  | `{"a": [{"__proto__": 1}]}` | **放行** | ❌ 洞 |
  | `{"a": [{"b": [{"__proto__": 1}]}]}` | **放行** | ❌ 洞 (两层 list) |
  | `{"cfg": {"a.b.c.d.e.f": 1}}` | **拒** (深度 6 > 5) | ❌ 过度拒绝 |

  **(A) 洞**: 递归只进 `dict` 值, 不进 `list`/`tuple` ⇒ 列表里的 dict 绕过**全部**
  校验。同一个坏 key, 挂 dict 上拒、挂 list 里过 —— 用户没法推理这套规则。

  **(B) 过度拒绝**: 嵌套 dict 里的点号 key 也被套了完整的 `_validate_path`
  (深度上限 / 只允许标识符字符)。可 `apply_overrides` 只在**顶层**
  (`if "." in k`) 才走 `_deep_set` 展开; 嵌套走 `_deep_merge`, 该 key
  **原样保留**成字面 key。所以 `{"cfg": {"a.b.c.d.e.f": 1}}` 是一个本会干净落盘的
  字面 key, 被"深度 6 > 5"拦在门外。原注释写的正是"apply_overrides 会展开"
  —— **注释描述的是一个不存在的行为**, 这次一并改掉。

  改法 (两条规则分开): **任何层级**都拒 空 key / dunder / 含空 segment (这三条与
  "该 key 当路径还是当字面"无关); **只有顶层**才按路径判 (深度 ≤5 / 标识符字符)。
  嵌套层的新规则只查 segment 畸形 —— 放宽深度与标识符是为了不误拒, 但 dunder 与
  空 segment 一个都没松, 否则就成了"为了修过度拒绝而打开一个注入口"。

  判别力 (两半各一次变异, 互不替代):
  | 变异 | 结果 |
  |------|------|
  | 去掉 list/tuple 递归 (退回 A) | **6 红** ✅ |
  | 嵌套层改回完整 `_validate_path` (退回 B) | **2 红** ✅ |

- ★ **`default_timeout=0` 静默废掉 run 级超时护栏 → 两条入口都在构造期拒 0**。
  这是一次典型的**假值陷阱**, 而且失效方向是最坏的那种:

  | 配置 | 实测行为 |
  |------|---------|
  | `EnginePolicy(default_timeout=0)` | `done`, 耗时 0.30s —— 护栏**不存在** |
  | `Runtime(default_timeout=0)` | `done`, 耗时 0.30s —— 同上 |
  | `@dag.stage(timeout=0)` | `failed` / `TimeoutError` (正确) |

  根因是三处判断方式不一致: run 级是 `run_deadline = time.time() + _to if _to
  else None` (**真值判断**) ⇒ `0` 与"没设"不可区分; stage 级是 `if stage.timeout is
  not None` ⇒ `0` 是真值设置, 超时照常生效。所以**同一件事, 两个入口, 两种行为**,
  而 run 级那个方向是错的。

  为什么这条比一般的"参数没校验"严重: `default_timeout` 的**全部意义**就是给 LLM
  调用封顶 —— "0 = 完全不管" 意味着用户以为护栏在, 实际 LLM 可以无限烧。所以不是
  补一条 `>= 0` 就能了事, 必须在**构造期**拒: 报错发生在写代码时, 而不是等某天
  一笔 LLM 账单出来才发现。

  只拒 `0`, **不并进 `max_steps` / `max_concurrent_runs` 那组 `< 1` 判据** —— 亚秒
  超时是完全正当的需求 (LLM 首 token 延迟就这么卡), 而 `default_timeout=0.2` 本仓
  测试就在用。第一版正是并进去了, 结果把合法配置一起拒掉 (变异 2 复现: `< 1`
  → 7 条测试红)。**不限要显式写 `None`**。

  `@dag.stage(timeout=0)` **不动**: 它那边的 0 已经是"真值设置 ⇒ 超时生效"。这里
  有一处容易记错的细节 —— 那边的 0 并非"立即超时", `_with_timeout` 里
  `timeout = max(0.1, deadline - now)` 有 0.1s 下限, 即"至少给 0.1s"。我第一版按
  "立即"写断言 (`fn` 不该被执行), 实测失败才发现是这个下限 —— 又是先读代码确认
  机制、再造形状。

  判别力 (4 次变异验证, 全部被逮):
  | 变异 | 结果 |
  |------|------|
  | 撤掉 `EnginePolicy` 侧拒 0 | 1 红 ✅ |
  | 拒 0 放宽成 `< 1` (第一版的错) | **7 红** ✅ |
  | 撤掉 `Runtime` 侧拒 0 | 1 红 ✅ |
  | stage 级 `is not None` 改回真值判断 | 1 红, 且该用例真跑满 30s ✅ |

- **`--set` / `apply_overrides` 的 dot-path 覆盖: 静默丢数据与静默拼错, 现在都留痕**。
  `_deep_set` 对两件事**完全沉默** (原 docstring 自称 "warning implicit in behavior",
  即没有 warning):

  - **key 拼错** (最常见): `--set llm.modle=x` 而真 key 是 `model` ⇒ 静默新建
    `modle`, 真 `model` 保持旧值 ⇒ **配置看起来设了, 实际没生效**, 无任何提示
  - **中间不是 dict** (会**丢数据**): `{"cfg": "a-string-value"}` 上写
    `cfg.model=x` ⇒ 那个原值被静默丢弃, 变成 `{"model": "x"}`

  **行为不改**, 只把"隐式"变成"可见": 覆盖非 dict 是 **WARNING** (消息含"原值
  丢失"与被覆盖值的类型), 中间层新建与叶子 key 新建是 **DEBUG**。

  为什么不报错: pavoz 是通用编排库, **不该猜用户的 state schema** —— 往已有结构
  里加新配置项 (`llm.temp`) 与首次注入 (`--set` 到空 state) 都是合法常态, 报错会
  把它们一起拒掉。而分级是这么定的: 覆盖非 dict 几乎总是写错**且丢数据**, 该显眼;
  叶子 key 新建在"加配置项"与"拼错"之间**无法区分** (两者都是叶子不存在), 只能
  DEBUG —— 升成 WARNING 会让"给 llm 加一个 temp"这种每天都在做的操作开始刷警告,
  用户只会把日志关掉。但消息里说清"真 key 仍在、本次设置不生效", 这样开 DEBUG 的人
  拿到的日志才有用。

  注: 上一版**漏了叶子新建那一层** —— 中间层新建有日志, 叶子没有, 而用户拼错的
  恰恰是叶子 (`llm.modle` 零输出)。补上后四种情形的分级都验过。

- ★ **事件流在两条取消路径上都不成对 → 监控算出错的 stage 指标**。消费方按
  `stage_start → stage_end` 配对算耗时/成功率/in-flight, 所以"孤儿事件"不是脏数据,
  是**错的统计结果**。实测六条路径: 正常 / stage 失败 / max_steps 熔断 / `run_stage`
  都配对, 但**两条取消路径各漏一半, 方向正好相反**:

  - **graceful** (stage 边界拦截): 只发 `stage_end` 不发 `stage_start` ⇒ 那个 stage
    根本没跑, 事件流里却凭空一次"瞬间完成"
  - **hard** (中途注入 `CancelledError`): `stage_start` 早发出去了, 取消时直接跳到
    `save_cp`/`run_end` ⇒ 那个 stage **永远停在 in-flight**, 超时告警反复响, 而
    runtime 侧早已把它标成 cancelled 并落了 cp —— 两边说法不一致

  两处都在 cancel 相关代码附近, 而本轮已反复改动那一段。改控制流时最容易漏掉的
  就是"事件流还自洽吗", 而它此前没有任何测试在守。补 `test_event_stream_pairing.py`
  (6 个用例, 含 `run_end` 恰好一条且与 `RunResult.status` 一致)。

- **`TestPipe.calls()` 的记录跨 `run()` 累积**。`run()` 的 finally 明说"实例可复用",
  而记录不清空 ⇒ 复用实例跑第二遍时 `calls()` 会数到第一遍的记录。
  失败形态是**假红**而非假绿 (该红的红), 但"只跑了一遍却拿到两条记录"没法用直觉
  排除, 用户得先怀疑自己的用法才查得到。测试工具出假红仍然算缺陷 —— 它消耗的是
  别人的信任。现在 `run()` 开头清空, `calls()` 的 docstring 写明"只覆盖最近一次",
  以及跨轮次累计要自己 `snap = pipe.calls()` 取快照 (`calls()` 返回的是拷贝,
  调用方改不动内部状态, 这条也钉住了)。

- ★ **两个并发 run 共用一个 task_id → 取消既杀不到、又误伤, 且绕过 `killable`**。
  `CancelRegistry` 以 `task_id` 为唯一键, 而 `_register` 是纯覆盖写入。实测:
  run#1 正常收尾时的 `_unregister("SAME")` 把**还在跑的 run#2** 的句柄一起摘掉,
  三层后果一层比一层糟:
  1. `cancel(task_id, "hard")` 拿不到句柄 ⇒ `Task.cancel()` **根本没执行**, 而
     `cancel()` 正常返回 —— 调用方以为杀成功了 (**硬杀静默失效**)
  2. `_current` 同时被清空 ⇒ 目标 stage 的 `killable=False` **读不到** ⇒ 不抛
     `NotKillable`, 那个"拒绝硬杀"的安全保证被**绕过**
  3. 但取消**标志**照样设上了 ⇒ 目标 run 在下一个 stage 边界被截停 ⇒ **误伤**
     (明明没被杀, 却拿到 `status="cancelled"`)

  修法: `_register` 检测到该 task_id 已有**活跃** run 时抛新的
  `TaskIdInUse`, 直接拒绝; `_unregister` 加身份校验 (只摘自己注册的那个句柄,
  防御性)。**之所以拒绝而不是想办法支持**, 是因为 `cancel(task_id)` 在两个 run
  共用时根本无法定义该取消哪一个 —— 没有句柄级寻址; 且 checkpoint 也按 task_id
  存, 两者本就会抢 latest 指针。与其选一个默认目标再静默出错, 不如启动时讲清。
  边界: **串行**复用同一 task_id (run → resume → fork) 照旧; 判据只认"活跃"
  run, "已结束但尚未 unregister"的窗口不算占用; 没传 registry 时不拦 (没有
  `cancel()` 可误伤)。

- ★ **外部取消被当成 registry 硬杀吞掉 → 调用方的超时保护静默失效**。
  `CancelledError` 本身不带"谁发起的"信息, 而 registry 硬杀与**外部**取消
  (`asyncio.wait_for` 超时 / 调用方 `t.cancel()` / loop 关闭) 走的是同一个异常。
  `Runtime` 此前只看"有没有取消标志"就把它转成 `RunResult(status="cancelled")`,
  而遗留标志正好满足那个条件。实测: `asyncio.wait_for(rt.run(...), 0.1)` **不抛
  `TimeoutError`**, 返回一个假的 RunResult —— 而超时保护通常是防止 run 无限
  挂着的最后一道防线。
  改法: `CancelRegistry` 新增 `_killing`, 只在**真的**向该 task 发过
  `Task.cancel()` 时置位 (拿不到句柄 / 已结束则不置), 生命周期与 `_tasks` /
  `_current` 一致; `Runtime` 改查它。真·registry 硬杀仍照旧转 RunResult。

- **run 正常收场时清掉竞态残留的取消标志**。取消请求在**最后一个** stage 执行
  期间到达时, 那个 stage 之后没有下一次 attempt 的边界检查 ⇒ run 正常 `done`,
  而标志留下了; 下一个用同一 task_id 的 run **一个 stage 都没跑**就被判
  `cancelled`。"正常结束却带着取消标志"只可能是竞态残留, 没有语义要保留它。
  边界: 被取消的 run **保留**标志 —— 那是 caller 明确表达过的意图, 复用前由
  caller 决定何时 `reset`。

- **取消日志不再把 caller 引导到一条走不通的路上**。graceful 与 hard 两条日志
  原先只说"已落 cp, **resume 可续跑**" / "已落 cp", 只字不提取消标志仍在, 而不
  `reset` 的新 run 会立刻再次 cancelled。现在两条都点名 `registry.reset(task_id)`,
  旧的"可续跑"措辞一并去掉。`CancelRegistry.reset` 的 docstring 也补上了
  "何时必须调 / 何时不用调"。

- ★ **条件边的分支闸门失效 → 未选中分支的下游跑到了闸门之前, 静默错结果**。
  形状 `s_a → s_b → s_gate{go→s_c, end→s_fin}`, `s_c → s_d`, router 选 `end`
  时实测执行序是

      s_a → s_b → **s_d** → s_gate → s_fin

  **`s_d` 在它自己的闸门 `s_gate` 之前就跑了**: `s_c` 一次没跑, `s_d` 读到的是
  "从未执行过的上游"的默认值, 而 run 报 `status=done`、无异常无日志。
  **最要命的是第三层**: `cp.state == 活跑 state` —— 落盘与活跑**一致**, 所以
  一致性类的不可变测试抓不到它。它潜伏至今不是因为状态矛盾, 是因为**整个系统
  一致地算出了错误的答案**。
  根因: `_next_auto` 从表头扫第一个"没 done"的, **完全不看静态依赖**。无环图靠
  拓扑序兜住 (上游没 done 时下游不会是第一个); 而 route-only 节点被从
  `auto_order` 剔除后, 它的静态下游**失去位置约束** —— Kahn 把 indeg=0 的
  route-only 提前弹出, 拓扑序的兜底随之失效。
  修法是**闸门状态检查** (逐项看静态上游闭包里的 route-only 是否已跑), 而不是
  把下游踢出 `auto_order` —— 后者是**永久**的, 会让选中分支的下游永远不跑
  (这个坑实测踩过, 测试用"条件取反"那一侧钉住)。无条件边图该检查恒不触发,
  行为逐字节不变。
  **闸门与路由回跳的交互也验过**: 分支目标 (无静态父 ⇒ route-only) 被回跳重入
  后, 它被闸门卡住的下游必须**放行**; 两个方向都要对 (早放行 = 抢跑, 永不放行
  = 回路第二圈直接停住)。另有专门用例钉住"回路目标有静态父 ⇒ 不算 route-only
  ⇒ 闸门前置为空"这一事实, 免得日后误以为某条用例测了闸门分支。
  **同源的另一半也修了**: 孤儿检查的豁免名单原本只含 router 声明的**直接
  target**, 不含其可达闭包 ⇒ "分支目标带静态下游"时那个下游既没人跑又不在豁免
  名单里 → 报 `OrphanStagesError`, 也就是**"选了某分支"这件事本身就让 run
  永远 failed**。现在豁免覆盖被声明分支的整条链 (深两级也覆盖)。
  升级后: 含条件边且**分支目标还有静态下游**的 DAG, 未选中分支的 stage 不再
  抢跑, 且跳过是合法的 (run 能报 done)。既有正确行为全部不变 (583 个既有用例
  全绿, 含 `test_conditional_edges.py` 的孤儿/stale 用例)。

- **`to_mermaid` 把 stage 名直接当节点 ID → 保留字名字渲染不出图**。stage 名来自
  `fn.__name__`, Python 侧完全合法: 实测 `async def end / graph / subgraph /
  style / click / flowchart / direction / classDef / linkStyle` 全都造得出来, 而
  它们是 Mermaid 的保留字, 当 ID 用会让整张图渲染失败 (嵌 README 的图直接挂)。
  含连字符的名字 (`load-data`) 同理。现在 ID 经 `_safe_id` 处理 (合法标识符 +
  非保留字), **但标签恒为 stage 原名** —— 替换只发生在 Mermaid 内部的 ID 上, 图上
  显示的一直是原名。**合法名字的输出逐字节不变** (`s_a` 之类一个都没动), 这是这次
  改动的兼容性底线。
  顺带修两处连带问题:
  - `_safe_id` 的注释写着"字母数字 + _ -. (兜底防 hyphen)", 而实现只有
    `name.replace(" ", "_")` —— **连字符根本没防** (`_safe_id("load-data")` 原样
    返回)。注释声称的边界与实现不符, 让读注释的人以为有防护。
  - 退化路径会把不同名字归一化成**同一个串** (实测 `"中文名"` / `"别的字"` /
    `"第三个"` 全变 `___`), 两个节点共用一个 ID 就是渲染错 —— 图少一个节点而
    **没有任何提示**。唯一性改由 `to_mermaid` 侧的 `_id_map` 保证 (按 sorted 建
    映射, 冲突加序号, 输出可复现)。
  **关于那份保留字清单**: 它来自我们对该语法的了解, **无法在本仓离线核对** (Mermaid
  语法表不在仓内, 且本环境的联网查证工具不可用)。所以机制不只靠清单 —— 正则同时
  兜住"不合法的 ID 形状", 保留字命中才替换。漏掉某个保留字的**后果 = 修复前的
  行为**, 不会更糟; 这也是清单可以先留在这里的理由。`to_graph_json` 刻意**不改**
  (`id` 是给 dagre/cytoscape 消费的, 那些库没有 Mermaid 的保留字限制, stage 原名
  才是稳定标识)。

- ★ **`export-input` 对"fork 重跑没跑完"的 checkpoint 静默丢掉 `fork_overrides`**。
  `rebuild_state_before(stage)` 的守卫放行了只存在于**历史** `stage_deltas`、不在
  `done_stages` 里的 stage (fork 截断点之后的那些) —— 这是对的, 它们确实跑过;
  但 `_rebuild_state` 的循环走的是 `done_stages`, 于是 `stop_at` 永远命不中, 循环
  走到底。末尾的兜底注入又判的是 `stop_at is None` —— 非空的 `stop_at` 把兜底挡了,
  `fork_overrides` 就此蒸发。**全程无提示, 退出码 0**。实测 (fork from `s_b`,
  `overrides={"topic": "OV"}`, 重跑在 `s_b` 失败 ⇒ `s_c` 没重跑):

      $ pavoz export-input --task-id T --stage s_c     rc=0
        {"a": 1}          ← 缺 topic=OV (runtime 早就把它注进 cp.state 了)

  危险不在少一个 key, 在于 `export-input` 的产物是**人拿去编辑、再喂回 `fork-run`
  的基座文件**: 少一个 key 就是下游 fork 悄悄用默认值跑完 —— 不是报错, 是错答案。
  修复: 兜底判据从"stop_at 是 None"改成"stop_at **没命中过**"。两者在正常形状下
  等价 (`stop_at` 命中即 `break`, 未命中即走完), 只在这一种形状下不同, 且改后
  语义更站得住 —— 命中 = 真回到"某 stage 执行前", overrides 那时还没注入; 没命中
  = 停在"这次 run 走到的最后", runtime 早就注入了, 重建反而比 `cp.state` 少东西
  是退步。`Runtime.run_stage` / `fork_run` 不受影响 (两者各自有守卫, 未完成的 stage
  走全量重建而非 `stop_at`)。反向验证: 改回旧判据, 库端与 CLI 端两个用例转红;
  改成"无条件兜底", 反向约束用例转红。

- ★ **空 delta 让 `done_stages` 与 `stage_deltas_visits` 永久错位 → checkpoint
  自相矛盾**。`_run_inner` 追加 delta 曾带 `if delta:` 前置, 而 `done_stages.append`
  是无条件的 ⇒ "stage 返回 `{}` / `None`"的那次执行只进 done 不进 deltas。
  `Checkpoint._rebuild_state` 按**序号位置**配对 (顺序对齐与尾对齐都建立在这个前提
  上), 错位之后会把某次 visit 的 delta 配到另一次 visit 上。**纯非 fork run 就能
  中招**, 触发条件: 循环图 (某 stage 被访问 ≥2 次) + 某次访问返回空 delta + 其间有
  链式覆盖同名 key。实测:

      活跑   {'n': 2, 'k': 'W2', 'report': 'final=W2'}
      cp     {'k': 'P',  'n': 2, 'report': 'final=W2'}     ← 同一份文件两个答案
      $ pavoz state --key k      → "P"
      $ pavoz state --key report → "final=W2"

  `report` 说 k=W2 而 `k` 字段是 P —— **不存在任何一次执行能同时产出这两个值**。
  无异常、无日志; 凡是读 cp 的路径 (`pavoz state` / resume / `diff_runs` /
  `run_stage` / fork 基线) 全部拿到错值。fork 路径上同一个根因表现为**幽灵 key**
  (尾对齐把截断点之前的历史 delta 重新钉回来, 本次 run 从未产生过该数据)。

  修法: append **无条件** (空 delta 也占一条; 代价是每个空 visit 多一个 `{}`)。
  两个列表从此逐条对齐 —— 这是重建正确性的前提, 不是优化。
  **发现方式**: 对 `_rebuild_state` 做对抗性审查时由独立子 agent 报出, 我先用自己的
  脚本独立复现 (第一版复现脚本还写错了 —— 循环不收敛) 才动手。

- **可调用对象当 `route_fn` → 每一次 checkpoint save 都静默失败, 而 run 报 done**。
  `_stage_source_id` 只认 plain function 与绑定方法; `class Router: def
  __call__(self, state)` 这种写法 (带查找表的 router 对象, 很常见) 会连撞两道:
  `inspect.getsource(实例)` 抛 `TypeError`, 老 fallback 取 `fn.__code__` 又抛
  `AttributeError`。而这个异常发生在 `_save_cp` 里, 被那条**"失败不杀 run"** 的
  兜底 (`except Exception: logger.exception`) 吞掉。实测后果:

      run status = done             ← 表面成功
      落盘 checkpoint 文件数 = 0     ← 什么都没存
      load_latest() = None          ← resume / replay / trace / fork / diff 全 unavailable

  只剩一行没人读的日志 —— 用户以为存下来了, 事发后才发现整条恢复链路是空的。
  现 `_stage_source_id` 改为**四层兜底且绝不再抛**: getsource → 字节码指纹 (对象取
  它的 `__call__`) → 类型限定名 (跨进程稳定, 不能用 id(): 两个进程算出的 hash
  不同, cp 就互相认不出来) → ③的 again (自定义 `__getattr__` 抛非 AttributeError
  时 getattr 的默认值对它无效, 连类型名都取不到)。
- **`async __call__` 的可调用对象绕过了 route_fn 的 async 守卫**:
  `inspect.iscoroutinefunction(实例)` 是 `False`, 只查函数会漏, 于是漏网后重演那条
  误导性错误 (运行期报 "route_fn 返回未声明 key <coroutine object ...>" —— 说
  路由返回了未声明的 key, 实际是路由压根没被调用)。守卫现同时看 `__call__`, 且
  报错里对可调用对象给**可读的名字** (退化到类型名, 而不是 `<X object at 0x...>`)。

- **cp 重建里 `fork_overrides` 的落点与 runtime 相反 → 落盘与活跑分叉**。同一把
  钥匙开两把锁: runtime 里 overrides 是**注入给 `from_stage` 的输入** (先注入,
  stage 重跑后可覆盖), 而 cp 重建把它们放在**最后** merge, 于是盖掉了 from_stage
  刚跑出来的值。实测:

      fork from s_a, overrides={"a": "A9"} (s_a 自己返回 {"a": "A"})
        活跑 state = {'a': 'A',    ...}      ← s_a 重跑后覆盖了注入值
        cp.state   = {'a': 'A9',  ...}      ← 又被 override 盖回去了

  同一根因还有第二个面: `stop_at` 命中 `from_stage` 时 (即 `run_stage` 重放
  from_stage 的基线) 重建**根本不落** overrides, 重放拿到的是未注入的旧输入 ——
  实测期望 `C[F7]`、实得 `C[none]`。
  现注入点改为"**跨过 fork 截断点的那一刻**", 且判定必须在 `stop_at` 的 break
  **之前** (from_stage 执行前的快照正是它们该出现的那个点)。**只有带
  `fork_keep_counts` 的新格式 cp 才按边界注入**: 旧 cp 无从判断截断点, 保持
  "最后 merge"的旧语义, 否则升级后老 checkpoint 的 state 会平白改写。
  这是 e8cec96 之后用**不变量探针** (把"cp.state == 活跑 state"铺到 12 种形状)
  压出来的 —— 刚提交的重建逻辑自己就是最该被怀疑的代码。

- **链式 fork 后 checkpoint 重建取到最老 visit 而非最近一次** (ai@home id=3241,
  2026-10-15 报告 / 2026-10-16 本地修复)。**数据完整性问题**: fork_run 当场返回的
  值是对的, 但此后任何读该 cp 的路径 (resume / load / diff_runs /
  `TestPipe.replay_from`) 拿到的都是 fork **之前**的值:

      run1 : C[none]   fork1: C[F1]   fork2: C[F2]     ← 活跑的值正确
      cp 中 s_c delta: [0] C[none] [1] C[F1] [2] C[F2]
      cp.state 重建   : C[none]                          ← 取了最老那次

  根因两层:
  1. `_save_cp` 没有 `fork_keep_counts` 参数 → fork 续跑的每轮 save 都把它清空成 {}
  2. 更根本 —— `fork_keep_counts` 的语义是"该 stage 的 done 条目里, 有几条来自
     fork 截断点**之前**", 而 `fork_run` 只给 keep 里的 stage 记条目 → from_stage
     及其下游一条都没有 → 重建时 `kc is None` 被当成"顺序索引" → 对截断点后的重跑
     集取 `deltas[0]`。重建逻辑现在**区分"fork cp"与"缺条目 = 全部重跑 (kc=0)"**,
     且 `fork_run` 对所有带 visits 的 stage 都记条目 (缺的记 0)。
  连带修好一个此前没人发现的分支: **从第一个 stage fork** (keep 为空) 时 kcounts
  本会得到空 dict, 连"这是 fork cp"都识别不出来。
  `tests/test_chained_fork_rebuild.py` 的 xfail 已 **XPASS** (该信号当初就是为验收
  设的), marker 摘除, 用例转为永久回归防线, 并补了上述分支的 2 条新例。

- **`workflow_hash` 不含 dag 名 → 两条流水线可以静默混流**。结构相同、名字不同的
  两个 DAG (两条不同流水线) 此前拿到同一指纹; task_id 撞车时 resume 拿错 cp 且
  毫无提示。实测:

      ① dag "extract" 首跑, 末 stage 失败 → cp 部分完成 (by=EXTRACT)
      ② dag "classify" resume 同一 task → status=done
         state  = {'by': 'EXTRACT', ..., 'final': 'EXTRACT'}   ← 全是 extract 的数据
         dag_name = 'classify'                                  ← 却报告是 classify

  即 `RunResult.dag_name` 撒谎、state 跨流水线污染, 而 task_id 是幂等键 —— 拿它
  续跑的前提本就是"同一个工作流", 名字是工作流身份的一部分。现 dag 名计入指纹,
  resume / fork_run / `TestPipe.replay_from` 三条入口一并拦住。
  **代价 (刻意)**: 改名 = 指纹变了 = 旧 cp 不能续跑; 想续就把名字改回去或换
  task_id。宁可拒绝, 不可静默混流。**数据面影响**: 已在盘的旧 cp 指纹与新算法不
  同, 升级后不能 resume (与 0.5.x 的向前看口径一致)。

- **DAG 指纹不匹配的报错收敛到唯一构造点** (`checkpoint.dag_mismatch_msg`)。三处
  (resume / fork_run / `TestPipe.replay_from`) 各写一份, 措辞**已经漂过**: runtime
  那份少了"只改 stage 函数体不影响 hash"的提示, testing 那份少了"强制重跑"的
  出路。而 dag 名计入后, 三处都该提"名字不同"这条最常见的原因 —— 三处各改一遍
  就是下一轮漂移的起点。名字不同时消息点名两个 dag 名。

- **`load_storage` 只包住 `TypeError`, adapter 连不上时报的是裸驱动异常**:
  四条失败路径里 (格式错 / 模块不可用 / 类不存在 / 实例化) 只有实例化那条按
  `TypeError` 包, 而真实 adapter 的 `__init__` 基本都要连服务 (Redis / PG / MinIO /
  S3) —— 连不上时抛的是驱动自己的异常, 于是**最常见的那类失败原样逃出去**: 用户
  看到一句"无法连接 redis://…", 既不知道是哪个 storage spec (它来自环境变量
  `PAVOZ_STORAGE_SPEC`), 也不知道传了哪些 kwargs, 而这两样正是排查的第一现场。
  实测 (修复前): `load_storage("my_adapter:RedisStore", host=…, port=…)` → 裸
  `ConnectionRefusedError`。现 `except Exception` 补一条 (排在 TypeError 之后, 不
  抢它的 kwargs 提示), 消息带 spec + kwargs + 原异常类型, 并用 `from e` 链住
  原始异常 —— 类型与 traceback 一个不少, 只补上下文。

- **`diff_runs` 在循环图上给不出"多跑了多少"的信号 (方向还反了)**: `output_diff` /
  `state_diff` 走的都是 `stage_deltas` —— **last-write 视图** (每个 stage 只取最后
  一次执行的 delta)。实测: 两个**终态逐字节相同**的 run (循环 stage 跑 3 轮 vs
  5 轮), `state_diff` 是空的, `output_diff` 只报出 `{"scratch": [3, 5]}` 这种
  "末轮 delta 差在哪"的细节 —— 该报的"多干了 67% 的活"没报, 不该当差异的末轮
  细节反倒成了唯一线索。
  现每 stage 的 diff 条目新增 **`visits: [a, b]`** (两个 run 里各跑了多少轮),
  循环图上"轮次"是这个图最该被看见的量。**纯新增键**, 原有键语义一个不动
  (`status` / `input_hash_changed` / `output_diff` / `state_diff` 全部照旧),
  逐轮全貌仍在 `cp.stage_deltas_visits` —— `output_diff` 有意保持 last-write 语义,
  逐轮比较是另一套语义, 不在这里改。docstring 与 cn/en api.md 同时写明该限制。

- **`EventRecorder` 的 JSONL sink 会写出非法 JSON** (`NaN` / `Infinity` 字面量):
  Python 的 `json.dumps` 默认 `allow_nan=True`, 而 sink 的存在意义就是给
  jq / Splunk / Loki 这类工具读 —— 一行读不了就等于少一条记录。实测 (修复前): 一个
  上报 `set_progress(float("nan"))` 的 stage 就能让 7 行 sink 里 2 行不是合法
  RFC 8259 JSON。
  **注意与 `set_progress` 不校验 fraction 的区别**: 那个"不校验"是**故意**的 ——
  它是 best-effort 瞬态信号, 让装饰性错误把业务 stage 带崩更糟; 而 sink 是 pavoz
  **自己产出的**机器可读产物, 格式该由 pavoz 负责。
  修法: 序列化时把非有限 float 换成 `"nan"` / `"inf"` / `"-inf"`, 并配
  `allow_nan=False` (前者兜住已知情况, 后者保证再有别的非标准值时**响**而不是悄悄
  写坏文件)。**不用**直接 `allow_nan=False` 一刀切: 那会让该事件整条丢失
  (`Runtime._emit` 隔离 observer 异常 → 只剩一行日志), 而 sink 的职责是留住记录。
  只动序列化, 内存里的 `recorder.events` 仍是原始 float。

- **扩展面的错误语义矩阵补齐回归防线** (`ctx.call` → 注入的 caller)。实测四行全部
  正确 (RetryableError 重试 3 次且 `error_category` 透传 / StageError 不重试 /
  自定义异常带 `.category` 也透传 / 裸异常 category=None), 但**零测试覆盖** ——
  而这个仓已经在这块真实翻过一次车 (泛型 `except Exception` 把
  `error_category` 硬编码成 None, ai@home id=2925), 那次是被人发现的, 不是被测试
  拦下的。现 `tests/test_caller_error_semantics.py` 钉住整张矩阵 + 重试时
  `CallMeta.attempt` 递增 + `ctx.cancelled()` 接入 + `CallMeta` 四字段与 params
  原样透传。**判据有效性已验**: 把 `getattr(e, "category", None)` 改回硬编码
  `None` (复现那次真实回归) 后新用例红 3 条, `git checkout` 恢复后全绿。

- **`EnginePolicy` 不校验字段, nonsense 取值变成运行期怪现象**: frozen dataclass
  此前接受任意取值, 而所有调用点一律写 `if policy.x` —— 于是:
  - `stage_alive_interval` 传负数 → `asyncio.sleep(负数)` 立即返回 → 心跳变成无节流
    忙循环。**实测: 0.2s 的 stage 喷出 58099 条 `stage_alive`** (CPU 空转 +
    事件流/日志/内存三重放大), 而 run 照样 `status=done`, 全程无任何提示。
  - `max_concurrent_runs=0` → 0 是假值 → 当成"没设" → 读起来像"不许并发",
    实际是**不限**; `max_steps=0` → 第一个 stage 立刻熔断。
  现 `EnginePolicy.__post_init__` 在构造时拒: 负数一律 `ValueError`, 限值类
  (`max_steps` / `max_concurrent_runs`) 的 `0` 也拒, 报错文案指明"不限请显式传
  `None`"。`backoff_max=0` / `stage_alive_interval=None` / `default_timeout=None`
  是合法用法, 保留。仓内现有 9 处构造点全部正值或 None, 零破坏。

- **TestPipe 的三处契约与实际对不上** (夹具本身行为无误, 是"承诺与实现不一致"):
  ① `calls()` 的 docstring 写返回 `[{stage, mock, args}]`, 实际键是
  `{stage, mock, state_keys}` —— 照旧文档写 `call["args"]` 直接 KeyError, 断言是
  **崩掉**而不是失败。现 docstring 写明真实键, 并说明 `state_keys` 是该 stage 调用
  时看到的 state 顶层 key (插入序), 以及"只有 mock 过的 stage 留记录"。
  ② `run(resume=True)` **永远失败** (TestPipe 不落盘, 无可续的 checkpoint), 而它
  一路传到 Runtime 才炸, 报的是内部不变量 "resume=True requires checkpoint_store"
  —— 与调用方毫无关系的一句话。现直接在 TestPipe 抛出 `ValueError` 并指出两条替代
  路径 (`replay_from` / 真 `Runtime`)。
  ③ `replay_from(cp, dag)` 在**环图** (条件边回路, R1/0.5.5 晚于本夹具) 上只保证
  终态自洽、不保证路径一致: `stage_deltas` 是 last-write 视图, 环里每轮都拿到该
  stage 的**最后一次** delta, 真实的 3 轮被压成 1 轮。终态多数时候仍等于
  `cp.state` (终值本就是终点), 但轮次敏感的 stage (计数/累加) 会算错。cn/en
  api.md 已写明该限制与替代做法。

- **`skip_unchanged` 的两处事实此前没写在任何地方** (行为本身无误, 是文档与
  可观测性缺口; 现补文档 + 补 characterization 测试钉住):
  ① **指纹覆盖边界**: `stage_input_hash` 只含 **fn 源码 + 执行前 state**, 不含
  闭包变量、模块级配置、环境变量、文件、时钟。而文档只写"改 fn (如改 prompt) →
  hash 变", 读起来像"改 prompt 就会重跑"。实测: 模块级 `PROMPT` 从 v1 改到 v2,
  fn 源码与 state 都没变 → fork **静默返回 v1 的产出**, `status=done`。现文档写明
  这条边界并给出两条出路 (prompt 写进 fn 体内 / 该 stage 标 `skip_unchanged=False`)。
  ② **"真跑了"与"复用了旧结果"只有一个区分点**: 命中时引擎发
  `stage_end{status:"skipped"}` 并打 INFO 日志 (实现里本来就有), 但
  `RunResult.stage_statuses` 仍记 `"done"` (该 stage 产出确实已在 state 里, 语义
  无误), 而**文档从未写过 `"skipped"` 这个 status 值** —— 于是唯一的信号没人知道
  去看。现 cn/en api.md 写明: 排查"改了 prompt 没生效"要看事件流, 不要只看 status。

- **`add_conditional_edges` 不拦 `async def route_fn`, 报错还会指错方向**: docs
  两处写 route_fn 是"同步纯函数", 但声明期没有守卫 —— stage fn 那边有
  `iscoroutinefunction` 检查 (要求 async), CLI 的 patch 模块也有 async 拒绝, 唯独
  route_fn 漏了。实测 (修复前): 声明一路绿灯 → 运行期 `route(state)` 返回 coroutine
  对象 → 查 mapping 必 miss → 报 `route_fn 返回未声明 key <coroutine object route
  at 0x...>`。**这条消息把排查方向带偏**: 不是"返回了未声明的 key", 而是路由压根
  没被调用; 外加一条 `coroutine was never awaited` 警告。现声明期抛 `TypeError`,
  并在报错里给出文档推荐的出路 (judge-stage 写 state, 边只读)。
  同步是硬约束而非风格偏好: docs 给的理由是"同 state 必同 key"(重放/resume 可判定),
  async 破坏的正是这条。
- **NaN / ±Inf 能进 state 并落盘成非法 JSON**: `deep_validate_state` 只查值**类型**,
  而 `float` 在白名单里, 于是非有限值一路畅通直到 `json.dump` 写出字面量
  `NaN`/`Infinity` —— 那不是 RFC 8259 的合法 JSON。**本仓内看不出来**: Python 的
  json 两侧都 lenient, 往返无损; 炸的是跨边界的地方 (其他语言解析器 / jq /
  PostgreSQL JSONB 拒收整个文件)。实测 (修复前): stage 直接返回 `float("nan")`
  → `run` 报 **done**、checkpoint.json 含 `NaN`、严格解析器拒收, 全程无提示。
  现 `_validate_value` 增查有限性, 非有限值抛 `ValueError` (值的问题而非类型
  的问题), 落在 `deep_validate_state` 这唯一 chokepoint 上 —— stage 返回值、
  `--set-file`、`--overrides` 走同一条路。
  **与 `--set-file` 的不对称**: JSON 分支早有 `parse_constant` 挡 NaN (注释明写
  "JSON 无法持久化"), 但那是解析层的局部防护; YAML 分支两样都没有 (`.nan` /
  `.inf` 是合法 YAML 标量)。现两个分支统一在解析现场走同一个校验点。
- **`--set-file` 值层校验提前到解析现场 + 参数层不再吐栈**: 能解析但存不下来的
  类型 (YAML 的 `date` → `datetime.date`) 此前一路进到落盘才炸, 且 `TypeError`
  从 `_cmd_fork_run` 的参数层逃出去变成整段 traceback (与 `run --input` /
  `prune --keep-last` 同族)。现 `parse_set_file` 解析完即校验值类型与有限性
  (复用 `deep_validate_state`, 不平行实现 walker), `_cmd_fork_run` 的参数层
  补接 `TypeError` (该 try 内全是参数解析, 不存在吞掉深层代码 bug 的风险)。
- **state 值校验报错的嵌套 path 指错位置**: 既有 `TypeError` 消息对嵌套 key 给出
  `path=.llm.nested` 这种**前导点**路径 (递归里无条件 `f"{path}.{k}"`, 而顶层
  path 是空串)。排查时最需要它说对的场景恰恰说错了。现由 `deep_validate_state`
  以顶层 key 播种 path, 消息为 `path=llm.nested`。
- **文档里的 `StateValidationError` 并不存在**: cn/en `api.md` 的 State 小节都写
  非白名单类型 → `StateValidationError`, 而代码里 grep 不到该异常 (实际抛
  `TypeError`; 同一小节里的 `StateConflictError` 是真实存在的)。照旧文档写
  `except StateValidationError` 会 ImportError。文档改为写明真实异常并标注该
  更正 —— 未新增异常类型 (那才是为迁就一句过时文档而设计)。

- **`EnginePolicy(max_concurrent_runs=)` 对单 stage 重放 (`run_stage`) 静默失效**:
  并发闸只内联在 `run()` 里, `run_stage()` 直接绕过 → 同一 policy 下 3 个 run
  串行 (peak=1)、3 个重放全并发 (peak=3)。这个策略的全部意义是护住**共享下游**
  (限流 / 连接池), 而重放打的是同一批 stage、同样的外部调用, 绕过等于上限
  没设, 且无任何提示。现抽出 `Runtime._run_slot()` 作为唯一闸门, 两个入口共用。
  **行为边界**: 此前"配了 `max_concurrent_runs` 又并发重放"的调用点此后会排队
  (闸门按调用计数, 故重入式调用在上限=1 时自锁 —— `run()` 一直如此, 非本次引入)。
- **`EnginePolicy(default_timeout=)` 对单 stage 重放 (`run_stage`) 静默失效**:
  `run()` 按"显式 `Runtime(default_timeout=)` 优先, 缺省回退 policy"两级解析
  run 级 deadline, 而 `run_stage()` 内联只读 `self.default_timeout` → 同一份
  policy 在 `run()` 上拦得住、在重放上完全不起作用。实测 (修复前): 同一
  `EnginePolicy(default_timeout=0.2)` 下 `run()` 0.2s 超时报 `failed`,
  `run_stage()` 跑满 1s 报 `done`, **无任何警告**。现两处统一走
  `Runtime._effective_timeout()` (单一解析点, 从此不可能再漂移); `fork_run`
  最终也走 `run()`, 故三个入口一致。
  **行为边界**: 只影响此前"配了 policy.default_timeout 又跑重放"的调用点 ——
  那些重放此后会在 policy 上限处超时 (可重试语义), 而不再无限跑。
- **`state --key` 打错的 key 不再与"值就是 null"输出相同**: `_cmd_state` 走
  `cp.state.get(args.key, None)` — key 不存在与 key 存在但值为 null 时, 输出
  **逐字节相同**且退出码都是 0。接管道时 (`X=$(pavoz state --task-id T --key K)`)
  调用方拿不到任何信号分辨两者, 而"查某一个字段"正是这个命令的全部意义,
  打错字是最自然的失误。现 key 缺失时点名报错 + 列出可用 key + 退出码 1。
  **行为边界**: key 存在时输出逐字节不变 (含显式 null 值, 那是真答案);
  仅"key 不存在"这一支从 `null`/0 变为报错/1 —— 依赖旧行为(拿 `null` 当"缺失")
  的脚本需改判退出码。
- **CLI 参数错误不再吐 traceback**: `run --input` 的 JSON 解析在 `try` 之外,
  `prune --keep-last` 的取值校验 (`store.prune` 内的 `keep_last >= 0`) 压根没有
  try, 敲错时抛整段 `json/decoder.py` / `checkpoint.py` 内部栈 —— 而 `prune` 是
  **删数据**的命令。现两者统一 `参数错误: ...` + 退出码 1, 与 `fork-run` /
  `replay` 既有口径一致。

### Tests

- ★ **pyflakes 门禁从 6 条违规清到 0 —— 此前 CI 是红的**。`.github/workflows/
  test.yml` 把 `python -m pyflakes pavoz tests examples tools` 当门禁, 而这几轮
  新增的测试文件带进 6 条违规 (2 个未用 import / 2 个无占位符的 f-string / 1 个
  未用的局部变量 / …), 全是"测试文件里的死代码"级别, 但门禁就是门禁 —— **测试绿了
  不等于 CI 绿**。顺带把那条未用的 `r` 用起来: 补 `assert r.status == "done"`,
  原用例只查了 `ran`, 漏掉 run 自身结论 (而"闸门把 stage 挡在门外"这类改动的
  典型失效恰恰是 status 变了)。

  教训记在这: 我一直把"全套 pytest 绿"当门禁全绿, 实际 CI 有 5 个门禁, 只跑了
  1 个。**别用最熟的那个门禁代表全部。**

- **条件边两条结构规则 (V2'/V3') 的组合行为补测**。两条规则各自已有用例, 但
  **单独看会骗人**: 复审时我把"下游 ∈ mapping"当成合法形状, 实测它也被拒 ——
  而且拒它的不是 V2' 而是 **V3'** (那个 stage 既是 mapping 目标、又静态依赖
  router, 两条规则都违反)。规则不是只看 mapping 的。补三个用例钉住组合行为:
  只违反 V2' / 只违反 V3' / **两条都违反时 V2' 先报且消息点名那个 stage**。
  最后一条同时钉住"一次只报一条" —— 两条都触发却只报后一条, 用户就永远看不到
  自己违反的另一条, 得来回改两轮。
  变异验证: 拆 V2' → 3 红 (含既有那条, 说明新用例不冗余); 拆 V3' → 2 红。

  **结论: `DAG.validate()` 的八类形状全部实测过, 检查完整、消息质量高
  (都点名具体 stage + 给出怎么改)、已有测试覆盖 —— 无需修复。**

- **`_ow` (overwrite_keys) 的判定边界补齐**。库层 `merge_state` 只知道"哪些 key
  允许覆盖", 是 runtime 负责算这个集合 (`producers[k] == "<fork>" or
  dag.reachable(producers[k], name)`) —— 判错两个方向都有害: 放得太宽则两个平行
  producer 写同一 key 被静默接受、后写者赢而顺序由拓扑定; 收得太窄则合法的流水线
  演进被误拒。2026-10-16 把六种形状实跑一遍, **全部正确** (平行拦 / 链式放行 /
  菱形平行拦 / 覆盖 initial 拦 / 环内放行 / fork override 放行)。既有用例只钉了
  其中两形状, 补齐其余, 重点是**环内逐轮覆盖**: 它的放行理由是"环内 SCC 里互相
  可达", 而那个理由在非环场景下会变成误放行的借口 —— 所以另配菱形那条钉反向。
  变异验证: 判据换恒真 → 4 红 (含两个既有 runtime 用例); 换恒假 → 3 红。

  **顺带删掉一条自己写的假判据**: 原先有一条想钉"`producers.get(k)` 的存在性检查
  是 fail-closed", 但它把判据表达式**抄进测试文件**再断言其成立 —— 而被改的是
  `runtime.py` 里那行, 两份代码没有联动, 于是变异 runtime 时它照样绿 (实测: 去掉
  存在性检查 → 17 个用例全绿)。教训写进文件注释: **判据必须在被测的那个地方取**;
  "把判据抄进测试再断言它成立"测的是抄写的正确性, 不是实现的正确性。

- **`FileStorage` 路径安全补上 symlink 逃逸这一支**。2026-10 复审时用 12 种穿越
  向量 × 3 个操作 + symlink 实际攻了一遍 `_path` (`../` / 中间目录跳出 / `./`
  前缀 / NUL / 大写段 / Unicode 段 / root 内 symlink 指向外部), **没有一个突破** ——
  受害文件原封不动, symlink 逃逸被拦。防护完好。

  但**测试没钉住其中最关键的一支**: 现有那批用例全用字面 `..` 穿越, 只能证明
  "路径里有 `..` 时被拒"。把 `_path` 里的 `os.path.realpath` 换成 `os.path.abspath`
  (纯词法拼接, 不解析 symlink), **上面每一个照样全绿** —— 而 symlink 正是
  "纯字符串前缀检查"与 "realpath containment" 的唯一分水岭。新增的 2 个 symlink
  用例正是这个分水岭: 变异验证下只有它们转红 (其余 32 个全绿)。

  同时补: 多段穿越的几种拼法 (证明大写/Unicode 段是被**第二道**挡的, 而非第一道
  —— 字符集放行那两类是有意的); NUL 字节明确归第一道; 以及一条**反向约束** ——
  长得像穿越但实际落在 root 内的 key (`..` → `root/...json`、Unicode key、
  `UPPER/../outside/secret` 归一化后仍在 root 内) 必须**放行**, 把"边界是 realpath
  containment 而不是'字符串里有 `..`'"钉死, 免得日后有人"顺手收紧"字符集而误伤
  CheckpointStore 的日常 key。

- **`TestPipe.replay_from` 那条"环图终态仍相等"的承诺, 一直没人验过**。
  docstring 写着"多数场景下终态仍与 cp.state 相等 (因为终值本身就是终点), 但
  轮次敏感的 stage 会算错" —— 前半句是用户照着写 `assert result.state ==
  cp.state` 的依据, 却从未被实测过。补的用例用**累加型**环图 (`acc` 逐轮累加,
  对轮次双敏感) 实测: 过程确实不同 (每轮被压成"返回最后一次 visit 的 delta"),
  但**终态相等**这个承诺成立。若哪天实现变了而这条失效, 会立刻转红而不是让用户
  在 `assert` 上撞墙。

- **形状矩阵: 空 delta × 环图 × 重叠 key × fork 点的交叉** (新增, `tests/
  test_shape_matrix.py`)。现有测试各钉一个维度, 但**交叉**没人钉 —— 而 bug 恰恰
  躲在交叉里。27 个形状覆盖三条不变式: lockstep (每个 stage 的 done 次数 ==
  visits 长度) / `cp.state == 活跑` / `rebuild_state_before` == 该 stage 首次执行
  时观测到的输入。**扫完 432 个形状零问题** (线性链 2-5 级 × 空 delta 位置 ×
  相邻 stage 重叠 key × fork 点, 加环图阈值 × 空 router × fork 点 × override)。
  顺带把两条**判据自己写错**的教训留在了文件头, 它们各自都给出过"漂亮的 0 问题":
  - 用 `done_stages` 的**全局下标**比该 stage 自己的 visits 长度 → 576 个误报;
  - 遍历 `stage_deltas_visits` 的 key 找 lockstep 断 → **漏掉"有 done 无 delta"的
    stage** (正是空 delta 的形态), 环图族因此报 0/0, 连人工变异都抓不到。

  判据有效性靠**变异**验证而非"跑通了": 退回 `if delta:` 前置 → 12 个用例转红;
  环图 visits 的尾对齐退化成顺序索引 → 6 个用例转红。覆盖边界也如实写在文件里 ——
  对 `export-input` 丢 `fork_overrides` 那一类 (a035821) 本矩阵**实测无判别力**,
  那条钉在 `test_rebuild_consistency.py`。绿色测试的边界不写清楚, 等于把下一个
  人送去信它。

- **CI 的文档链接闸门只查文件、不查锚点** —— 而且 `SKIP_PREFIXES` 里带了个 `"#"`,
  连**同文件内**的纯锚点跳转都整体跳过。后果: 写错或改名的锚点照发, 读者点过去
  落在文件顶部, 而 **CI 绿**。docs 之间互链密度很高 (架构 → API → 快速上手), 这类
  死锚点正是读者流失的地方。
  现 `tools/check_doc_links.py` 增查锚点 (GitHub 风格 slug, 支持 CJK / 重复标题
  后缀, 忽略围栏代码块里的 `#`), 并新增 `tests/test_doc_link_checker.py` (10 例)
  钉住它**该抓的抓、不该报的不报** —— 后半句更重要: 满屏误报的检查等于没有检查,
  下一个人就会开始忽略它 (故"目标文件取不到标题集时放过"、"代码块里的 # 不是标题"
  两条都钉了, 另有一条直接跑真实 docs 全过)。

- **`FileStorage` 字符集过滤的注释与实现不符, 已改注释 + 钉住实测边界**。原注释写
  "key 只允许 `[a-z0-9_/.-]`", 读起来像白名单; 实际 `str.isalnum()` 对**大写**与
  **全部 Unicode 字母/数字**都成立 —— 实测 `UPPER` / `任务` / `café` / `٣` 一律
  放行。安全上无害 (真正的边界是 `realpath` containment, 注释后半句自己也写了),
  但注释误导: 读注释的人会以为这道是白名单, 而 task_id 那侧的 ASCII-only
  (`_validate_task_id`) 才是刻意的显式限定 (它把键当**身份**用)。两处口径不同是
  有意的, 现补测试把实测边界钉住 (放行什么 / 挡什么), 免得注释再漂。

- **两套合并语义并排写清 + 各自钉住**。同一个系统里有两个 merge, 语义**相反**,
  文档各提了一句却没并排, 于是容易互相类推:
  - stage `return dict` → `merge_state` → **shallow**, 嵌套 dict **整体替换**。
    实测: s_a 写 `{"cfg": {"model": "m1", "temp": 0.1}}`, s_b 返回
    `{"cfg": {"temp": 0.9}}` → `cfg` 变成 `{"temp": 0.9}`, `model` 没了 → 下游读
    `cfg["model"]` 直接 `KeyError`, run 报 failed, **全程无警告**。
  - `--set` / `--set-file` / `--overrides` / `fork_run(overrides=)` →
    `apply_overrides` → **deep**, 只打 patch leaf, 兄弟键保留 (v0.9 fix)。

  两边的行为都没错 (各自都有文档), 但"stage 返回是浅、注入值是深"这件事此前
  只能靠读者自己把两段拼起来。现 cn/en api.md 并排写明 + 给出两种正确写法,
  `tests/test_merge_semantics.py` 把两侧各钉一遍, 含一条"两条路径若变成同一语义
  就转红"的守卫 —— 任何一侧改动都是破坏性行为变更, 不该在无人察觉时发生。
  变异验证: 把 `merge_state` 改成深合并 → 红 4 例。

  (本轮探针本身没找到缺陷: "resume ≡ 一次跑完"在四种形状下都成立, 包括链式
  覆盖与条件边循环; 中途我一度以为踩到了 bug, 查下来是我的试例自己违反了既有
  契约 —— 同 key 二次写要先过单 producer 契约, 才轮得到合并语义。)

- **两条不变式从一次性探针升级成每次都验** (`tests/test_invariant_checks.py`):
  ① **Checkpoint 字段往返必须穷尽** —— v0.8 起 state 不落盘, 其余字段全靠
  `to_dict`/`from_dict` 往返; 新增字段忘了写进 `to_dict` ⇒ reload 时**静默**用
  默认值, 零报错, resume 拿到的 cp 与落盘时的不是同一个东西。用
  `dataclasses.fields` 枚举 (非手抄清单), 15 个字段全覆盖, 外加"转两代必须与转
  一代相同"的幂等检查。
  ② **事件流必须与 RunResult 说的是同一件事** —— 监控挂在 `on_event`、消费方读
  `RunResult`, 两者分叉则埋点在撒谎而没人知道。四种形状各钉一遍: 成功 / 中途失败
  / 重试耗尽 / skip_unchanged 复用 (后者是唯一允许的分叉: 事件记 `skipped`、结果
  记 `done`)。
  写第 ② 条时踩了一次**判据失明**: 前两条失败用例分别抛 `StageError` 与
  `RetryableError`, 各有专门的 except 分支, **裸异常走的泛型
  `except Exception` 分支完全没被覆盖** —— 验证时我把该分支的
  `stage_end{status}` 改成 `"done"`, 8 条用例**照样全绿**。补上抛裸异常的用例后,
  同样的变异才转红。这条记在这里, 因为"绿了"本身不构成证据。

## [0.5.6] — 2026-10-10

### Security

- **FileStorage 路径穿越修复**: `_path` 的守卫只查字符集 `[a-z0-9_/.-]`, 而 `.`
  在允许集内, 故 `..` 段可穿过过滤。实测 (修复前): `get("../../x/secret")` 可读出
  storage root 之外的文件, `delete` 可将其删除; `put` 看似被挡, 实为 `os.makedirs`
  逐段创建时撞上字面量 `..` 目录而**偶然**失败, 非防护, 且只挡 put。
  现 `_path` 将路径解析为 `realpath` 后断言仍落在 root 内, 对 get/put/delete
  一视同仁; `list_keys` 的 prefix 原本完全不过滤, 一并收口。
  **0.5.5 及更早版本带此缺陷**, 建议升级。

### Added

- **`PavozError` 顶层导出 + 条件边异常归入基类**: `PavozError` (原先只在
  `pavoz.types`, 未顶层导出) 现可 `from pavoz import PavozError`;
  `UnmappedRouteError` / `MaxVisitsExceeded` / `OrphanStagesError` 由直接继承
  `Exception` 改挂 `PavozError` 下 — 库自身抛出的异常统一走一个基类, 消费方
  包 SDK / 转错误码时 `except PavozError` 一次兜住。
  `CheckpointMismatchError` (cp 与 DAG 结构不匹配 = 调用方用错) 刻意留在树外。
  实际影响面小: `run()` 不抛这些 (引擎判的失败转 `RunResult(status="failed",
  error_class=...)`), 统一捕获用于 CLI / TestPipe 等 run() 之外的入口。
- **stage_alive 心跳事件 (R2, ai@home id=1128 提案)**: stage 执行超过
  `EnginePolicy.stage_alive_interval` (默认 30s, None 关) 后, runtime 经
  on_event 每 interval 发一条
  `stage_alive {task_id, run_id, stage, visit, attempt, elapsed}` —
  长 stage 无 fraction 汇报 (不调 Ctx.set_progress) 也能观测, 免 caller
  靠 ps 判活。快 stage 零事件; stage 结束心跳即停; 单 stage 重放不发。
- **源码漂移可见化 (R2, ai@home id=1132 请求)**: cp 增 `stage_fn_sources`
  (上次真实执行的 fn 源码指纹; 重放不覆盖)。fork_run / run_stage 时比对当前
  源码, 漂移则 log WARNING 一行 — fn 体不计 workflow_hash 是有意设计
  (改 prompt 免全链重跑), 本项把"改了代码跑的是新是旧"从静默变可见,
  不阻断不拒续。

### Fixed

- **文档与实际能力不符**: README 中"循环留在业务层 / 框架只提供 3 种能力"的
  表述与已发布的条件边矛盾 (条件边即框架级跨 stage 循环原语), 改为按真实设计
  分两层表述; `docs/cn/conditional-edges.md` 停留在 R1, 仍称条件边图上
  `fork_run` / `run_stage` 不支持 (R2 已交付), 已按实际语义重写。
- **`docs/en/` 缺条件边与 diff_runs 指南** (中英双侧特性文档现对齐)。

### Tests

- 文档页可达性闸 (抓孤儿页); CLI 解析器契约 (走真实 `main(argv)`, 断言 flag
  落到 handler 实际读取的属性); `load_compatible` 契约; diff 输出整形与
  `ReadOnlyStateView` 访问器。覆盖率 89% → 93%。

## [0.5.5] — 2026-09-28

### Added

- **`DAG.add_conditional_edges(from_node, route_fn, mapping, *, max_visits=None)`
  (R1)**: 条件路由 — 编排器持所有边, 节点纯数据处置。route_fn 同步纯函数
  (state → key), mapping 封闭集 (未声明 key → `UnmappedRouteError`);
  回路边目标必须显式 max_visits (超限 → `MaxVisitsExceeded`),
  EnginePolicy.max_steps 仍全局兜底。route_fn 源码 + mapping + max_visits
  计入 workflow_hash (改路由 = 结构变, 拒续)。cp 增 `stage_deltas_visits`
  (完成序含重复, 循环 resume 重建正确)。新事件 `route`; stage 事件加
  `visit` 序号; stage_timings 循环累计。viz 渲染条件边 (mermaid 虚线带 key
  标签 / graph_json kind+key+max_visits)。**终止合法性 (fail-loud)**: 正常终止
  时未执行且未被任何决策未过期的已执行 router 声明覆盖的 stage →
  `OrphanStagesError` (回路没闭合/分支挂空决不静默报 done)。
  route 事件增 `declared` 字段 (mapping 全 key, 本轮未选分支 = declared − key
  可读, R2 可观测性)。
  **循环静态链重入 (R2)**: 环头无环外静态父 → 豁免为首圈入口 (原判定把环内
  静态链误当入口, 环头被错标 route-only); router 回跳已执行 stage → 其静态下游
  闭包重跑 (新 visit, cp 增 `reset_pending`, resume 断点续跑) — 修复"第二圈静态链
  不重跑却静默报 done"。
  **条件边图 fork_run / run_stage 已支持 (R2)**: fork = 截断 + overrides +
  路由续跑; run_stage 单 stage 重放 (before 折叠); 折叠按完成序逐 visit
  尾对齐 (`fork_keep_counts`) — fork 重跑循环 stage 不串轮; `<fork>` 注入
  key 可被续跑链改写。指南: docs/cn/conditional-edges.md。

- **CI 质量门禁**: pytest 覆盖率 `--cov-fail-under=80` + pyright (basic, 0 errors)。
  dev extras 收编 pytest-cov / pyflakes / pyyaml / pyright (CI 免散装安装)。
  测试铁律的 "覆盖率 ≥80% / pyright 0 errors" 从文档声明变为机器可执行
  (实测基线: 覆盖率 88%, runtime.py 12 处 `str | None` 过时注解已收窄)。

## [0.5.4] — 2026-09-22

### Added (0.5.4 — diff/观测/viz/错误分类, 2026-09-22)

- **`diff_runs` (R4)**: 两 run 的 stage 级 structured diff —
  `CheckpointStore.diff_runs(task_id, run_id_a, run_id_b="")` 便捷形式 + 底层
  `diff_runs(cp_a, cp_b)` (可跨 task)。输出全 JSON-serializable
  (workflow_hash_changed / stages[].status,input_hash_changed,output_diff /
  state_diff), 不做 AI 摘要 — 消费者自行解读。
- **`pavoz.viz` (D1)**: `to_mermaid(dag)` / `to_graph_json(dag)` — DAG 只读导出
  (mermaid 文本 / nodes+edges+topo_order), 零依赖 ≤51 行; pavoz 出数据不出 UI。
- **`StageError.category` + `RunResult.error_category` (D1.5)**: caller raise 时
  传业务类别 (9 类建议值 LLM_TIMEOUT/SEARCH_NO_RESULT/CODE_BUG/NETWORK/AUTH/
  INFRA/CHECKPOINT_LOAD_FAIL/STATE_VALIDATION/PROTOCOL_BREACH, 自由 string 不强制
  枚举), runtime 透传; 未传/未知异常/done/cancelled → None。additive 0 迁移。
- **docs (D2+D2.5)**: 根级 `MIGRATION.md` (0.5.2 → 0.5.3 升级指南) +
  `docs/cn/diff-runs.md` (diff_runs 使用指南, 含 ai-writing paper flow 真实案例
  与薄委托 wrapper 指纹陷阱) + EnginePolicy docstring 补字段默认值/何时覆写。

### Fixed

- tests: `test_state_apply` 负路径测试字面量 `/tmp/...` 迁 `tmp_path` (B3 e2e
  复测附带, 硬编码路径扫描归零)。

## [0.5.3] — 2026-09-22

### Added (0.5.3 — caller 观测/操控面, 2026-09-22)

- **RunResult 错误上下文 (W1)**: 失败/取消时带 `failed_stage` / `retryable`
  (RetryableError/TimeoutError → True) / `state_summary` (state 截断 JSON ≤2048 字符) —
  caller 接到结果即有"哪个 stage 错、能否重试、state 现场"。
- **`EventRecorder` (W2)**: `Runtime(on_event=EventRecorder(sink_path=...))` —
  事件流收集 + 可选 JSONL 落盘。
- **`Runtime(debug_dir=...)` (W2)**: 每 stage 后落 `<debug_dir>/<task_id>/<n>_<stage>_<status>.json`
  state 快照 (失败不杀 run); 落盘失败仅 log。
- **`EnginePolicy` (W3, additive)**: run 级策略外置 — `default_timeout` (显式参数优先) /
  `max_steps` (attempt 总数熔断, error_class=MaxStepsExceeded) / `backoff_max` (替代硬编码 30) /
  `max_concurrent_runs` (进程内并发闸) / `skip_unchanged`。不传 policy 行为与旧版完全一致。
- **stage input hash + `fork_run(skip_unchanged=True)` (R2)**: 每 stage 执行前记录
  `stage_input_hashes` (fn 源码 + 执行前 state 的 sha256); fork 续跑时逐 stage 比对,
  命中即重放历史 delta — 改 1 个 prompt 只重跑受影响链路。副作用 stage 标
  `Stage.skip_unchanged=False` 拒跳。
- **`CancelRegistry` graceful|hard (R1a)**: `cancel(task_id, mode="hard")` 立即打断运行中
  stage (前提幂等), run 落 cp 后返回 cancelled 结果; `Stage.killable=False` 拒硬杀 →
  `NotKillable`。外部 `task.cancel()` 行为不变。
- **`Runtime.get_state(task_id)` + `CheckpointStore.list_tasks()` (R1b)**: 程序化查询
  最新 run 全量 state / 跨 task 巡检。
- **R3 端到端测试**: `fork_run` 即 rerun_from_stage 原语 (v0.6 已有), 补 prompt override +
  history 保留验证。
- **docs**: `docs/{cn,en}/api.md` 补 EnginePolicy / EventRecorder / CancelRegistry /
  stage_input_hash 四节 (api_docs_coverage 测试钉住)。
- **docs**: 新增根级 `MIGRATION.md` — 0.5.2 → 0.5.3 升级指南 (additive 清单 +
  各新能力最小用法, caller 0 必改项)。

### Fixed
- README 移除失效的 ROADMAP.md 链接 (017fdb9 移出仓遗留, CI 文档检查红)。

## [0.5.2] — 2026-09-14

### Changed
- **版本线对齐**: 延续 stageflow 时代的 0.5.x 版本线 (v0.5.1 → 0.5.2), 不再使用改名后重起的 0.3.x —
  避免 PyPI 版本序与项目历史割裂。
- README / quickstart / llms.txt 等全部安装指令从 `git+https://...` 切换为 `pip install pavoz`
  (0.3.0 已发布 PyPI, CI Trusted Publishing 免密发版流程已验证)。

## [0.3.0] — 2026-09-14

### Added (文档工程 — 2026-09-13)

- **`llms.txt`**: 仓库根的 agent 自读入口 — 这是什么 / 从哪读起 / 不可变规则 / 提交前跑什么命令。
- **`tools/check_doc_links.py` + CI 步骤**: markdown 相对链接必须可解析 (上线即抓出 3 处死链,
  含 `docs/storage.md` 与 `docs/en/*.md` 的错路径)。
- **`tools/check_doc_style.py` + CI 步骤**: 只做高精度风格检查 — AI 禁用词、README 装饰性版本号、
  中文句尾半角句号; 过程类文档 (CLAUDE/CHANGELOG/ROADMAP) 与 `docs/design/` 显式豁免, 误报挡 PR 比漏报贵。
- **`tests/test_api_docs_coverage.py`**: 断言 `pavoz.__all__` 每个名字都出现在
  `docs/{cn,en}/api.md` — 防"文档悄悄落后代码"。

### Changed (文档工程 — 2026-09-13)

- **API 参考补齐滞后项** (`docs/cn/api.md` + `docs/en/api.md`): 补上已 ship 但未写进参考的能力 —
  `Runtime(on_event=...)` / `cancel_check`(含事件表)、`Runtime.fork_run`、
  `ctx.set_progress` / `cancelled` / `on_event`、`RunResult.error_class` / `stage_timings`、
  `CheckpointStore.prune`、参数覆盖四个函数 (`parse_set_args` / `parse_set_file` /
  `merge_overrides` / `apply_overrides`) 与 `__version__`。
- **CONTRIBUTING 对齐 CI**: 质量门槛改为 CI 实际执行的两条命令 (原写 `ruff check` 与
  "38+ tests" 均与现状不符); 修正 3 处指向不存在文件的文档链接。
- **CI lint 范围加入 `examples/` 与 `tools/`**。

### Changed (扩展面文档 + 示例升级 — 2026-09-13)

- **架构文档新增《扩展面》一节** (`docs/cn/architecture.md` + `docs/en/architecture.md`):
  五个扩展点 (stage 函数 / 生命周期事件 / `ctx.call` / `StorageBackend` / 取消) + 事件表 +
  装饰器注意事项 (`functools.wraps`) + 版本策略。**核心 API 无改动。**
- **`examples/agent_loop.py` 升级**: 保留原内联循环写法, 新增 `quality_gate` 装饰器版本
  (多 lens 评分 / `min` 聚合 / 阈值 / 分数写 `on_event`), 并加回归测试 (`tests/test_examples_agent_loop.py`)
  防止示例腐化。
- **pyflakes 范围加入 `examples/`**; 顺带修掉 `tests/` 中一处未使用 import (存量 lint 失败)。
- 修正 `pavoz/dag.py` docstring 中过期的 stage 签名 (`async def s_x(req, ctx)` → `async def s_x(ctx)`,
  实际调用见 `runtime.py` 的 ctx-only 约定)。

### Rejected (2026-09-13 评审决议 — 见 `docs/design/extension-plan-2026-09-13.md`)

- 核心不新增全局 hook 注册表: `Runtime(on_event=...)` 已提供同等观察能力, 且注入式优于进程级全局状态。
- 核心不新增 `ctx._meta`: `Ctx` 在每次 attempt 重建, 不能承载 run 级记账 (计数会被重试绕过)。
- 下游质量门 / 契约校验以**独立扩展包**形式提供 (pavoz-extensions), 核心保持零依赖、零改动。

### Added (consumer PR: ai-research R1/R2/R3 — 2026-09-10, 见 docs/design/pr-reply-ai-research-2026-09-10.md)

- **R2 `RunResult.error_class`**: 原始异常类名 (失败时保留, cancelled/done 为 None)。
  未知业务异常 (如 PipelineError) 保真原始类名; `error` 字符串格式不变 (API 冻结)。
  用途: 错误分诊 (业务失败→调参重跑 vs 代码 bug→找人)。
- **R1 `CheckpointStore.prune(task_id, *, keep_last, dry_run)` + `pavoz prune` CLI**:
  按保留数修剪旧 run checkpoint。排序 = max(stage_ts); **latest 指针指向的 run
  无条件保留**; `--dry-run` 列将删 run + 近似字节。不做 max_runs_per_task 自动修剪
  (v0.4 adapters REVERSED 先例: 不替客户决定保留策略)。
- **R3 `Ctx.set_progress(fraction, note=None)`**: stage 内进度心跳 → `stage_progress`
  生命周期事件 (第 5 种)。best-effort 瞬态信号: 不落 checkpoint, 不碰 deadline/重试,
  同 fraction 引擎内去抖, 未启用 on_event 时 no-op。

### 2026-09-10 之前 (已在 0.3.0-rc1 tag 之后, 未入版本)

- ai-write 需求 PR (530f4c2): CLI storage 解耦 `PAVOZ_STORAGE_SPEC` + fork-run `--dry-run` 预演。

## [0.3.0-rc1] — 2026-09-09

### Added (fork-run 通用 param override — Declarative stage-level debug)

**4 个新 CLI 标志** (`pavoz fork-run`):
- `--set KEY=VALUE` (可重复, 点分路径 KEY, 类型自动推断)
- `--set-file PATH` (JSON/YAML 文件, 1MB 上限, `yaml.safe_load`)
- `--compare-with RUN_ID` (跑完后输出 state leaf diff)
- `--dry-run` flag (仅解析 + 显示, 不执行 stage)

**新模块级函数** (`parse_set_args` / `parse_set_file` / `merge_overrides` /
`apply_overrides` 顶层也导出; `_state_diff` 是 `pavoz.state` 内部 helper,
仅 compare-with 内部使用, 不在顶层导出面):
- `parse_set_args(items)` — `--set` 列表解析
- `parse_set_file(path)` — JSON/YAML 文件解析 (1MB + safe_load + dunder 校验)
- `merge_overrides(*sources)` — 多源合并 (按优先级)
- `apply_overrides(base, *patches)` — 接受 dot-path 或 nested dict, 不 mutate base

**CLI storage 解耦 (R1)**: `PAVOZ_STORAGE_SPEC` env (`pkg.module:Class`) +
`PAVOZ_STORAGE_KWARGS` (JSON, `{task_id}` 插值) — CLI 全子命令可操作任意
StorageBackend (如 consumer 的 MinIO adapter); 不设时现 FileStorage 行为不变。

**dry-run 预演 (R2)**: `fork-run --dry-run` 从 "显示解析结果" 升级为
"预演 apply 效果" — 基于 `rebuild_state_before(stage)` 的执行前 state 做
leaf 级 diff (`preview.diff_keys_count` / `diff_sample` / `would_rerun`),
试错不再需要真跑。

**Priority merge**: `--set > --set-file > --overrides > --input` (后写覆盖前写;
`--overrides` > `--input` 沿袭 v0.8 组合使用时的既有胜者)

**Safety**:
- 拒绝 dunder key (`__proto__`, `__class__` 等) — 防 Python object injection
- 路径深度 ≤ 5 限制
- YAML 用 `safe_load` (不 `yaml.load`) — 防任意代码执行
- 文件大小 ≤ 1MB 限制

**新 examples** (`examples/`):
- `01_model_switch.sh` — 4-model composer A/B (ai-research 场景)
- `02_prompt_patch.py` — 改 prompt 不重跑前序 stage
- `03_compare_diff.py` — 多 run state diff 对比
- `04_deep_merge.py` — dot-path 兄弟键保留演示
- `05_yaml_file.sh` — YAML 文件批改 (dry-run first)

**新 tests**:
- `tests/test_state_apply.py` (54 case) — state.py utilities
- `tests/test_fork_overrides.py` (6 case) — 深合并语义
- `tests/test_cli_fork_set.py` (8 case) — CLI 直接调用

**新 docs**:
- `docs/replay-params.md` (完整指南)

### Fixed

- `fork_run` overrides 应用从顶层浅替换 (`state.update`) 改为深合并
  (`apply_overrides`) — 嵌套 patch 不再抹掉兄弟键 (如 `--set llm.model=x`
  不再丢 `llm.temperature`). 顶层标量覆盖行为不变.

### Compatibility

| 0.2.0 | 0.3.0 |
|---|---|
| `--overrides '{"k":"v"}'` | ✅ unchanged |
| `--input edited.json` | ✅ unchanged |
| `Runtime.fork_run(overrides=dict)` | ✅ 签名不变 (嵌套 dict 语义见 Fixed) |
| `replay --patch patch.py` | ✅ unchanged |

### Breaking changes

无 (behavior fix 见 Fixed; 顶层标量覆盖不变).

### Stdlib

保持 zero runtime deps. YAML 是 `[yaml]` optional extra.

### Motivation

ai-research (consumer) 做 composer 4-model A/B 测试时, 当前痛点:
- 直接 DB INSERT 绕 API (侵入性)
- writer_chain 列存 model id (ai_writer-specific)
- 4 task 并发无 isolation (race condition)

pavoz v0.6 已有 `fork_run(overrides=dict)` (LangGraph fork 范式), 但 CLI UX 差
(JSON string 嵌套痛苦). 本 PR 加 declarative 层, 让 1 行命令 = 切 model 重跑
stage + diff 输出, 把 2 小时 hack 缩到 15 分钟.

## [0.2.0] — 2026-09-08

### Changed (stageflow → pavoz 品牌重命名)

- 包名: `stageflow` → **`pavoz`** (PyPI 包 + import 路径 + CLI 命令全统一)
- 仓库: `ebziw/stageflow` → **`liyong-labs/pavoz`** (含 pyproject URLs / docs / CI)
- 内部目录: `stageflow/` → `pavoz/` (12 文件 git mv)
- Author: `ebziw` → `liyong-labs`

### Added (协作式取消 + 生命周期事件 — 0.8.0 → 0.2.0 之间)

- 协作式取消: `Runtime(cancel_check=...)` — stage 间/重试间检查点拦截, `RunResult.status`
  新增 `"cancelled"`; 已完成 stage 照常落 checkpoint, `resume=True` 无缝续跑.
  stage 内长循环用 `ctx.cancelled()` 轮询自行退出 (循环留业务层).
  cancel_check 异常视为未取消 (fail-open).
- 生命周期事件钩子: `Runtime(on_event=...)` — run_start / stage_start / stage_end /
  stage_retry / run_end 五种结构化事件, observer 异常隔离. `RunResult.stage_timings`
  记录每 stage 墙钟耗时 (含 retry 退避). 单 stage 重放 (run_stage) 不发事件.
- 重试退避 full-jitter: `uniform(0, min(2^(attempt-1), 30))` — 多 task 同步重试防雷群.

## [0.8.0] — 2026-09-06

### Added

- 协作式取消: `Runtime(cancel_check=...)` — stage 间/重试间检查点拦截, `RunResult.status`
  新增 `"cancelled"`; 已完成 stage 照常落 checkpoint, `resume=True` 无缝续跑.
  stage 内长循环用 `ctx.cancelled()` 轮询自行退出 (循环留业务层).
  cancel_check 异常视为未取消 (fail-open).
- 生命周期事件钩子: `Runtime(on_event=...)` — run_start / stage_start / stage_end /
  stage_retry / run_end 五种结构化事件, observer 异常隔离. `RunResult.stage_timings`
  记录每 stage 墙钟耗时 (含 retry 退避). 单 stage 重放 (run_stage) 不发事件.
- 重试退避 full-jitter: `uniform(0, min(2^(attempt-1), 30))` — 多 task 同步重试防雷群.


## [0.8.0] — 2026-09-06

### Changed (checkpoint 格式: state 不落盘 — 旧格式 cp 兼容读但不再写)

- `Checkpoint.state` 字段 → property (从 `initial_state` + `stage_deltas` 惰性
  rebuild) — 落盘 2x 冗余消除. 实测 ai_writer 真实 cp 2.82MB → 2.05MB (-28%),
  rebuild 0ms 等价. 旧存量 cp (带 state) 照常读 (rebuild 覆盖 state 键).
- 2026-09-06 user 拍板: 发布前向前看, 不做向后兼容 — 序列化层不再写 state,
  无兼容分支
- `fork_overrides` 新字段: fork_run 的 overrides 持久化 (曾靠全量 state 落盘),
  随每次 checkpoint save 携带 — fork 分支的 overrides 活到分支终点

### Fixed

- fork resume 丢 overrides: fork_cp 保存后 resume 续跑, 每轮 save 的新 cp 若
  不带 overrides → 分支输入丢失 (从 from_stage 重跑用旧输入)
- run_stage/replay_from 的 legacy-cp 检查依赖 cp.state truthy — state 变
  property 后恒有值, 检查短路失效 → 防静默空输入重放的防护恢复 (只看 deltas)

### Chore

- 审计清理 (project-doctor): cli `importlib.sys.modules` → `sys.modules`,
  Ctx.caller sync-lambda default (await dict 隐患) → 单 async `_noop_caller`,
  删死 API `validate_state`, ruff UP034, `.gitignore` + data/

## [0.7.0] — 2026-09-06

### Added

- `Checkpoint.stage_ts`: stage 完成 epoch 时刻 (每 node 落盘, resume/fork 保留前缀) —
  恢复侧内容过期 (TTL) gate 的判定依据 (ai_writer CP 迁移 Step 1 用)

## [0.6.0] — 2026-09-06

### Added (time-travel fork: 任意历史节点取输入 → 改 → 装回)

- `Runtime.fork_run(dag, task_id, *, from_stage, overrides=None, run_id=None)`:
  LangGraph fork 范式 — truncate 到 from_stage 前, initial + 保留 deltas +
  overrides 重建 state, 新 run_id 落盘且 latest 指向 fork, 原 run cp 不动,
  委托 `run(resume=True)` 从 from_stage 续跑 (前序 stage 结果复用不重跑)
- `Checkpoint.state_stats()`: state 体积观测 (key top N + 构成), CLI `state --stats`
- CLI: `export-input --task-id X --stage Y` (打印 stage 执行前输入 JSON, 可编辑)
  + `fork-run ... --input file | --overrides JSON` (装回续跑)
- 失败/中断的 stage 也可 fork (依赖已全完成) — 修输入重跑失败点

## [0.5.1] — 2026-09-05

### Added (M2 stage replay + M3 single-stage replay)

- **Checkpoint 存每 stage 原始 return** (`stage_deltas`, 按完成序) + run 的
  `initial_state` (M2) — 两者合并可精确重建任意 stage 执行前 state
  (cp.state 是最终合并态, 不能当 stage 输入逆向拆)
- `Checkpoint.rebuild_state()` / `rebuild_state_before(stage_name)`: 重建
  已完成 stage merge 后的 state / 某 stage 执行前的 state (链式覆盖由
  完成序天然处理)
- `TestPipe.replay_from(cp, dag)`: 已完成 stage 用历史 delta 全 mock —
  真实 run 的 cp 直接当回归 fixture (改 stage 实现后对比终态);
  hash mismatch / v0.5.0 旧 cp → 明确报错
- `Runtime.run_stage(dag, task_id, stage_name)`: 从 cp 重建输入单 stage
  重放 (调 prompt/参数秒级迭代) — 不落 cp 不动 latest 指针 (A6/A7 防
  replay 污染 resume 目标); 失败/中断的 stage (依赖已全完成) 也可重放
- CLI `replay <dag.py> --task-id X --stage Y [--patch P.py]`: 单 stage 重放;
  patch 文件约定 `patch(dag) -> None` 替换 stage fn (async patch 拒绝 —
  防静默 no-op)
- CLI run 恒落盘 (R5 fix)

### Changed

- CLI `run` 不再只在 `--resume` 时 attach CheckpointStore — 单跑也写
  `~/.pavoz/data` (`PAVOZ_STORAGE` 可覆盖)。行为: 单次 run 即
  落盘, replay/trace/state 对 CLI 产物直接可用

### Compatibility

- Checkpoint 新字段 (`initial_state` / `stage_deltas`) `from_dict` `.get`
  默认 `{}` — v0.5.0 旧 cp 可读可 resume
- v0.5.0 旧 cp (无 stage_deltas) 上重放 (CLI replay / run_stage /
  TestPipe.replay_from) → 明确报错提示重跑一次, 不静默错配

### Internal

- 81 tests, ruff clean, 零运行时依赖 (stdlib only)

## [0.5.0] — 2026-09-05

### Added (industry-standard ID model)

- `task_id` 参数可选: 省略 → pavoz 自动生成 UUID4。跨 retry/resume 稳定 —
  幂等键 (Temporal WorkflowId / DBOS workflow_id 模式)。入口校验: 非空、
  ≤128、字符集 `[A-Za-z0-9_.-]` (禁 `/`, 防 storage key 路径注入)
- `run_id`: 每次 `Runtime.run()` 自动生成 UUID4, 返回在 `RunResult.run_id`。
  同 task 多次 run 互不覆盖 (original / resume / 重跑)
- `Ctx.run_id` + `Ctx.attempt` (1-based, stage retry 时 +1) 自动注入
- `ctx.call` 底层 caller 签名 3 参 → 4 参 (新增 `CallMeta`) —
  caller 落 trace/llm_calls 时可直接关联 task/run/stage/attempt (A1)
- `CheckpointStore.list_runs(task_id)` + `load_latest(task_id)` (指针文件) helpers

### Changed (BREAKING)

- Checkpoint 存储 key: `runs/{task_id}/{run_id}/checkpoint` (was
  `runs/{task_id}/checkpoint`) + 指针文件 `runs/{task_id}/latest` =
  `{"run_id": ...}` (uuid4 字典序 ≠ 时间序, 不靠排序判 "最新")
- `RunResult` / `Checkpoint` 新增必填 `run_id` 字段 (was absent)
- `CheckpointStore.load(task_id, run_id)` 2 参 (was 1 参); "最新 run" 用
  `load_latest(task_id)`
- `CheckpointStore.delete(task_id, run_id)` 2 参 (was 1 参 task 级); 指针文件不随
  删除更新 (删的恰是最新 run → 后续 load_latest 返 None, 属可接受边界)
- `CheckpointStore.load_compatible(task_id, run_id, dag)` 3 参 (was 2 参);
  `run_id=""` = 加载最新
- `Runtime(caller=...)` 自定义 caller 需接第 4 参 `CallMeta` (默认 no-op 已同步)
- `Runtime.run()` 移除 `deadline` kwarg (was per-run 绝对超时, 优先于
  `default_timeout`) — run 级超时现在只走 `Runtime.default_timeout`
- `Runtime.run()` `resume` 默认值 True → False (默认起新 run_id; 续跑需显式
  `resume=True`)
- resume 语义收紧: `resume=True` 无 checkpoint → RuntimeError; run 已全部完成
  (done guard) → RuntimeError 防静默 no-op。重跑 = `resume=False` 起新 run_id
- run 完成后 checkpoint **不再自动删除** (v0.4.1 前会清) — done guard 取代

### Rationale

Adopts Temporal WorkflowId/RunId, DBOS workflow_id idempotency key, and
Airflow `dag_id + task_id + run_id + try_number` composite key patterns.
Industry-standard, no caller-invented ID schemes.

### Migration (hard cut)

No legacy loader. Old `runs/{task_id}/checkpoint` keys become orphaned
on disk — caller can clean up with their storage backend.
旧数据 (无 run_id 字段) 同样不可加载。

### Internal

- 60 tests (52 core + 8 storage_loader), ruff clean, 零运行时依赖 (stdlib only)
- CLI: resume 语义错误 (无 cp / 已全部完成) → 友好消息 + exit 1, 不炸 traceback


## [0.1.1] — 2026-09-05

### Added

- **链式覆盖 (chain overwrite)**: 下游 stage 可以覆盖**传递上游 producer** 写的
  state key。数据流水线 (如 `search → filter → compress` 逐级演进同一产物) 是
  通用模式, 不再被 one-producer-per-key 误判为平行冲突。
  - `DAG.reachable(upstream, downstream)`: 传递依赖判定
  - Runtime 追踪 `state key → producer stage`, checkpoint 持久化 `producers`
  - 平行 producer (无依赖链) 冲突仍抛 `StateConflictError` (v1 语义保留)
  - 旧 checkpoint (无 producers 字段) resume → 宽松模式 (视同单链), 向后兼容

### Changed

- `merge_state(prev, delta, stage_name, overwrite_keys=None)`: 新增可选参数
- `Checkpoint`: 新增 `producers` 字段 (序列化向后兼容, 缺省 `{}`)

## [0.1.0] — 2026-09-05

### Added

首个可用版本 (micro/in-process workflow engine):

- **DAG DSL**: `@dag.stage(depends_on, retries, timeout)` 静态声明, Kahn 拓扑
  排序, Tarjan SCC 环检测 (含自环), validate 后冻结
- **Runtime**: 顺序执行 + per-node 重试 (指数退避) + absolute deadline 传播 +
  state merge + 每 node checkpoint
- **State 契约**: json-serializable 类型白名单 (str/int/float/bool/None/
  list/dict), `ctx.state` 为只读深拷贝 (写即 raise), `return dict` 是唯一写
  路径, 平行 producer 冲突 → `StateConflictError`
- **异常契约**: `StageError` (业务失败, 不重试) / `RetryableError` (退避重试,
  耗尽 fail) / `FatalError` (程序 bug, 立即 fail)
- **Checkpoint**: 每 node 落盘, `workflow_hash` (stage 结构指纹) mismatch →
  拒 resume (只改函数体不影响 hash), run 完成自动清理
- **ctx.call(kind, op, params)**: 对外调用统一入口 (caller 由业务注入),
  默认 no-op echo (demo/TestPipe 可用)
- **TestPipe**: 第一天可用的回归 fixture — mock stage 输出跑全图
- **StorageBackend Protocol + FileStorage**: core 零依赖, 业务可注入
  DB/MinIO adapter
- **TaskTrigger Protocol**: 业务 task 表边界抽象
- **CLI**: `run / trace / state / inspect` (v0.1 面)
- 38 tests, ruff clean, 零运行时依赖 (stdlib only)

## [0.1.2] — 2026-09-05

### Removed (ponytail cleanup)

- `trigger.py` (`TaskTrigger` / `TaskRef` — zero callers)
- `Ctx.log` / `Ctx.remaining_seconds` (zero callers)
- `Stage.metadata` field + `@dag.stage` `**metadata` (zero callers)
- `DAG.entrypoints` / `DAG.depth_of` (zero callers)
- CLI `inspect` subcommand (空壳 — "v0.1 无 call 级 trace" 提示)

### Internal

- API 冻结面 zero change: `@dag.stage` / `Runtime.run` / `Ctx` / `State` / 异常 / Checkpoint 全部不动
- 38 tests 全绿, ruff 0 violation

## [0.4.0] — 2026-09-05

### Added (pavoz.contrib.storage)

- **PostgresStorage** (psycopg3 + JSONB, upsert via ON CONFLICT)
- **MySQLStorage** (PyMySQL + LONGTEXT, upsert via ON DUPLICATE KEY)
- **RedisStorage** (redis-py, prefix-scoped, SCAN-based list_keys)
- **MinioStorage** (boto3, **delete/get 容忍 NoSuchKey/404** 对齐 ai_writer `sf_storage.py`)
- **SqliteStorage** (stdlib sqlite3, 单文件 DB)
- 公共 helpers: `_validate_key` (路径安全) + `_encode_payload` / `_decode_payload` (UTF-8 JSON)

### Internal

- core 12 模块 + `StorageBackend` Protocol 零变化 (API 冻结); 0 行 core 功能修改 (fix1 仅同步 manifest 版本号)
- contrib 是独立 subpackage (`pavoz.contrib.storage`); core 不引入任何 driver
- pyproject extras 分组: `[postgres]` / `[mysql]` / `[redis]` / `[minio]` / `[sqlite]` (空) / `[contrib]` (4 个) / `[all]`
- 缺 driver 时 adapter 模块顶 try/except ImportError + 安装提示

### Tests

- 38 core tests + 44 contrib tests (10 标 `@pytest.mark.integration` 需 docker)
- MinioStorage 用 moto[s3] mock; RedisStorage 用 fakeredis mock (无 docker 依赖)

## [0.4.1] — 2026-09-05

### Removed (reversal of v0.4 contrib approach)

- `pavoz.contrib.storage` 5 个 vendor adapter (SqliteStorage / PostgresStorage / MySQLStorage / RedisStorage / MinioStorage) — over-engineering, user 自管 deps + 自写 adapter 更合适
- `pavoz.contrib` subpackage 完全移除
- pyproject.toml extras (`postgres` / `mysql` / `redis` / `minio` / `sqlite` / `contrib` / `all`) 移除 — 用户自己装自己要的
- 相关 tests (tests/contrib/) 移除

### Added

- `pavoz.storage_loader.load_storage(spec: str, **kwargs) -> StorageBackend` — 按 `"pkg.module:ClassName"` 字符串 importlib 加载
- `pavoz.PavozStorageError` — 友好错误 (module 未装 / class 名错 / kwargs 错 / 类型错 5 类)
- 用户可自己写 adapter, pavoz 不带 driver, 通过 `pip install` 自管 deps
- ai_writer 兼容: `load_storage("backend.integration.sf_storage.MinioStorage", ...)` 直接可用

### Internal

- core 仍 stdlib-only + 零第三方依赖
- 0 行 core 修改 (除新增 storage_loader.py + 2 行 export)
- reversal 是真实设计演化: v0.4 contrib 是反向教材, git history 保留以做记录

## [0.1.3] — 2026-09-05

### Fixed

- `pyproject.toml` version + `pavoz/__init__.py` `__version__` 与 tag 同步 (0.1.1 → 0.1.3)。final-review B1: 之前 `pip install -e .` / `import pavoz; pavoz.__version__` 返 0.1.1,与 v0.1.2 tag 不符。
- 不 amend v0.1.2 (keep git history honest) — 走新 patch tag v0.1.3
