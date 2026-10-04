# 接收抽样后端服务（OC 曲线 / 风险 / 方案设计 / GB/T 2828.1 检验流）

Go 1.23 + Echo + PostgreSQL 16 的纯 HTTP 后端，覆盖：

- **方案档管理**：一次 / 二次抽样档的新建、修改、删除、按名查询。方案档**带不可变修订历史**：每次修改追加一个带修订号与生效时刻的修订，生效时刻可在将来也可补记到过去；检验流按每批自身检验时间点上生效的修订判定。
- **统计计算**：接收概率 Pa、二次抽样 ASN、带批量 N 时的 AOQ / AOQL / ATI、生产方风险 α 与使用方风险 β、OC 曲线。
- **反向设计**：按 (AQL, α, LTPD, β) 搜索最小样本量一次方案；二次方案限定 n2=n1 或 n2=2·n1，在可行方案中取 AQL 处 ASN 最小者。
- **检验流**：一条流绑定正常 / 加严 / 放宽三个档，逐批录入，按 GB/T 2828.1 转移规则推进严格度；补录、删改、追加修订后逐字段与“拿全部事件 + 全部修订从头重放”一致。

只提供 HTTP/JSON 接口，无前端。

---

## 1. 快速开始

```bash
docker compose up --build
# 服务: http://localhost:8080   数据库: localhost:5432
curl http://localhost:8080/health
```

环境变量：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `DATABASE_DSN` | `postgres://sampling:sampling@localhost:5432/sampling?sslmode=disable` | PostgreSQL 连接串 |
| `HTTP_ADDR` | `:8080` | 监听地址 |

服务启动时自动执行幂等建表迁移（`internal/store/schema.sql`）。

本地运行（已有 Go 1.23 与 PG16）：

```bash
make build
DATABASE_DSN="postgres://...?sslmode=disable" ./sampling-server
```

---

## 2. 概率模型与对数域计算

- 带批量 `N`：**超几何分布**（不放回）；二次抽样第二阶段用条件超几何（抽走 n1 后，剩余批量 N−n1、剩余不合格品 K−d1）。
- 不带 `N`：**二项分布**。
- 显式 `distribution=poisson`：**泊松近似**，响应带 `"approximate": true`；当 `n·p > 5` 时在 `warnings` 中给警告。

所有组合数与概率都在**对数域**计算：

- `LogComb(n,k) = lgamma(n+1) − lgamma(k+1) − lgamma(n−k+1)`，`n` 到几千也不溢出；越界输入返回 `−Inf`，不可能事件永远不会“悄悄变 0”被计入概率质量。
- 概率求和用 `log-sum-exp`，仅在最后取一次指数；端点 `p=0`、`p=1` 给出**精确**的 1 / 0（三种分布一致）。

关键端点（测试可证）：

| 性质 | 结论 |
| --- | --- |
| p = 0 | Pa 恰为 1 |
| p = 1 且 c < n | Pa 恰为 0 |
| n 不变、c 增大 | 整条 Pa 曲线不下降 |
| c 不变、n 增大 | 0 < p < 1 处 Pa 下降 |
| N → ∞ | 超几何 Pa 单调逼近二项 |
| 二次退化为 n2=0, c1=c2, r1=c1+1 | 与对应一次方案**逐位相同** |
| 二次 ASN | 始终落在 [n1, n1+n2] |
| ATI | 始终落在 [n, N] |
| AOQL | 不小于曲线上任一点 AOQ |

### 手算核对例子：n = 80, c = 2（二项）

Pa(p) = Σₖ₌₀² C(80,k) pᵏ (1−p)^(80−k)：

- p = 0.01 → Pa ≈ **0.95345**（α ≈ 0.04655）
- p = 0.02 → Pa ≈ 0.78442
- p = 0.05 → Pa ≈ **0.23062**（若 AQL=1%、LTPD=5%，β ≈ 0.2306）
- p = 0 → 1；p = 1 → 0

---

## 3. 反向设计

**一次方案**：对固定 n，令

- `cMin(n)` = 使 Pa(AQL) ≥ 1−α 的最小 c；
- `cMax(n)` = 使 Pa(LTPD) ≤ β 的最大 c。

n 可行 ⇔ `cMin(n) ≤ cMax(n)`，该谓词随 n 单调，故用二分找最小 n；同 n 取最小 c。返回前由统一 OC 引擎复核风险。搜索上限默认 `n ≤ 2000`（硬上限 20000，可用 `n_max` 覆盖）；找不到明确返回 `404 no feasible plan`。测试证明：**n−1 时任何 c 都不再可行**。

**二次方案**：仅允许 n2 = n1（`equal`）或 n2 = 2·n1（`twice`）。利用“固定 c2、c1 时 Pa 与 P2 随 r1 单调”，对每个 (n1, c2, c1) 在 r1 上二分，取满足两风险约束的最小 r1（同时最小化 AQL 处 ASN），整体复杂度 O(n1²·log n1)。默认 n1 上限 200（硬上限 1000，`n1_max` 可调）。测试以内部前缀求值器对整个可行空间做暴力比对，确认全局 ASN 最优。

---

## 4. GB/T 2828.1 检验流转移规则

状态：`normal → tightened → reduced`，加严可 `suspended`（暂停检验，需人工恢复后以加严重新开始）。

- **正常 → 加严**：最近连续不超过 5 批中有 2 批不接收。
- **加严 → 正常**：连续 5 批接收（任一拒收清零连收计数）。
- **正常 → 放宽**：转移得分累计达到 30，**且**流上同时标记 `stable`（生产稳定）与 `approved`（主管同意）。
- **放宽 → 正常**：任一批不接收；或“生产稳定”标记被撤销（立即回正常）。
- **加严 → 暂停**：加严期间累计 5 批不接收。暂停后到达的批次记录为 `not_inspected`；人工 `resume` 后以加严重新计数。
- **转移得分（正常档）**：拒收清零；接收按 GB/T 2828.1 加分表——
  - c = 0：d = 0 加 2；
  - c = 1：d = 0 加 2、d = 1 加 1；
  - c ≥ 2：d ≤ c−2 加 3、d = c−1 加 2、d = c 加 1。
  - 二次抽样：一阶段接收按 c1、二阶段接收按合计 c2 查同一表。

每条批次记录都带：当时严格度 `severity`、所用方案 `plan_id/plan_name`、**生效修订号 `plan_revision`**、判定 `decision/accepted`、转移后 `score`、转移说明 `note`。

---

## 4b. 方案修订历史

方案档不再只存一份“当前值”。`plan_revisions` 为每个方案保存**不可变**的修订行；`plans` 上的数值列只是“最新（生效时刻最晚）修订”的便利副本，供分析 / 查询接口使用。

- **修订**：每次改档 `POST /plans/{id}/revisions` 追加一行，有按追加顺序分配的 `revision_no` 与 `effective_at`。已落库的修订永不修改、永不删除；纠正只能再追加。
- **生效解析**：判某一批时，其所在严格度槽位取 `effective_at <= 检验时刻` 中最大的修订；**生效时刻恰好等于检验时刻按新修订算**，同一生效时刻再按修订号大者优先。因此修订可以生效在将来，也可以补记到过去、插在两个已有修订之间。
- **转移得分延续**：加分表只依赖“该批实际使用的那个修订”的接收数 c。正常检验中途换修订，得分按现有规则自然延续（c=2 的 d=0 加 3，换成 c=1 后同一段继续累计、d=0 改加 2），不引入任何特例。
- **不能判的修订被拒绝**：追加前用候选修订历史对每条绑定流做一次**诊断重放**。样本量改小后某批不合格数超出新样本量等情形会逐批收集为冲突（含流、批号、严格度、修订号、字段与原因），整体回滚（HTTP 422），绝不悄悄当作接收 / 拒收。
- **旧客户端兼容**：`PUT /plans/{id}` 语义改为“追加一个即刻生效的新修订”，不带 `effective_at` 也不报错；它不再覆盖此前的历史。

### 为什么重算放在追加修订的同一事务里（同步）

**结论：追加修订 + 锁定受影响流 + 逐条整流重放，全部在同一个数据库事务内提交。**

- **任何时刻读到的流都是自洽的**：提交前读者只看到旧修订历史与旧派生结果；提交后一次性看到新修订与全部已按它重算好的结果。不存在“新旧修订混着判的半成品”，也不需要“尚未追上”的中间标记、追赶任务状态和差异暂存。
- **差异直接随操作返回**：重放前先读旧 `batch_results`，与新结果逐批比对生成差异（严格度 / 判定 / 得分 / 所用修订的 from→to），追加接口同步返回；一批没变也会列出该流且 `batches` 为空数组，明确表示“没有变化”。
- **排队与死锁**：事务先取方案锁，再取**该方案绑定的每条流**的咨询锁，所有键按全局升序获取；录批事务只持单条流锁。双方因而只会按同一顺序等待，**不可能成环死锁**。同一流的录批与改档串行；不同流仍互不阻塞。新建 / 删除流同样取相关方案锁，避免“刚绑定的流漏算”。
- **代价**：一个档挂着多条很长的流时，一次改档要在一个事务里占住这些流并重放（O(总批数)）。这是用可预期的写放大换取“读到即一致”的硬保证；单流数千批的常规规模下为毫秒级。若将来单档挂数万条长流，可再演进为“快照表 + 有序追赶”的异步方案，但那必须引入可见性屏障与补偿，本版不提前承担这层复杂度。

### 原地升级（旧数据卷直接启动新版本）

启动迁移（幂等）在旧库上：为每个已有方案插入**唯一一个覆盖全部历史的初始修订**（`revision_no=1`、生效于公元 1 年），并给 `batch_results` 补 `plan_revision` 列（默认 1）。因此升级后每条流的当前状态与每批的严格度 / 判定 / 得分**逐字段不变**；`INSERT ... WHERE NOT EXISTS` 保证反复重启既不重复生成修订也不再次改写数据。

---

## 5. 历史可改：为什么选择“整条重放”

**结论：每次增 / 删 / 改都在一个事务里把该流的事件按时间顺序从头完整重放，重算所有派生结果。**

事件表 `stream_events` 是唯一事实源（批次、标记变更、人工恢复），按 `(inspected_at, seq)` 全序排列；`seq` 是每流单调递增的分配序号，因此物理到达顺序与结果无关——补录更早的批、删批、改不合格数都只改变事实集合，重放结果是该集合的纯函数。派生表 `batch_results` 与流当前状态只是缓存，在同一事务内整体替换。

选择整条重放而不是“从被改批起增量 + 检查点”的理由：

1. **正确性边界太多，增量极易漏算。** 一次补录的拒收可能改变十几批之前的转移触发点，进而改变此后每一批“当时用哪个档判定”；转移得分清零、正常↔加严窗口滑动、放宽标记撤销、暂停区间（区间内批次为 not_inspected，恢复后又恢复判定）这些都是跨批、可级联、非局部的。增量重算要为每一类边界维护正确的失效传播，任何一处漏掉都会产生静默的状态分叉。
2. **一致性是硬需求、性能是可优化项。** “逐字段等于从头重放”本身就是正确性判据；直接执行该判据，使存储层不可能偏离它，测试也可以直接拿“独立重放”做逐字段断言。
3. **代价清晰、可控。** 代价是 O(L) 的重放 + O(L) 的派生行重写（L=批次数）。一台普通实例上，数千批 / 次写入是亚毫秒到毫秒级；若将来单流达到数万、数十万批，再引入检查点（按时间分段存 State 快照，定位到受影响批次之前最近的检查点继续），且检查点同样由“重放生成、重放校验”，不改变正确性模型。当前不提前引入这层复杂度。

**并发**：每个写事务先取该流的 PostgreSQL 事务级咨询锁（`pg_advisory_xact_lock`，键由流 id 哈希）。同一流的并发录入 / 补录因此串行化，配合唯一约束 `(stream_id, seq)` 与 `(stream_id, batch_id)`，不会丢批、不会重复计分、不会状态交错；咨询锁键随流不同，**不同流之间互不阻塞**。并发压测（16 协程 × 60 批）后核对：事件数、seq 无缺号无重复、派生结果数=事件数、最终状态与独立重放逐字段一致。

---

## 6. HTTP 接口（前缀 `/api/v1`）

### 方案档

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/plans` | 新建（同时生成覆盖全部历史的初始修订；body 见下） |
| PUT | `/plans/{id}` | **旧客户端兼容**：追加即刻生效的新修订（不再原地覆盖历史） |
| DELETE | `/plans/{id}` | 删除（被流引用时 409；修订历史级联删除） |
| GET | `/plans/{id}` / `/plans/by-name/{name}` / `/plans` | 查询（当前值 = 生效最晚的修订） |
| POST | `/plans/{id}/revisions` | **追加修订**（可带 `effective_at`，缺省=现在；同步重算受影响流并返回差异） |
| GET | `/plans/{id}/revisions` | 修订历史（按生效时刻、修订号排序） |
| GET | `/plans/{id}/revisions/{number}` | 按修订号取一个修订（不存在 404，号非数字 400） |
| POST | `/plans/{id}/evaluate` | 给定 `p[]` 算 Pa（二次带 ASN；带 N 带 AOQ/ATI） |
| POST | `/plans/{id}/curve` | OC 等曲线，`p_min/p_max/points`（points ≤ 500） |
| POST | `/plans/{id}/risks` | `{aql, ltpd}` → α、β、Pa(AQL)、Pa(LTPD) |
| POST | `/plans/{id}/aoql` | AOQL 及对应 p（需带 N） |

一次档 body：

```json
{ "name": "n80c2", "kind": "single",
  "single": { "n": 80, "c": 2, "n_lot": 1000, "distribution": "hypergeometric" } }
```

二次档 body（约束 `c1 < r1 ≤ c2+1`，`r1` 可取 `n1+1` 表示一阶段不直接拒收）：

```json
{ "name": "dbl", "kind": "double",
  "double": { "n1": 50, "c1": 2, "r1": 5, "n2": 50, "c2": 6 } }
```

追加修订 body（与新建同形，多一个可选 `effective_at`；`name`/`kind` 可省略，分别沿用当前名与不可变的 kind）：

```json
{ "name": "n80c1", "kind": "single",
  "single": { "n": 80, "c": 1 },
  "effective_at": "2026-03-15T00:00:00Z" }
```

响应里 `streams[]` 是受影响流的差异；每批的变化形如：

```json
{ "stream_id": "...", "name": "...", "batches": [
  { "batch_id": "B7", "lot_no": "B7", "inspected_at": "...",
    "severity": { "from": "normal", "to": "tightened" },
    "decision": { "from": "accepted", "to": "rejected" },
    "score":    { "from": 6, "to": 0 },
    "plan":     { "plan_id": "...", "from_revision": 1, "to_revision": 2 } } ] }
```

没有任何批变化时 `batches` 为 `[]`。修订使已录批次无法判定时返回 **422**：

```json
{ "error": "revision cannot judge one or more recorded batches",
  "revision_no": 3,
  "conflicts": [ { "stream_id": "...", "batch_id": "B9", "lot_no": "B9",
                   "severity": "normal", "revision_no": 3,
                   "field": "d1",
                   "message": "defect count must be within [0, first sample size]" } ] }
```

### 手算例子：正常档 n=80、c=2 → c=1

正常档在时刻 T 从 `n=80,c=2` 改为 `n=80,c=1`（生效时刻=T）。转移得分按 GB/T 2828.1 加分表（c≥2：d≤c−2 加 3、d=c−1 加 2、d=c 加 1；c=1：d=0 加 2、d=1 加 1；拒收清零）：

| 批 | 检验时刻 | d | 生效修订 | 判定 | 得分 |
| --- | --- | --- | --- | --- | --- |
| a | T−4h | 0 | 1 (c=2) | 接收（d≤c−2） | 3 |
| b | T−3h | 1 | 1 | 接收（d=c−1） | 5 |
| c | T−2h | 2 | 1 | **接收（d=c）** | 6 |
| d | T−1h | 0 | 1 | 接收 | 9 |
| e | T+1h | 0 | 2 (c=1) | 接收 | 11 |
| f | T+2h | 1 | 2 | 接收（d=1 加 1） | 12 |
| g | T+3h | 2 | 2 | **拒收（c=1 时 d=2）** | 0 |
| h | T+4h | 0 | 2 | 接收 | 2 |

改档前 g 在 c=2 下本为接收；追加修订后它改为不接收，且流上的严格度 / 得分随之级联重算。该例子在 `internal/store/revision_integration_test.go::TestHandComputedC2ToC1` 中逐批断言。

### 反向设计

- `POST /design/single`：`{aql, alpha, ltpd, beta, n_lot?, distribution?, n_max?}`
- `POST /design/double`：同上加 `n2_mode: "equal"|"twice"`、`n1_max`
- 超上限无可行方案 → `404` 且错误信息明确说明。

### 检验流

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/streams` | 绑定三个档 `{name, normal_plan_id, tightened_plan_id, reduced_plan_id}` |
| GET | `/streams` / `/streams/{id}` | 列表 / 当前状态+逐批记录 |
| DELETE | `/streams/{id}` | 删除（级联事件与结果） |
| POST | `/streams/{id}/batches` | 录入 / 补录（`inspected_at` 可早于历史） |
| PUT | `/streams/{id}/batches/{batch_id}` | 修正批号 / 时间 / 不合格数（保留原 seq） |
| DELETE | `/streams/{id}/batches/{batch_id}` | 删除一批 |
| POST | `/streams/{id}/flags` | `{flag:"stable"|"approved", value, at?}` |
| POST | `/streams/{id}/resume` | 暂停后人工恢复 |

批次 body：`{"batch_id"?: "...", "lot_no": "...", "inspected_at"?: RFC3339, "d1": 3, "d2"?: 2}`。
二次抽样当 `c1 < d1 < r1` 时必须给 `d2`。

### 校验与错误

`n<1`、`c<0`、`c≥n`、`p∉[0,1]`、`N<n`、`AQL≥LTPD`、`α/β∉(0,1)`、不合格数超过样本量等一律 `400`；修订时间格式非法报 `effective_at`，修订号不存在返回 `404`、号非数字返回 `400`（字段 `revision_no`）；响应形如：

```json
{ "error": "validation error",
  "fields": [ { "field": "single.c", "message": "must satisfy c < n" } ] }
```

---

## 7. 代码结构（按职责拆包 / 拆文件）

```
cmd/server/main.go              引导：连接、迁移、启动 HTTP
internal/
  plan/                         方案档领域模型、字段级校验、修订模型(plan.go, revision.go)
  dist/                         对数域组合数；二项/泊松/超几何(含条件二阶段)模型
    combinatorics.go  model.go
  oc/                           Pa/ASN(prob.go)、AOQ/AOQL/ATI(rectify.go)、风险与曲线(curve.go)
  design/                       一次(single.go)、二次(double.go)两点方案搜索
  inspection/                   类型与修订槽(types.go)、判定(decision.go)、转移得分(score.go)、
                                状态机(state_machine.go)、历史/诊断重放(replay.go)
  store/                        PG 连接/迁移/锁(store.go)、方案与修订仓储(plan_repo.go,
                                revision_repo.go)、流与事件(stream_repo.go)、批变更(mutations.go)、
                                追加修订+同步重算+差异(revision_mutations.go)
  httpapi/                      Echo 路由与 plan/revision/analyze/design/stream 五组 handler
```

状态机与重放分属 `state_machine.go` 与 `replay.go`；修订相关的仓储与事务单独放在
`revision_repo.go` / `revision_mutations.go`，handler 在 `revision_handlers.go`，未挤进既有文件。

## 8. 测试

- 纯逻辑：`make test-unit`（无需数据库）——端点精确性、c/n 单调性、超几何→二项收敛、二次退化逐位一致、ASN/ATI 区间、AOQL 上界、n=80/c=2 手算值、设计最优性（含 n−1 不可行、二次全空间暴力比对）、每种转移与暂停的构造序列、300 轮随机打乱补录 / 删改与从头重放逐字段比对，以及修订生效时刻边界（恰相等按新修订）、补记插在中间、换修订时得分延续、无法判定批的冲突收集。
- 集成：设置 `PG_TEST_DSN` 后 `make test-int`（带 `-race`）——PG16 上的 CRUD、转移持久化、补录 / 修正 / 删除重算、暂停恢复、二次抽样判定；修订专项：n=80 c=2→c=1 手算逐批断言、**随机交错追加修订（含补到过去 / 插在中间）+ 补录 + 删批 + 改数后与从头重放一致且差异完全吻合**、多客户端同时追加修订 + 同时录批不丢修订 / 不丢批 / 不重复计分且每批修订号正确、样本量缩小的修订被 422 拒绝并逐批指出、旧 PUT 即刻生效且不改写历史、**旧表结构灌数后原地升级逐字段比对且反复重启不重复迁移**、HTTP 端到端；并发压测（同流 16×60 不丢批 / 不重号，跨流不阻塞）。
