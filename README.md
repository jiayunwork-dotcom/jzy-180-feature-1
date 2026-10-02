# 接收抽样后端服务（OC 曲线 / 风险 / 方案设计 / GB/T 2828.1 检验流）

Go 1.23 + Echo + PostgreSQL 16 的纯 HTTP 后端，覆盖：

- **方案档管理**：一次 / 二次抽样档的新建、修改、删除、按名查询。
- **统计计算**：接收概率 Pa、二次抽样 ASN、带批量 N 时的 AOQ / AOQL / ATI、生产方风险 α 与使用方风险 β、OC 曲线。
- **反向设计**：按 (AQL, α, LTPD, β) 搜索最小样本量一次方案；二次方案限定 n2=n1 或 n2=2·n1，在可行方案中取 AQL 处 ASN 最小者。
- **检验流**：一条流绑定正常 / 加严 / 放宽三个档，逐批录入，按 GB/T 2828.1 转移规则推进严格度；补录、删改后逐字段与“从头重放”一致。

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

每条批次记录都带：当时严格度 `severity`、所用方案 `plan_id/plan_name`、判定 `decision/accepted`、转移后 `score`、转移说明 `note`。

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
| POST | `/plans` | 新建（body 见下） |
| PUT | `/plans/{id}` | 修改 |
| DELETE | `/plans/{id}` | 删除（被流引用时 409） |
| GET | `/plans/{id}` / `/plans/by-name/{name}` / `/plans` | 查询 |
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

`n<1`、`c<0`、`c≥n`、`p∉[0,1]`、`N<n`、`AQL≥LTPD`、`α/β∉(0,1)`、不合格数超过样本量等一律 `400`，响应形如：

```json
{ "error": "validation error",
  "fields": [ { "field": "single.c", "message": "must satisfy c < n" } ] }
```

---

## 7. 代码结构（按职责拆包 / 拆文件）

```
cmd/server/main.go              引导：连接、迁移、启动 HTTP
internal/
  plan/                         方案档领域模型与字段级校验
  dist/                         对数域组合数；二项/泊松/超几何(含条件二阶段)模型
    combinatorics.go  model.go
  oc/                           Pa/ASN(prob.go)、AOQ/AOQL/ATI(rectify.go)、风险与曲线(curve.go)
  design/                       一次(single.go)、二次(double.go)两点方案搜索
  inspection/                   类型(types.go)、判定(decision.go)、转移得分(score.go)、
                                状态机(state_machine.go)、历史重放(replay.go)
  store/                        PG 连接/迁移/锁(store.go)、方案仓储、流与事件、变更重算
  httpapi/                      Echo 路由与 plan/analyze/design/stream 四组 handler
```

状态机与重放逻辑分属 `state_machine.go` 与 `replay.go`，未挤进同一文件。

## 8. 测试

- 纯逻辑：`make test-unit`（无需数据库）——端点精确性、c/n 单调性、超几何→二项收敛、二次退化逐位一致、ASN/ATI 区间、AOQL 上界、n=80/c=2 手算值、设计最优性（含 n−1 不可行、二次全空间暴力比对）、每种转移与暂停的构造序列、300 轮随机打乱补录 / 删改与从头重放逐字段比对。
- 集成：设置 `PG_TEST_DSN` 后 `make test-int`（带 `-race`）——PG16 上的 CRUD、转移持久化、补录 / 修正 / 删除重算、暂停恢复、二次抽样判定，以及并发压测（同流 16×60 并发核对不丢批 / 不重号 / 状态一致，跨流不阻塞）、HTTP 端到端。
