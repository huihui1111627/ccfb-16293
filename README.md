# woodagg — 自适应并行分组聚合执行器

一个零第三方依赖的 Go 示例：木材市场检尺/成交明细在程序内按固定种子生成，执行器根据每批抽样的基数估计，在「线程本地预聚合」和「按分区直接透传」之间自适应切换。附带内嵌 Web 监控页。

## 启动页面

```bash
go run ./cmd/woodagg serve -addr :8090
# 打开 http://localhost:8090
```

页面支持实时调整：

- 工作线程数（1–32）
- 每线程本地哈希表组数上限
- 基础分区数
- 高唯一键比例阈值、批大小
- 自动扩容分区开关

参数在下一批/下一个安全边界生效；分区数变化时会在批次边界重 shard 已有中间状态。

## 命令行校验

```bash
# 单个场景，三种执行策略都与单线程全量朴素实现比较
go run ./cmd/woodagg verify --scenario dozens

# 所有内置场景（包含一千万、三千万组；内存和耗时较高）
go run ./cmd/woodagg verify --scenario all --modes adaptive

# 自定义小规模矩阵
go run ./cmd/woodagg verify \
  --scenario million --groups 2000 --rows 100000 \
  --threads 4 --local 2048 --partitions 8
```

校验内容包括组数、每个键、`count/sum/min/max/avg/distinct/topN` 的结果。浮点求和使用相对误差容限 `1e-9`，其余结果要求精确一致。

## 三种策略基准

```bash
go run ./cmd/woodagg bench --scenario dozens
go run ./cmd/woodagg bench --scenario ten-million --threads 12 --partitions 32
```

依次运行：

1. `adaptive`：抽样估计 + 低基数预聚合/高基数透传 + 少组单线程 + 有序流；
2. `always_pre`：每批始终写线程本地哈希表，再合并中间状态；
3. `never_pre`：不做本地预聚合，逐行按哈希发到分区表。

输出耗时、rows/s、峰值采样 HeapAlloc、累计分配、GC 后存活堆和实际分区数。页面上的「统计三种策略耗时/内存」按钮会运行同一基准并展示表格。

## 支持的聚合

- `COUNT`
- `SUM`
- `MIN` / `MAX`
- `AVG`：两阶段之间传 `Sum + Count`，最终才相除
- `DISTINCT`：两阶段之间传精确集合；单值时使用快速路径，第二个不同值出现后才建 map
- `TOP N`：支持组内最大 N 个值的可合并堆；报告另外给出最终 `SUM(材积)` 前 10 名分组

## 分组键规则

- 最多 4 列，可为空；`NULL` 归为同一组。
- 浮点键先规范化：
  - `-0.0` 与 `+0.0` 都变为 bits `0`；
  - 所有 NaN payload 都变为同一个 quiet NaN bits。
- 字符串列只复制 `string` header，不复制底层字节：
  - FNV 哈希直接索引 `s[i]` 读原始字节；
  - 比较使用 Go 字符串字节序；
  - 有单元测试用 `unsafe.StringData` 断言存入引擎前后的底层指针不变。

## 自适应执行规则

- 每批系统抽样最多 16,384 行，统计唯一值、singleton/doubleton，并用 Chao1 估计批内基数。
- 样本唯一率超过阈值（默认 `0.72`）或估计组数接近本地表上限时，该批直接按 `hash(key) % partitions` 透传。
- 否则在线程本地表预聚合；本地表达到上限后把中间状态 flush 到分区，再继续。
- 首批估计只有不超过 256 组时，改为单线程一张表汇总，避免线程和 shuffle 开销。
- 全局估计组数很多时，实际分区数自动增加到约每 10 万组一个分区，最多 512。
- 自适应模式先用有界相邻键探针判断排序；若输入确实全局有序，则进入单线程有序流，组切换即输出，不建哈希表。完整扫描仍逐对检查顺序；若发现探针之后出现逆序，会确定性重建数据源并回退哈希路径。

## 代码结构

```
cmd/woodagg/          CLI：serve / verify / bench
internal/model/       可比较分组键、浮点规范化、原始字节哈希/比较
internal/agg/         聚合规格、可合并中间状态、distinct/topN
internal/data/        固定种子场景与分批生成器
internal/engine/      自适应执行器、线程/批次/分区/内存监控
internal/check/       全量物化的单线程朴素 oracle 与逐组比较
internal/server/      JSON API 与静态页面服务
web/static/           零依赖原生 JS/CSS/Canvas 监控页
```

## 主要 HTTP API

- `GET /api/state`：完整实时快照（线程、批次、分区、内存曲线、最近报告、基准/校验结果）
- `POST /api/config`：热更新线程数、本地表上限、分区数等；JSON 字段可部分提交，未传字段保持原值
- `POST /api/run`：`{scenario_id, mode}`
- `POST /api/benchmark`：`{scenario_id}`，顺序运行三种策略
- `POST /api/verify`：`{scenario_id}`，运行朴素对照并逐组校验

## 已执行的验证

开发机：darwin/arm64、14 CPU、48 GiB、Go 1.26.1。

- `go test -race ./...` 通过。
- 单元矩阵覆盖木材多列键、高基数买家、有序键、长字符串，每种均运行 adaptive / always-pre / never-pre，并与朴素 map 逐组比较。
- 额外覆盖 TopN 合并、分区 reshard、`-0/+0/NaN` 归组、长字符串底层指针不复制。
- CLI 实测通过的规模：
  - `dozens`：41 组，adaptive 降为单线程，三种策略 PASS；
  - `hundreds`：261 组，三种策略 PASS；
  - `ten-thousand`：10,005 组，三种策略 PASS；
  - `million`：1,000,002 组，三种策略 PASS；
  - `sorted`：200,001 组，adaptive 走有序流并 PASS；
  - `long-string`：100,001 组，三种策略 PASS；
  - `ten-million`：10,000,002 组，三种策略 PASS；
  - `thirty-million`：30,000,002 组，adaptive 与全量朴素实现逐组一致，0 mismatch，最大浮点相对误差约 `6.0e-16`。

一组一千万买家键、12,000,000 行的实测基准（12 workers，基础 32 分区，自动扩到 120）：

| 策略 | 耗时 | 吞吐 | 峰值 HeapAlloc | 累计分配增量 |
|---|---:|---:|---:|---:|
| adaptive | 6.71 s | 1.79M rows/s | 5.5 GB | 8.9 GB |
| always pre-agg | 6.44 s | 1.86M rows/s | 6.1 GB | 11.7 GB |
| never pre-agg | 5.55 s | 2.16M rows/s | 6.1 GB | 8.9 GB |

这组数据几乎每个买家都是新组，所以 adaptive 正确选择透传；always-pre 额外产生本地表写入和中间状态分配。低基数字符串/树种场景中 adaptive 会降为单线程汇总，页面可看到 `single` 决策和很高的命中率。不同机器/负载下绝对数值会变化，重点看同机三种策略的相对差异。
