# s3bench

基于 AWS SDK v2 的 S3 集群性能压测工具。构建后是一个独立二进制，支持自建 S3 endpoint、path-style、写入/读取/清理、实时分位延迟和 JSON 结果输出。

## 编译

需要 **Go 1.24+**。

```bash
cd s3bench
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o s3bench .
```

如果测试机是 Linux ARM64：

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o s3bench-linux-arm64 .
```

首次构建会下载 Go module 依赖。得到 Linux 二进制后，可直接拷贝到 Linux 测试机运行。

## 使用

配置全部读 `.env`（与二进制同目录，或用 `-env` 指定）：

```bash
./s3bench                  # 读 .env，默认 both（先写后读）
./s3bench -mode=write      # 只压写
./s3bench -mode=read       # 只压读（读取此前写入的对象）
./s3bench -mode=clean      # 清理当前 RUN_ID 对应对象
./s3bench -env=/etc/s3.env # 指定配置文件
```

真实环境变量优先级高于 `.env`，可临时覆盖：

```bash
WRITE_CONCURRENCY=64 OBJECT_SIZE=4MB TOTAL_SIZE=10GB ./s3bench -mode=write
```

## 配置项（.env）

| 变量 | 说明 | 默认 |
|------|------|------|
| `S3_ENDPOINT` | S3 网关地址 | `http://127.0.0.1:8333` |
| `S3_REGION` | 区域（签名用，自建集群随意） | `us-east-1` |
| `S3_BUCKET` | 桶名 | `testec` |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | AK / SK | `321` / `321` |
| `S3_USE_PATH_STYLE` | path-style 寻址 | `true` |
| `S3_CREATE_BUCKET` | 启动时幂等建桶 | `true` |
| `S3_SKIP_TLS_VERIFY` | 跳过自签名证书校验 | `true` |
| `MODE` | `write` / `read` / `both` / `clean` | `both` |
| `RUN_ID` | 本轮测试 ID；为空自动生成，并拼到 key 前缀后 | 自动时间戳 |
| `KEY_PREFIX` | 对象 key 基础前缀；实际为 `KEY_PREFIX/RUN_ID/` | `s3bench/` |
| `WRITE_CONCURRENCY` | 写并发 | `16` |
| `OBJECT_SIZE` | 单对象大小（B/KB/MB/GB，1024 进制） | `1MB` |
| `OBJECT_SIZE_PATTERN` | 混合对象大小，如 `4KB:70%,1MB:30%` | 空 |
| `TOTAL_SIZE` | 总上传量；对象数按平均对象大小估算 | `1GB` |
| `READ_CONCURRENCY` | 读并发 | `16` |
| `READ_OPS` | 读负载类型：`get,head,range,list` 可混合 | `get` |
| `READ_PATTERN` | `random`=随机有放回；`sequential`=顺序遍历整批 key，保证全覆盖且均匀 | `random` |
| `RANGE_SIZE` | `range` 读取大小；`0` 表示读完整对象范围 | `0` |
| `READ_COUNT` | 读请求总数；`0` = 读「对象数」次（对象**随机有放回**抽取，非逐个遍历） | `0` |
| `READ_DURATION` | 按时长压读（如 `30s`），>0 时优先 | `0` |
| `WARMUP_DURATION` | 读预热时长，不计入正式统计 | `0` |
| `REPORT_INTERVAL` | 实时指标打印间隔 | `2s` |
| `HTTP_TIMEOUT` | 单请求超时 | `60s` |
| `RETRY` | 工具侧重试次数；SDK 内部重试关闭 | `0` |
| `CLEAN_AFTER` | 压测完成后删除本轮对象 | `false` |
| `RESULT_FILE` | JSON 结果输出路径；为空不写文件 | 空 |

## 输出说明

**实时（每 `REPORT_INTERVAL` 一行）**：

```
[WRITE     6s] 1234/1024(120%) | win: 210MiB/s 210 op/s p50=4.10ms p95=9.80ms p99=21.0ms | cum: 205MiB/s 205 op/s | err=0
```

- `win`：本窗口（区间）吞吐与延迟分位
- `cum`：从开始至今的累计吞吐
- `p50/p95/p99`：窗口内分位延迟

**阶段汇总**：成功/失败数、错误率、总传输量、吞吐（MiB/s + op/s）、延迟 `min / avg / p50 / p90 / p95 / p99 / p99.9 / max`，以及错误分类和错误样例。

**JSON 结果**：设置 `RESULT_FILE=result.json` 后，会写入运行环境、配置、各阶段吞吐、分位延迟、错误分类等结构化结果，便于多轮对比。

## AI 调用（Claude Code Skill）

仓库自带 `.claude/skills/s3bench/SKILL.md`。用 Claude Code 打开本仓库时,AI 会自动识别该 skill,可直接让它"用 s3bench 压测某个 S3 网关 / 清理某批次",由 AI 设置环境变量并运行、解析 `RESULT_FILE` 结果。

## 实现要点

- **AWS SDK v2 客户端**，使用成熟 S3 协议实现，支持自建 endpoint 和 path-style，减少自维护签名/编码细节。
- **每 worker 独立延迟直方图**（指数桶，内存固定 ~440 桶），周期合并，避免高并发锁竞争影响测量精度。
- GET / RANGE GET 会读完整 body 后丢弃，确保延迟覆盖完整传输并复用连接。
- 写入复用预生成随机数据，避免数据生成开销混入吞吐。
- SDK 内部 retry 被关闭，`RETRY` 由工具侧控制并体现在错误率/延迟中。

## 注意

- 延迟分位来自直方图桶，相对精度约 5%；`min`/`max` 为精确值。
- **读取模式由 `READ_PATTERN` 控制**：
  - `random`（默认）：每次从对象集合随机挑一个 key（随机有放回）。`READ_COUNT=0`（默认读「对象数」次）时约 63% 对象被覆盖，部分对象读多次、部分漏读；贴近真实负载，但被测网关有缓存时热点会反复命中导致延迟偏低。
  - `sequential`：按序号顺序遍历（`keys[i%N]`）。`READ_COUNT=0` 时恰好**每个对象读且仅读一遍**（100% 覆盖）；`READ_COUNT` 为对象数整数倍时每个对象被读相同次数（均匀）。想确保整批 key 都读到，用这个。
- **`both` 模式只读取写入成功的对象**。
- **`read` 单独模式**按写入算法（`KEY_PREFIX/RUN_ID/` + 12 位序号）重建全部 `ObjectCount` 个 key，须保证 `RUN_ID`、对象大小配置（含 `OBJECT_SIZE_PATTERN`）、`TOTAL_SIZE` 与写入时一致，否则 key 对不上。
- **`clean` 模式**按前缀 `KEY_PREFIX/RUN_ID/` **实际列举**对象后删除（翻页 LIST），因此删除一个批次**只需 `RUN_ID`（和 `KEY_PREFIX`）一致**即可删干净，不依赖对象大小/总量配置。
- `LIST` 操作只取首页（最多 1000 个 key），不翻页；用于测 LIST 延迟，返回计数封顶 1000。
- `S3_SKIP_TLS_VERIFY` 默认 `true`（跳过证书校验），仅适合自建/内网集群。**生产或公网环境请设为 `false`**。
