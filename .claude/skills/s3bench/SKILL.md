---
name: s3bench
description: Use when benchmarking or load-testing an S3-compatible object store (SeaweedFS, MinIO, Ceph RGW, or any S3 gateway) — measuring write/read throughput, latency percentiles (p50/p95/p99), or cleaning up benchmark data with the s3bench-lite tool in this repo.
---

# s3bench

## Overview

`s3bench-lite` 是本仓库的单二进制 S3 压测工具(Go, AWS SDK v2)。配置走 `.env` 文件或环境变量,输出实时分位延迟、阶段汇总,以及可选的 JSON 结果。每轮测试用 `RUN_ID` 隔离成一个**批次**(key 前缀 `KEY_PREFIX/RUN_ID/`),写、读、删都围绕批次进行。

## When to Use

- 压测自建/兼容 S3 网关的吞吐与延迟(写、读、HEAD/RANGE/LIST)
- 对比不同并发、对象大小、负载类型下的性能
- 测后按批次清理测试数据

**不适用**:生产数据操作、非 S3 协议存储。

## 构建

需要 Go 1.24+。仓库根目录已可能有现成二进制;否则编译:

```bash
# 本机运行
go build -o s3bench .
# 交叉编译到 Linux x86-64(拷到测试机)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o s3bench-linux-amd64 .
```

## 运行

真实环境变量优先级 > `.env`,可临时覆盖任意配置项:

```bash
S3_ENDPOINT=http://10.0.0.1:8333 S3_BUCKET=testec \
  MODE=write WRITE_CONCURRENCY=64 OBJECT_SIZE=4MB TOTAL_SIZE=10GB \
  RESULT_FILE=result.json ./s3bench
```

`-mode` 命令行参数会覆盖 `MODE`。**用 AI 解析结果时务必设 `RESULT_FILE`**,然后读该 JSON,而不要去解析 stdout。

## 配置速查(env / .env)

| 变量 | 说明 | 默认 |
|---|---|---|
| `S3_ENDPOINT` | 网关地址 | `http://127.0.0.1:8333` |
| `S3_BUCKET` | 桶名 | `testec` |
| `S3_ACCESS_KEY`/`S3_SECRET_KEY` | AK/SK | `321`/`321` |
| `S3_USE_PATH_STYLE` | path-style 寻址(自建多为 true) | `true` |
| `S3_SKIP_TLS_VERIFY` | 跳过证书校验(仅内网) | `true` |
| `MODE` | `write`/`read`/`both`/`clean` | `both` |
| `RUN_ID` | 批次 ID;空则自动时间戳 | 自动 |
| `KEY_PREFIX` | key 基础前缀 → `KEY_PREFIX/RUN_ID/` | `s3bench/` |
| `WRITE_CONCURRENCY` / `READ_CONCURRENCY` | 写/读并发 | `16` |
| `OBJECT_SIZE` | 单对象大小(B/KB/MB/GB) | `1MB` |
| `OBJECT_SIZE_PATTERN` | 混合大小,如 `4KB:70%,1MB:30%` | 空 |
| `TOTAL_SIZE` | 总上传量;对象数=总量/平均大小 | `1GB` |
| `READ_OPS` | `get,head,range,list` 可混合 | `get` |
| `READ_PATTERN` | `random`(随机有放回)/`sequential`(顺序全覆盖) | `random` |
| `READ_COUNT` | 读总数;`0`=读「对象数」次 | `0` |
| `READ_DURATION` | 按时长压读(如 `30s`),>0 优先 | `0` |
| `WARMUP_DURATION` | 读预热,不计入统计 | `0` |
| `RETRY` | 工具侧重试(SDK 内部重试已关) | `0` |
| `CLEAN_AFTER` | 压测后删本批次 | `false` |
| `RESULT_FILE` | JSON 结果路径(机读首选) | 空 |

完整说明见仓库 `README.md` 与 `.env.example`。

## 批次生命周期(RUN_ID)

同一个 `RUN_ID` 串起写/读/删:

```bash
RUN_ID=batchA MODE=write ./s3bench                         # 写一批
RUN_ID=batchA MODE=read  READ_PATTERN=sequential ./s3bench # 顺序读这批,保证每个对象都读到
RUN_ID=batchA MODE=clean ./s3bench                         # 按前缀列举删干净这批
```

- `read` 单独模式按算法**重建** key,需 `RUN_ID` + 对象大小配置 + `TOTAL_SIZE` 与写入时一致。
- `clean` 模式按前缀**实际列举**删除,只需 `RUN_ID`(和 `KEY_PREFIX`)一致即可。

## 输出解读

- 实时行:`win`=本窗口吞吐/分位延迟,`cum`=累计吞吐,`err`=累计错误。
- 阶段汇总:成功/失败、错误率、总传输量、吞吐(MiB/s + op/s)、`min/avg/p50/p90/p95/p99/p99.9/max`、错误分类。
- JSON(`RESULT_FILE`):含运行环境、配置、各阶段 `latency_ms`/`throughput_bps`/`ops_per_second`/`error_classes`,适合多轮对比。

## Common Mistakes

- **想测「每个对象都读到」却用了默认 `random`**:默认随机有放回约 37% 对象漏读。要全覆盖用 `READ_PATTERN=sequential`。
- **`read`/`clean` 时 `RUN_ID` 不一致**:对不上批次,读不到/删不到。读还需大小与总量配置一致。
- **用 stdout 解析性能数字**:改设 `RESULT_FILE` 读 JSON。
- **缓存导致读延迟偏低**:被测网关有缓存时,小批次随机读会反复命中;增大对象数或用 `sequential`。
- **对生产桶跑 `clean`/`CLEAN_AFTER`**:会删该前缀下对象,先确认 `S3_BUCKET`/`KEY_PREFIX`/`RUN_ID`。
