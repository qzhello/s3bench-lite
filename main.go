// s3bench —— 一个基于 AWS SDK v2 的 S3 集群性能压测工具。
//
// 设计目标：
//   - 构建后单二进制运行：适合拷贝到 Linux 测试机执行。
//   - 写入可控：并发数、单对象大小、总上传量。
//   - 查询全面：GET/HEAD/RANGE/LIST，给出 min/avg/p50/p90/p95/p99/p999/max。
//   - 实时指标：按可配置间隔（默认 2s）打印窗口吞吐 + 分位延迟 + 错误。
//   - 配置专门读 .env 文件（也允许少量命令行覆盖），支持 JSON 结果和清理。
//
// 用法：
//
//	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o s3bench .
//	./s3bench                 # 读同目录 .env，默认 both（先写后读）
//	./s3bench -mode=write     # 只压写
//	./s3bench -mode=read      # 只压读（读取之前写入的对象）
//	./s3bench -mode=clean     # 清理当前 RUN_ID 对应对象
//	./s3bench -env=/path/.env # 指定配置文件
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------

type Config struct {
	Endpoint      string
	Region        string
	Bucket        string
	AccessKey     string
	SecretKey     string
	PathStyle     bool
	CreateBucket  bool
	SkipTLSVerify bool

	Mode string // write | read | both

	WriteConcurrency int
	ObjectSize       int64
	ObjectSizes      []objectSizeWeight
	TotalSize        int64
	ObjectCount      int64 // 由 TotalSize/平均对象大小推导

	ReadConcurrency int
	ReadCount       int64         // 读取请求总数；0 = 每个对象读一遍
	ReadDuration    time.Duration // >0 时按时间持续压读（优先于 ReadCount）
	ReadOps         []readOp
	RangeSize       int64
	ReadPattern     string // random | sequential：sequential 顺序遍历整批 key，保证全覆盖

	ReportInterval time.Duration
	HTTPTimeout    time.Duration
	KeyPrefix      string
	RunID          string
	WarmupDuration time.Duration
	CleanAfter     bool
	ResultFile     string
	Retry          int
}

type objectSizeWeight struct {
	Size   int64 `json:"size"`
	Weight int   `json:"weight"`
}

type readOp string

const (
	opGET   readOp = "GET"
	opHEAD  readOp = "HEAD"
	opRANGE readOp = "RANGE"
	opLIST  readOp = "LIST"
)

const (
	patternRandom     = "random"     // 随机有放回：贴近真实负载
	patternSequential = "sequential" // 顺序遍历：保证整批 key 全覆盖、均匀
)

func loadConfig(envPath string) (*Config, error) {
	env := parseEnvFile(envPath) // 文件不存在则返回空 map，不报错

	get := func(key, def string) string {
		if v, ok := os.LookupEnv(key); ok && v != "" { // 真实环境变量优先
			return v
		}
		if v, ok := env[key]; ok && v != "" {
			return v
		}
		return def
	}

	c := &Config{
		Endpoint:      get("S3_ENDPOINT", "http://127.0.0.1:8333"),
		Region:        get("S3_REGION", "us-east-1"),
		Bucket:        get("S3_BUCKET", "testec"),
		AccessKey:     get("S3_ACCESS_KEY", "321"),
		SecretKey:     get("S3_SECRET_KEY", "321"),
		PathStyle:     parseBool(get("S3_USE_PATH_STYLE", "true")),
		CreateBucket:  parseBool(get("S3_CREATE_BUCKET", "true")),
		SkipTLSVerify: parseBool(get("S3_SKIP_TLS_VERIFY", "true")),
		Mode:          strings.ToLower(get("MODE", "both")),
		RunID:         get("RUN_ID", ""),
		ResultFile:    get("RESULT_FILE", ""),
		CleanAfter:    parseBool(get("CLEAN_AFTER", "false")),
	}
	if c.RunID == "" {
		c.RunID = defaultRunID()
	}
	c.KeyPrefix = scopedKeyPrefix(get("KEY_PREFIX", "s3bench/"), c.RunID)

	var err error
	if c.WriteConcurrency, err = atoiDef(get("WRITE_CONCURRENCY", "16"), 16); err != nil {
		return nil, fmt.Errorf("WRITE_CONCURRENCY: %w", err)
	}
	if c.ReadConcurrency, err = atoiDef(get("READ_CONCURRENCY", "16"), 16); err != nil {
		return nil, fmt.Errorf("READ_CONCURRENCY: %w", err)
	}
	if c.ObjectSize, err = parseSize(get("OBJECT_SIZE", "1MB")); err != nil {
		return nil, fmt.Errorf("OBJECT_SIZE: %w", err)
	}
	if c.ObjectSizes, err = parseObjectSizePattern(get("OBJECT_SIZE_PATTERN", ""), c.ObjectSize); err != nil {
		return nil, fmt.Errorf("OBJECT_SIZE_PATTERN: %w", err)
	}
	if c.TotalSize, err = parseSize(get("TOTAL_SIZE", "1GB")); err != nil {
		return nil, fmt.Errorf("TOTAL_SIZE: %w", err)
	}
	if c.ReadOps, err = parseReadOps(get("READ_OPS", "get")); err != nil {
		return nil, fmt.Errorf("READ_OPS: %w", err)
	}
	if c.RangeSize, err = parseSizeDef(get("RANGE_SIZE", "0"), 0); err != nil {
		return nil, fmt.Errorf("RANGE_SIZE: %w", err)
	}
	if c.ReadPattern, err = parseReadPattern(get("READ_PATTERN", "random")); err != nil {
		return nil, fmt.Errorf("READ_PATTERN: %w", err)
	}
	if c.ReadCount, err = atoi64Def(get("READ_COUNT", "0"), 0); err != nil {
		return nil, fmt.Errorf("READ_COUNT: %w", err)
	}
	if c.ReadDuration, err = parseDurDef(get("READ_DURATION", "0"), 0); err != nil {
		return nil, fmt.Errorf("READ_DURATION: %w", err)
	}
	if c.WarmupDuration, err = parseDurDef(get("WARMUP_DURATION", "0"), 0); err != nil {
		return nil, fmt.Errorf("WARMUP_DURATION: %w", err)
	}
	if c.ReportInterval, err = parseDurDef(get("REPORT_INTERVAL", "2s"), 2*time.Second); err != nil {
		return nil, fmt.Errorf("REPORT_INTERVAL: %w", err)
	}
	if c.HTTPTimeout, err = parseDurDef(get("HTTP_TIMEOUT", "60s"), 60*time.Second); err != nil {
		return nil, fmt.Errorf("HTTP_TIMEOUT: %w", err)
	}
	if c.Retry, err = atoiDef(get("RETRY", "0"), 0); err != nil {
		return nil, fmt.Errorf("RETRY: %w", err)
	}

	if c.ObjectSize <= 0 {
		return nil, fmt.Errorf("OBJECT_SIZE 必须 > 0")
	}
	avgObjectSize := weightedAverageSize(c.ObjectSizes)
	if avgObjectSize <= 0 {
		avgObjectSize = c.ObjectSize
	}
	c.ObjectCount = c.TotalSize / avgObjectSize
	if c.ObjectCount < 1 {
		c.ObjectCount = 1
	}
	if c.WriteConcurrency < 1 {
		c.WriteConcurrency = 1
	}
	if c.ReadConcurrency < 1 {
		c.ReadConcurrency = 1
	}
	if c.Retry < 0 {
		c.Retry = 0
	}
	c.Endpoint = strings.TrimRight(c.Endpoint, "/")
	return c, nil
}

func (c *Config) summary() string {
	return fmt.Sprintf(
		"endpoint=%s bucket=%s region=%s pathStyle=%v\n"+
			"mode=%s objectSize=%s totalSize=%s objectCount=%d\n"+
			"runID=%s keyPrefix=%s readOps=%s readPattern=%s rangeSize=%s warmup=%s cleanAfter=%v retry=%d\n"+
			"writeConc=%d readConc=%d readCount=%d readDuration=%s reportInterval=%s resultFile=%s",
		c.Endpoint, c.Bucket, c.Region, c.PathStyle,
		c.Mode, humanBytes(c.ObjectSize), humanBytes(c.TotalSize), c.ObjectCount,
		c.RunID, c.KeyPrefix, formatReadOps(c.ReadOps), c.ReadPattern, humanBytes(c.RangeSize), c.WarmupDuration, c.CleanAfter, c.Retry,
		c.WriteConcurrency, c.ReadConcurrency, c.ReadCount, c.ReadDuration, c.ReportInterval, emptyDash(c.ResultFile),
	)
}

// ---------------------------------------------------------------------------
// 延迟直方图（指数桶，内存固定；每 worker 一个实例，避免锁竞争）
// ---------------------------------------------------------------------------

const (
	histMinNs   = 1000.0 // 1µs 起步
	histGrowth  = 1.05   // 每桶 ~5% 相对精度
	histBuckets = 440    // 覆盖到约 120s
)

type latHist struct {
	mu     sync.Mutex
	counts [histBuckets]uint64
	total  uint64
	sumNs  uint64
	minNs  uint64
	maxNs  uint64
}

func bucketIndex(ns float64) int {
	if ns < histMinNs {
		return 0
	}
	i := int(math.Log(ns/histMinNs) / math.Log(histGrowth))
	if i < 0 {
		return 0
	}
	if i >= histBuckets {
		return histBuckets - 1
	}
	return i
}

func bucketValueNs(i int) float64 {
	return histMinNs * math.Pow(histGrowth, float64(i))
}

func (h *latHist) record(d time.Duration) {
	ns := uint64(d)
	idx := bucketIndex(float64(ns))
	h.mu.Lock()
	h.counts[idx]++
	h.total++
	h.sumNs += ns
	if h.minNs == 0 || ns < h.minNs {
		h.minNs = ns
	}
	if ns > h.maxNs {
		h.maxNs = ns
	}
	h.mu.Unlock()
}

// snapshot 是直方图某一时刻的合并快照，可做区间差值。
type snapshot struct {
	counts [histBuckets]uint64
	total  uint64
	sumNs  uint64
	minNs  uint64
	maxNs  uint64
}

func mergeSnapshot(hists []*latHist) snapshot {
	var s snapshot
	for _, h := range hists {
		h.mu.Lock()
		for i := 0; i < histBuckets; i++ {
			s.counts[i] += h.counts[i]
		}
		s.total += h.total
		s.sumNs += h.sumNs
		if h.minNs != 0 && (s.minNs == 0 || h.minNs < s.minNs) {
			s.minNs = h.minNs
		}
		if h.maxNs > s.maxNs {
			s.maxNs = h.maxNs
		}
		h.mu.Unlock()
	}
	return s
}

// diff 返回 s 相对 prev 的区间增量（仅 counts 与 total，用于窗口分位数）。
func (s snapshot) diff(prev snapshot) ([histBuckets]uint64, uint64) {
	var d [histBuckets]uint64
	for i := 0; i < histBuckets; i++ {
		d[i] = s.counts[i] - prev.counts[i]
	}
	return d, s.total - prev.total
}

func percentile(counts [histBuckets]uint64, total uint64, p float64) time.Duration {
	if total == 0 {
		return 0
	}
	target := uint64(math.Ceil(p * float64(total)))
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i := 0; i < histBuckets; i++ {
		cum += counts[i]
		if cum >= target {
			return time.Duration(bucketValueNs(i))
		}
	}
	return time.Duration(bucketValueNs(histBuckets - 1))
}

// ---------------------------------------------------------------------------
// 阶段统计
// ---------------------------------------------------------------------------

type phaseStats struct {
	ops     int64 // 成功请求数
	errs    int64 // 失败请求数
	bytes   int64 // 成功传输字节
	hists   []*latHist
	elapsed time.Duration

	errMu      sync.Mutex
	errSamples []string
	errClasses map[string]int64
}

func newPhaseStats(concurrency int) *phaseStats {
	ps := &phaseStats{hists: make([]*latHist, concurrency), errClasses: map[string]int64{}}
	for i := range ps.hists {
		ps.hists[i] = &latHist{}
	}
	return ps
}

func (ps *phaseStats) recordOK(worker int, d time.Duration, n int64) {
	atomic.AddInt64(&ps.ops, 1)
	atomic.AddInt64(&ps.bytes, n)
	ps.hists[worker].record(d)
}

func (ps *phaseStats) recordErr(err error) {
	atomic.AddInt64(&ps.errs, 1)
	msg := "<nil>"
	if err != nil {
		msg = err.Error()
	}
	class := classifyError(err)
	ps.errMu.Lock()
	ps.errClasses[class]++
	if len(ps.errSamples) < 5 {
		ps.errSamples = append(ps.errSamples, msg)
	}
	ps.errMu.Unlock()
}

// ---------------------------------------------------------------------------
// S3 客户端（AWS SDK v2）
// ---------------------------------------------------------------------------

type s3Client struct {
	cfg *Config
	api *s3.Client
}

type s3Error struct {
	Op         string
	Key        string
	StatusCode int
	Status     string
	Err        error
}

func (e *s3Error) Error() string {
	target := e.Op
	if e.Key != "" {
		target += " " + e.Key
	}
	if e.StatusCode > 0 {
		if e.Err != nil {
			return fmt.Sprintf("%s status=%d: %v", target, e.StatusCode, e.Err)
		}
		return fmt.Sprintf("%s status=%d", target, e.StatusCode)
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", target, e.Err)
	}
	return target
}

func (e *s3Error) Unwrap() error { return e.Err }

func newS3Client(cfg *Config, maxConns int) (*s3Client, error) {
	if _, err := url.Parse(cfg.Endpoint); err != nil {
		return nil, fmt.Errorf("解析 endpoint 失败: %w", err)
	}
	tr := &http.Transport{
		MaxIdleConns:        maxConns * 2,
		MaxIdleConnsPerHost: maxConns + 4,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: cfg.SkipTLSVerify},
		WriteBufferSize:     64 * 1024,
		ReadBufferSize:      64 * 1024,
	}
	httpClient := &http.Client{Transport: tr, Timeout: cfg.HTTPTimeout}
	awsCfg, err := awscfg.LoadDefaultConfig(context.Background(),
		awscfg.WithRegion(cfg.Region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")),
		awscfg.WithHTTPClient(httpClient),
		awscfg.WithRetryMaxAttempts(1),
	)
	if err != nil {
		return nil, fmt.Errorf("加载 AWS SDK 配置失败: %w", err)
	}
	api := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = cfg.PathStyle
		o.RetryMaxAttempts = 1
	})
	return &s3Client{
		cfg: cfg,
		api: api,
	}, nil
}

func (c *s3Client) put(key string, body []byte) (int64, error) {
	_, err := c.api.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(c.cfg.Bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	})
	if err != nil {
		return 0, wrapS3Error("PUT", key, err)
	}
	return int64(len(body)), nil
}

func (c *s3Client) get(key string) (int64, error) {
	return c.getWithRange(key, 0)
}

func (c *s3Client) getWithRange(key string, rangeSize int64) (int64, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(key),
	}
	if rangeSize > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=0-%d", rangeSize-1))
	}
	resp, err := c.api.GetObject(context.Background(), input)
	if err != nil {
		op := "GET"
		if rangeSize > 0 {
			op = "RANGE"
		}
		return 0, wrapS3Error(op, key, err)
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body) // 读完整 body 才算完整 GET 耗时，并能复用连接
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (c *s3Client) head(key string) (int64, error) {
	resp, err := c.api.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, wrapS3Error("HEAD", key, err)
	}
	if resp.ContentLength != nil {
		return *resp.ContentLength, nil
	}
	return 0, nil
}

func (c *s3Client) list(prefix string) (int64, error) {
	resp, err := c.api.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(c.cfg.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1000),
	})
	if err != nil {
		return 0, wrapS3Error("LIST", "", err)
	}
	return int64(len(resp.Contents)), nil
}

func (c *s3Client) delete(key string) error {
	_, err := c.api.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(c.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return wrapS3Error("DELETE", key, err)
	}
	return nil
}

// createBucket 幂等地创建桶（已存在视为成功）。
func (c *s3Client) createBucket() error {
	_, err := c.api.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(c.cfg.Bucket),
	})
	if err == nil {
		return nil
	}
	if statusCodeFromError(err) == http.StatusConflict {
		return nil
	}
	return wrapS3Error("CREATE_BUCKET", c.cfg.Bucket, err)
}

func wrapS3Error(op, key string, err error) error {
	if err == nil {
		return nil
	}
	return &s3Error{Op: op, Key: key, StatusCode: statusCodeFromError(err), Err: err}
}

func statusCodeFromError(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// ---------------------------------------------------------------------------
// 压测执行
// ---------------------------------------------------------------------------

func keyFor(prefix string, i int64) string {
	return fmt.Sprintf("%s%012d", prefix, i)
}

type objectRef struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

func expectedObjects(cfg *Config) []objectRef {
	keys := make([]objectRef, 0, cfg.ObjectCount)
	for i := int64(0); i < cfg.ObjectCount; i++ {
		keys = append(keys, objectRef{
			Key:  keyFor(cfg.KeyPrefix, i),
			Size: objectSizeForIndex(cfg.ObjectSizes, i),
		})
	}
	return keys
}

// runWrite 并发上传 ObjectCount 个对象，返回成功写入的对象列表。
func runWrite(c *s3Client, cfg *Config) ([]objectRef, *phaseStats) {
	ps := newPhaseStats(cfg.WriteConcurrency)

	// 预生成各尺寸随机数据，所有上传复用（避免数据生成开销影响吞吐测量）。
	payloads := map[int64][]byte{}
	for _, sw := range cfg.ObjectSizes {
		if _, ok := payloads[sw.Size]; ok {
			continue
		}
		payload := make([]byte, sw.Size)
		rng := rand.New(rand.NewSource(sw.Size))
		rng.Read(payload)
		payloads[sw.Size] = payload
	}

	var nextIdx int64 = -1
	var okKeys sync.Map // index -> struct{}
	var okSizes sync.Map
	start := time.Now()
	stop := startReporter("WRITE", ps, cfg, start, cfg.ObjectCount)

	var wg sync.WaitGroup
	for w := 0; w < cfg.WriteConcurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&nextIdx, 1)
				if i >= cfg.ObjectCount {
					return
				}
				key := keyFor(cfg.KeyPrefix, i)
				size := objectSizeForIndex(cfg.ObjectSizes, i)
				payload := payloads[size]
				t0 := time.Now()
				n, err := withRetry(cfg.Retry, func() (int64, error) {
					return c.put(key, payload)
				})
				if err != nil {
					ps.recordErr(err)
					continue
				}
				ps.recordOK(worker, time.Since(t0), n)
				okKeys.Store(i, struct{}{})
				okSizes.Store(i, size)
			}
		}(w)
	}
	wg.Wait()
	stop()
	elapsed := time.Since(start)
	ps.elapsed = elapsed

	keys := make([]objectRef, 0, cfg.ObjectCount)
	for i := int64(0); i < cfg.ObjectCount; i++ {
		if _, ok := okKeys.Load(i); ok {
			size := objectSizeForIndex(cfg.ObjectSizes, i)
			if v, ok := okSizes.Load(i); ok {
				size = v.(int64)
			}
			keys = append(keys, objectRef{Key: keyFor(cfg.KeyPrefix, i), Size: size})
		}
	}
	printSummary("WRITE", ps, elapsed)
	return keys, ps
}

// runRead 并发读取对象，支持按次数或按时长两种停止条件。
func runRead(c *s3Client, cfg *Config, keys []objectRef) *phaseStats {
	if cfg.WarmupDuration > 0 && len(keys) > 0 {
		fmt.Printf("[READ] warmup %s，不计入正式统计\n", cfg.WarmupDuration)
		runReadPhase(c, cfg, keys, cfg.WarmupDuration, 0, false)
	}
	return runReadPhase(c, cfg, keys, cfg.ReadDuration, cfg.ReadCount, true)
}

func runReadPhase(c *s3Client, cfg *Config, keys []objectRef, duration time.Duration, readCount int64, report bool) *phaseStats {
	ps := newPhaseStats(cfg.ReadConcurrency)
	if len(keys) == 0 {
		fmt.Println("[READ] 无可读对象，跳过")
		return ps
	}

	// 决定停止条件
	byTime := duration > 0
	totalReq := readCount
	if !byTime && totalReq <= 0 {
		totalReq = int64(len(keys)) // 默认读「对象数」次（sequential 时恰好每个对象一遍）
	}

	var seq int64 = -1 // 全局请求序号：sequential 模式据此顺序遍历 key
	var progressTotal int64
	if byTime {
		progressTotal = 0 // 时长模式无固定总数
	} else {
		progressTotal = totalReq
	}

	start := time.Now()
	deadline := start.Add(duration)
	stop := func() {}
	if report {
		stop = startReporter("READ", ps, cfg, start, progressTotal)
	}

	var wg sync.WaitGroup
	for w := 0; w < cfg.ReadConcurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(worker) + 100))
			for {
				i := atomic.AddInt64(&seq, 1)
				if byTime {
					if time.Now().After(deadline) {
						return
					}
				} else if i >= totalReq {
					return
				}
				var obj objectRef
				var op readOp
				if cfg.ReadPattern == patternSequential {
					obj = keys[i%int64(len(keys))]
					op = cfg.ReadOps[i%int64(len(cfg.ReadOps))]
				} else {
					obj = keys[rng.Intn(len(keys))]
					op = cfg.ReadOps[rng.Intn(len(cfg.ReadOps))]
				}
				t0 := time.Now()
				n, err := runReadOp(c, cfg, obj, op)
				if err != nil {
					ps.recordErr(err)
					continue
				}
				ps.recordOK(worker, time.Since(t0), n)
			}
		}(w)
	}
	wg.Wait()
	stop()
	elapsed := time.Since(start)
	ps.elapsed = elapsed
	if report {
		printSummary("READ", ps, elapsed)
	}
	return ps
}

func runReadOp(c *s3Client, cfg *Config, obj objectRef, op readOp) (int64, error) {
	switch op {
	case opGET:
		return withRetry(cfg.Retry, func() (int64, error) {
			n, err := c.get(obj.Key)
			if err != nil {
				return 0, err
			}
			if obj.Size > 0 && n != obj.Size {
				return 0, fmt.Errorf("GET %s size mismatch: got %d want %d", obj.Key, n, obj.Size)
			}
			return n, nil
		})
	case opHEAD:
		return withRetry(cfg.Retry, func() (int64, error) {
			return c.head(obj.Key)
		})
	case opRANGE:
		rangeSize := cfg.RangeSize
		if rangeSize <= 0 || (obj.Size > 0 && rangeSize > obj.Size) {
			rangeSize = obj.Size
		}
		return withRetry(cfg.Retry, func() (int64, error) {
			return c.getWithRange(obj.Key, rangeSize)
		})
	case opLIST:
		return withRetry(cfg.Retry, func() (int64, error) {
			return c.list(cfg.KeyPrefix)
		})
	default:
		return 0, fmt.Errorf("unsupported read op %q", op)
	}
}

func runClean(c *s3Client, cfg *Config, keys []objectRef) *phaseStats {
	ps := newPhaseStats(cfg.WriteConcurrency)
	if len(keys) == 0 {
		fmt.Println("[CLEAN] 无可清理对象，跳过")
		return ps
	}
	var nextIdx int64 = -1
	start := time.Now()
	stop := startReporter("CLEAN", ps, cfg, start, int64(len(keys)))
	var wg sync.WaitGroup
	for w := 0; w < cfg.WriteConcurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&nextIdx, 1)
				if i >= int64(len(keys)) {
					return
				}
				obj := keys[i]
				t0 := time.Now()
				_, err := withRetry(cfg.Retry, func() (int64, error) {
					return 0, c.delete(obj.Key)
				})
				if err != nil {
					ps.recordErr(err)
					continue
				}
				ps.recordOK(worker, time.Since(t0), 0)
			}
		}(w)
	}
	wg.Wait()
	stop()
	elapsed := time.Since(start)
	ps.elapsed = elapsed
	printSummary("CLEAN", ps, elapsed)
	return ps
}

func withRetry(retries int, fn func() (int64, error)) (int64, error) {
	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		n, err := fn()
		if err == nil {
			return n, nil
		}
		lastErr = err
		if attempt < retries {
			time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
		}
	}
	return 0, lastErr
}

// ---------------------------------------------------------------------------
// 实时报告 + 汇总
// ---------------------------------------------------------------------------

func startReporter(phase string, ps *phaseStats, cfg *Config, start time.Time, progressTotal int64) func() {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(cfg.ReportInterval)
		defer ticker.Stop()
		var prev snapshot
		prevTime := start
		for {
			select {
			case <-done:
				return
			case now := <-ticker.C:
				cur := mergeSnapshot(ps.hists)
				winCounts, winTotal := cur.diff(prev)
				winSecs := now.Sub(prevTime).Seconds()
				cumSecs := now.Sub(start).Seconds()

				winOps := float64(winTotal) / winSecs
				curBytes := atomic.LoadInt64(&ps.bytes)
				// 区间字节量无法从直方图取，用累计字节差近似窗口吞吐
				winBytes := float64(curBytes-prevBytesHolder.get(phase)) / winSecs
				prevBytesHolder.set(phase, curBytes)
				cumOps := float64(cur.total) / cumSecs
				cumBytes := float64(curBytes) / cumSecs

				errs := atomic.LoadInt64(&ps.errs)
				p50 := percentile(winCounts, winTotal, 0.50)
				p95 := percentile(winCounts, winTotal, 0.95)
				p99 := percentile(winCounts, winTotal, 0.99)

				progress := ""
				if progressTotal > 0 {
					pct := 100 * float64(cur.total+uint64(errs)) / float64(progressTotal)
					progress = fmt.Sprintf(" %d/%d(%.1f%%)", cur.total, progressTotal, pct)
				}
				fmt.Printf("[%s %5.0fs]%s | win: %s/s %.0f op/s p50=%s p95=%s p99=%s | cum: %s/s %.0f op/s | err=%d\n",
					phase, cumSecs, progress,
					humanBytes(int64(winBytes)), winOps, fmtDur(p50), fmtDur(p95), fmtDur(p99),
					humanBytes(int64(cumBytes)), cumOps, errs,
				)
				prev = cur
				prevTime = now
			}
		}
	}()
	return func() { close(done); wg.Wait() }
}

// prevBytesHolder 为各阶段保存上次累计字节，用于窗口吞吐计算。
var prevBytesHolder = &bytesHolder{m: map[string]int64{}}

type bytesHolder struct {
	mu sync.Mutex
	m  map[string]int64
}

func (b *bytesHolder) get(k string) int64 { b.mu.Lock(); defer b.mu.Unlock(); return b.m[k] }
func (b *bytesHolder) set(k string, v int64) {
	b.mu.Lock()
	b.m[k] = v
	b.mu.Unlock()
}

func printSummary(phase string, ps *phaseStats, elapsed time.Duration) {
	s := mergeSnapshot(ps.hists)
	ops := atomic.LoadInt64(&ps.ops)
	errs := atomic.LoadInt64(&ps.errs)
	bytesN := atomic.LoadInt64(&ps.bytes)
	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1e-9
	}
	avg := time.Duration(0)
	if s.total > 0 {
		avg = time.Duration(s.sumNs / s.total)
	}
	errRate := 0.0
	if ops+errs > 0 {
		errRate = 100 * float64(errs) / float64(ops+errs)
	}

	fmt.Printf("\n========== %s 汇总 ==========\n", phase)
	fmt.Printf("耗时           : %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("成功 / 失败    : %d / %d (错误率 %.3f%%)\n", ops, errs, errRate)
	fmt.Printf("传输总量       : %s\n", humanBytes(bytesN))
	fmt.Printf("吞吐           : %s/s | %.1f op/s\n", humanBytes(int64(float64(bytesN)/secs)), float64(ops)/secs)
	fmt.Printf("延迟 min       : %s\n", fmtDur(time.Duration(s.minNs)))
	fmt.Printf("延迟 avg       : %s\n", fmtDur(avg))
	fmt.Printf("延迟 p50       : %s\n", fmtDur(percentile(s.counts, s.total, 0.50)))
	fmt.Printf("延迟 p90       : %s\n", fmtDur(percentile(s.counts, s.total, 0.90)))
	fmt.Printf("延迟 p95       : %s\n", fmtDur(percentile(s.counts, s.total, 0.95)))
	fmt.Printf("延迟 p99       : %s\n", fmtDur(percentile(s.counts, s.total, 0.99)))
	fmt.Printf("延迟 p99.9     : %s\n", fmtDur(percentile(s.counts, s.total, 0.999)))
	fmt.Printf("延迟 max       : %s\n", fmtDur(time.Duration(s.maxNs)))
	if errs > 0 {
		ps.errMu.Lock()
		fmt.Printf("错误分类       :\n")
		classes := make([]string, 0, len(ps.errClasses))
		for k := range ps.errClasses {
			classes = append(classes, k)
		}
		sort.Strings(classes)
		for _, k := range classes {
			fmt.Printf("  - %s: %d\n", k, ps.errClasses[k])
		}
		fmt.Printf("错误样例       :\n")
		for _, e := range ps.errSamples {
			fmt.Printf("  - %s\n", e)
		}
		ps.errMu.Unlock()
	}
	fmt.Printf("================================\n\n")
}

type benchmarkResult struct {
	RunID        string                  `json:"run_id"`
	StartedAt    string                  `json:"started_at"`
	FinishedAt   string                  `json:"finished_at"`
	GoVersion    string                  `json:"go_version"`
	GOOS         string                  `json:"goos"`
	GOARCH       string                  `json:"goarch"`
	CPU          int                     `json:"cpu"`
	Config       resultConfig            `json:"config"`
	Phases       map[string]phaseSummary `json:"phases"`
	CleanSummary *phaseSummary           `json:"clean_summary,omitempty"`
}

type resultConfig struct {
	Endpoint         string             `json:"endpoint"`
	Region           string             `json:"region"`
	Bucket           string             `json:"bucket"`
	Mode             string             `json:"mode"`
	KeyPrefix        string             `json:"key_prefix"`
	ObjectSize       int64              `json:"object_size"`
	ObjectSizes      []objectSizeWeight `json:"object_sizes"`
	TotalSize        int64              `json:"total_size"`
	ObjectCount      int64              `json:"object_count"`
	WriteConcurrency int                `json:"write_concurrency"`
	ReadConcurrency  int                `json:"read_concurrency"`
	ReadOps          string             `json:"read_ops"`
	ReadPattern      string             `json:"read_pattern"`
	ReadCount        int64              `json:"read_count"`
	ReadDurationMs   int64              `json:"read_duration_ms"`
	WarmupMs         int64              `json:"warmup_ms"`
	ReportIntervalMs int64              `json:"report_interval_ms"`
	Retry            int                `json:"retry"`
}

type phaseSummary struct {
	ElapsedMs     int64            `json:"elapsed_ms"`
	Success       int64            `json:"success"`
	Errors        int64            `json:"errors"`
	ErrorRate     float64          `json:"error_rate"`
	Bytes         int64            `json:"bytes"`
	ThroughputBps float64          `json:"throughput_bps"`
	OpsPerSecond  float64          `json:"ops_per_second"`
	LatencyMs     latencySummary   `json:"latency_ms"`
	ErrorClasses  map[string]int64 `json:"error_classes,omitempty"`
	ErrorSamples  []string         `json:"error_samples,omitempty"`
}

type latencySummary struct {
	Min  float64 `json:"min"`
	Avg  float64 `json:"avg"`
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p999"`
	Max  float64 `json:"max"`
}

func summarizePhase(ps *phaseStats, elapsed time.Duration) phaseSummary {
	s := mergeSnapshot(ps.hists)
	ops := atomic.LoadInt64(&ps.ops)
	errs := atomic.LoadInt64(&ps.errs)
	bytesN := atomic.LoadInt64(&ps.bytes)
	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1e-9
	}
	avg := time.Duration(0)
	if s.total > 0 {
		avg = time.Duration(s.sumNs / s.total)
	}
	errRate := 0.0
	if ops+errs > 0 {
		errRate = 100 * float64(errs) / float64(ops+errs)
	}
	out := phaseSummary{
		ElapsedMs:     elapsed.Milliseconds(),
		Success:       ops,
		Errors:        errs,
		ErrorRate:     errRate,
		Bytes:         bytesN,
		ThroughputBps: float64(bytesN) / secs,
		OpsPerSecond:  float64(ops) / secs,
		LatencyMs: latencySummary{
			Min:  durationMillis(time.Duration(s.minNs)),
			Avg:  durationMillis(avg),
			P50:  durationMillis(percentile(s.counts, s.total, 0.50)),
			P90:  durationMillis(percentile(s.counts, s.total, 0.90)),
			P95:  durationMillis(percentile(s.counts, s.total, 0.95)),
			P99:  durationMillis(percentile(s.counts, s.total, 0.99)),
			P999: durationMillis(percentile(s.counts, s.total, 0.999)),
			Max:  durationMillis(time.Duration(s.maxNs)),
		},
	}
	ps.errMu.Lock()
	if len(ps.errClasses) > 0 {
		out.ErrorClasses = make(map[string]int64, len(ps.errClasses))
		for k, v := range ps.errClasses {
			out.ErrorClasses[k] = v
		}
	}
	if len(ps.errSamples) > 0 {
		out.ErrorSamples = append([]string(nil), ps.errSamples...)
	}
	ps.errMu.Unlock()
	return out
}

func durationMillis(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}

func newBenchmarkResult(cfg *Config, start time.Time) benchmarkResult {
	return benchmarkResult{
		RunID:     cfg.RunID,
		StartedAt: start.Format(time.RFC3339),
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		CPU:       runtime.NumCPU(),
		Config: resultConfig{
			Endpoint:         cfg.Endpoint,
			Region:           cfg.Region,
			Bucket:           cfg.Bucket,
			Mode:             cfg.Mode,
			KeyPrefix:        cfg.KeyPrefix,
			ObjectSize:       cfg.ObjectSize,
			ObjectSizes:      cfg.ObjectSizes,
			TotalSize:        cfg.TotalSize,
			ObjectCount:      cfg.ObjectCount,
			WriteConcurrency: cfg.WriteConcurrency,
			ReadConcurrency:  cfg.ReadConcurrency,
			ReadOps:          formatReadOps(cfg.ReadOps),
			ReadPattern:      cfg.ReadPattern,
			ReadCount:        cfg.ReadCount,
			ReadDurationMs:   cfg.ReadDuration.Milliseconds(),
			WarmupMs:         cfg.WarmupDuration.Milliseconds(),
			ReportIntervalMs: cfg.ReportInterval.Milliseconds(),
			Retry:            cfg.Retry,
		},
		Phases: map[string]phaseSummary{},
	}
}

func writeResultFile(path string, result benchmarkResult) error {
	if path == "" {
		return nil
	}
	result.FinishedAt = time.Now().Format(time.RFC3339)
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

// ---------------------------------------------------------------------------
// 工具函数
// ---------------------------------------------------------------------------

func defaultRunID() string {
	return time.Now().Format("20060102-150405")
}

func scopedKeyPrefix(prefix, runID string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		prefix = "s3bench"
	}
	if runID == "" {
		return prefix + "/"
	}
	return prefix + "/" + strings.Trim(runID, "/") + "/"
}

func parseReadOps(s string) ([]readOp, error) {
	if strings.TrimSpace(s) == "" {
		return []readOp{opGET}, nil
	}
	parts := strings.Split(s, ",")
	ops := make([]readOp, 0, len(parts))
	seen := map[readOp]bool{}
	for _, part := range parts {
		name := strings.ToLower(strings.TrimSpace(part))
		var op readOp
		switch name {
		case "get":
			op = opGET
		case "head":
			op = opHEAD
		case "range", "range_get", "range-get":
			op = opRANGE
		case "list":
			op = opLIST
		default:
			return nil, fmt.Errorf("未知读操作 %q（支持 get,head,range,list）", part)
		}
		if !seen[op] {
			ops = append(ops, op)
			seen[op] = true
		}
	}
	if len(ops) == 0 {
		return []readOp{opGET}, nil
	}
	return ops, nil
}

func formatReadOps(ops []readOp) string {
	if len(ops) == 0 {
		return string(opGET)
	}
	names := make([]string, 0, len(ops))
	for _, op := range ops {
		names = append(names, string(op))
	}
	return strings.Join(names, ",")
}

func parseReadPattern(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "random", "rand":
		return patternRandom, nil
	case "sequential", "seq":
		return patternSequential, nil
	default:
		return "", fmt.Errorf("未知读取模式 %q（支持 random|sequential）", s)
	}
}

func parseObjectSizePattern(pattern string, defaultSize int64) ([]objectSizeWeight, error) {
	if strings.TrimSpace(pattern) == "" {
		return []objectSizeWeight{{Size: defaultSize, Weight: 100}}, nil
	}
	parts := strings.Split(pattern, ",")
	out := make([]objectSizeWeight, 0, len(parts))
	for _, part := range parts {
		pair := strings.Split(strings.TrimSpace(part), ":")
		if len(pair) != 2 {
			return nil, fmt.Errorf("格式应为 size:weight，例如 4KB:70%%")
		}
		size, err := parseSize(pair[0])
		if err != nil {
			return nil, err
		}
		weightText := strings.TrimSpace(strings.TrimSuffix(pair[1], "%"))
		weight, err := strconv.Atoi(weightText)
		if err != nil {
			return nil, fmt.Errorf("无法解析权重 %q: %w", pair[1], err)
		}
		if size <= 0 || weight <= 0 {
			return nil, fmt.Errorf("size 和 weight 必须 > 0")
		}
		out = append(out, objectSizeWeight{Size: size, Weight: weight})
	}
	return out, nil
}

func weightedAverageSize(sizes []objectSizeWeight) int64 {
	var totalWeight int64
	var totalSize int64
	for _, sw := range sizes {
		totalWeight += int64(sw.Weight)
		totalSize += sw.Size * int64(sw.Weight)
	}
	if totalWeight == 0 {
		return 0
	}
	return totalSize / totalWeight
}

func objectSizeForIndex(sizes []objectSizeWeight, idx int64) int64 {
	if len(sizes) == 0 {
		return 0
	}
	var totalWeight int64
	for _, sw := range sizes {
		totalWeight += int64(sw.Weight)
	}
	if totalWeight <= 0 {
		return sizes[0].Size
	}
	slot := idx % totalWeight
	var acc int64
	for _, sw := range sizes {
		acc += int64(sw.Weight)
		if slot < acc {
			return sw.Size
		}
	}
	return sizes[len(sizes)-1].Size
}

func parseSizeDef(s string, def int64) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return def, nil
	}
	return parseSize(s)
}

func classifyError(err error) string {
	if err == nil {
		return "ok"
	}
	var se *s3Error
	if errors.As(err, &se) {
		return fmt.Sprintf("%s status=%d", se.Op, se.StatusCode)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return "timeout"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "connection reset"):
		return "connection_reset"
	case strings.Contains(msg, "connection refused"):
		return "connection_refused"
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline exceeded"):
		return "timeout"
	case strings.Contains(msg, "signature"):
		return "signature"
	default:
		return "other"
	}
}

func emptyDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func parseEnvFile(path string) map[string]string {
	m := map[string]string{}
	data, err := os.ReadFile(path)
	if err != nil {
		return m
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		k := strings.TrimSpace(line[:eq])
		v := strings.TrimSpace(line[eq+1:])
		v = strings.Trim(v, `"'`)
		m[k] = v
	}
	return m
}

func parseBool(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "y", "on":
		return true
	}
	return false
}

func atoiDef(s string, def int) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return def, nil
	}
	return strconv.Atoi(s)
}

func atoi64Def(s string, def int64) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return def, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

func parseDurDef(s string, def time.Duration) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return def, nil
	}
	return time.ParseDuration(s)
}

// parseSize 解析 1024 进制大小：B/KB/MB/GB/TB（也接受 KiB 等同义）。
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, fmt.Errorf("空大小")
	}
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "TIB"), strings.HasSuffix(s, "TB"):
		mult = 1 << 40
		s = strings.TrimSuffix(strings.TrimSuffix(s, "TIB"), "TB")
	case strings.HasSuffix(s, "GIB"), strings.HasSuffix(s, "GB"):
		mult = 1 << 30
		s = strings.TrimSuffix(strings.TrimSuffix(s, "GIB"), "GB")
	case strings.HasSuffix(s, "MIB"), strings.HasSuffix(s, "MB"):
		mult = 1 << 20
		s = strings.TrimSuffix(strings.TrimSuffix(s, "MIB"), "MB")
	case strings.HasSuffix(s, "KIB"), strings.HasSuffix(s, "KB"):
		mult = 1 << 10
		s = strings.TrimSuffix(strings.TrimSuffix(s, "KIB"), "KB")
	case strings.HasSuffix(s, "B"):
		s = strings.TrimSuffix(s, "B")
	}
	s = strings.TrimSpace(s)
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("无法解析大小 %q: %w", s, err)
	}
	return int64(f * float64(mult)), nil
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func fmtDur(d time.Duration) string {
	switch {
	case d == 0:
		return "0"
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d.Nanoseconds())/1e3)
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	envPath := flag.String("env", ".env", "配置文件路径")
	modeOverride := flag.String("mode", "", "覆盖运行模式：write|read|both|clean")
	flag.Parse()

	cfg, err := loadConfig(*envPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "配置错误: %v\n", err)
		os.Exit(1)
	}
	if *modeOverride != "" {
		cfg.Mode = strings.ToLower(*modeOverride)
	}

	fmt.Println("=== s3bench S3 集群性能压测 ===")
	fmt.Println(cfg.summary())
	fmt.Println()

	maxConns := cfg.WriteConcurrency
	if cfg.ReadConcurrency > maxConns {
		maxConns = cfg.ReadConcurrency
	}
	client, err := newS3Client(cfg, maxConns)
	if err != nil {
		fmt.Fprintf(os.Stderr, "客户端初始化失败: %v\n", err)
		os.Exit(1)
	}

	if cfg.CreateBucket {
		if err := client.createBucket(); err != nil {
			fmt.Fprintf(os.Stderr, "警告: 创建桶失败（继续尝试压测）: %v\n", err)
		} else {
			fmt.Printf("桶 %q 就绪\n\n", cfg.Bucket)
		}
	}

	startedAt := time.Now()
	result := newBenchmarkResult(cfg, startedAt)
	var keys []objectRef
	switch cfg.Mode {
	case "write":
		var ps *phaseStats
		keys, ps = runWrite(client, cfg)
		result.Phases["write"] = summarizePhase(ps, ps.elapsed)
	case "read":
		// 单独 read 模式：按写入算法重建 key 列表
		keys = expectedObjects(cfg)
		ps := runRead(client, cfg, keys)
		result.Phases["read"] = summarizePhase(ps, ps.elapsed)
	case "both":
		var writeStats *phaseStats
		keys, writeStats = runWrite(client, cfg)
		result.Phases["write"] = summarizePhase(writeStats, writeStats.elapsed)
		readStats := runRead(client, cfg, keys)
		result.Phases["read"] = summarizePhase(readStats, readStats.elapsed)
	case "clean":
		keys = expectedObjects(cfg)
		cleanStats := runClean(client, cfg, keys)
		summary := summarizePhase(cleanStats, cleanStats.elapsed)
		result.Phases["clean"] = summary
		result.CleanSummary = &summary
	default:
		fmt.Fprintf(os.Stderr, "未知 mode: %q（应为 write|read|both|clean）\n", cfg.Mode)
		os.Exit(1)
	}
	if cfg.CleanAfter && cfg.Mode != "clean" {
		cleanStats := runClean(client, cfg, keys)
		summary := summarizePhase(cleanStats, cleanStats.elapsed)
		result.CleanSummary = &summary
	}
	if err := writeResultFile(cfg.ResultFile, result); err != nil {
		fmt.Fprintf(os.Stderr, "写结果文件失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("完成。")
}
