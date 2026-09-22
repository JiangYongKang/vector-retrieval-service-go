package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/highcumontoa/vector-retrieval-service-go/vector"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP 监听地址")
	dataDir := flag.String("data-dir", "data", "本地数据目录（传空则纯内存、不持久化）")
	maxRecords := flag.Int("max-records", 100_000, "最大记录数（容量上限）")
	maxDim := flag.Int("max-dim", 4096, "最大向量维度")
	maxBatch := flag.Int("max-batch", 500, "单批写入最大条目数")
	maxConc := flag.Int("max-concurrent", 64, "读写最大并发数（背压阈值）")
	flag.Parse()

	store, err := vector.Open(vector.Config{
		DataDir:       *dataDir,
		MaxRecords:    *maxRecords,
		MaxVectorDim:  *maxDim,
		MaxBatchSize:  *maxBatch,
		MaxConcurrent: *maxConc,
	})
	if err != nil {
		log.Fatalf("打开存储失败: %v", err)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           vector.NewServer(store).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("向量检索服务已启动: %s (data=%s)", *addr, *dataDir)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP 服务异常: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("收到退出信号，开始优雅关闭…")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("HTTP 关闭超时: %v", err)
	}
	if err := store.Close(); err != nil {
		log.Printf("存储关闭失败: %v", err)
	}
	log.Printf("已安全关闭，再见")
}
