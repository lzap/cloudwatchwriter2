package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cww "github.com/lzap/cloudwatchwriter2"
)

func main() {
	aws_region := os.Getenv("AWS_REGION")
	aws_key := os.Getenv("AWS_KEY")
	aws_secret := os.Getenv("AWS_SECRET")
	aws_session := os.Getenv("AWS_SESSION")
	logGroupName := os.Getenv("LOG_GROUP_NAME")
	logStreamName := os.Getenv("LOG_STREAM_NAME")

	options := cloudwatchlogs.Options{
		Region:      aws_region,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(aws_key, aws_secret, aws_session)),
	}
	client := cloudwatchlogs.New(options)

	cloudWatchWriter, err := cww.NewWithClient(client, 500*time.Millisecond, logGroupName, logStreamName)
	if err != nil {
		panic(err)
	}
	defer func() {
		fmt.Printf("Total logs queued: %d\n", cloudWatchWriter.Stats.QueuedEventCount.Load())
		fmt.Printf("Total logs sent: %d\n", cloudWatchWriter.Stats.SentEventCount.Load())
		fmt.Printf("Total batches: %d\n", cloudWatchWriter.Stats.BatchCount.Load())
	}()
	defer func() {
		cloudWatchWriter.Flush()
		err := cloudWatchWriter.Close()
		if err != nil {
			fmt.Printf("Close: %v\n", err)
		}
	}()

	// log/slog
	h := slog.NewJSONHandler(cloudWatchWriter, &slog.HandlerOptions{})
	slog.SetDefault(slog.New(h))

	for i := 1; i <= 10000; i++ {
		slog.Info("this is a test log message", "from", "slog", "i", i)
	}

	time.Sleep(2 * time.Second)
}
