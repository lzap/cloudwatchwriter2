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

	"github.com/rs/zerolog"
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
	defer cloudWatchWriter.Close() // this is important to flush the remaining batch

	// zerolog
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout}
	logger := zerolog.New(zerolog.MultiLevelWriter(consoleWriter, cloudWatchWriter)).With().Timestamp().Logger()

	// log/slog
	h := slog.NewJSONHandler(cloudWatchWriter, &slog.HandlerOptions{})
	slog.SetDefault(slog.New(h))

	for i := 1; i <= 5; i++ {
		slog.Info("log", "from", "slog", "i", i)
		logger.Info().Str("from", "zerolog").Msgf("Log %d", i)
	}
}
