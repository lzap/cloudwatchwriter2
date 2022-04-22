package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cww "github.com/lzap/cloudwatchwriter2"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	logGroupName  = "cloudwatchwriter"
	logStreamName = "this-computer"
)

func main() {
	logger, close, err := newCloudWatchLogger()
	if err != nil {
		log.Error().Err(err).Msg("newCloudWatchLogger")
		return
	}
	// very important to close the writer to flush the remaining batch
	defer close()

	for i := 0; i < 10000; i++ {
		logger.Info().Str("package", "cloudwatchwriter").Msg(fmt.Sprintf("Log %d", i))
	}
}

func newCloudWatchLogger() (zerolog.Logger, func(), error) {
	aws_region := os.Getenv("AWS_REGION")
	aws_key := os.Getenv("AWS_KEY")
	aws_secret := os.Getenv("AWS_SECRET")
	aws_session := os.Getenv("AWS_SESSION")

	options := cloudwatchlogs.Options{
		Region:      aws_region,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(aws_key, aws_secret, aws_session)),
	}
	client := cloudwatchlogs.New(options)

	cloudWatchWriter, err := cww.NewWithClient(client, 500*time.Millisecond, logGroupName, logStreamName)
	if err != nil {
		return log.Logger, nil, fmt.Errorf("cloudwatchwriter.New: %w", err)
	}

	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout}
	logger := zerolog.New(zerolog.MultiLevelWriter(consoleWriter, cloudWatchWriter)).With().Timestamp().Logger()

	return logger, cloudWatchWriter.Close, nil
}
