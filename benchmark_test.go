package cloudwatchwriter2_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	cloudwatchwriter "github.com/lzap/cloudwatchwriter2"
)

// benchmarkMockClient is a mock client for benchmarking purposes. It is a stripped-down
// version of mockClient that does not do any locking.
type benchmarkMockClient struct {
	events int
	length int
}

func (c *benchmarkMockClient) DescribeLogStreams(context.Context, *cloudwatchlogs.DescribeLogStreamsInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogStreamsOutput, error) {
	return &cloudwatchlogs.DescribeLogStreamsOutput{
		LogStreams: []types.LogStream{
			{
				LogStreamName:       aws.String("logStream"),
				UploadSequenceToken: aws.String(sequenceToken),
			},
		},
	}, nil
}

func (c *benchmarkMockClient) CreateLogGroup(context.Context, *cloudwatchlogs.CreateLogGroupInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error) {
	return nil, nil
}

func (c *benchmarkMockClient) CreateLogStream(context.Context, *cloudwatchlogs.CreateLogStreamInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error) {
	return nil, nil
}

func (c *benchmarkMockClient) PutLogEvents(_ context.Context, putLogEvents *cloudwatchlogs.PutLogEventsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error) {
	c.events += len(putLogEvents.LogEvents)
	for _, event := range putLogEvents.LogEvents {
		c.length += len(*event.Message)
	}

	return &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken: aws.String(sequenceToken),
	}, nil
}

func BenchmarkCloudWatchWriterThroughput(b *testing.B) {
	eventSizes := map[string]int{
		"10B":   10,
		"100B":  100,
		"1KB":   1024,
		"10KB":  10 * 1024,
		"100KB": 100 * 1024,
	}

	for name, size := range eventSizes {
		b.Run(name, func(b *testing.B) {
			client := &benchmarkMockClient{}
			cloudwatchwriter.MinBatchInterval = 20 * time.Millisecond
			writer, err := cloudwatchwriter.NewWithClient(client, 20*time.Millisecond, "logGroup", "logStream")
			if err != nil {
				b.Fatalf("NewWithClient: %v", err)
			}

			msg := []byte(strings.Repeat(".", size))
			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := writer.Write(msg)
				if err != nil {
					// full queue
					b.StopTimer()
					time.Sleep(20 * time.Millisecond)
					b.StartTimer()
					_, err = writer.Write(msg)
					if err != nil {
						// should not happen queue must be empty at this point
						b.Fatalf("Write: %v", err)
					}
				}
			}

			b.StopTimer()

			err = writer.Close()
			if err != nil {
				b.Fatalf("writer.Close: %v", err)
			}

			if client.events != b.N {
				b.Fatalf("expected %d log events, got %d", b.N, client.events)
			}

			if client.length != b.N*size {
				b.Fatalf("expected %d bytes, got %d", b.N*size, client.length)
			}
		})
	}
}
