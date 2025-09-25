package cloudwatchwriter2

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

var (
	// MinBatchInterval is 200 ms as the maximum rate of PutLogEvents is 5
	// requests per second. This is required by the AWS CloudWatch Logs API.
	// Do not change this value unless you know what you are doing. Used for testing only.
	MinBatchInterval time.Duration = 200 * time.Millisecond
)

const (
	// PayloadsChannelSize is the size of the channel that holds payloads, default 4k
	PayloadsChannelSize = 4096

	// BatchSizeLimit is 1MB in bytes, the limit imposed by AWS CloudWatch Logs
	// on the size the batch of logs we send, see:
	// https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
	BatchSizeLimit = 1048576

	// MaxNumLogEvents is the maximum number of messages that can be sent in one
	// batch, also an AWS limitation, see:
	// https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
	MaxNumLogEvents = 10000

	// AdditionalBytesPerLogEvent is the number of additional bytes per log
	// event, other than the length of the log message, see:
	// https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
	AdditionalBytesPerLogEvent = 26
)

// CloudWatchLogsClient represents the AWS cloudwatchlogs client that we need to talk to CloudWatch
type CloudWatchLogsClient interface {
	DescribeLogStreams(context.Context, *cloudwatchlogs.DescribeLogStreamsInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogStreamsOutput, error)
	CreateLogGroup(context.Context, *cloudwatchlogs.CreateLogGroupInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error)
	CreateLogStream(context.Context, *cloudwatchlogs.CreateLogStreamInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error)
	PutLogEvents(context.Context, *cloudwatchlogs.PutLogEventsInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error)
}

// ErrFullOrClosed is returned when the payloads channel is full or closed via close().
var ErrFullOrClosed = errors.New("cannot create new event: channel full or closed")

// ErrBatchIntervalTooSmall is returned when the batch interval is too small.
var ErrBatchIntervalTooSmall = errors.New("batch interval is too small")

// CloudWatchWriter can be inserted into zerolog to send logs to CloudWatch.
type CloudWatchWriter struct {
	client CloudWatchLogsClient

	payloads  chan types.InputLogEvent
	wg        sync.WaitGroup
	closeOnce sync.Once
	flushMu   sync.Mutex

	batchInterval     time.Duration
	lastErr           *LastErr
	logGroupName      *string
	logStreamName     *string
	nextSequenceToken *string

	Stats Stats
}

type Stats struct {
	// Total number of events queued for sending
	QueuedEventCount atomic.Uint64

	// Total number of events sent (EventsEnqueued >= SentEventCount)
	SentEventCount atomic.Uint64

	// Total number of requests sent
	BatchCount atomic.Uint64

	// Total number of HTTP retries
	RetryCount atomic.Uint64
}

type LastErr struct {
	err error
	mu  sync.Mutex
}

func (l *LastErr) get() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

func (l *LastErr) set(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.err = err
}

// NewWithClientContext creates a new CloudWatchWriter with the given client, batch interval, log group name and log stream name.
// Use Close method to properly close the writer. The writer will not accept any new events after Close is called.
// The writer will flush the buffer and close the payloads channel when Close is called. Use context cancellation to
// stop the writer and Close to properly close it.
//
// Log group and log stream will be created immediately if they do not exist. There is no lazy-initialization supported.
func NewWithClientContext(ctx context.Context, client CloudWatchLogsClient, batchInterval time.Duration, logGroupName, logStreamName string) (*CloudWatchWriter, error) {
	if batchInterval < MinBatchInterval {
		return nil, ErrBatchIntervalTooSmall
	}

	writer := &CloudWatchWriter{
		client:   client,
		payloads: make(chan types.InputLogEvent, PayloadsChannelSize),

		batchInterval: batchInterval,
		lastErr:       &LastErr{},

		logGroupName:  aws.String(logGroupName),
		logStreamName: aws.String(logStreamName),
		Stats:         Stats{},
	}

	logStream, err := writer.getOrCreateLogStream(ctx)
	if err != nil {
		return nil, err
	}
	writer.nextSequenceToken = logStream.UploadSequenceToken

	ticker := time.NewTicker(MinBatchInterval)
	writer.wg.Add(1)
	go writer.queueMonitor(ctx, ticker.C)

	return writer, nil
}

// NewWithClient does the same as NewWithClientContext but uses context.Background() as the context.
func NewWithClient(client CloudWatchLogsClient, batchInterval time.Duration, logGroupName, logStreamName string) (*CloudWatchWriter, error) {
	return NewWithClientContext(context.Background(), client, batchInterval, logGroupName, logStreamName)
}

func (c *CloudWatchWriter) Write(log []byte) (int, error) {
	event := types.InputLogEvent{
		Message: aws.String(string(log)),
	}

	// Non-blocking write, return error if the channel is full or closed.
	// This is important as we do not want to block the application if CloudWatch
	// is slow or unavailable.
	select {
	case c.payloads <- event:
	default:
		return 0, ErrFullOrClosed
	}

	// report last sending error
	lastErr := c.lastErr.get()
	if lastErr != nil {
		c.lastErr.set(nil)
		return len(log), lastErr
	}

	return len(log), nil
}

var flushEvent = types.InputLogEvent{}

func (c *CloudWatchWriter) queueMonitor(ctx context.Context, ticker <-chan time.Time) {
	var batch []types.InputLogEvent
	var batchSize int
	defer c.wg.Done()

	sendPayloads := func() {
		c.sendBatch(ctx, batch, 0)
		batchSize = 0
		batch = nil
	}

	for {
		select {
		case <-ctx.Done():
			sendPayloads()
			return
		case event, ok := <-c.payloads:
			// close call
			if !ok {
				sendPayloads()
				return
			}

			// flush call
			if event == flushEvent {
				sendPayloads()
				continue
			}

			messageSize := len(*event.Message) + AdditionalBytesPerLogEvent
			// Make sure the time is monotonic - the input time is ignored.
			// AWS expects the timestamp to be in milliseconds since the epoch.
			event.Timestamp = aws.Int64(time.Now().UTC().UnixMilli())
			batch = append(batch, event)
			batchSize += messageSize
			c.Stats.QueuedEventCount.Add(1)

			if batchSize > BatchSizeLimit {
				sendPayloads()
			}

			if len(batch) >= MaxNumLogEvents {
				sendPayloads()
			}

		case <-ticker:
			sendPayloads()
		}
	}
}

// Only allow 1 retry of an invalid sequence token.
func (c *CloudWatchWriter) sendBatch(ctx context.Context, batch []types.InputLogEvent, retryNum int) {
	if retryNum > 1 || len(batch) == 0 {
		return
	}

	input := &cloudwatchlogs.PutLogEventsInput{
		LogEvents:     batch,
		LogGroupName:  c.logGroupName,
		LogStreamName: c.logStreamName,
		SequenceToken: c.nextSequenceToken,
	}

	output, err := c.client.PutLogEvents(ctx, input)

	if err != nil {
		if invalidSequenceTokenErr, ok := err.(*types.InvalidSequenceTokenException); ok {
			c.Stats.RetryCount.Add(1)
			c.nextSequenceToken = invalidSequenceTokenErr.ExpectedSequenceToken
			c.sendBatch(ctx, batch, retryNum+1)
			return
		}
		c.lastErr.set(err)
		return
	}

	c.Stats.BatchCount.Add(1)
	c.Stats.SentEventCount.Add(uint64(len(batch)))
	c.nextSequenceToken = output.NextSequenceToken
}

// Flush will cause the logger to flush the current buffer. It does not block, there is no
// guarantee that the buffer will be flushed immediately. Use Close in order to properly
// close during application termination.
func (c *CloudWatchWriter) Flush() {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	c.payloads <- flushEvent
}

// Close will flush the buffer, close the channel and wait until all payloads
// are sent, not longer than 2 seconds. It is safe to call close multiple times.
// After close is called the client will not accept any new events, all attemtps
// to send new events will return ErrFullOrClosed. Use CloseWithTimeout to
// specify a custom timeout.
func (c *CloudWatchWriter) Close() error {
	return c.CloseWithTimeout(2 * time.Second)
}

var ErrCloseTimeout = errors.New("close timeout reached")

// Close will flush the buffer, close the channel and wait until all payloads
// are sent, not longer than specified amount of time. It is safe to call close
// multiple times. After close is called the client will not accept any new
// events, all attemtps to send new events will return ErrFullOrClosed.
//
// Returns ErrCloseTimeout if the timeout was reached before the close could be
// completed.
func (c *CloudWatchWriter) CloseWithTimeout(timeout time.Duration) error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	var result error
	c.closeOnce.Do(func() {
		close(c.payloads)

		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(timeout):
			result = ErrCloseTimeout
		}
	})

	return result
}

// getOrCreateLogStream gets info on the log stream for the log group and log
// stream we're interested in -- primarily for the purpose of finding the value
// of the next sequence token. If the log group doesn't exist, then we create
// it, if the log stream doesn't exist, then we create it.
func (c *CloudWatchWriter) getOrCreateLogStream(ctx context.Context) (*types.LogStream, error) {
	// Get the log streams that match our log group name and log stream
	output, err := c.client.DescribeLogStreams(ctx, &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        c.logGroupName,
		LogStreamNamePrefix: c.logStreamName,
	})
	if err != nil {
		// i.e. the log group does not exist
		if _, ok := err.(*types.ResourceNotFoundException); ok {
			_, err = c.client.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
				LogGroupName: c.logGroupName,
			})
			if err != nil {
				// In case another process created the log group in the meantime
				if _, ok := err.(*types.ResourceAlreadyExistsException); !ok {
					return nil, fmt.Errorf("cloudwatchlog.Client.CreateLogGroup: %w", err)
				}
			}
			return c.getOrCreateLogStream(ctx)
		}

		return nil, fmt.Errorf("cloudwatchlogs.Client.DescribeLogStreams: %w", err)
	}

	if len(output.LogStreams) > 0 {
		return &output.LogStreams[0], nil
	}

	// No matching log stream, so we need to create it
	_, err = c.client.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  c.logGroupName,
		LogStreamName: c.logStreamName,
	})
	if err != nil {
		return nil, fmt.Errorf("cloudwatchlogs.Client.CreateLogStream: %w", err)
	}

	// We can just return an empty log stream as the initial sequence token would be nil anyway.
	return &types.LogStream{}, nil
}
