package cloudwatchwriter2

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

// Handler is a slog.Handler that sends logs to AWS CloudWatch.
type Handler struct {
	*slog.JSONHandler

	client *CloudWatchWriter
}

// HandlerConfig is the configuration for the Cloudwatch handler.
type HandlerConfig struct {
	// Level is the logging level for this output.
	Level slog.Leveler

	// AddSource is a flag to add source to the log record.
	AddSource bool

	// AWSRegion is the AWS region.
	AWSRegion string

	// AWSKey is the AWS access key.
	AWSKey string

	// AWSSecret is the AWS secret key.
	AWSSecret string

	// AWSSession is an optional AWS session token.
	AWSSession string

	// AWSLogGroup is the AWS CloudWatch log group.
	AWSLogGroup string

	// AWSLogStream is the AWS CloudWatch log stream.
	AWSLogStream string
}

// NewHandler creates a new log/slog handler.
func NewHandler(config HandlerConfig) (*Handler, error) {
	// configure AWS CloudWatch client
	options := cloudwatchlogs.Options{
		Region:      config.AWSRegion,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(config.AWSKey, config.AWSSecret, config.AWSSession)),
	}
	client, err := NewWithClient(cloudwatchlogs.New(options), 500*time.Millisecond, config.AWSLogGroup, config.AWSLogStream)
	if err != nil {
		return nil, fmt.Errorf("cannot create new cloudwatch client: %w", err)
	}

	// configure slog handler
	opts := &slog.HandlerOptions{
		Level:       config.Level,
		AddSource:   config.AddSource,
		ReplaceAttr: replaceAttr,
	}
	cwh := &Handler{
		JSONHandler: slog.NewJSONHandler(client, opts),
		client:      client,
	}

	return cwh, nil
}

func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	// timestamp is added by CloudWatch library automatically
	if groups == nil && a.Key == slog.TimeKey {
		return slog.Attr{}
	}

	return a
}

// Close will flush the buffer, close the channel and wait until all payloads
// are sent, not longer than 2 seconds. It is safe to call close multiple times.
// After close is called the client will not accept any new events, all attemtps
// to send new events will return ErrFullOrClosed. Use CloseWithTimeout to
// specify a custom timeout.
func (h *Handler) Close() {
	if h.client == nil {
		return
	}

	h.client.Close()
}

// Close will flush the buffer, close the channel and wait until all payloads
// are sent, not longer than specified amount of time. It is safe to call close
// multiple times. After close is called the client will not accept any new
// events, all attemtps to send new events will return ErrFullOrClosed.
//
// Returns true if the close was successful, false if the timeout was reached
// before the close could be completed or if the client was already closed.
func (h *Handler) CloseWithTimeout(timeout time.Duration) bool {
	if h.client == nil {
		return false
	}

	return h.client.CloseWithTimeout(timeout)
}

// Flush will cause the logger to flush the current buffer. It does not block, there is no
// guarantee that the buffer will be flushed immediately. Use Close in order to properly
// close during application termination.
func (h *Handler) Flush() {
	if h.client == nil {
		return
	}

	h.client.Flush()
}
