package cloudwatchwriter2_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	cloudwatchwriter "github.com/lzap/cloudwatchwriter2"
	"github.com/stretchr/testify/assert"
)

const sequenceToken = "next-sequence-token"

type mockClient struct {
	sync.RWMutex
	putLogEventsShouldError bool
	logEvents               []types.InputLogEvent
	logGroupName            *string
	logStreamName           *string
	expectedSequenceToken   *string
}

func (c *mockClient) DescribeLogStreams(ctx context.Context, input *cloudwatchlogs.DescribeLogStreamsInput, opts ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogStreamsOutput, error) {
	c.RLock()
	defer c.RUnlock()

	if input.LogGroupName == nil {
		msg := "blah"
		return nil, &types.ResourceNotFoundException{Message: &msg}
	}

	var streams []types.LogStream
	if c.logStreamName != nil {
		streams = append(streams, types.LogStream{
			LogStreamName:       c.logStreamName,
			UploadSequenceToken: c.expectedSequenceToken,
		})
	}

	return &cloudwatchlogs.DescribeLogStreamsOutput{
		LogStreams: streams,
	}, nil
}

func (c *mockClient) CreateLogGroup(ctx context.Context, input *cloudwatchlogs.CreateLogGroupInput, opts ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error) {
	c.Lock()
	defer c.Unlock()

	c.logGroupName = input.LogGroupName
	return nil, nil
}

func (c *mockClient) CreateLogStream(ctx context.Context, input *cloudwatchlogs.CreateLogStreamInput, opts ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error) {
	c.Lock()
	defer c.Unlock()

	c.logStreamName = input.LogStreamName
	return nil, nil
}

func (c *mockClient) PutLogEvents(ctx context.Context, putLogEvents *cloudwatchlogs.PutLogEventsInput, opts ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error) {
	c.Lock()
	defer c.Unlock()
	if c.putLogEventsShouldError {
		return nil, errors.New("should error")
	}

	if putLogEvents == nil {
		return nil, errors.New("received nil *cloudwatchlogs.PutLogEventsInput")
	}

	// At the first PutLogEvents c.expectedSequenceToken should be nil, as we
	// set it in this call. If it is not nil then we can compare the received
	// sequence token and the expected one.
	if c.expectedSequenceToken != nil {
		if putLogEvents.SequenceToken == nil || *putLogEvents.SequenceToken != *c.expectedSequenceToken {
			return nil, &types.InvalidSequenceTokenException{
				ExpectedSequenceToken: c.expectedSequenceToken,
			}
		}
	} else {
		c.expectedSequenceToken = aws.String(sequenceToken)
	}

	c.logEvents = append(c.logEvents, putLogEvents.LogEvents...)
	output := &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken: c.expectedSequenceToken,
	}
	return output, nil
}

func (c *mockClient) getLogEvents() []types.InputLogEvent {
	c.RLock()
	defer c.RUnlock()

	logEvents := make([]types.InputLogEvent, len(c.logEvents))
	copy(logEvents, c.logEvents)

	return logEvents
}

func (c *mockClient) setExpectedSequenceToken(token *string) {
	c.Lock()
	defer c.Unlock()

	c.expectedSequenceToken = token
}

func (c *mockClient) waitForLogs(numberOfLogs int, timeout time.Duration) error {
	endTime := time.Now().Add(timeout)
	for {
		if c.numLogs() >= numberOfLogs {
			return nil
		}

		if time.Now().After(endTime) {
			return errors.New("ran out of time waiting for logs")
		}

		time.Sleep(time.Millisecond)
	}
}

func (c *mockClient) numLogs() int {
	c.RLock()
	defer c.RUnlock()

	return len(c.logEvents)
}

type exampleLog struct {
	Time, Message, Filename string
	Port                    uint16
}

type logsContainer struct {
	sync.Mutex
	logs []exampleLog
}

func (l *logsContainer) addLog(log exampleLog) {
	l.Lock()
	defer l.Unlock()

	l.logs = append(l.logs, log)
}

func (l *logsContainer) getLogEvents() ([]types.InputLogEvent, error) {
	l.Lock()
	defer l.Unlock()

	var logEvents []types.InputLogEvent
	for _, log := range l.logs {
		message, err := json.Marshal(log)
		if err != nil {
			return nil, fmt.Errorf("json.Marshal: %w", err)
		}

		logEvents = append(logEvents, types.InputLogEvent{
			Message: aws.String(string(message)),
			// Timestamps for CloudWatch Logs should be in milliseconds since the epoch.
			Timestamp: aws.Int64(time.Now().UTC().UnixNano() / int64(time.Millisecond)),
		})
	}
	return logEvents, nil
}

func helperWriteLogs(t *testing.T, writer io.Writer, logs ...interface{}) {
	for _, log := range logs {
		message, err := json.Marshal(log)
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}
		_, err = writer.Write(message)
		if err != nil {
			t.Fatalf("writer.Write(%s): %v", string(message), err)
		}
	}
}

// assertEqualLogMessages asserts that the log messages are all the same, ignoring the timestamps.
func assertEqualLogMessages(t *testing.T, expectedLogs []types.InputLogEvent, logs []types.InputLogEvent) {
	if !assert.Equal(t, len(expectedLogs), len(logs), "expected to have the same number of logs") {
		return
	}

	for index, log := range logs {
		assert.Equal(t, *expectedLogs[index].Message, *log.Message)
	}
}

func TestCloudWatchWriter(t *testing.T) {
	client := &mockClient{}

	cloudWatchWriter, err := cloudwatchwriter.NewWithClient(client, 200*time.Millisecond, "logGroup", "logStream")
	if err != nil {
		t.Fatalf("NewWithClient: %v", err)
	}
	defer cloudWatchWriter.Close()

	// give the queueMonitor goroutine time to start up
	time.Sleep(time.Millisecond)

	log1 := exampleLog{
		Time:     "2009-11-10T23:00:02.043123061Z",
		Message:  "Test message 1",
		Filename: "filename",
		Port:     666,
	}
	log2 := exampleLog{
		Time:     "2009-11-10T23:00:02.043123061Z",
		Message:  "Test message 2",
		Filename: "filename",
		Port:     666,
	}
	log3 := exampleLog{
		Time:     "2009-11-10T23:00:02.043123061Z",
		Message:  "Test message 3",
		Filename: "filename",
		Port:     666,
	}
	log4 := exampleLog{
		Time:     "2009-11-10T23:00:02.043123061Z",
		Message:  "Test message 4",
		Filename: "filename",
		Port:     666,
	}

	helperWriteLogs(t, cloudWatchWriter, log1, log2, log3, log4)

	logs := logsContainer{}
	logs.addLog(log1)
	logs.addLog(log2)
	logs.addLog(log3)
	logs.addLog(log4)

	expectedLogs, err := logs.getLogEvents()
	if err != nil {
		t.Fatal(err)
	}

	if err = client.waitForLogs(2, 201*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	assertEqualLogMessages(t, expectedLogs, client.getLogEvents())
}

func TestCloudWatchWriterTime(t *testing.T) {
	client := &mockClient{}

	cloudWatchWriter, err := cloudwatchwriter.NewWithClient(client, 200*time.Millisecond, "logGroup", "logStream")
	if err != nil {
		t.Fatalf("NewWithClient: %v", err)
	}
	defer cloudWatchWriter.Close()

	// give the queueMonitor goroutine time to start up
	time.Sleep(time.Millisecond)

	log1 := exampleLog{
		Time:     "2013-05-13T19:00:02.000000000Z",
		Message:  "Test message 1",
		Filename: "filename",
		Port:     666,
	}
	log2 := exampleLog{
		Time:     "2013-05-13T19:00:01.000000000Z",
		Message:  "Test message 2",
		Filename: "filename",
		Port:     666,
	}

	helperWriteLogs(t, cloudWatchWriter, log1, log2)
	if err = client.waitForLogs(2, 201*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	result := client.getLogEvents()
	assert.True(t, *result[0].Timestamp <= *result[1].Timestamp)
}

func TestCloudWatchWriterBatchInterval(t *testing.T) {
	client := &mockClient{}

	_, err := cloudwatchwriter.NewWithClient(client, 199*time.Millisecond, "logGroup", "logStream")
	if err == nil {
		t.Fatal("expected an error")
	}

	cloudWatchWriter, err := cloudwatchwriter.NewWithClient(client, 200*time.Millisecond, "logGroup", "logStream")
	if err != nil {
		t.Fatalf("NewWithClient: %v", err)
	}

	aLog := exampleLog{
		Time:     "2009-11-10T23:00:02.043123061Z",
		Message:  "Test message",
		Filename: "filename",
		Port:     666,
	}

	// The client shouldn't have received any logs at this time
	assert.Equal(t, 0, client.numLogs())

	helperWriteLogs(t, cloudWatchWriter, aLog)
	cloudWatchWriter.Close()

	assert.Equal(t, 1, client.numLogs())
}

// Hit the 1MB limit on batch size of logs to trigger an earlier batch
func TestCloudWatchWriterHit1MBLimit(t *testing.T) {
	client := &mockClient{}

	cloudWatchWriter, err := cloudwatchwriter.NewWithClient(client, 200*time.Millisecond, "logGroup", "logStream")
	if err != nil {
		t.Fatalf("NewWithClient: %v", err)
	}
	defer cloudWatchWriter.Close()

	// give the queueMonitor goroutine time to start up
	time.Sleep(time.Millisecond)

	logs := logsContainer{}
	numLogs := 9999
	for i := 0; i < numLogs; i++ {
		aLog := exampleLog{
			Time:     "2009-11-10T23:00:02.043123061Z",
			Message:  fmt.Sprintf("longggggggggggggggggggggggggggg test message %d", i),
			Filename: "/home/deadpool/src/github.com/superlongfilenameblahblahblahblah.txt",
			Port:     666,
		}
		helperWriteLogs(t, cloudWatchWriter, aLog)
		logs.addLog(aLog)
	}

	// Main assertion is that we are triggering a batch early as we're sending
	// so much data
	assert.True(t, client.numLogs() > 0)

	if err = client.waitForLogs(numLogs, 400*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	expectedLogs, err := logs.getLogEvents()
	if err != nil {
		t.Fatal(err)
	}

	assertEqualLogMessages(t, expectedLogs, client.getLogEvents())
}

// Hit the 10k limit on number of logs to trigger an earlier batch
func TestCloudWatchWriterHit10kLimit(t *testing.T) {
	client := &mockClient{}

	cloudWatchWriter, err := cloudwatchwriter.NewWithClient(client, 200*time.Millisecond, "logGroup", "logStream")
	if err != nil {
		t.Fatalf("NewWithClient: %v", err)
	}
	defer cloudWatchWriter.Close()

	// give the queueMonitor goroutine time to start up
	time.Sleep(time.Millisecond)

	var expectedLogs []types.InputLogEvent
	numLogs := 10000
	for i := 0; i < numLogs; i++ {
		message := fmt.Sprintf("hello %d", i)
		_, err = cloudWatchWriter.Write([]byte(message))
		if err != nil {
			t.Fatalf("cloudWatchWriter.Write: %v", err)
		}
		expectedLogs = append(expectedLogs, types.InputLogEvent{
			Message:   aws.String(message),
			Timestamp: aws.Int64(time.Now().UTC().UnixNano() / int64(time.Millisecond)),
		})
	}

	// give the queueMonitor goroutine time to catch-up (sleep is far less than
	// the minimum of 200 milliseconds)
	time.Sleep(10 * time.Millisecond)

	// Main assertion is that we are triggering a batch early as we're sending
	// so many logs
	assert.True(t, client.numLogs() > 0)

	if err = client.waitForLogs(numLogs, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	assertEqualLogMessages(t, expectedLogs, client.getLogEvents())
}

func TestCloudWatchWriterParallel(t *testing.T) {
	client := &mockClient{}

	cloudWatchWriter, err := cloudwatchwriter.NewWithClient(client, 200*time.Millisecond, "logGroup", "logStream")
	if err != nil {
		t.Fatalf("NewWithClient: %v", err)
	}
	defer cloudWatchWriter.Close()

	logs := logsContainer{}
	numLogs := 8000
	for i := 0; i < numLogs; i++ {
		go func() {
			aLog := exampleLog{
				Time:     "2009-11-10T23:00:02.043123061Z",
				Message:  "Test message",
				Filename: "filename",
				Port:     666,
			}
			logs.addLog(aLog)
			helperWriteLogs(t, cloudWatchWriter, aLog)
		}()
	}

	// allow more time as there are a lot of goroutines to set off!
	if err = client.waitForLogs(numLogs, 4*time.Second); err != nil {
		t.Fatal(err)
	}

	expectedLogs, err := logs.getLogEvents()
	if err != nil {
		t.Fatal(err)
	}

	assertEqualLogMessages(t, expectedLogs, client.getLogEvents())
}

func TestCloudWatchWriterClose(t *testing.T) {
	client := &mockClient{}

	cloudWatchWriter, err := cloudwatchwriter.NewWithClient(client, 200*time.Millisecond, "logGroup", "logStream")
	if err != nil {
		t.Fatalf("NewWithClient: %v", err)
	}
	defer cloudWatchWriter.Close()

	// The logs shouldn't have come through yet
	assert.Equal(t, 0, client.numLogs())

	numLogs := 99
	for i := 0; i < numLogs; i++ {
		aLog := exampleLog{
			Time:     "2009-11-10T23:00:02.043123061Z",
			Message:  fmt.Sprintf("Test message %d", i),
			Filename: "filename",
			Port:     666,
		}
		helperWriteLogs(t, cloudWatchWriter, aLog)
	}

	// Close should block until the queue is empty
	cloudWatchWriter.Close()

	// The logs should have all come through now
	assert.Equal(t, numLogs, client.numLogs())
}

func TestCloudWatchWriterReportError(t *testing.T) {
	client := &mockClient{
		putLogEventsShouldError: true,
	}

	cloudWatchWriter, err := cloudwatchwriter.NewWithClient(client, 200*time.Millisecond, "logGroup", "logStream")
	if err != nil {
		t.Fatalf("NewWithClient: %v", err)
	}
	defer cloudWatchWriter.Close()

	// give the queueMonitor goroutine time to start up
	time.Sleep(time.Millisecond)

	log1 := exampleLog{
		Time:     "2009-11-10T23:00:02.043123061Z",
		Message:  "Test message 1",
		Filename: "filename",
		Port:     666,
	}

	helperWriteLogs(t, cloudWatchWriter, log1)

	// sleep until the batch should have been sent
	time.Sleep(201 * time.Millisecond)

	_, err = cloudWatchWriter.Write([]byte("hello world"))
	if err == nil {
		t.Fatal("expected the last error from PutLogEvents to appear here")
	}
}

func TestCloudWatchWriterReceiveInvalidSequenceTokenException(t *testing.T) {
	// Setup
	client := &mockClient{}

	cloudWatchWriter, err := cloudwatchwriter.NewWithClient(client, 200*time.Millisecond, "logGroup", "logStream")
	if err != nil {
		t.Fatalf("NewWithClient: %v", err)
	}
	defer cloudWatchWriter.Close()

	// give the queueMonitor goroutine time to start up
	time.Sleep(time.Millisecond)

	// At this point the cloudWatchWriter should have the normal sequence token.
	// So we change the mock cloudwatch client to expect a different sequence
	// token:
	client.setExpectedSequenceToken(aws.String("new sequence token"))

	// Action
	logs := logsContainer{}
	log := exampleLog{
		Time:     "2009-11-10T23:00:02.043123061Z",
		Message:  "Test message 1",
		Filename: "filename",
		Port:     666,
	}

	helperWriteLogs(t, cloudWatchWriter, log)
	logs.addLog(log)

	// Result
	if err = client.waitForLogs(1, 201*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	expectedLogs, err := logs.getLogEvents()
	if err != nil {
		t.Fatal(err)
	}
	assertEqualLogMessages(t, expectedLogs, client.getLogEvents())
}

func TestCloudWatchWriterSendOnClose(t *testing.T) {
	for i := 0; i < 100; i++ {
		client := &mockClient{}
		cloudWatchWriter, err := cloudwatchwriter.NewWithClient(client, 200*time.Millisecond, "logGroup", "logStream")
		if err != nil {
			t.Fatalf("NewWithClient: %v", err)
		}

		// give the queueMonitor goroutine time to start up
		time.Sleep(time.Millisecond)

		numLogs := 100
		expectedLogs := make([]types.InputLogEvent, numLogs)
		for j := 0; j < numLogs; j++ {
			message := fmt.Sprintf("hello %d", j)
			_, err = cloudWatchWriter.Write([]byte(message))
			if err != nil {
				t.Fatalf("cloudWatchWriter.Write: %v", err)
			}
			expectedLogs[j] = types.InputLogEvent{
				Message:   aws.String(message),
				Timestamp: aws.Int64(time.Now().UTC().UnixNano() / int64(time.Millisecond)),
			}
		}

		startTime := time.Now()
		cloudWatchWriter.Close()
		duration := time.Since(startTime)
		if duration >= 200*time.Millisecond {
			t.Fatal("close sends all the messages straight away so should not have to wait for the next batch")
		}
		assertEqualLogMessages(t, expectedLogs, client.getLogEvents())
	}
}
