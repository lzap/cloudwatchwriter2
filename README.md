# cloudwatchwriter2 aka slog-cloudwatch

A robust Zerolog or slog writer for AWS CloudWatch using Go SDK v2. It can be used for writing logs from any kind of logging library as it implements `io.Writer` interface. Each call of the `Write` method contain a byte slice payload with valid JSON, longer payloads must be sent in a single call.

This library assumes that you have IAM credentials to allow you to talk to AWS CloudWatch Logs.
The specific permissions that are required are:
- CreateLogGroup,
- CreateLogStream,
- DescribeLogStreams,
- PutLogEvents.

If these permissions aren't assigned to the user who's IAM credentials you're using then this package will not work.
There are two exceptions to that:
- if the log group already exists, then you don't need permission to CreateLogGroup;
- if the log stream already exists, then you don't need permission to CreateLogStream.

## Usage with log/slog

Use the high-performance built-in JSON handler from Go library to directly write data.

```
aws_region := os.Getenv("AWS_REGION")
aws_key := os.Getenv("AWS_KEY")
aws_secret := os.Getenv("AWS_SECRET")
aws_session := os.Getenv("AWS_SESSION")

options := cloudwatchlogs.Options{
    Region:      aws_region,
    Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(aws_key, aws_secret, aws_session)),
}
client := cloudwatchlogs.New(options)

w, err := cww.NewWithClient(client, 500*time.Millisecond, logGroupName, logStreamName)
h := slog.NewJSONHandler(w, &slog.HandlerOptions{AddSource: true})
slog.SetDefault(slog.New(h))

slog.Info("this is a message)
```

## Usage with Zerolog

See [the example](internal/example/example.go).

Make sure to close the writer to flush the queue, you can `defer` the `Close()` call in main.
The `Close()` function blocks until all the logs have been processed.

The library was tested with 1.18 as it uses the new `go.work` feature for the example, but it would work with any Go version supported by Zerolog and AWS SDK v2.

### Write to CloudWatch and the console

What I personally prefer is to write to both CloudWatch and the console, e.g.

```
log.Logger := zerolog.New(zerolog.MultiLevelWriter(consoleWriter, cloudWatchWriter)).With().Timestamp().Logger()
```

### Changing the default settings

#### Batch interval

The logs are sent in batches because AWS has a maximum of 5 [PutLogEvents requests per second](https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html) per log stream.
The default value of the batch period is 5 seconds, which means it will send the a batch of logs at least once every 5 seconds.
Batches of logs will be sent earlier if the size of the collected logs exceeds 1MB (another AWS restriction).
To change the batch frequency, you can set the time interval between batches to a smaller or larger value, e.g. 1 second:

```
err := cloudWatchWriter.SetBatchInterval(time.Second)
```

If you set it below 200 milliseconds it will return an error.

The batch interval is not guaranteed as two things can alter how often the batches get delivered:
- as soon as 1MB of logs or 10k logs have accumulated, they are sent (due to AWS restrictions on batch size);
- we have to send the batches in sequence (an AWS restriction) so a long running request to CloudWatch can delay the next batch.

## LICENSE

MIT

## Acknowledgements

The original library was written by mac07 (https://github.com/mec07/cloudwatchwriter), I upgraded it to SDK v2. The work is based on logrus implementation (https://github.com/kdar/logrus-cloudwatchlogs) and a gist (https://gist.github.com/asdine/f821abe6189a04250ae61b77a3048bd9). Thanks all!

In 2025, I have decided to rewrite the whole queueing functionality as it turned out the original code had some issues with performance. The maximum bandwidth was 1000 events per second. I cleaned up the code, added statistics, simplified and updated dependencies. Finally, added some information on how to use the library with `log/slog`.
