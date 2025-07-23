module github.com/lzap/cloudwatchwriter2/internal/example

go 1.22

toolchain go1.23.6

replace github.com/lzap/cloudwatchwriter2 => ../..

require (
	github.com/aws/aws-sdk-go-v2 v1.36.6
	github.com/aws/aws-sdk-go-v2/credentials v1.17.71
	github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs v1.53.1
	github.com/lzap/cloudwatchwriter2 v0.0.0-00010101000000-000000000000
	github.com/rs/zerolog v1.33.0
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.37 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.37 // indirect
	github.com/aws/smithy-go v1.22.4 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/sys v0.29.0 // indirect
)
