all:
	CGO_ENABLED=0 installsuffix=cgo go build -o ./cmd/spanner/spanner ./cmd/spanner/main.go
