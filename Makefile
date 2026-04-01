.PHONY: test coverage clean

test:
	go test -v ./tests/...

coverage:
	@echo "Running tests with coverage..."
	@go test -v -p 1 -coverpkg=./internal/... -coverprofile=coverage.out ./tests/... > /dev/null
	@echo "Filtering out generated files..."
	@grep -v ".capnp.go" coverage.out > coverage_filtered.out
	@go tool cover -func=coverage_filtered.out
	@echo ""
	@echo "Coverage report (excluding generated code) generated in coverage_filtered.out"

coverage-html: coverage
	@go tool cover -html=coverage_filtered.out -o coverage.html
	@echo "HTML report generated in coverage.html"

clean:
	rm -f coverage.out coverage_filtered.out coverage.html
