TARGET=$(GOPATH)/bin/yahoo-free-csv-poller.so

$(TARGET): yahoo-free-csv-poller.go
	go build -buildmode=plugin -o $(TARGET)

testing: $(TARGET)
	cd test;go run main.go
clean:
	rm -f $(TARGET)
