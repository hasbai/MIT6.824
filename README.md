# MIT 6.824 Lab

Modified to feat windows.

## Usage
first cd to the directory src
```bash
cd src
```

Run map reduce sequential
```bash
go run main/main.go -race sequential
```
  
Run map reduce coordinator
```bash
go run main/main.go -race coordinator
```

Run map reduce worker(three a time)
```bash
go run main/main.go -race worker
```

Test map reduce
```bash
go test 6.824/mr
```