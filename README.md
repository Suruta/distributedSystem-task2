# Distributed System with gRPC

## commands

compile:
```cmd
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```
run server:
```cmd
python server.py --port 50052
```
send request:
```cmd
python client.py --target localhost:50052 incr likes:post-42 --by 5
python client.py --target localhost:50052 get likes:post-42
```

## script senarios

run three replicas:
```cmd
python server.py --port 50051 --name replica-A
```