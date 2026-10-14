# Distributed System with gRPC

## commands

compile with:
`python3 -m grpc_tools.protoc \
    -I. \
    --python_out=. \
    --grpc_python_out=. \
    counter.proto`

run server:
```
python server.py
```

run clients:
```
python client.py incr likes:post-42 --by 1
python client.py get likes:post-42
```

### pytest

```
python3 -m pytest -v tests/test_counter.py tests/test_failures.py
```