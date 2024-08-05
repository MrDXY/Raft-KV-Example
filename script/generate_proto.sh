#!/usr/bin/env bash

protoc --gogofast_out=../pkg/pb \
  --proto_path=../pkg/pb \
  --proto_path=/Users/dxinyuan/go/pkg/mod/github.com/gogo/protobuf@v1.3.2/ \
  ../pkg/pb/walpb/record.proto