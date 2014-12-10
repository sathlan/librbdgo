#!/usr/bin/env bats
# -*- mode: sh -*-
. /etc/profile.d/golang.sh
. /etc/profile.d/ceph.sh

cd /vagrant

@test "Check format: gofmt -d -e -l ." {
    run gofmt -d -e -l .
    [ ${#lines[*]}  -eq 0 ]
}

@test "Get dependancies: go get -d -v ./..." {
    run go get -d -v ./...
    [ $status -eq 0 ]
}

@test "Build: go build -v ./..." {
    run go build -v ./...
    [ $status -eq 0 ]
}


@test "Run all tests: go test -v ./..." {
    run go test -v ./...
    [ $status -eq 0 ]
}

@test "Coverage >=61%: go test -v -cover ./..." {
    run go test -cover -v ./...
    echo "$output" | awk '/coverage/{if($2 < 61){exit 1}}'
}
