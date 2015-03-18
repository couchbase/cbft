#!/bin/sh -e

project=github.com/couchbaselabs/cbft
projectCmd=$project/cmd/cbft
top=`go list -f '{{.Dir}}' $project`
version=`git describe`

cd $top

DIST=$top/dist

testProject() {
    go test $project/...
    go vet $project/...
}

emitVersion() {
    echo "{\"version\": \"$version\"}" > $DIST/version.json
}

build() {
    goflags="-v -ldflags '-X main.VERSION $version' -tags debug"

    eval env GOOS=linux   GOARCH=386         go build $goflags -o $DIST/cbft.linux.386 $projectCmd &
    eval env GOOS=linux   GOARCH=arm         go build $goflags -o $DIST/cbft.linux.arm $projectCmd &
    eval env GOOS=linux   GOARCH=arm GOARM=5 go build $goflags -o $DIST/cbft.linux.arm5 $projectCmd &
    eval env GOOS=linux   GOARCH=amd64       go build $goflags -o $DIST/cbft.linux.amd64 $projectCmd &
    eval env GOOS=freebsd GOARCH=amd64       go build $goflags -o $DIST/cbft.freebsd.amd64 $projectCmd &
    eval env GOOS=windows GOARCH=386         go build $goflags -o $DIST/cbft.windows.386.exe $projectCmd &
    eval env GOOS=windows GOARCH=amd64       go build $goflags -o $DIST/cbft.windows.amd64.exe $projectCmd &
    eval env GOOS=darwin  GOARCH=amd64       go build $goflags -o $DIST/cbft.darwin.amd64 $projectCmd &

    wait
}

compress() {
    rm -f $DIST/cbft.*.gz || true

    for i in $DIST/cbft.*; do
        gzip -9v $i &
    done

    wait
}

testProject
emitVersion
build
compress
