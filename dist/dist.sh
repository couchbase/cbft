#!/bin/sh -e

project=github.com/couchbaselabs/cbft

projectCmd=$project/cmd/cbft

version=`git describe --long`

top=`go list -f '{{.Dir}}' $project`

cd $top

DIST=$top/dist

mkdir -p $DIST/out

testProject() {
    go test $project/...
    go vet $project/...
}

emitVersion() {
    echo $version > $DIST/out/version.txt
}

emitManifest() {
    ./dist/go-manifest > $DIST/out/manifest.txt
}

build() {
    goflags="-v -ldflags '-X main.VERSION $version' -tags debug"

    eval env GOOS=linux   GOARCH=386         go build $goflags -o $DIST/out/cbft.linux.386 $projectCmd &
    eval env GOOS=linux   GOARCH=arm         go build $goflags -o $DIST/out/cbft.linux.arm $projectCmd &
    eval env GOOS=linux   GOARCH=arm GOARM=5 go build $goflags -o $DIST/out/cbft.linux.arm5 $projectCmd &
    eval env GOOS=linux   GOARCH=amd64       go build $goflags -o $DIST/out/cbft.linux.amd64 $projectCmd &
    eval env GOOS=freebsd GOARCH=amd64       go build $goflags -o $DIST/out/cbft.freebsd.amd64 $projectCmd &
    eval env GOOS=windows GOARCH=386         go build $goflags -o $DIST/out/cbft.windows.386.exe $projectCmd &
    eval env GOOS=windows GOARCH=amd64       go build $goflags -o $DIST/out/cbft.windows.amd64.exe $projectCmd &
    eval env GOOS=darwin  GOARCH=amd64       go build $goflags -o $DIST/out/cbft.darwin.amd64 $projectCmd &

    wait
}

compress() {
    rm -f $DIST/out/cbft.*.gz || true

    for i in $DIST/out/cbft.*; do
        gzip -9v $i &
    done

    wait
}

testProject
emitVersion
emitManifest
build
compress
