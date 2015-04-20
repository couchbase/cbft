# For cbft developers

## Dev environment setup

    go get -u github.com/couchbaselabs/cbft/...
    cd $GOPATH/src/github.com/couchbaselabs/cbft
    make prereqs

## Building cbft

    make

## Unit tests

    make test

## Coverage reports

To get local coverage reports with heatmaps...

    make coverage

To get more coverage reports that include dependencies like the bleve library...

    go test -coverpkg github.com/couchbaselabs/cbft,github.com/blevesearch/bleve,github.com/blevesearch/bleve/index \
        -coverprofile=coverage.out \
        -covermode=count && \
    go tool cover -html=coverage.out

## Documentation

Generating documentation...

We use the [MkDocs](http://mkdocs.org) tool to help generate an HTML
docs website from the markdown files in the ```./docs``` subdirectory.

To generate the REST API markdown documentation...

    make gen-docs

For a local development testing web server that automatically
re-generates the HTML website of ```./docs``` changes, run...

    mkdocs serve

Then browse to http://127.0.0.1:8000 to see the HTML docs website.

To deploy the HTML docs website to github's gh-pages, run...

    mkdocs gh-deploy

## Coding conventions

You must pass ```go fmt```.

Error message conventions...

In the cbft project, fmt.Errorf() and log.Printf() messages follow a
rough formatting convention, like...

    <source_file_base_name>: <short static msg string>, <arg0>: <val0>, <arg1>: <val1>

The "short static msg string" should be unique enough so that ```git grep```
works well.

## Contributing fixes/improvements

We require a contributor license agreement (CLA) to be signed before
we can merge any pull requests.  To sign this agreement, please
register at the [couchbase code review
site](http://review.couchbase.org/). The cbft project currently does
not use this code review app, but it is still used to track acceptance
of the CLA.

All types of contributions are welcome, but please keep the following
in mind:

Existing tests should continue to pass ("make test"), and new tests
for contributions are quite nice to have.

All code should have pass "go fmt ./..." and "go vet ./...".

## Releasing

To do a full release with a new (semver) tag, see the Makefile's
"release" target.
