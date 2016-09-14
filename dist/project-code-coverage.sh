#!/bin/bash

# Originally from bleve/docs/project-code-coverage.sh

echo "mode: count" > acc.out
for Dir in . $(find ./* -maxdepth 10 -type d | grep -v vendor);
do
    if ls $Dir/*.go &> /dev/null;
    then
        returnval=`go test -coverprofile=profile.out -covermode=count $Dir`
        echo ${returnval}
        if [[ ${returnval} != *FAIL* ]]
        then
            if [ -f profile.out ]
            then
                cat profile.out | grep -v "mode: count" >> acc.out
            fi
        else
            exit 1
        fi
    fi
done

cat acc.out | go run dist/merge-coverprofile.go > merged.out

if [ -n "$COVERALLS" ]
then
    export GIT_BRANCH=$TRAVIS_BRANCH
    $HOME/gopath/bin/goveralls -service=travis-ci -coverprofile=merged.out
fi

if [ -n "$COVERHTML" ]
then
    go tool cover -html=merged.out
fi

rm -rf ./acc.out
rm -rf ./profile.out
rm -rf ./merged.out
