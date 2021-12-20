#!/usr/bin/env bash

# We are evaluating all files for now (later we could change it so it only uses changed files)
# pkgs_to_test=`git diff --name-status $diff |  grep -E '^(A|M|R)'  | awk '{ print $NF }' | grep '\.go$' | xargs -n1 dirname | sort -u`
pkgs_to_test=`find . -type f | grep -E ".go$" | xargs -n1 dirname | sort -u`


function parse_test {

    exit_code=0
    while read line; do
        if [[ `echo "$line" | grep -cE '\-+ FAIL'` -gt 0 ]]
        then
            echo -e "\e[0;31m$line\e[0;0m"
            exit_code=1
        else
            echo "$line"
        fi
    done <"$test_file"

    return $exit_code
}

function parse_coverage {
    exit_code=0
    output=`go tool cover -func=$coverage_file`
    while read line; do
        if [[ `echo "$line" | grep -cE '\b([0-7][0-9]|[0-9])\.[0-9]+%'` -gt 0 ]] && [[ `echo "$line" | grep -cE '\s(init|main|\(statements\))\s'` -eq 0 ]] && [[ `echo "$line" | grep -cE '\s\S+SC\s'` -eq 0 ]]
        then
            echo -e "\e[0;31m$line\e[0;0m"
            exit_code=1
        else
            echo -e "\e[0;32m$line\e[0;0m"
        fi
    done <<<"$output"

    return $exit_code
}

for pkg in $pkgs_to_test
do
    echo "# Checking $pkg"

    coverage_dir=.coverage/$pkg
    coverage_file=$coverage_dir/coverage.out
    mkdir -p $coverage_dir
    timeout=160s

    test_file=$coverage_dir/test.out
    go test -gcflags=all=-d=checkptr=0 -race -cover -coverprofile=$coverage_file -timeout=$timeout -short $pkg &>> $test_file

    parse_test
    exit_code=$?

    parse_coverage
    
    if [[ $? == 1 ]]; then
        exit_code=1
    fi

done

# Clean up
rm -r .coverage/

exit $exit_code
