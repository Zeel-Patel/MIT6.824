#!/usr/bin/env bash

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

TIMEOUT=
(cd ../../mrapps && go clean)
(cd .. && go clean)
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin grep.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

rm -f mr-out*

echo '***' Starting wc test.

timestamp=$(date +"%T")
echo $timestamp
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
timestamp=$(date +"%T")
echo $timestamp

$TIMEOUT ../mrcoordinator ../pg*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1
timestamp=$(date +"%T")
echo $timestamp
# start multiple workers.
$TIMEOUT ../mrworker ../../mrapps/wc.so &
$TIMEOUT ../mrworker ../../mrapps/wc.so &
$TIMEOUT ../mrworker ../../mrapps/wc.so &
$TIMEOUT ../mrworker ../../mrapps/wc.so &
$TIMEOUT ../mrworker ../../mrapps/wc.so &
$TIMEOUT ../mrworker ../../mrapps/wc.so &
$TIMEOUT ../mrworker ../../mrapps/wc.so &
$TIMEOUT ../mrworker ../../mrapps/wc.so &

timestamp=$(date +"%T")
echo $timestamp

# wait for the coordinator to exit.
wait $pid

timestamp=$(date +"%T")
echo $timestamp