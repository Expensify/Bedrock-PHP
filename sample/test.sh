#!/bin/bash
# This is a trival test to demonstrate Bedrock::Jobs

# -----------------------
echo "Confirming bedrock is running"
BEDROCK_PID=`pgrep bedrock`
if [ -z "$BEDROCK_PID" ]
then
    echo "Please start bedrock, eg: sudo ./bedrock -clean -fork"
    exit
fi

# -----------------------
echo "Clean up after the last test"
RESULT=`echo 'Query
query: DELETE FROM jobs;
connection: close

' | nc localhost 8888 | head -n 1`
if [[ "$RESULT" != 200* ]]
then
    echo "ERROR: Cleanup failed ($RESULT)"
    exit
fi


# -----------------------
echo 'Creating 2 jobs...'
echo "CreateJob
name: SampleWorker

CreateJob
name: SampleWorker
connection: close

" | nc localhost 8888 > /dev/null
sleep 1

# -----------------------
echo "Confirming jobs are QUEUED"
COUNT=`echo "Query: SELECT COUNT(*) FROM jobs;
connection: close

" | nc localhost 8888 | tail -n 1`
if [ "$COUNT" != 2 ]
then
    echo "ERROR: Failed to queue jobs (count=$COUNT)"
    exit
fi

# -----------------------
# Simulate a broken primary server just to test the failover
echo "Starting BWM..."
php ../bin/BedrockWorkerManager.php --workerPath=. --host=0.0.0.0 --port=1234 --failoverHost=localhost --failoverPort=8888 &
PID=$!

# -----------------------
while [ "$COUNT" != 0 ]
do
echo "Waiting for job to finish"
COUNT=`echo "Query: SELECT COUNT(*) FROM jobs;
connection: close

" | nc localhost 8888 | tail -n 1`
sleep 1
done

# -----------------------
echo "Done"
kill $PID
