#!/bin/bash
# This is a trival script to demonstrate Bedrock::Jobs

# -----------------------
echo "Confirming bedrock is running"
BEDROCK_PID=`pgrep bedrock`
if [ -z "$BEDROCK_PID" ]
then
    echo "Please start bedrock, eg: sudo ./bedrock -clean -fork"
    exit
fi

# -----------------------
echo "Clean up after the last demo"
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
echo 'Creating a SampleWorker job...'
echo "CreateJob
name: SampleWorker
connection: close

" | nc localhost 8888 > /dev/null
sleep 1

# -----------------------
echo "Confirming job is QUEUED"
COUNT=`echo "Query: SELECT COUNT(*) FROM jobs;
connection: close

" | nc localhost 8888 | tail -n 1`
if [ "$COUNT" != 1 ]
then
    echo "ERROR: Failed to queue job (count=$COUNT)"
    exit
fi

# -----------------------
echo "Starting BWM..."
php ../bin/BedrockWorkerManager.php --workerPath=. &
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
