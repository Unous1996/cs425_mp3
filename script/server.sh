port=7000
cd cmd/server
for i in {1..5}
do
	newport=`expr $port + $i`
	./server $newport &
done
