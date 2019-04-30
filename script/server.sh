port=7000
cd ../cmd/server
go build
for i in {1..5}
do
	newport=`expr $port + $i`
	./server $newport &
done
