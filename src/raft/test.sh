
go test -run PartA | tee out.txt
go test -run PartA -race
$env:VERBOSE = 0
python dslogs.py -c 3 -t ../raft/out.txt