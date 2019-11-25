var N = 500000
var BATCH_SIZE = 100

function test() {
    var startTime = new Date
    for(var i=0; i<N/BATCH_SIZE; i++) {
        var chunck = []
        for (var j=0; j<BATCH_SIZE; j++) {
            chunck[j] = {key: (j*i).toString(), value: "01234567"}
        }
        db.test.insertMany(chunck)
    }
    
    var endTime = new Date

    var elapsed = (endTime.getTime() - startTime.getTime())/1000
    var txSec = (N/elapsed)

    print("Iterations:  " + N)
    print("Elapsed t.:  " + elapsed + " sec")
    print("Throughput:  " + txSec)
}

print("Starting test...")
test()