var N = 100000
function test() {
    var startTime = new Date
    for(var i=0; i<N; i++) {
        db.test.insert({key: i.toString(), value: "01234567"})
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