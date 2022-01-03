# Streams

Immudb provides stream capabilities. Internally it uses “delimited” messages technique, every chunk has a trailer that describe the length of the message. In this way the receiver can recompose chunk by chunk the original payload. Stream methods accepts a `readers` as a part of input and output arguments. In this way the large value is decomposed in small chunks that are streamed over the wire. Client don't need to allocate the entire value when sending and can read the received one progressively. For example a client could send a large file much greater than available ram memory.

> At the moment `immudb` is not yet able to write the data without allocating the entire received object, but in the next release it will be possible a complete communication without allocations. The maximum size of a transaction sent with streams is temporarily limited to a payload of 32M.

Supported stream method now available in the SDK are:

* StreamSet
* StreamGet
* StreamVerifiedSet
* StreamVerifiedGet
* StreamScan
* StreamZScan
* StreamHistory
* StreamExecAll

#### Chunked reading <a href="#chunked-reading" id="chunked-reading"></a>

It's possible to read returned value chunk by chunk if needed. This grant to the clients capabilities to handle data coming from immudb chunk by chunk
