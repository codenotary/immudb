# Chacha20Poly1305

This repository implements the ChaCha20Poly1305 AEAD construction. ChaCha20Pol1305 is combination
of the [ChaCha20 stream cipher](https://github.com/aead/chacha20) and the [Poly1305 authenticator](https://github.com/aead/poly1305) and is standardized in [RFC 7539](https://tools.ietf.org/html/rfc7539).

This repository also implements 8 and 24 byte variants additional to the IETF 12 byte version of ChaCha20Poly1305. Further this package provides a streaming API to wrap an [io.Writer](https://golang.org/pkg/io/#Writer) with an en/decrypting [io.WriteCloser](https://golang.org/pkg/io/#WriteCloser). 

### Recommendations

It is recommended to use the [official Chacha20Poly1305 implementation](https://godoc.org/golang.org/x/crypto/chacha20poly1305) if possible.  
This repository just provides additional functionality like 8 and 24 byte variants and a streaming API.

### Install

Install in your GOPATH: `go get -u github.com/aead/chacha20poly1305`  
Please notice, that the amd64 AVX2 asm implementation requires go1.7 or newer.

### Performance

**AMD64**  
Hardware: Intel i7-6500U 2.50GHz x 2  
System: Linux Ubuntu 16.04 - kernel: 4.8.0-54-generic  
Go version: 1.8.1
```
AVX2

name                       speed                cpb
Chacha20Poly1305Open_64-4   131MB/s ± 0%        18.20
Chacha20Poly1305Seal_64-4   134MB/s ± 0%        17.79
Chacha20Poly1305Open_1K-4   830MB/s ± 0%         2.87
Chacha20Poly1305Seal_1K-4   839MB/s ± 0%         2.84
Chacha20Poly1305Open_8K-4  1.15GB/s ± 0%         2.03
Chacha20Poly1305Seal_8K-4  1.15GB/s ± 0%         2.03
EncryptedWriter64-4         392MB/s ± 0%         6.08
DecryptedWriter64-4         255MB/s ± 0%         9.35
EncryptedWriter1K-4        1.97GB/s ± 1%         1.18
DecryptedWriter1K-4        1.70GB/s ± 0%         1.37
EncryptedWriter8K-4        2.23GB/s ± 0%         1.04
DecryptedWriter8K-4        2.17GB/s ± 1%         1.07


SSSE3

name                       speed                cpb
Chacha20Poly1305Open_64-4   133MB/s ± 1%        17.93
Chacha20Poly1305Seal_64-4   137MB/s ± 0%        17.40
Chacha20Poly1305Open_1K-4   626MB/s ± 0%         3.80
Chacha20Poly1305Seal_1K-4   629MB/s ± 0%         3.79
Chacha20Poly1305Open_8K-4   788MB/s ± 0%         3.03
Chacha20Poly1305Seal_8K-4   790MB/s ± 0%         3.02
EncryptedWriter64-4         397MB/s ± 0%         6.01
DecryptedWriter64-4         263MB/s ± 1%         9.07
EncryptedWriter1K-4        1.10GB/s ± 1%         2.12
DecryptedWriter1K-4        1.01GB/s ± 1%         2.31 
EncryptedWriter8K-4        1.18GB/s ± 0%         1.97
DecryptedWriter8K-4        1.16GB/s ± 0%         2.01
```