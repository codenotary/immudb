FROM golang:1.20
RUN apt-get update && apt-get install -y netcat patch

WORKDIR /src/immudb
COPY go.mod go.sum /src/
RUN go mod download -x
COPY . .
RUN rm -rf /src/webconsole/dist && patch -Np1 < test/e2e/t0.patch
RUN GOOS=linux GOARCH=amd64 make immuadmin-static immudb-static immuclient-static
RUN GOOS=linux GOARCH=amd64 go build -C test/e2e/truncation

WORKDIR /src
RUN git clone https://github.com/codenotary/immudb-tools.git /src/tools
RUN GOOS=linux GOARCH=amd64 go build -C /src/tools/stresser2
