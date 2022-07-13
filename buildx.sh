#!/bin/sh

REPO=gcr.io/astute-synapse-332322
#REPO=zauberhaus

TAG=1.3.1-mtls #`git describe --tags --always --dirty | sed -e 's/^v//'`
IMAGE=${REPO}/immudb
IMAGE2=${REPO}/immuclient
IMAGE3=${REPO}/immuadmin
PLATFORMS=linux/amd64,linux/arm64
#PLATFORMS=linux/arm64

echo "Build tag $TAG"

docker buildx build --platform=$PLATFORMS -t $IMAGE2 -t $IMAGE2:$TAG -f Dockerfile.immuclient --output type=image,push=true  . 
docker buildx build --platform=$PLATFORMS -t $IMAGE3 -t $IMAGE3:$TAG -f Dockerfile.immuadmin --output type=image,push=true  . 
docker buildx build --platform=$PLATFORMS -t $IMAGE -t $IMAGE:$TAG --target scratch --output type=image,push=true  . 
#docker buildx build --platform=$PLATFORMS -t $IMAGE -t $IMAGE:$TAG --target scratch --output type=image  . 
