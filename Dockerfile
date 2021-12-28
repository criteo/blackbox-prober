FROM alpine:3.7
COPY ./build/aerospike_probe /build/aerospike_probe
EXPOSE 8080
ENTRYPOINT [ "/build/aerospike_probe" ]
