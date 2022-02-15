FROM redhat/ubi8-minimal:latest as builder

RUN microdnf install golang make -y

# ADD LOCAL REPO
ADD . /blackbox-prober

WORKDIR /blackbox-prober

RUN make build

FROM redhat/ubi8-minimal:latest

RUN microdnf upgrade -y && \
    microdnf clean all

COPY --from=builder /blackbox-prober/build/aerospike_probe /build/aerospike_probe

EXPOSE 8080

ENTRYPOINT [ "/build/aerospike_probe" ]
