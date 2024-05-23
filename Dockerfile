FROM golang:1.22.0 as build

WORKDIR /

#COPY ./build ./build
COPY ./build/benthos-linux-amd64 benthos
COPY ./streams streams

#CMD [ "benthos" ]
ENTRYPOINT ["/benthos"]

#CMD ["-c", "/config/opctrigger.yaml", "-t", "/templates/*.yaml"]
CMD ["streams", "/streams/*.yaml"]

EXPOSE 4195

#USER benthos

#LABEL org.opencontainers.image.source https://github.com/GirishBhutiya/benthos-umh