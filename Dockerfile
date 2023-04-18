FROM golang:1.20-bullseye
EXPOSE 8080

WORKDIR /bfserver

RUN curl -o evergreen 'https://evergreen.mongodb.com/clients/linux_amd64/evergreen' && \
    chmod +x evergreen && \
    mkdir -p ./cache
ENV PATH="/bfserver:$PATH"
ENV EVG_USER="$EVG_USER"
ENV EVG_API_KEY="$EVG_API_KEY"

ADD . .

RUN go build cmd/bfserver.go

CMD ["consumer.sh"]
