ARG ERLANG_VERSION=28.0.2.0
ARG GLEAM_VERSION=v1.13.0

FROM ghcr.io/gleam-lang/gleam:${GLEAM_VERSION}-scratch AS gleam

FROM erlang:${ERLANG_VERSION}-alpine AS build
RUN apk add --update gcc make g++ zlib-dev
COPY --from=gleam /bin/gleam /bin/gleam
COPY . /app/
RUN cd /app && gleam export erlang-shipment

FROM erlang:${ERLANG_VERSION}-alpine
RUN apk add --update bind-tools curl sqlite
COPY --from=build /app/build/erlang-shipment /app
COPY ./fly-start.sh /app/fly-start.sh
RUN chmod +x /app/fly-start.sh
WORKDIR /app
ENTRYPOINT [ "/app/fly-start.sh" ]
CMD ["run"]
