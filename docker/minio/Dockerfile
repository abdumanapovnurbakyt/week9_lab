# MinIO больше не публикует образы ни в Docker Hub, ни в quay.io (сообщество-версия заархивирована),
# поэтому сервер собирается из исходников GitHub на зафиксированном теге - последнем релизе.
# Сборка занимает несколько минут и выполняется один раз.
FROM golang:1.24-alpine AS build
ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z
RUN apk add --no-cache git
WORKDIR /src
RUN git clone --depth 1 --branch "${MINIO_TAG}" https://github.com/minio/minio.git .
RUN CGO_ENABLED=0 go build -trimpath -o /out/minio .

FROM alpine:3.20
RUN adduser -D -u 10001 minio && mkdir /data && chown minio /data
COPY --from=build /out/minio /usr/bin/minio
USER minio
VOLUME /data
EXPOSE 9000 9001
ENTRYPOINT ["/usr/bin/minio"]
CMD ["server", "/data", "--console-address", ":9001"]
