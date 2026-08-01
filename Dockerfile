FROM golang:1.26.5-alpine AS build

# renovate: datasource=github-releases depName=rclone packageName=rclone/rclone
ARG RCLONE_VERSION="1.75.0"
# renovate: datasource=github-releases depName=restic packageName=restic/restic
ARG RESTIC_VERSION="0.19.1"

RUN apk add --no-cache curl unzip

RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then ARCH="amd64"; \
    elif [ "$ARCH" = "aarch64" ]; then ARCH="arm64"; fi && \
    curl -sfSL -o /rclone-v${RCLONE_VERSION}-linux-${ARCH}.zip https://github.com/rclone/rclone/releases/download/v${RCLONE_VERSION}/rclone-v${RCLONE_VERSION}-linux-${ARCH}.zip && \
    unzip -j /rclone-v${RCLONE_VERSION}-linux-${ARCH}.zip -d /usr/local/bin rclone-v${RCLONE_VERSION}-linux-${ARCH}/rclone

RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then ARCH="amd64"; \
    elif [ "$ARCH" = "aarch64" ]; then ARCH="arm64"; fi && \
    curl -sfSL -o /restic-${RESTIC_VERSION}-linux-${ARCH}.bz2 https://github.com/restic/restic/releases/download/v${RESTIC_VERSION}/restic_${RESTIC_VERSION}_linux_${ARCH}.bz2 && \
    bzip2 -d /restic-${RESTIC_VERSION}-linux-${ARCH}.bz2 && \
    install -m 0755 /restic-${RESTIC_VERSION}-linux-${ARCH} /usr/local/bin/restic

WORKDIR /go/src/github.com/ionutbalutoiu/home-backup/
COPY go.mod go.sum ./
RUN go mod download
COPY . .

WORKDIR /go/src/github.com/ionutbalutoiu/home-backup/build/
RUN go build -ldflags="-s -w" -trimpath -o ./home-backup ../cmd/home-backup

FROM alpine:3.24.1

RUN apk add --no-cache \
    lvm2 lvm2-extra util-linux device-mapper \
    btrfs-progs xfsprogs xfsprogs-extra e2fsprogs e2fsprogs-extra \
    ca-certificates

COPY --from=build /go/src/github.com/ionutbalutoiu/home-backup/build/home-backup /usr/local/bin/home-backup
COPY --from=build /usr/local/bin/rclone /usr/local/bin/rclone
COPY --from=build /usr/local/bin/restic /usr/local/bin/restic

ENTRYPOINT ["/usr/local/bin/home-backup"]
