# Built on the build platform and cross-compiled for the target one, so a
# multi-platform image needs no emulation.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG OCB_VERSION=v0.164.0
ARG VERSION=0.1.0-dev
ARG TARGETOS TARGETARCH
WORKDIR /src
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go install go.opentelemetry.io/collector/cmd/builder@${OCB_VERSION}
COPY builder-config.yaml .
COPY components/ components/
# The module and build caches are shared by both platforms' builds, which
# would otherwise each fill the runner's disk with their own copy.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    sed -i "s/^  version: .*/  version: ${VERSION}/" builder-config.yaml \
 && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH builder --config builder-config.yaml

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /src/_build/fusescope-collector /fusescope-collector
USER nonroot:nonroot
EXPOSE 4317 4318 13133
ENTRYPOINT ["/fusescope-collector"]
CMD ["--config=/etc/fusescope-collector/config.yaml"]
