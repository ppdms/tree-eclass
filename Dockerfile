FROM docker.io/oven/bun:1.4.2-slim AS browser-build
WORKDIR /src/frontend
COPY frontend/package.json frontend/bun.lock ./
RUN bun install --frozen-lockfile
COPY frontend ./
RUN bun run build

# Optional future Linux deployment. The laptop uses ./tree and native services.
FROM docker.io/library/golang:1.27.1 AS go-build
WORKDIR /src/backend
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/cmd ./cmd
COPY backend/internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tree-eclass ./cmd/tree-eclass

FROM docker.io/library/python:3.14.7-slim-trixie AS pdf-build
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ autoconf automake make git pkg-config libpoppler-glib-dev libwxgtk3.2-dev \
    && rm -rf /var/lib/apt/lists/*
ARG DIFF_PDF_REVISION=8e22eaf8140cb7536f4f8e01efd37411b146aa03
RUN git init -q /tmp/diff-pdf && cd /tmp/diff-pdf \
    && git remote add origin https://github.com/vslavik/diff-pdf.git \
    && git fetch --depth=1 origin "$DIFF_PDF_REVISION" && git checkout --detach FETCH_HEAD \
    && ./bootstrap && ./configure && make && install -m 755 diff-pdf /diff-pdf-bin

FROM docker.io/library/python:3.14.7-slim-trixie AS api
ARG TARGETARCH
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 GOMEMLIMIT=192MiB
RUN sed -i 's/^Components: main$/Components: main non-free/' /etc/apt/sources.list.d/debian.sources \
    && apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates 7zip 7zip-rar poppler-utils tesseract-ocr \
    libpoppler-glib8t64 libwxgtk3.2-1t64 xvfb xauth libicu76 libssl3t64 \
    && rm -rf /var/lib/apt/lists/*
COPY requirements-parser.txt /requirements-parser.txt
RUN pip install --no-cache-dir --require-hashes -r /requirements-parser.txt
# The pinned helper contains its .NET runtime; no persistent exporter service.
RUN case "$TARGETARCH" in \
      amd64) arch=x64; checksum=3e253e28ec7ea034b2201443fa84571142945299296541ecbe196ffceef8bc3c ;; \
      arm64) arch=arm64; checksum=02a47fc8e0192fd509fbb082aadd9322035b18feae96849699fefc424a1e3379 ;; \
      *) exit 1 ;; esac \
    && curl -fsSL "https://github.com/Tyrrrz/DiscordChatExporter/releases/download/2.48/DiscordChatExporter.Cli.linux-${arch}.zip" -o /tmp/exporter.zip \
    && echo "$checksum  /tmp/exporter.zip" | sha256sum -c - \
    && python3 -m zipfile -e /tmp/exporter.zip /opt/discord-exporter \
    && chmod 755 /opt/discord-exporter/DiscordChatExporter.Cli && rm /tmp/exporter.zip
RUN mkdir /opt/tessdata \
    && curl -fsSL https://raw.githubusercontent.com/tesseract-ocr/tessdata_fast/87416418657359cb625c412a48b6e1d6d41c29bd/eng.traineddata -o /opt/tessdata/eng.traineddata \
    && curl -fsSL https://raw.githubusercontent.com/tesseract-ocr/tessdata_fast/87416418657359cb625c412a48b6e1d6d41c29bd/ell.traineddata -o /opt/tessdata/ell.traineddata \
    && curl -fsSL https://raw.githubusercontent.com/tesseract-ocr/tessdata_fast/87416418657359cb625c412a48b6e1d6d41c29bd/LICENSE -o /opt/tessdata/LICENSE \
    && echo 'cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30  /opt/tessdata/LICENSE' | sha256sum -c - \
    && echo '7d4322bd2a7749724879683fc3912cb542f19906c83bcc1a52132556427170b2  /opt/tessdata/eng.traineddata' | sha256sum -c - \
    && echo '4fba8a0b461038d51f1c20d043d4f2ac38c4e778f1b90830847f7bd8fa3ba726  /opt/tessdata/ell.traineddata' | sha256sum -c -
COPY --from=pdf-build /diff-pdf-bin /usr/local/bin/diff-pdf-bin
RUN printf '#!/bin/sh\nexec xvfb-run -a /usr/local/bin/diff-pdf-bin "$@"\n' > /usr/local/bin/diff-pdf \
    && chmod 755 /usr/local/bin/diff-pdf \
    && sha256sum /usr/local/bin/diff-pdf | cut -d ' ' -f 1 > /opt/pdf-diff.sha256
COPY parser/__init__.py parser/parser_helper.py parser/models.py parser/source_files.py parser/vision.py parser/archive_limits.py /app/parser/
COPY parser/extractors /app/parser/extractors
COPY parser/archive_members /app/parser/archive_members
COPY --from=browser-build /src/frontend/dist /app/frontend
COPY --from=go-build /tree-eclass /usr/local/bin/tree-eclass
RUN useradd --uid 10001 --create-home tree && mkdir /jobs && chown tree:tree /jobs
USER tree
EXPOSE 8001
ENTRYPOINT ["/usr/local/bin/tree-eclass"]
CMD ["container-serve"]
