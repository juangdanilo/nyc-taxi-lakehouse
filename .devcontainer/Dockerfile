FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

# The base image ships a Yarn apt repo with a missing signing key, which breaks
# `apt-get update` inside dev container features. We don't use Yarn, so drop it.
# The apt-get update makes the build fail here, with a clear error, if apt breaks again.
RUN rm -f /etc/apt/sources.list.d/yarn* && apt-get update && rm -rf /var/lib/apt/lists/*
