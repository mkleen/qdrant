#!/usr/bin/env bash

set -ex

cd "$(dirname "$0")"

poetry run pytest openapi
