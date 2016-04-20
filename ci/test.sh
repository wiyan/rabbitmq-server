#!/bin/sh

set -ex

(
  cd rabbit
  make DEPS_DIR="$PWD/.." tests
)
