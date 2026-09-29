#!/bin/sh
# Makes the hivebox.v1 stubs in hivebox/v1 from the protos in crates/hive-proto. Run it after a
# proto changes, with grpcio-tools installed (pip install -e '.[dev]').
set -eu
here=$(cd "$(dirname "$0")" && pwd)
protos="$here/../../crates/hive-proto/proto"
rm -rf "$here/hivebox/v1"
python -m grpc_tools.protoc -I "$protos" --python_out="$here" --pyi_out="$here" --grpc_python_out="$here" "$protos"/hivebox/v1/*.proto
# The stubs land in hivebox/v1, inside this package, and import each other as hivebox.v1.
touch "$here/hivebox/v1/__init__.py"
