Group Members:
1. Aayush Jain (2023H1030083P)
2. Kunjan Shah (2019HS030072P)

## Getting Started

The code requires Java 20+ for building and executing.

_The below commands assume that Java 20+ is already on the `PATH`._

1. To build the code, run `./gradlew jar`. On Windows, run `.\gradlew.bat jar`.
2. To run the code, run `java -jar ./build/libs/KeyValueStore-1.0-SNAPSHOT.jar args`
3. See below section for arguments reference.

## Command-Line Arguments

Use the following in-order:
- `mode` (`tcp`, `udp` or `rmi`)
- `host` (e.g. 127.0.0.1)
- `port` (e.g. 10000)
- `peers` (comma-separated list of peers)
  - 127.0.0.1:10001,127.0.0.1:10002