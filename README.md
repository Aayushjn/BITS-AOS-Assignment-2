Group Members:
1. Aayush Jain (2023H1030083P)
2. Kunjan Shah (2019HS030072P)

## Getting Started

1. Setup IntelliJ IDEA and use the Eclipse Temurin 20 JDK as the project JDK (set it as the `JAVA_HOME` optionally).
2. Build the code into a JAR file with `./gradlew jar` or use the Gradle task window on the right side of the IDE.
3. If running in RMI mode, also run `$JAVA_HOME/bin/rmiregistry <port> &`
4. Run the JAR with `$JAVA_HOME/bin/java -jar ./build/libs/KeyValueStore-1.0-SNAPSHOT.jar <args>` (check the file name in the 
build directory).

## Command-Line Arguments

Use the following in-order:
- `mode` ("tcp", "udp" or "rmi")
- `node-id` (e.g. "h1", "h2", etc.)
- `host` (e.g. "127.0.0.1")
- `port`
- `peers` (comma-separated list of peers)
  - "127.0.0.1:10001,127.0.0.1:10002" -> use when running TCP or UDP mode
  - "h2,h3" -> use when running RMI mode (comma-separated list of node-id)

_Note: The host-port combination is used to identify the `rmiregistry` and not the current node._