# Hyperscale

Hyperscale is a prototype sharded Layer 1 blockchain demonstrating novel consensus mechanisms for high-throughput, low-finality transactions with atomic cross-shard communication in a permissionless environment.

## Key Features

### Cassandra Consensus

Hyperscale introduces Cassandra, a hybrid consensus mechanism that combines:

- Classical BFT-style safety guarantees during healthy network conditions
- Nakamoto-style probabilistic safety during network distress
- Maintained liveness throughout all network conditions

### Cross-Shard Atomic Commitment

Built on Cerberus principles, Hyperscale implements atomic cross-shard transactions through:

- Coordinated consensus orchestration between shards
- Deterministic commit/abort decisions
- Fault-tolerant message propagation

## Technical Implementation

The prototype includes core cryptographic and consensus elements:

- Validator signature verification
- Merkle proof generation and validation
- Validator voting mechanics
- Cross-shard message verification
- State transition validation

## Scope and Limitations

This prototype is designed to demonstrate the feasibility of:

1. High-throughput transaction processing
2. Low-finality confirmation times
3. Atomic cross-shard consensus
4. Permissionless validator participation

### Intellectual Property Protection

Several critical mechanisms required to develop a main net deployment are intentionally omitted to protect valuable intellectual property, including:

- Shard reconfiguration algorithms to dynamically adjust to changing load over time
- Validator shuffling mechanisms between shards (designed to prevent adversarial shard control)
- Novel asymmetric proposal weighting function that creates computational overhead for attackers while remaining efficient for honest participants

### Implementation Notes

- This codebase was developed as a research prototype to demonstrate consensus mechanisms and cross-shard atomic commitment
- Code quality reflects research priorities rather than production standards
- Contains expected "code smell" as the focus was on proving theoretical concepts rather than maintaining optimal code structure
- Should be viewed as a proof-of-concept implementation rather than production-ready code

While the implementation includes all essential elements to prove these capabilities, it does not address all peripheral security considerations that would be required in a production system. The focus is on validating the core consensus and atomic commitment mechanisms.

## Architecture Overview

The system operates through several key components:

- Consensus engine (Cassandra)
- Cross-shard communication protocol
- Validator management system
- State transition engine

## Development Status

This is a research prototype intended to demonstrate the viability of specific consensus and atomic commitment mechanisms. It should not be used in production without substantial additional security hardening.

## Research Background

Hyperscale builds on and extends several key areas of distributed systems research:

- Byzantine Fault Tolerance protocols
- Nakamoto consensus
- Cross-shard atomic commitment
- Distributed validator coordination

## Installation / building from source

### MacOS

On MacOS, you can install the correct OpenJDK using [Homebrew](https://brew.sh):

```
brew install openjdk@21
```

On Apple Silicon macs, homebrew should install the openjdk to `/opt/homebrew/opt/openjdk@21`
This may be different for intel macs, it could be `/usr/local/opt/openjdk@21`.

To use this openjdk in the current shell session, export the JAVA_HOME variable using the following command:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@21"
```

This will only change the Java version in your current running terminal, so you'd need to run it every time you want to build/run Hyperscale. To make this persist across shell restarts, add it to `~/.zshrc`:

```bash
cat << 'EOF' >> ~/.zshrc
# Set the openjdk version 21 as JAVA_HOME
export JAVA_HOME="/opt/homebrew/opt/openjdk@21"
EOF
```

You can build Hyperscale by running the gradle wrapper script

```
./gradlew jar
```

## Running

After building Hyperscale you can find a `hyperscale.jar` file in `core/build/libs/`.

- Copy the `hyperscale.jar` jar file into a new directory.
- Copy the `node-0.key` and `universe.key` files from `core/` into the same directory.

You can now start Hyperscale by going to the newly created directory and run:

```
java -jar hyperscale.jar -console -singleton
```

## Contributing

While this is primarily a research prototype, we welcome discussion and feedback on the consensus mechanisms and cross-shard protocols demonstrated here.

## Disclaimer

This software is a prototype intended for research and demonstration purposes. It successfully demonstrates the core concepts of hybrid consensus and atomic cross-shard commitment but should not be deployed in production without substantial additional security hardening. Certain security-critical components are intentionally omitted to protect intellectual property.
