# Consensus Pipeline Flow for Transaction Processing

A high-level overview of  transaction consensus within the Hyperscale network, focusing on the transaction lifecycle and associated ledger handlers.

## Transaction Lifecycle Overview

A transaction passes through several clearly defined phases to achieve consensus and finality:

### 1. Submission
- A transaction is submitted to the network.
- Handled initially by `ledger.AtomHandler`.

### 2. Provisioning Phase
- **Purpose:** Checks if the transaction:
  - Is unique and not previously processed.
  - Hasn't already been stored on disk.

### 3. Preparation Phase
- **Activities:**
  - Signature verification.
  - Parsing instruction manifest.
  - Loading necessary smart contract packages.
  - Determining required shards and validator sets for consensus.
  - Preparing necessary state for execution.

### 4. Proposal Construction (`ledger.BlockHandler`)
- Prepared transactions collected by `ledger.BlockHandler` from `ledger.AtomHandler`.
- Ensures transactions:
  - Are not duplicated.
  - Do not conflict with each other.

### 5. Proposal Voting
- A validator node prepares a proposal including the transaction.
- Proposal is voted upon by validators.
- Upon achieving a supermajority vote, transaction marked as **ACCEPTED**.

### 6. State Provisioning (`ledger.StateHandler`)
- **Activities:**
  - State required for execution is provisioned from:
    - Local database, or
    - Remote shard replicas.
- Upon successful provisioning, transaction marked as **EXECUTABLE**.
- If provisioning fails (timeout), transaction marked as **EXECUTE_TIMEOUT**.

### 7. Proposal Referencing for Execution
- `ledger.BlockHandler` includes EXECUTABLE or EXECUTE_TIMEOUT transactions in proposals.
- Validators vote based on successful state provisioning.

### 8. Execution Phase
- Transactions executed locally by validators.
- Execution outputs recorded in a `ledger.StateVoteBlock`:
  - Success/failure per transaction.
  - Non-execution marked if timeout occurred.
- Transaction marked as **FINALIZING**.

### 9. StateVoteBlock Collation (`ledger.StatePool`)
- Execution information compressed, signed, and broadcast to shard replicas.
- Validators collate received `ledger.StateVoteBlocks` in `ledger.StateVoteBlockCollector`.
- Achieve supermajority agreement on execution correctness (2f + 1 agreement required).

### 10. Creation of StateCertificate
- Generated upon supermajority agreement for local state executions.
- Represents consensus outcome and participating validators.

### 11. Certificate Broadcasting (`ledger.StateHandler`)
- Local `ledger.primitives.StateCertificate` broadcasted to relevant remote shards.
- Collects `ledger.primitives.StateCertificate` for required remote state.
- Upon receiving all required state certificates, creates an `ledger.primitives.AtomCertificate`.
- Transaction marked as **FINALIZED**.

### 12. Finalizing Proposals (`ledger.BlockHandler`)
- Validators include `ledger.primitives.AtomCertificate` in proposals.
- Proposals containing agreed-upon `ledger.primitives.AtomCertificate` accepted via supermajority vote.
- Accepted transactions committed, and state updates applied to the ledger.

## End of Transaction Lifecycle

At this point, transactions have successfully navigated through consensus, execution, and finalization, with their state securely updated in the distributed ledger.

