package org.radix.hyperscale.ledger.sme;

import java.util.Objects;
import org.radix.hyperscale.time.Time;

public final class StateMachineStatus {
  public enum State {
    NONE(0),
    PREPARED(1),
    PROVISIONED(2),
    EXECUTING(3),
    COMPLETED(4);

    private final int index;

    State(int index) {
      this.index = index;
    }

    public int index() {
      return this.index;
    }

    public boolean before(final State state) {
      return this.index < state.index;
    }

    public boolean after(final State state) {
      return this.index > state.index;
    }
  }

  private final StateMachine stateMachine;
  private State state;
  private long timestamp;
  private Exception thrown;

  StateMachineStatus(final StateMachine stateMachine) {
    this.stateMachine = Objects.requireNonNull(stateMachine, "State machine is null");
    this.timestamp = Time.getSystemTime();
    this.state = State.NONE;
  }

  @Override
  public String toString() {
    return this.state.name() + " @ " + this.timestamp;
  }

  synchronized State current() {
    return this.state;
  }

  long timestamp() {
    return this.timestamp;
  }

  public synchronized boolean current(final State state) {
    return this.state.equals(Objects.requireNonNull(state, "State machine status state is null"));
  }

  public synchronized boolean before(final State state) {
    return this.state.before(Objects.requireNonNull(state, "State machine status state is null"));
  }

  public synchronized boolean after(final State state) {
    return this.state.after(Objects.requireNonNull(state, "State machine status state is null"));
  }

  public synchronized Exception thrown() {
    return this.thrown;
  }

  public synchronized void thrown(final Exception ex) {
    if (this.thrown != null)
      throw new IllegalStateException(
          "State machine exception already set for " + this.stateMachine.getPendingAtom().getHash(),
          this.thrown);

    this.thrown = Objects.requireNonNull(ex, "Exception is null");
    set(State.COMPLETED);
  }

  synchronized State set(final State state) {
    Objects.requireNonNull(state, "State machine status state is null");

    if (this.state.equals(state))
      throw new IllegalStateException(
          "State machine for atom "
              + this.stateMachine.getPendingAtom().getHash()
              + " is already set to "
              + state);

    // TODO review FINALIZED skipping needed by sync
    if (state.equals(StateMachineStatus.State.COMPLETED) == false
        && this.state.index() < state.index() - 1
        && this.thrown == null)
      throw new IllegalStateException(
          "State machine for atom "
              + this.stateMachine.getPendingAtom().getHash()
              + " can not set to state "
              + state
              + " from "
              + this.state);

    this.timestamp = Time.getSystemTime();

    State oldState = this.state;
    this.state = state;
    return oldState;
  }
}
