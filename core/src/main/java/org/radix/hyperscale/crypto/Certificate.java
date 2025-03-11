package org.radix.hyperscale.crypto;

import java.util.Objects;

import org.radix.hyperscale.common.ExtendedObject;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.ledger.CommitDecision;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("crypto.certificate")
public abstract class Certificate extends ExtendedObject implements Primitive
{
	@JsonProperty("decision")
	@DsonOutput(Output.ALL)
	private CommitDecision decision;

	protected Certificate()
	{
		// For serializer
	}
	
	protected Certificate(final CommitDecision decision)
	{
		Objects.requireNonNull(decision, "Decision is null");

		this.decision = decision;
		if (decision.equals(CommitDecision.ERROR) || decision.equals(CommitDecision.UNKNOWN))
			throw new IllegalArgumentException("Decision "+decision+" is unsupported at present");
	}

	public final CommitDecision getDecision()
	{
		return this.decision;
	}

	public abstract <T> T getObject();
}
