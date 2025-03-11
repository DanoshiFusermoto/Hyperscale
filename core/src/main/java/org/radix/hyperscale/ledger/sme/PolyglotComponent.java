package org.radix.hyperscale.ledger.sme;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.Value;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.database.SystemMetaData;
import org.radix.hyperscale.ledger.Epoch;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.utils.Numbers;

import com.google.common.collect.LinkedListMultimap;

public final class PolyglotComponent extends Component
{
	private static int MAX_CODE_SIZE = 16384;
	
	static 
	{
		// TODO turn off annoying interpreter warning!
		System.setProperty("polyglot.engine.WarnInterpreterOnly", "false");
	}
	
	// TODO sharing the engine between polyglot contexts should be fine, but need to check all the security
	//		constraints are not violated under certain operating circumstances!
	private static final Map<Identity, Engine> engines = Collections.synchronizedMap(new HashMap<>());
	private static final Engine getEngine(Identity identity)
	{
		return engines.computeIfAbsent(identity, i -> Engine.create());
	}
	
	private static final class ContextInstance
	{
		private final Context context;
		private final Value bindings;
		
		ContextInstance(final Context context, final Value bindings)
		{
			this.context = context;
			this.bindings = bindings;
		}
		
		Context get()
		{
			return this.context;
		}
		
		Value bindings()
		{
			return this.bindings;
		}
	}
	private static final LinkedListMultimap<Hash, ContextInstance> contexts = LinkedListMultimap.create();
	
	public final class PolyglotInterface
	{
		private final PolyglotComponent polyglot;
		
		PolyglotInterface(PolyglotComponent polyglot)
		{
			this.polyglot = polyglot;
		}
		
		public final Identity identity()
		{
			return this.polyglot.getIdentity();
		}

		public final long random()
		{
			return ThreadLocalRandom.current().nextLong();
		}
		
		public final SystemMetaData systemMetaData()
		{
			return this.polyglot.getStateMachine().getContext().getMetaData();
		}

		public final Universe universe()
		{
			return Universe.getDefault();
		}

		public final ShardGroupID shardGroup()
		{
			final Epoch epoch = this.polyglot.getStateMachine().getContext().getLedger().getEpoch();
			final int numShardGroups = this.polyglot.getStateMachine().getContext().getLedger().numShardGroups(epoch);
			return ShardMapper.toShardGroup(this.polyglot.getStateMachine().getContext().getNode().getIdentity(), numShardGroups);
		}

		public final StateAddress address()
		{
			return this.polyglot.getAddress();
		}

		public final Bucket bucket(String label)
		{
			return this.polyglot.bucket(label);
		}
		
		public final Vault vault(Identity identity)
		{
			return this.polyglot.vault(identity);
		}

		public final Vault vault(Hash identifier, Identity authority)
		{
			return this.polyglot.vault(StateAddress.from(Vault.class.getAnnotation(StateContext.class).value(), identifier), authority);
		}

		public final Vault vault(StateAddress address, Identity authority)
		{
			return this.polyglot.vault(address, authority);
		}

		public final void lock(StateAddress stateAddress, StateLockMode lockMode)
		{
			this.polyglot.lock(stateAddress, lockMode);
		}
		
		public final void assertExists(StateAddress address)
		{
			this.polyglot.assertExists(address);
		}

		public final void assertNotExists(StateAddress address)
		{
			this.polyglot.assertNotExists(address);
		}

		public final void assertCreate(StateAddress address, Identity authority)
		{
			this.polyglot.assertCreate(address, authority);
		}
		
		public final <T> T get(String field)
		{
			return this.polyglot.get(address(), field);
		}

		public final <T> T get(final StateAddress stateAddress, String field)
		{
			return this.polyglot.get(stateAddress, field);
		}

		public final <T> T get(String field, T def)
		{
			return this.polyglot.get(address(), field, def);
		}

		public final <T> T get(final StateAddress stateAddress, String field, T def)
		{
			return this.polyglot.get(stateAddress, field, def);
		}
		
		public final <T> void set(String field, T value)
		{
			this.polyglot.set(address(), field, value);
		}

		public final <T> void set(final StateAddress stateAddress, String field, T value)
		{
			this.polyglot.set(stateAddress, field, value);
		}

		public final void associate(StateAddress stateAddress, final Hash association)
		{
			this.polyglot.associate(stateAddress, association);
		}
	}
	
	private final Hash		codeBlob;
	private final String 	language;
	private final ContextInstance context;
	private final PolyglotInterface	interfaze;

	PolyglotComponent(StateMachine stateMachine, final Blob codeBlob, SubstateLog substateLog) 
	{
		super(stateMachine, substateLog);

		this.language = substateLog.get("language");
		this.codeBlob = codeBlob.getHash();
		this.interfaze = new PolyglotInterface(this);
		
		synchronized(contexts)
		{
			final ContextInstance contextInstance;
			final List<ContextInstance> contextInstancesByKey = contexts.get(this.codeBlob);
			if (contextInstancesByKey.isEmpty())
			{
				final String code = new String(codeBlob.getBytes(), StandardCharsets.UTF_8);
				Numbers.greaterThan(code.length(), MAX_CODE_SIZE, "Code size "+code.length()+" is greater than max "+MAX_CODE_SIZE);

				final Value bindings;
				final Engine engine = getEngine(getStateMachine().getContext().getNode().getIdentity());
				final Context context = Context.newBuilder(this.language).engine(engine).allowAllAccess(true).build();
				context.enter();
				try
				{
					bindings = context.getBindings(this.language);
					bindings.putMember("_self", this.interfaze);
					context.eval("js", code);
				}
				finally
				{
					context.leave();
				}
				
				contextInstance = new ContextInstance(context, bindings);
			}
			else
			{
				contextInstance = contextInstancesByKey.removeFirst();

				contextInstance.get().enter();
				try
				{
					contextInstance.bindings().putMember("_self", this.interfaze);
				}
				finally
				{
					contextInstance.get().leave();
				}
			}

			this.context = contextInstance;
		}
	}
	
	@Override
	protected void close() 
	{
		synchronized(contexts)
		{
			this.context.bindings().removeMember("_self");
			contexts.put(this.codeBlob, this.context);
		}
	}

	@Override
	protected void prepare(String method, Object[] arguments) throws ReflectiveOperationException
	{
		// NOTHING TO PREPARE
	}

	@SuppressWarnings("unchecked")
	@Override
	protected final <T> T call(String method, Object[] arguments) throws ReflectiveOperationException
	{
		synchronized(this.context)
		{
			this.context.get().enter();
			try
			{
				final Value methodValue = this.context.bindings().getMember(method);
				final Object[] methodArguments = new Object[arguments.length];
				for(int i = 0 ; i < arguments.length ; i++)
		        {
		        	if (arguments[i] instanceof Argument argument)
		        		methodArguments[i] = argument.get();
		        	else
		        		methodArguments[i] = arguments[i];
		        }
				
				return (T) methodValue.execute(methodArguments);
			}
			finally
			{
				this.context.get().resetLimits();
				this.context.get().leave();
			}
		}
	}
}
