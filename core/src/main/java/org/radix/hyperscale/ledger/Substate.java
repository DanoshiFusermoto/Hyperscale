package org.radix.hyperscale.ledger;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import org.eclipse.collections.api.factory.Maps;
import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.sme.SubstateTransitions;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Strings;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("ledger.substate")
public final class Substate extends BasicObject implements StateAddressable
{
	public enum NativeField
	{
		// AUTH / ID
		AUTHORITY, 
		
		// VALIDATORS
		SHUFFLE_SEEDS, SHARD_GROUP, VOTE_POWER,
		
		// CONSENSUS
		ATOM, BLOCK, TIMEOUT, CERTIFICATE, BLOB, 
		
		// BLUEPRINTS
		BLUEPRINT, CODE, LANGUAGE, CONSTRUCTOR,
		
		// COMPONENTS
		COMPONENT;
		
		// Java Enum.valueOf calls are extremely slow, just cache in a set and check that way ffs!
		private static final Set<String> cache = new HashSet<String>();
		static
		{
			for(NativeField nf : NativeField.values())
				cache.add(nf.lower());
		}

		public static boolean isNativeField(final String field)
		{
			return cache.contains(Strings.toLowerCase(field));
		}
		
		private final String lower;
		private NativeField()
		{
			this.lower = name().toLowerCase(); 
		}
		
		public String lower()
		{
			return this.lower;
		}
	}
		
	@JsonProperty("address")
	@DsonOutput(Output.ALL)
	private StateAddress address;
	
	@JsonProperty("fields")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=TreeMap.class)
	private Map<String, Object> fields;

	@SuppressWarnings("unused")
	private Substate()
	{
		// FOR SERIALIZER
	}
	
	public Substate(final StateAddress address)
	{
		this();
		
		Objects.requireNonNull(address, "State address is null");
		
		this.address = address;
		this.fields = new TreeMap<String, Object>();
	}

	public Substate(final Substate other)
	{
		this();

		Objects.requireNonNull(other, "Substate to copy is null");
		
		this.address = other.address;
		synchronized(other)
		{
			this.fields = new TreeMap<String, Object>(other.fields);
		}
	}

	Substate(final StateAddress address, final Identity authority)
	{
		this();

		Objects.requireNonNull(address, "State address is null");
		Objects.requireNonNull(authority, "Authority is null");
		
		this.address = address;
		this.fields = new TreeMap<String, Object>();

		set(NativeField.AUTHORITY, authority);
	}
	
	@Override
	public StateAddress getAddress() 
	{
		return this.address;
	}
	
	public boolean isVoid()
	{
		synchronized(this)
		{
			return this.fields.isEmpty();
		}
	}
	
	public int size()
	{
		synchronized(this)
		{
			return this.fields.size();
		}
	}
	
	public Substate merge(final Substate other) 
	{
		Objects.requireNonNull(other, "Substate to merge is null");

		final Substate merged = new Substate(this.address);
		synchronized(this)
		{
			merged.fields = new TreeMap<String, Object>(this.fields);
		}
		
		synchronized(other)
		{
			merged.fields.putAll(other.fields);
		}
		
		return merged;
	}
	
	void merge(final SubstateTransitions transitions) 
	{
		Objects.requireNonNull(transitions, "Substate transitions to merge is null");

		synchronized(this)
		{
			this.fields.putAll(transitions.getWrites());
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T get(final NativeField field) 
	{
		Objects.requireNonNull(field, "Native field is null");

		return (T) get(field.lower());
	}

	@SuppressWarnings("unchecked")
	public <T> T get(final String field) 
	{
		Objects.requireNonNull(field, "Field is null");

		synchronized(this)
		{
			return (T) this.fields.get(Strings.toLowerCase(field));
		}
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getOrDefault(final String field, final T def) 
	{
		Objects.requireNonNull(field, "Field is null");
		Objects.requireNonNull(def, "Default field value is null");

		synchronized(this)
		{
			T value = (T) this.fields.get(Strings.toLowerCase(field));
			if (value != null)
				return value;
		}
		
		return def;
	}

	public <T> Substate set(final NativeField field, final T value) 
	{
		Objects.requireNonNull(field, "Native field is null");
		Objects.requireNonNull(value, "Field value is null");

		return set(field.lower(), value);
	}

	public <T> Substate set(final String field, final T value) 
	{
		Objects.requireNonNull(field, "Field is null");
		Objects.requireNonNull(value, "Field value is null");

		synchronized(this)
		{
			this.fields.put(Strings.toLowerCase(field), value);
		}
		
		return this;
	}

	public Identity getAuthority() 
	{
		return get(NativeField.AUTHORITY);
	}
	
	public boolean references(final Object object) 
	{
		synchronized(this)
		{
			for (Object value : this.fields.values())
				if (object.equals(value))
					return true;
		}

		return false;
	}
	
	public void forEach(final BiConsumer<String, Object> function)
	{
		synchronized(this)
		{
			this.fields.forEach(function);
		}		
	}
	
	public Map<String, Object> values()
	{
		synchronized(this)
		{
			return Maps.immutable.ofAll(this.fields).castToMap();
		}
	}

	@Override
	public String toString() 
	{
		return "Substate [hash="+getHash()+" address="+this.address+"]";
	}
}

