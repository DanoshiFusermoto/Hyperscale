package org.radix.hyperscale.serialization.mapper;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.radix.hyperscale.serialization.SerializationException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

/**
 * A field filter for DSON output modes.
 */
public class DsonFieldFilter extends SimpleBeanPropertyFilter 
{
	private static final class DsonFieldFilterEntry
	{
		private static final long getID(Class<?> clazz, String field)
		{
			long id = 31l;
			id = 17l * id + clazz.hashCode();
			id = 17l * id + field.hashCode();
			return id;
		}
		
		private final Class<?> clazz;
		private final String field;
		private final int cachedCode;
		private final long cachedID;
		
		private DsonFieldFilterEntry(Class<?> clazz, String field)
		{
			this.clazz = clazz;
			this.field = field;
			this.cachedCode = Objects.hash(clazz, field);
			this.cachedID = getID(clazz, field);
		}
		
		public long getID()
		{
			return this.cachedID;
		}

		@Override
		public int hashCode() 
		{
			return this.cachedCode;
		}

		@Override
		public boolean equals(Object obj) 
		{
			if (this == obj)
				return true;
		
			if (obj == null)
				return false;
			
			if (getClass() != obj.getClass())
				return false;
			
			if (this.clazz.equals(((DsonFieldFilterEntry)obj).clazz) == false)
				return false;
			
			if (this.field.equals(((DsonFieldFilterEntry)obj).field) == false)
				return false;
			
			return true;
		}
	}
	
	private final LongObjectMap<DsonFieldFilterEntry> includedFields;

	/**
	 * Create a {@link FilterProvider} with a {@link DsonFieldFilter}
	 * @param includedFields Set of {@code Class<?>} and field names to include for this filter.
	 * @return A freshly created {@link FilterProvider} with the specified DSON filter included.
	 * @throws SerializationException 
	 */
	public static FilterProvider filterProviderFor(final Map<Class<?>, Set<String>> includedFields) throws SerializationException 
	{
		// Create an optimised version of the includedFields map
		MutableLongObjectMap<DsonFieldFilterEntry> optimizedIncludedFields = LongObjectMaps.mutable.ofInitialCapacity(includedFields.size()*2);
		for (Class<?> clazz : includedFields.keySet())
		{
			Set<String> fieldSet = includedFields.get(clazz);
			for (String field : fieldSet)
			{
				DsonFieldFilterEntry dsonFieldFilterEntry = new DsonFieldFilterEntry(clazz, field);
				if (optimizedIncludedFields.put(dsonFieldFilterEntry.getID(), dsonFieldFilterEntry) != null)
					throw new SerializationException("Duplicate field ID found in FilterProvider for "+clazz+":"+field);
			}
		}
		
		return new SimpleFilterProvider().addFilter(MapperConstants.DSON_FILTER_NAME, new DsonFieldFilter(optimizedIncludedFields));
	}

	private DsonFieldFilter(final LongObjectMap<DsonFieldFilterEntry> includedFields) 
	{
		// No need to make copy of immutable set.
		this.includedFields = includedFields;
	}

	@Override
	public void serializeAsField(final Object pojo, final JsonGenerator jgen, final SerializerProvider provider, final PropertyWriter writer) throws Exception 
	{
		if (shouldInclude(jgen.getOutputContext().getCurrentValue().getClass(), writer.getName()))
			writer.serializeAsField(pojo, jgen, provider);
		else if (!jgen.canOmitFields()) // since 2.3
			writer.serializeAsOmittedField(pojo, jgen, provider);
	}

	@Override
	protected boolean include(final BeanPropertyWriter writer) 
	{
		return true;
	}

	@Override
	protected boolean include(final PropertyWriter writer) 
	{
		return true;
	}

	private boolean shouldInclude(final Class<?> cls, final String property) 
	{
		long fieldID = DsonFieldFilterEntry.getID(cls, property);
		DsonFieldFilterEntry fieldEntry = this.includedFields.get(fieldID);
		if (fieldEntry == null)
		{
			// FIXME: Special cases ideally handled better
			if (Map.class.isAssignableFrom(cls) || Collection.class.isAssignableFrom(cls))
				return true;
		}
		else if (fieldEntry.clazz == cls && fieldEntry.field.equals(property))
			return true;
		
		return false;
	}
}
