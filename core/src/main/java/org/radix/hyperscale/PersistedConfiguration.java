package org.radix.hyperscale;

import org.apache.commons.cli.ParseException;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Persisted properties are property sets that are committed to storage and may be loaded on the next client execution
 * 
 * @author Dan Hughes
 *
 */

public class PersistedConfiguration
{
	private static final Logger log = Logging.getLogger();
	
	Properties properties;
	
	PersistedConfiguration()
	{
		this.properties = new Properties();
	}

	PersistedConfiguration(Properties properties) 
	{
		this.properties = (Properties) properties.clone();
	}

	public void load(String filename) throws FileNotFoundException, ParseException 
	{
		InputStream propertiesInput = null;
		
		try
		{
			File file = new File(filename);

			if (file.exists()) 
			{
				propertiesInput = new FileInputStream(file);
				log.info("Loaded "+file+" properties");
			} 
			else 
				log.info("Properties file " + file + " not found, using default");
		}
		catch (Exception ex)
		{
			log.error("Can not open properties file " + filename + ", using default", ex);
		}
			
		// Try in resource ? //
		if (propertiesInput == null)
		{
			propertiesInput = this.getClass().getResourceAsStream("/" + filename);
			
			if (propertiesInput == null)
				throw new FileNotFoundException(filename+" properties not found in resources");

			log.info("Loaded "+filename+" properties from resources");
		}
		
		try
		{
			this.properties.load(propertiesInput);
		
			propertiesInput.close();

			log.info("Finished parsing properties ");
			if (new File(filename).canWrite())
				save(filename);
		}
		catch (Exception ex)
		{
			log.error("Can not load properties file, fatal!", ex);
			throw new ParseException("Can not load properties file, fatal!");
		}
	}
	
	public void save(String filename) throws IOException
	{
		OutputStream propertiesOutput = new FileOutputStream(new File(filename));
		this.properties.store(propertiesOutput, "");
		propertiesOutput.close();
	}
	
	/**
	 * Returns true of property contains the key, otherwise false
	 */
	public boolean has(String key)
	{
		return this.properties.containsKey(key);
	}
	
	/**
	 * Returns a property value
	 */
	public String get(String key)
	{
		return this.properties.getProperty(key);
	}
	
	/**
	 * Returns a property as a Set<String>
	 */
	public Set<String> getAsSet(String key)
	{
		Set<String> set = new HashSet<String>();
		
		if (this.properties.containsKey(key))
		{
			StringTokenizer strings = new StringTokenizer(this.properties.getProperty(key), ",");
			
			while(strings.hasMoreTokens())
				set.add(strings.nextToken());
		}
		
		return set;
	}
	
	/**
	 * Returns a property value with default if not found
	 */
	@SuppressWarnings("unchecked")
	public <T> T get(String key, T _default)
	{
		String value = this.properties.getProperty(key);
		
		if (value == null)
			return _default;
		
		if (_default == null)
			return (T) value;
		else if (_default instanceof Byte)
			return (T) Byte.valueOf(value);
		else if (_default instanceof Short)
			return (T) Short.valueOf(value);
		else if (_default instanceof Integer)
			return (T) Integer.valueOf(value);
		else if (_default instanceof Long)
			return (T) Long.valueOf(value);
		else if (_default instanceof Float)
			return (T) Float.valueOf(value);
		else if (_default instanceof Double)
			return (T) Double.valueOf(value);
		else if (_default instanceof Boolean)
			return (T) Boolean.valueOf(value);
		else if (_default instanceof String)
			return (T) value;
		
		return null;
	}
	
	/**
	 * Returns a boolean primitive property value with default if not found
	 */
	public boolean get(String key, boolean _default)
	{
		final String value = this.properties.getProperty(key);
		if (value == null)
			return _default;
		
		return Boolean.parseBoolean(value);
	}

	/**
	 * Returns an int primitive property value with default if not found
	 */
	public int get(String key, int _default)
	{
		final String value = this.properties.getProperty(key);
		if (value == null)
			return _default;
		
		return Integer.parseInt(value);
	}

	/**
	 * Returns a long primitive property value with default if not found
	 */
	public long get(String key, long _default)
	{
		final String value = this.properties.getProperty(key);
		if (value == null)
			return _default;
		
		return Long.parseLong(value);
	}

	public Enumeration<?> propertyNames()
	{
		return this.properties.propertyNames();
	}

	/**
	 * Sets a property value
	 */
	public void set(String key, Object value)
	{
		this.properties.setProperty(key, value.toString());
	}
}
