package org.radix.hyperscale;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;

public class Configuration extends PersistedConfiguration
{
	private static Configuration instance = null;
	
	public static Configuration getDefault()
	{
		if (instance == null)
			throw new RuntimeException("Configuration not set");
		
		return instance;
	}

	static Configuration createAsDefault(String commandLineConfig, String[] commandLineArguments) throws IOException
	{
		if (instance != null)
			throw new RuntimeException("Default configuration already set");
		
		instance = new Configuration(commandLineConfig, commandLineArguments);
		
		return instance;
	}
	
	static Configuration clearDefault()
	{
		Configuration configuration = getDefault();
		instance = null;
		return configuration;
	}

	private final Map<String, Option> commandLine;

	public Configuration(Configuration configuration)
	{
		super(configuration.properties);
		
		this.commandLine = new HashMap<>(configuration.commandLine);
	}

	Configuration(String commandLineConfig, String[] commandLineArguments) throws IOException
	{
		try
		{
			JSONObject commandLineConfigJSON = new JSONObject();
			InputStream commandLineConfigStream = Configuration.class.getResourceAsStream(commandLineConfig);
			if (commandLineConfigStream != null)
				commandLineConfigJSON = new JSONObject(IOUtils.toString(commandLineConfigStream, StandardCharsets.UTF_8.name()));
	
			CommandLineParser parser = new DefaultParser ();
			Options gnuOptions = new Options();
			for (String clKey : commandLineConfigJSON.keySet())
			{
				JSONObject clOption = commandLineConfigJSON.getJSONObject(clKey);
				Option.Builder optionBuilder;
				
				if (clOption.has("short"))
					optionBuilder = Option.builder(clOption.getString("short")).longOpt(clKey);
				else
					optionBuilder = Option.builder(clKey);
				
				optionBuilder.desc(clOption.optString("desc", ""));
				
				if (clOption.optBoolean("has_arg"))
					optionBuilder.hasArg();
				else if (clOption.optBoolean("opt_arg"))
				{
					optionBuilder.optionalArg(true);
					optionBuilder.numberOfArgs(1);
				}
				else
					optionBuilder.hasArg(false);
				
				gnuOptions.addOption(optionBuilder.build());
			}
			
			// Using the Apache CommandLine class itself is extremely inefficient if it needs to be queried a lot as it is a List lookup. 
			// Throw the options into a Map for faster lookups and value retrieval.
			this.commandLine = new HashMap<>();
			for (Option option : parser.parse(gnuOptions, commandLineArguments).getOptions())
			{
				String opt = option.getOpt();
				String longOpt = option.getLongOpt();
				if (opt != null) this.commandLine.put(opt, option);
				if (longOpt != null) this.commandLine.put(longOpt, option);
			}
	
			load(getCommandLine("config", "default.config"));
		}
		catch (JSONException | ParseException ex)
		{
			throw new IOException(ex);
		}
	}
	
	@Override
	public String get(String key)
	{
		String value = getCommandLine(key, null);

		if (value == null)
			value = super.get(key);
		
		return value;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(String key, T _default)
	{
		String value = getCommandLine(key, null);

		if (value == null)
			return super.get(key, _default);
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
		final String value = getCommandLine(key, null);
		if (value == null)
			return super.get(key, _default);
	
		return Boolean.parseBoolean(value);
	}

	/**
	 * Returns an int primitive property value with default if not found
	 */
	public int get(String key, int _default)
	{
		final String value = getCommandLine(key, null);
		if (value == null)
			return super.get(key, _default);
	
		return Integer.parseInt(value);
	}

	/**
	 * Returns a long primitive property value with default if not found
	 */
	public long get(String key, long _default)
	{
		final String value = getCommandLine(key, null);
		if (value == null)
			return super.get(key, _default);
	
		return Long.parseLong(value);
	}


	public boolean hasCommandLine(String key)
	{
		return this.commandLine.get(key) == null ? false : true;
	}

	public String getCommandLine(String key)
	{
		return getCommandLine(key, null);
	}

	@SuppressWarnings("unchecked")
	public <T> T getCommandLine(String key, T _default)
	{
		Option commandLineOption = this.commandLine.get(key);
		if (commandLineOption == null)
			return _default;
		
		if (commandLineOption.hasArg() == false)
			return (T) Boolean.TRUE;
		
		String value = commandLineOption.getValue();
		if (value != null && _default != null)
		{
			if (_default instanceof Byte)
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
		}

		return _default;
	}
}
