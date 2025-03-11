package org.radix.hyperscale.network.discovery;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.WebService;
import org.radix.hyperscale.executors.Executable;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.ConnectionState;
import org.radix.hyperscale.network.Protocol;
import org.radix.hyperscale.network.peers.Peer;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.DsonOutput.Output;

public class BootstrapService extends Executable
{
	private static final Logger networkLog = Logging.getLogger();

	private final Context context;
	private final Set<URI> hosts = new HashSet<URI>();
	
	public BootstrapService(final Context context)
	{
		this.context = Objects.requireNonNull(context);
		
		final HashSet<String> unparsedHosts = new HashSet<String>();

		// allow nodes to connect to others, bypassing TLS handshake
		if(this.context.getConfiguration().get("network.discovery.allow_tls_bypass", 0) == 1) 
		{
			networkLog.info("Allowing TLS handshake bypass...");
			SSLFix.trustAllHosts();
		}

		for (String unparsedURL : this.context.getConfiguration().get("network.discovery.urls", "").split(","))
		{
			unparsedURL = unparsedURL.trim();
			if (unparsedURL.isEmpty())
				continue;

			unparsedHosts.add(unparsedURL);
		}

		for (String unparsedHost : this.context.getConfiguration().get("network.seeds", "").split(","))
			unparsedHosts.add(unparsedHost);

		for (String unparsedHost : unparsedHosts)
		{
			unparsedHost = unparsedHost.trim();
			if (unparsedHost.isEmpty())
				continue;
			try
			{
				if (unparsedHost.contains("://") == false)
					unparsedHost = WebService.DEFAULT_SCHEME+unparsedHost;
				
				URI unparsedHostURI = URI.create(unparsedHost.trim());
				this.hosts.add(unparsedHostURI);
			}
			catch (Exception ex)
			{
				networkLog.error("Could not add bootstrap "+unparsedHost.trim(), ex);
			}
		}
	}

	@Override
	public void execute()
	{
		boolean isCompleted = false;
		long completionDelay = 30000;
		
		// Add a pause on initial execution to allow all other subsystems to be started
		try 
		{ 
			Thread.sleep(completionDelay); 
		} 
		catch (InterruptedException e) 
		{
			// Dont care, just re-raise and continue
			Thread.currentThread().interrupt();
		}
		
		final List<URI> shuffledHosts = new ArrayList<URI>(this.hosts);
		Collections.shuffle(shuffledHosts);

		final int connectionTimeout = this.context.getConfiguration().get("network.discovery.connection.timeout", 10000);
		final int readTimeout = this.context.getConfiguration().get("network.discovery.read.timeout", 10000);
		while(isCompleted == false && isTerminated() == false)
		{
			if (this.context.getNetwork().count(Protocol.TCP, ConnectionState.CONNECTED) > 0)
			{
				isCompleted = true;
				continue;
			}

			for (URI bootstrapHost : shuffledHosts)
			{
				networkLog.info(this.context.getName()+": Contacting boostrap host "+bootstrapHost+" for known peers ...");

				// open connection
				HttpURLConnection conn = null;
				BufferedInputStream input = null;
				try
				{
					// No custom port for bootstrap
					if (bootstrapHost.getPort() == -1)
						bootstrapHost = new URI(bootstrapHost.getScheme(), bootstrapHost.getUserInfo(), bootstrapHost.getHost(), WebService.DEFAULT_PORT,
												bootstrapHost.getPath(), bootstrapHost.getQuery(), bootstrapHost.getFragment());

					// No custom path for bootstrap
					if (bootstrapHost.getPath() == null || bootstrapHost.getPath().isEmpty())
						bootstrapHost = new URI(bootstrapHost.getScheme(), bootstrapHost.getUserInfo(), bootstrapHost.getHost(), bootstrapHost.getPort(),
											    WebService.DEFAULT_API_PATH+"/bootstrap", bootstrapHost.getQuery(), bootstrapHost.getFragment());
					
					conn = (HttpURLConnection) bootstrapHost.toURL().openConnection();
					// spoof User-Agents otherwise some CDNs do not let us through.
					conn.setRequestMethod("POST");
					conn.setRequestProperty("User-Agent", "curl/7.54.0");
					conn.setRequestProperty("Content-Type", "application/json; utf-8");
					conn.setRequestProperty("Accept", "application/json");
					conn.setAllowUserInteraction(false); // no follow symlinks - just plain old direct links
					conn.setUseCaches(false);
					conn.setConnectTimeout(connectionTimeout);
					conn.setReadTimeout(readTimeout);
					
					conn.setDoOutput(true);
					OutputStream os = conn.getOutputStream();
					os.write(Serialization.getInstance().toJson(this.context.getNode(), Output.WIRE).getBytes(StandardCharsets.UTF_8));
					os.flush();
					os.close();
					
					// read data
					input = new BufferedInputStream(conn.getInputStream());
					JSONTokener tokener = new JSONTokener(input);
					JSONObject response = new JSONObject(tokener);
					
					if (response.has("peers"))
					{
						JSONArray peersArray = response.getJSONArray("peers");
						if (peersArray != null)
						{
							for (int p = 0 ; p < peersArray.length() ; p++)
							{
								final Peer peer = new Peer(URI.create(peersArray.getString(p)), Protocol.TCP, Protocol.UDP);
								if (this.context.getNetwork().getPeerHandler().get(peer.getIdentity()) == null)
									this.context.getNetwork().getPeerHandler().update(peer);
							}
						}
					}
					else
						networkLog.info(this.context.getName()+": host "+bootstrapHost+" responsed with null peers array");
				}
				catch (URISyntaxException e)
				{
					networkLog.error("unable to create bootstrap URI for host "+bootstrapHost, e);
				}
				catch (SocketTimeoutException e)
				{
					networkLog.error("host "+bootstrapHost+" is not reachable");
				}
				catch (IOException e)
				{
					networkLog.error("Exception when connecting to bootstrap host "+bootstrapHost, e);
				}
				catch (RuntimeException e)
				{
					// rejected, offline, etc. - this is expected
					networkLog.warn("invalid host returned by node finder: "+bootstrapHost, e);
				}
				finally
				{
					if (input != null)
						try { input.close(); } catch (IOException ignoredExceptionOnClose) { }
				}
			}
			
			if (isCompleted == false)
			{
				try
				{
					Thread.sleep(completionDelay);
					
					if (completionDelay < 180000)
						completionDelay += completionDelay;
				}
				catch (InterruptedException ex)
				{
					Thread.currentThread().interrupt();
					networkLog.warn(this.context.getName()+": Bootstrap service polling delay interrupted", ex);
				}
				
				if (this.context.isStopped())
					break;
			}
		}		
		
		networkLog.info(this.context.getName()+": Boostrapper terminating");
		terminate(false);
	}
}
