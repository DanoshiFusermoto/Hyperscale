package org.radix.hyperscale.network;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public enum ConnectionState
{
	NONE, CONNECTING, CONNECTED, DISCONNECTING, DISCONNECTED;
	
	public static Set<ConnectionState> SELECT_CONNECTING = Collections.unmodifiableSet(EnumSet.of(CONNECTING));
	public static Set<ConnectionState> SELECT_CONNECTED = Collections.unmodifiableSet(EnumSet.of(CONNECTED));
	public static Set<ConnectionState> SELECT_CONNECTING_CONNECTED = Collections.unmodifiableSet(EnumSet.of(CONNECTING, CONNECTED));
	
	public static Set<ConnectionState> SELECT_DISCONNECTING = Collections.unmodifiableSet(EnumSet.of(DISCONNECTING));
	public static Set<ConnectionState> SELECT_DISCONNECTED = Collections.unmodifiableSet(EnumSet.of(DISCONNECTED));
	public static Set<ConnectionState> SELECT_DISCONNECTING_DISCONNECTED = Collections.unmodifiableSet(EnumSet.of(DISCONNECTING, DISCONNECTED));
}
