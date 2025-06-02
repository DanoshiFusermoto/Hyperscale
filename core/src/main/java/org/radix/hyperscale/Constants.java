package org.radix.hyperscale;

import java.util.concurrent.TimeUnit;

public final class Constants 
{
	// GENERAL
	public static final int		TRUNCATED_IDENTITY_LENGTH = 10;
	public static final int 	WARN_ON_QUEUE_SIZE = 1000;
	
	// RUNTIME //
	public static final int 	JAVA_VERSION_MIN = 17;
	public static final int 	JAVA_VERSION_MAX = 21;
	
	// NETWORK
	public static final int 	DEFAULT_TCP_CONNECTIONS_OUT_SYNC = 3; 
	public static final int 	DEFAULT_TCP_CONNECTIONS_OUT_SHARD = 2;
	public static final int 	DEFAULT_CONNECTION_INACTIVITY = 60; 
	public static final int 	DEFAULT_CONNECTION_TIMEOUT = 60; 
	public static final int 	DEFAULT_MINIMUM_CONNECTION_DURATION = 30; 
	public static final int 	DEFAULT_CONNECTION_STICKY_DURATION_SECONDS = (int) TimeUnit.MINUTES.toSeconds(30); 
	public static final int 	DEFAULT_CONNECTION_ROTATION_INTERVAL_SECONDS = (int) TimeUnit.MINUTES.toSeconds(10); 
	public static final int 	DEFAULT_TCP_BUFFER = 1<<20;
	public static final int 	MIN_CONNECTION_LATENCY_FOR_TASKS = 100;
	public static final int 	MAX_STRIKES_FOR_DISCONNECT = 3;
	
	// MESSAGING
	public static final int 	COMPRESS_PAYLOADS_THRESHOLD = 256;
	public static final int 	DEFAULT_MESSAGE_TTL_SECONDS = 10;
	public static final int 	DEFAULT_MESSAGE_TTW_SECONDS = 1;
	
	// GOSSIP
	public static final int 	QUEUE_POLL_TIMEOUT = 50;
	public static final int 	QUEUE_TIMESTAMP_INTERVAL = 250;
	public static final int 	MAX_BROADCAST_INVENTORY_ITEMS = 128;
	public static final int 	MAX_REQUEST_INVENTORY_ITEMS = 32;
	public static final int 	MAX_REQUEST_INVENTORY_ITEMS_TOTAL = 1024;
	public static final int 	MAX_FETCH_INVENTORY_ITEMS = 32;
	public static final int 	INVENTORY_TRANSMIT_AT_SIZE = 1<<20;	// Transmit if size is 1M or greater
	public static final int 	GOSSIP_REQUEST_LATENT_MILLISECONDS = 5000;
	public static final int 	MIN_GOSSIP_REQUEST_TIMEOUT_MILLISECONDS = 10000;
	public static final int 	MAX_GOSSIP_REQUEST_TIMEOUT_MILLISECONDS = 30000;
	public static final int 	MAX_GOSSIP_ITEM_RETRIES = MAX_STRIKES_FOR_DISCONNECT;

	public static final int 	MIN_DIRECT_REQUEST_TIMEOUT_MILLISECONDS = 2000;
	public static final int 	MAX_DIRECT_REQUEST_TIMEOUT_MILLISECONDS = 6000;
	public static final int 	MAX_DIRECT_ITEM_RETRIES = MAX_STRIKES_FOR_DISCONNECT;

	// CONSENSUS
	public static final int 	MIN_COMMIT_SUPERS = 2;
	public static final int 	MIN_COMMIT_ROUNDS = 6;
	public static final int 	MAX_EPHEMERAL_DELAY = 250;
	public static final int 	ESCALATE_COMMIT_URGENCY = 8;
	public static final int 	PROPOSAL_PHASE_TIMEOUT_ROUNDS = 6;
	public static final int 	VOTE_PHASE_TIMEOUT_ROUNDS = 6;
	public static final int 	TRANSITION_PHASE_TIMEOUT_ROUNDS = 2;
	
	// STATE
	public static final boolean STATE_VOTE_UNGROUPED = false;
	public final static int 	MAX_ATOM_TO_STATE_PROVISION = 256;
	public final static int 	MAX_STATE_CERTIFICATES_TO_PROCESS = 256;
	public final static int		MAX_STATE_VOTE_COLLECTOR_ITEMS = 256;
	
	// EXECUTION
	public static final boolean SKIP_EXECUTION_SIGNALS = false;

	// PRIMITIVES
	public static final int 	PRIMITIVE_STALE = 60;
	public static final int 	PRIMITIVE_EXPIRE = 90;
	public static final int 	PRIMITIVE_GC_INTERVAL = 3;
	
	// PROPOSALS 
	public static final int		MAX_PROPOSAL_PACKAGES = 32;
	public static final int		MAX_PROPOSAL_TYPE_PRIMITIVES = 4096;

	// ATOMS
	public static final int 	ATOM_DISCARD_AT_PENDING_LIMIT = 100000;
	public static final int 	ATOM_PREPARE_TIMEOUT_SECONDS = 15;
	public static final int 	ATOM_ACCEPT_TIMEOUT_SECONDS = 15;
	public static final int 	ATOM_EXECUTE_LATENT_SECONDS = 15;			
	public static final int 	ATOM_EXECUTE_TIMEOUT_SECONDS = 30;			
	public static final int 	ATOM_COMMIT_TIMEOUT_SECONDS = 30;
	
	// SYNC THRESHOLDS
	public static final int     SYNC_BROADCAST_INTERVAL_MS = 1000;
	public static final int 	SYNC_INVENTORY_HEAD_OFFSET = 25;
	public static final int 	OOS_TRIGGER_LIMIT_BLOCKS = 120;
	public static final int 	OOS_RESOLVED_LIMIT_BLOCKS = 10;
	public static final int 	OOS_PREPARE_BLOCKS = OOS_TRIGGER_LIMIT_BLOCKS*2;
	
	// VOTE POWER 
	public static final long 	VOTE_POWER_BOOTSTRAP = 1000l;
	public static final int 	VOTE_POWER_PROPOSAL_REWARD = 1;
	public static final int 	VOTE_POWER_MATURITY_EPOCHS = 2;
	
	// SEARCH
	public static final long 	SEARCH_TIMEOUT_SECONDS = 10;
	
	private Constants() {}
}
