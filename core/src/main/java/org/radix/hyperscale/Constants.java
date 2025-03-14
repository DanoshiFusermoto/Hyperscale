package org.radix.hyperscale;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

public final class Constants 
{
	// GENERAL
	public static final int		TRUNCATED_IDENTITY_LENGTH = 10;
	public static final int 	WARN_ON_QUEUE_SIZE = 1000;
	
	// RUNTIME //
	public static final int 	JAVA_VERSION_MIN = 17;
	public static final int 	JAVA_VERSION_MAX = 21;
	
	// TODO put these in universe config
	public static final double 	BASELINE_DISTANCE_FACTOR = 2.5;
	public static final long 	BASELINE_DISTANCE_TARGET = BigInteger.valueOf(Long.MIN_VALUE).abs().subtract(BigInteger.valueOf((long) (Long.MIN_VALUE / (BASELINE_DISTANCE_FACTOR * Math.log(BASELINE_DISTANCE_FACTOR)))).abs()).longValue(); // TODO rubbish, but produces close enough needed output
	public static final long 	BLOCK_DISTANCE_FACTOR = 64;
	public static final long 	BLOCK_DISTANCE_TARGET = BigInteger.valueOf(Long.MIN_VALUE).abs().subtract(BigInteger.valueOf((long) (Long.MIN_VALUE / (BLOCK_DISTANCE_FACTOR * Math.log(BLOCK_DISTANCE_FACTOR)))).abs()).longValue(); // TODO rubbish, but produces close enough needed output
	
	// NETWORK
	public static final int 	DEFAULT_TCP_CONNECTIONS_OUT_SYNC = 3; 
	public static final int 	DEFAULT_TCP_CONNECTIONS_OUT_SHARD = 2;
	public static final int 	DEFAULT_CONNECTION_INACTIVITY = 60; 
	public static final int 	DEFAULT_CONNECTION_TIMEOUT = 60; 
	public static final int 	DEFAULT_MINIMUM_CONNECTION_DURATION = 30; 
	public static final int 	DEFAULT_CONNECTION_STICKY_DURATION_SECONDS = (int) TimeUnit.MINUTES.toSeconds(30); 
	public static final int 	DEFAULT_CONNECTION_ROTATION_INTERVAL_SECONDS = (int) TimeUnit.MINUTES.toSeconds(10); 
	public static final int 	DEFAULT_TCP_BUFFER = 1<<20;
	public static final int 	COMPRESS_PAYLOADS_THRESHOLD = 256;
	public static final int 	MIN_CONNECTION_LATENCY_FOR_TASKS = 100;
	public static final int 	MAX_STRIKES_FOR_DISCONNECT = 3;
	
	// GOSSIP
	public static final int 	BROADCAST_POLL_TIMEOUT = 50;
	public static final int 	MAX_PUSH_INVENTORY_ITEMS = 32;
	public static final int 	MAX_BROADCAST_INVENTORY_ITEMS = 256;
	public static final int 	MAX_REQUEST_INVENTORY_ITEMS = 256;
	public static final int 	MAX_FETCH_INVENTORY_ITEMS = 32;
	public static final int 	INVENTORY_TRANSMIT_AT_SIZE = 1<<20;	// Transmit if size is 1M or greater
	public static final int 	MIN_GOSSIP_REQUEST_TIMEOUT_MILLISECONDS = 5000;
	public static final int 	MAX_GOSSIP_REQUEST_TIMEOUT_MILLISECONDS = 15000;
	public static final int 	MIN_DIRECT_REQUEST_TIMEOUT_MILLISECONDS = 2000;
	public static final int 	MAX_DIRECT_REQUEST_TIMEOUT_MILLISECONDS = 6000;
	public static final int 	MAX_GOSSIP_ITEM_RETRIES = MAX_STRIKES_FOR_DISCONNECT;
	public static final int 	MAX_DIRECT_ITEM_RETRIES = MAX_STRIKES_FOR_DISCONNECT;

	// BLOCK PERIODS AND INTERVALS
	public static final int 	EPOCH_PERIOD_DURATION_MILLISECONDS = 60000;
	public static final int 	EPOCH_DURATION_MILLISECONDS = (int) TimeUnit.DAYS.toMillis(1);
	public static final int		BLOCK_INTERVAL_TARGET_MILLISECONDS = 500;
	public static final int 	BLOCKS_PER_PERIOD = EPOCH_PERIOD_DURATION_MILLISECONDS / BLOCK_INTERVAL_TARGET_MILLISECONDS;
	public static final int 	BLOCKS_PER_EPOCH = EPOCH_DURATION_MILLISECONDS / BLOCK_INTERVAL_TARGET_MILLISECONDS;
	public static final long 	getDurationToBlockCount(long duration, TimeUnit unit)
	{
		long milliseconds = unit.toMillis(duration);
		return milliseconds / BLOCK_INTERVAL_TARGET_MILLISECONDS;
	}
	
	// CONSENSUS
	public static final int 	MIN_COMMIT_SUPERS = 2;
	public static final int 	MIN_COMMIT_ROUNDS = 6;
	public static final int 	ESCALATE_COMMIT_URGENCY = 8;
	public static final int 	PROPOSAL_PHASE_TIMEOUT_MS = BLOCK_INTERVAL_TARGET_MILLISECONDS*6;
	public static final int 	VOTE_PHASE_TIMEOUT_MS = BLOCK_INTERVAL_TARGET_MILLISECONDS*6;
	public static final int 	TRANSITION_PHASE_TIMEOUT_MS = BLOCK_INTERVAL_TARGET_MILLISECONDS*2;
	public static final int 	MINIMUM_ROUND_DURATION_MILLISECONDS = BLOCK_INTERVAL_TARGET_MILLISECONDS;

	// PRIMITIVES
	public static final int 	PRIMITIVE_STALE = 60;
	public static final int 	PRIMITIVE_EXPIRE = 90;
	public static final int 	PRIMITIVE_GC_INTERVAL = 3;
	public static final int 	MIN_PRIMITIVE_POW_DIFFICULTY = 12;

	// ATOMS
	public static final int 	ATOM_DISCARD_AT_PENDING_LIMIT = 50000;
	public static final int 	ATOM_PREPARE_TIMEOUT_SECONDS = 5;
	public static final int 	ATOM_ACCEPT_TIMEOUT_SECONDS = 10;
	public static final int 	ATOM_EXECUTE_LATENT_SECONDS = 10;			
	public static final int 	ATOM_EXECUTE_TIMEOUT_SECONDS = 20;			
	public static final int 	ATOM_COMMIT_TIMEOUT_SECONDS = 20;
	
	// SYNC THRESHOLDS
	public static final int     SYNC_BROADCAST_INTERVAL_MS = 1000;
	public static final int 	SYNC_INVENTORY_HEAD_OFFSET = 25;
	public static final int 	OOS_TRIGGER_LIMIT_BLOCKS = 250;
	public static final int 	OOS_RESOLVED_LIMIT_BLOCKS = 10;
	public static final int 	OOS_PREPARE_BLOCKS = OOS_TRIGGER_LIMIT_BLOCKS*2;
	
	// VOTE POWER 
	public static final long 	VOTE_POWER_BOOTSTRAP = 1000l;
	public static final int 	VOTE_POWER_PROPOSAL_REWARD = 1;
	public static final int 	VOTE_POWER_MATURITY_EPOCHS = 2;
	
	private Constants() {}
}
