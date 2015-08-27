package edu.kit.aifb.cumulus.datasource;

/**
 * An interface that declares all configuration parameters needed by this module.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public interface ConfigurationParameter {
	String HOSTS = "cassandra-hosts";
	String KEYSPACE = "cassandra-keyspace";
	
	// Socket options
	String CONNECT_TIMEOUT_MILLIS = "cassandra-connect-timeout-millis";
	String READ_TIMEOUT_MILLIS = "cassandra-read-timeout-millis";
	String KEEP_ALIVE = "cassandra-keep-alive";
	String REUSE_ADDRESS = "cassandra-reuse-address";
	String RECEIVE_BUFFER_SIZE = "cassandra-receive-buffer-size";
	String SO_LINGER = "cassandra-so-linger";
	String TCP_NO_DELAY = "cassandra-tcp-no-delay";
	
	// Query options
	String CONSISTENCY_LEVEL = "cassandra-consistency-level";
	String SERIAL_CONSISTENCY_LEVEL = "cassandra-serial-consistency-level";
	String FETCH_SIZE = "cassandra-fetch-size";
	
	String RETRY_POLICY = "cassandra-retry-policy";
	String RECONNECTION_POLICY = "cassandra-reconnection-policy";
	String LOAD_BALANCING_POLICY = "cassandra-load-balancing-policy";
	
	String TRANSPORT_COMPRESSION = "cassandra-transport-compression";
	
	// Pooling options
	String LOCAL_CORE_CONNECTIONS_PER_HOST = "cassandra-local-core-connections-per-host";
	String LOCAL_MAX_CONNECTIONS_PER_HOST = "cassandra-local-max-connections-per-host";
	String LOCAL_MAX_SIMULTANEOUS_REQUEST_PER_CONNECTION_THRESHOLD = "cassandra-local-max-simultaneous-request-per-connection-threshold";
	String LOCAL_MIN_SIMULTANEOUS_REQUEST_PER_CONNECTION_THRESHOLD = "cassandra-local-min-simultaneous-request-per-connection-threshold";
	String REMOTE_CORE_CONNECTIONS_PER_HOST = "cassandra-remote-core-connections-per-host";
	String REMOTE_MAX_CONNECTIONS_PER_HOST = "cassandra-remote-max-connections-per-host";
	String REMOTE_MAX_SIMULTANEOUS_REQUEST_PER_CONNECTION_THRESHOLD = "cassandra-remote-max-simultaneous-request-per-connection-threshold";
	String REMOTE_MIN_SIMULTANEOUS_REQUEST_PER_CONNECTION_THRESHOLD = "cassandra-remote-min-simultaneous-request-per-connection-threshold";
	
	String REPLICATION_FACTOR = "cassandra-replication-factor";
	
	String TTL = "cassandra-ttl-value";
}