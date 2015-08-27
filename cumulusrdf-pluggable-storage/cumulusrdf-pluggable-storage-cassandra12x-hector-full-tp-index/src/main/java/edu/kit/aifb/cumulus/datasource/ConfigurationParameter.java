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
	String REPLICATION_FACTOR = "cassandra-replication-factor";
	String READ_CONSISTENCY = "cassandra-read-consistency";
	String WRITE_CONSISTENCY = "cassandra-write-consistency";
	String THRIFT_SOCKET_TIMEOUT = "cassandra-thrift-socket-timeout";
	String RETRY_DOWNED_HOSTS_DELAY_IN_SECONDS = "cassandra-retry-downed-hosts-delay-in-seconds";
	String RETRY_DOWNED_HOSTS_QUEUE_SIZE = "cassandra-retry-downed-hosts-queue-size";
	String RETRY_DOWNED_HOSTS = "cassandra-retry-downed-hosts";
	String MAX_WAIT_TIME_WHEN_EXHAUSTED = "cassandra-max-wait-time-when-exhausted";
	String LOAD_BALANCING_POLICY = "cassandra-load-balancing-policy";
}