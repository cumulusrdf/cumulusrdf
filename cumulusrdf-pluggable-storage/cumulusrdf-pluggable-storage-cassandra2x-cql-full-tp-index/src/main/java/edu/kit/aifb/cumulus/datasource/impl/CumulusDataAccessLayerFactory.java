package edu.kit.aifb.cumulus.datasource.impl;

import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.CONNECT_TIMEOUT_MILLIS;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.CONSISTENCY_LEVEL;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.FETCH_SIZE;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.HOSTS;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.KEEP_ALIVE;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.KEYSPACE;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.LOAD_BALANCING_POLICY;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.LOCAL_CORE_CONNECTIONS_PER_HOST;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.LOCAL_MAX_CONNECTIONS_PER_HOST;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.LOCAL_MAX_SIMULTANEOUS_REQUEST_PER_CONNECTION_THRESHOLD;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.LOCAL_MIN_SIMULTANEOUS_REQUEST_PER_CONNECTION_THRESHOLD;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.READ_TIMEOUT_MILLIS;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.RECEIVE_BUFFER_SIZE;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.RECONNECTION_POLICY;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.REMOTE_CORE_CONNECTIONS_PER_HOST;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.REMOTE_MAX_CONNECTIONS_PER_HOST;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.REMOTE_MAX_SIMULTANEOUS_REQUEST_PER_CONNECTION_THRESHOLD;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.REMOTE_MIN_SIMULTANEOUS_REQUEST_PER_CONNECTION_THRESHOLD;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.REPLICATION_FACTOR;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.RETRY_POLICY;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.REUSE_ADDRESS;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.SERIAL_CONSISTENCY_LEVEL;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.SO_LINGER;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.TCP_NO_DELAY;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.TRANSPORT_COMPRESSION;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.TTL;

import java.util.Map;

import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import edu.kit.aifb.cumulus.datasource.serializer.Serializer;
import edu.kit.aifb.cumulus.datasource.serializer.SerializerTypeInferer;
import edu.kit.aifb.cumulus.framework.datasource.CounterDAO;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.datasource.MapDAO;
import edu.kit.aifb.cumulus.framework.datasource.QuadIndexDAO;
import edu.kit.aifb.cumulus.framework.datasource.StorageLayout;
import edu.kit.aifb.cumulus.framework.datasource.TripleIndexDAO;
import edu.kit.aifb.cumulus.framework.domain.configuration.Configuration;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * Concrete factory for creating members of Cassandra 2.x data access layer family.
 * 
 * @author Andrea Gazzarini
 * @author Sebastian Schmidt
 * @since 1.1.0
 */
public class CumulusDataAccessLayerFactory extends DataAccessLayerFactory implements DAOManager {
	private static final Log LOGGER = new Log(LoggerFactory.getLogger(CumulusDataAccessLayerFactory.class));

	private Session _session;
	private final String _keyspaceNameSuffix;
	private String _keyspaceName;
	private int _replicationFactor;
	private int _ttl = -1;
	
	private static Cluster cluster = null;
	private static int daoCount = 0;
	private static final Object CLUSTER_LOCK;

	static {
		CLUSTER_LOCK = new Object();
	}

	/**
	 * Builds a new Cassandra 2.x data access layer factory.
	 * 
	 * @param layout the storage layout.
	 */
	public CumulusDataAccessLayerFactory(final StorageLayout layout) {
		super(layout);
		_keyspaceNameSuffix = layout == StorageLayout.TRIPLE ? "_T" : "_Q";
	}

	@Override
	public void accept(final Configuration<Map<String, Object>> configuration) {

		synchronized (CLUSTER_LOCK) {
			if (cluster == null) {
				
				final String hosts = configuration.getAttribute(HOSTS, "localhost");

				final Compression compression = compression(configuration);
				final Cluster.Builder builder = Cluster.builder()
						.addContactPoints(hosts.split("\\s*,\\s*"))
						.withRetryPolicy(retryPolicy(configuration))
						.withReconnectionPolicy(reconnectionPolicy(configuration))
						.withLoadBalancingPolicy(loadBalancingPolicy(configuration))
						.withPoolingOptions(poolingOptions(configuration))
						.withQueryOptions(queryOptions(configuration))
						.withSocketOptions(socketOptions(configuration));

				if (compression != null) {
					builder.withCompression(compression);
				}
				
				cluster = builder.build();
				
				LOGGER.debug(MessageCatalog._00107_CONNECTED_TO_CLUSTER);
			}
		}

		_session = cluster.connect();

		_keyspaceName = configuration.getAttribute(KEYSPACE, "KeyspaceCumulus") + _keyspaceNameSuffix;
		_replicationFactor = configuration.getAttribute(REPLICATION_FACTOR, Integer.valueOf(1));
		_ttl = configuration.getAttribute(TTL, Integer.valueOf(-1));
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K> CounterDAO<K> getCounterDAO(final Class<K> keyClass, final String counterName) {
		return new Cassandra2xCounterDAO<K>(this, counterName, (Serializer<K>) SerializerTypeInferer.getSerializer(keyClass));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public <K, V> MapDAO<K, V> getMapDAO(
			final Class<K> keyClass,
			final Class<V> valueClass,
			final boolean isBidirectional,
			final String mapName) {
		return new Cassandra2xMapDAO(
				this,
				isBidirectional,
				mapName,
				SerializerTypeInferer.getSerializer(keyClass),
				SerializerTypeInferer.getSerializer(valueClass),
				-1);
	}

	@Override
	public QuadIndexDAO getQuadIndexDAO(final ITopLevelDictionary dictionary) {
		synchronized (CLUSTER_LOCK) {
			daoCount++;
		}
		
		return new Cassandra2xQuadIndexDAO(this, dictionary);
	}

	@Override
	public TripleIndexDAO getTripleIndexDAO(final ITopLevelDictionary dictionary) {
		synchronized (CLUSTER_LOCK) {
			daoCount++;
		}
		
		return new Cassandra2xTripleIndexDAO(this, dictionary);
	}

	@Override
	public String getUnderlyingStorageInfo() {
		return "Cassandra 2.x";
	}

	// Cassandra 2.x specific methods

	/**
	 * Returns the Cassandra connection holder.
	 * 
	 * @return the Cassandra connection holder.
	 */
	public Session getSession() {
		return _session;
	}

	/**
	 * Returns the keyspace name associated with this factory.
	 * 
	 * @return the keyspace name associated with this factory.
	 */
	public String getKeyspaceName() {
		return _keyspaceName;
	}

	/**
	 * Returns the replication factor in use.
	 * 
	 * @return the replication factor in use.
	 */
	public int getReplicationFactor() {
		return _replicationFactor;
	}

	/**
	 * Returns the TTL in use.
	 * 
	 * @return the TTL in use.
	 */
	public int getTtl() {
		return _ttl;
	}

	@Override
	public void daoWasClosed() {
		synchronized (CLUSTER_LOCK) {
			daoCount--;

			if (daoCount == 0) {
				cluster.close();
				cluster = null;
				
				LOGGER.debug(MessageCatalog._00108_DISCONNECTED_FROM_CLUSTER);
			}
		}
	}
	
	/**
	 * Creates the socket options for this factory.
	 * 
	 * @param configuration the configuration.
	 * @return the socket options for this factory.
	 */
	private SocketOptions socketOptions(final Configuration<Map<String, Object>> configuration) {
		final SocketOptions socketOptions = new SocketOptions();
		socketOptions.setConnectTimeoutMillis(
				configuration.getAttribute(
						CONNECT_TIMEOUT_MILLIS, 
						(Integer)SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS));
		
		socketOptions.setReadTimeoutMillis(
				configuration.getAttribute(
						READ_TIMEOUT_MILLIS, 
						(Integer)SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS));
		
		final Boolean keepAlive = configuration.getAttribute(KEEP_ALIVE, null);
		if (keepAlive != null) {
			socketOptions.setKeepAlive(keepAlive);			
		}
		
		final Boolean reuseAddress = configuration.getAttribute(REUSE_ADDRESS, null);
		if (reuseAddress != null) {
			socketOptions.setReuseAddress(reuseAddress);
		}
		
		final Integer receiveBufferSize = configuration.getAttribute(RECEIVE_BUFFER_SIZE, null);
		if (receiveBufferSize != null) {
			socketOptions.setReceiveBufferSize(receiveBufferSize);
		}
		
		final Integer soLinger = configuration.getAttribute(SO_LINGER, null);
		if (soLinger != null) {
			socketOptions.setSoLinger(soLinger);
		}
		
		final Boolean tcpNoDelay = configuration.getAttribute(TCP_NO_DELAY, null);
		if (tcpNoDelay != null) {
			socketOptions.setTcpNoDelay(tcpNoDelay);				
		}
		return socketOptions;
	}
	
	/**
	 * Creates the query options for this factory.
	 * 
	 * @param configuration the configuration.
	 * @return the query options for this factory.
	 */
	private QueryOptions queryOptions(final Configuration<Map<String, Object>> configuration) {
		final QueryOptions queryOptions = new QueryOptions();
		
		try {
			queryOptions.setConsistencyLevel(
					ConsistencyLevel.valueOf(
							configuration.getAttribute(
									CONSISTENCY_LEVEL, 
									(String)null)));
		} catch (final Exception ignore) {
			// Nothing, just use the default value.
		}

		try {
			queryOptions.setSerialConsistencyLevel(
					ConsistencyLevel.valueOf(
							configuration.getAttribute(
									SERIAL_CONSISTENCY_LEVEL, 
									(String)null)));
		} catch (final Exception ignore) {
			// Nothing, just use the default value.
		}

		queryOptions.setFetchSize(
				configuration.getAttribute(
						FETCH_SIZE, 
						(Integer)QueryOptions.DEFAULT_FETCH_SIZE));
		return queryOptions;
	}

	/**
	 * Creates the pooling options for this factory.
	 * 
	 * @param configuration the configuration.
	 * @return the pooling options for this factory.
	 */
	private PoolingOptions poolingOptions(final Configuration<Map<String, Object>> configuration) {
		final PoolingOptions poolingOptions = new PoolingOptions();
		
		Integer value = configuration.getAttribute(LOCAL_CORE_CONNECTIONS_PER_HOST, null);
		if (value != null) {
			poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, value);
		}

		value = configuration.getAttribute(LOCAL_MAX_CONNECTIONS_PER_HOST, null);
		if (value != null) {
			poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, value);
		}
		
		value = configuration.getAttribute(LOCAL_MAX_SIMULTANEOUS_REQUEST_PER_CONNECTION_THRESHOLD, null);
		if (value != null) {
			poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, value);
		}
		
		value = configuration.getAttribute(LOCAL_MIN_SIMULTANEOUS_REQUEST_PER_CONNECTION_THRESHOLD, null);
		if (value != null) {
			poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, value);
		}
		
		value = configuration.getAttribute(REMOTE_CORE_CONNECTIONS_PER_HOST, null);
		if (value != null) {
			poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, value);
		}

		value = configuration.getAttribute(REMOTE_MAX_CONNECTIONS_PER_HOST, null);
		if (value != null) {
			poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, value);
		}
		
		value = configuration.getAttribute(REMOTE_MAX_SIMULTANEOUS_REQUEST_PER_CONNECTION_THRESHOLD, null);
		if (value != null) {
			poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, value);
		}
		
		value = configuration.getAttribute(REMOTE_MIN_SIMULTANEOUS_REQUEST_PER_CONNECTION_THRESHOLD, null);
		if (value != null) {
			poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, value);
		}
		
		return poolingOptions;
	}

	/**
	 * Returns the retry balancing policy according with a given configuration.
	 * 
	 * @param configuration the configuration.
	 * @return the retry balancing policy according with a given configuration.
	 */
	private RetryPolicy retryPolicy(final Configuration<Map<String, Object>> configuration) {
		RetryPolicy retryPolicy = Policies.defaultRetryPolicy();
		try {
			String retryPolicyClassName = configuration.getAttribute(RETRY_POLICY, null);
			if (retryPolicyClassName != null) {
				retryPolicy = (RetryPolicy) Class.forName(retryPolicyClassName).newInstance();						
			}
		} catch (final Exception ignore) {
			// just use the default value.
		}
		return retryPolicy;
	}

	/**
	 * Returns the reconnection balancing policy according with a given configuration.
	 * 
	 * @param configuration the configuration.
	 * @return the reconnection balancing policy according with a given configuration.
	 */
	private ReconnectionPolicy reconnectionPolicy(final Configuration<Map<String, Object>> configuration) {
		ReconnectionPolicy reconnectionPolicy = Policies.defaultReconnectionPolicy();
		try {
			String reconnectionPolicyClassName = configuration.getAttribute(RECONNECTION_POLICY, null);
			if (reconnectionPolicyClassName != null) {
				reconnectionPolicy = (ReconnectionPolicy) Class.forName(reconnectionPolicyClassName).newInstance();						
			}
		} catch (final Exception ignore) {
			// just use the default value.
		}
		return reconnectionPolicy;
	}

	/**
	 * Returns the load balancing policy according with a given configuration.
	 * 
	 * @param configuration the configuration.
	 * @return the load balancing policy according with a given configuration.
	 */
	private LoadBalancingPolicy loadBalancingPolicy(final Configuration<Map<String, Object>> configuration) {
		LoadBalancingPolicy lbPolicy = Policies.defaultLoadBalancingPolicy();
		try {
			String lbPolicyClassName = configuration.getAttribute(LOAD_BALANCING_POLICY, null);
			if (lbPolicyClassName != null) {
				lbPolicy = (LoadBalancingPolicy) Class.forName(lbPolicyClassName).newInstance();						
			}
		} catch (final Exception ignore) {
			// just use the default value.
		}
		return lbPolicy;
	}

	/**
	 * Returns the compression options according with a given configuration.
	 * 
	 * @param configuration the configuration.
	 * @return the compression options according with a given configuration.
	 */
	private Compression compression(final Configuration<Map<String, Object>> configuration) {
		Compression compression = null;
		try {
			String compressionOption = configuration.getAttribute(TRANSPORT_COMPRESSION, null);
			if (compressionOption != null) {
				compression = Compression.valueOf(compressionOption);
			}
		} catch (Exception e) {
			// Ignore and don't set the compression.
		}
		return compression;
	}	
}