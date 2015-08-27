package edu.kit.aifb.cumulus.datasource.impl;

import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.HOSTS;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.KEYSPACE;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.LOAD_BALANCING_POLICY;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.MAX_WAIT_TIME_WHEN_EXHAUSTED;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.READ_CONSISTENCY;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.REPLICATION_FACTOR;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.RETRY_DOWNED_HOSTS;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.RETRY_DOWNED_HOSTS_DELAY_IN_SECONDS;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.RETRY_DOWNED_HOSTS_QUEUE_SIZE;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.THRIFT_SOCKET_TIMEOUT;
import static edu.kit.aifb.cumulus.datasource.ConfigurationParameter.WRITE_CONSISTENCY;

import java.util.Map;

import me.prettyprint.cassandra.connection.LoadBalancingPolicy;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import edu.kit.aifb.cumulus.framework.datasource.CounterDAO;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.datasource.MapDAO;
import edu.kit.aifb.cumulus.framework.datasource.QuadIndexDAO;
import edu.kit.aifb.cumulus.framework.datasource.StorageLayout;
import edu.kit.aifb.cumulus.framework.datasource.TripleIndexDAO;
import edu.kit.aifb.cumulus.framework.domain.configuration.Configuration;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
 
/**
 * Concrete factory for creating members of Cassandra 1.2.x data access layer family.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class CumulusDataAccessLayerFactory extends DataAccessLayerFactory {

	private final String _keyspaceNameSuffix;
	private String _keyspaceName;
	private String _hosts;
	private int _replicationFactor;
	private String _readConsistency;
	private String _writeConsistency;
	
	private Cluster _cluster;
	private Keyspace _keyspace;

	/**
	 * Builds a new Cassandra 1.2.x data access layer factory.
	 * 
	 * @param layout the storage layout.
	 */
	public CumulusDataAccessLayerFactory(final StorageLayout layout) {
		super(layout);
		_keyspaceNameSuffix = layout == StorageLayout.TRIPLE ? "_T" : "_Q";
	}

	@Override
	public void accept(final Configuration<Map<String, Object>> configuration) {
		_hosts = configuration.getAttribute(HOSTS, "127.0.0.1:9160");
		_keyspaceName = configuration.getAttribute(KEYSPACE, "KeyspaceCumulus") + _keyspaceNameSuffix;
		_replicationFactor = configuration.getAttribute(REPLICATION_FACTOR, Integer.valueOf(1));
		_readConsistency = configuration.getAttribute(READ_CONSISTENCY, "ONE");
		_writeConsistency = configuration.getAttribute(WRITE_CONSISTENCY, "ONE");
		
		final CassandraHostConfigurator config = new CassandraHostConfigurator(_hosts);

		config.setRetryDownedHosts(configuration.getAttribute(RETRY_DOWNED_HOSTS, Boolean.TRUE));

		final Integer socketTimeout = configuration.getAttribute(THRIFT_SOCKET_TIMEOUT, null);
		if (socketTimeout != null) {
			config.setCassandraThriftSocketTimeout(socketTimeout);
		}
		
		config.setRetryDownedHostsDelayInSeconds(configuration.getAttribute(RETRY_DOWNED_HOSTS_DELAY_IN_SECONDS, Integer.valueOf(10)));
		config.setRetryDownedHostsQueueSize(configuration.getAttribute(RETRY_DOWNED_HOSTS_QUEUE_SIZE, Integer.valueOf(256)));
		
		
		config.setMaxWaitTimeWhenExhausted(configuration.getAttribute(MAX_WAIT_TIME_WHEN_EXHAUSTED, Integer.valueOf(0)));
		
		final String lbPolicy = configuration.getAttribute(LOAD_BALANCING_POLICY, null);
		if (lbPolicy != null) {
			try {
				config.setLoadBalancingPolicy((LoadBalancingPolicy) Class.forName(lbPolicy).newInstance());				
			} catch (Exception ignore) {
				// Just use the default value
			}
		}
		
		_cluster = HFactory.getOrCreateCluster("CumulusRDFCluster", config);		
	}

	@Override
	public <K, V> MapDAO<K, V> getMapDAO(
			final Class<K> keyClass, 
			final Class<V> valueClass,
			final boolean isBidirectional, 
			final String mapName) {
		return new Cassandra12xMapDAO<K, V>(this, keyClass, valueClass, isBidirectional, mapName);
	}
	
	@Override
	public <K> CounterDAO<K> getCounterDAO(final Class<K> keyClass, final String counterName) {
		return new Cassandra12xCounterDAO<K>(this, keyClass, counterName);
	}	
	
	@Override
	public TripleIndexDAO getTripleIndexDAO(final ITopLevelDictionary dictionary) {
		DAOManager.getInstance().daoWasCreated();
		return new Cassandra12xTripleIndexDAO(this, dictionary);
	}

	@Override
	public QuadIndexDAO getQuadIndexDAO(final ITopLevelDictionary dictionary) {
		DAOManager.getInstance().daoWasCreated();
		return new Cassandra12xQuadIndexDAO(this, dictionary);
	}

	@Override
	public String getUnderlyingStorageInfo() {
		return "Cassandra 1.2.x";
	}

	// Cassandra 1.2.x (Hector) specific methods
	// Note that these are not part of the DataAccessLayerFactory public interface.
	
	/**
	 * Returns the cluster currently in use.
	 * 
	 * @return the cluster currently in use.
	 */
	public Cluster getCluster() {
		return _cluster;
	}

	/**
	 * Returns the keyspace currently in use.
	 * 
	 * @return the keyspace currently in use.
	 */
	public Keyspace getKeyspace() {
		return _keyspace;
	}

	/**
	 * Returns the Cassandra hosts which we want to connect to.
	 * 
	 * @return the Cassandra hosts which we want to connect to.
	 */
	public String getCassandraHosts() {
		return _hosts;
	}

	/**
	 * Returns the replication factor in use.
	 * 
	 * @return the replication factor in use.
	 */
	public int getReplication() {
		return _replicationFactor;
	}

	/**
	 * Returns the keyspace name in use.
	 * 
	 * @return the keyspace name in use.
	 */
	public String getKeyspaceName() {
		return _keyspaceName;
	}

	/**
	 * Sets the keyspace name in use.
	 * 
	 * @param keyspace the keyspace name in use.
	 */
	public void setKeyspace(final Keyspace keyspace) {
		_keyspace = keyspace;
	}
	
	/**
	 * Returns the write consistency level in use.
	 * 
	 * @return the write consistency level in use.
	 */
	public String getWriteConsistencyLevel() {
		return _writeConsistency;
	}
	
	/**
	 * Returns the read consistency level in use.
	 * 
	 * @return the read consistency level in use.
	 */
	public String getReadConsistencyLevel() {
		return _readConsistency;
	}
}