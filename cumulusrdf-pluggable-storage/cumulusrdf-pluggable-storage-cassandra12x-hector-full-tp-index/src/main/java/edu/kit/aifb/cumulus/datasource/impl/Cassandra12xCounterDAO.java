package edu.kit.aifb.cumulus.datasource.impl;

import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.STRING_SERIALIZER;
import static edu.kit.aifb.cumulus.datasource.impl.Utils.hasColumnFamily;
import static me.prettyprint.hector.api.factory.HFactory.createCounterColumn;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.CounterQuery;
import edu.kit.aifb.cumulus.framework.datasource.CounterDAO;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;

/**
 * Cassandra 1.2.x / Hector implementation of {@link CounterDAO}.
 * 
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 *
 * @param <K> the key kind.
 */
public class Cassandra12xCounterDAO<K> extends Cassandra12xMapDAO<K, Long> implements CounterDAO<K> {

	protected static final String COLUMN_NAME_AS_STRING = "v";

	/**
	 * Builds a new counter DAO with the given data.
	 * 
	 * @param factory the data access layer factory.
	 * @param keyClass the key class.
	 * @param name the name of the counter.
	 */
	Cassandra12xCounterDAO(final DataAccessLayerFactory factory, final Class<K> keyClass, final String name) {
		super(factory, keyClass, Long.class, false, name);
	}

	@Override
	public void decrement(final K key, final Long delta) {
		createMutator(_keyspace, _serializer_k).decrementCounter(key, _cf_name, COLUMN_NAME_AS_STRING, delta);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void delete(final K... keys) {

		for (K key : keys) {
			createMutator(_keyspace, _serializer_k).deleteCounter(key, _cf_name, COLUMN_NAME_AS_STRING, STRING_SERIALIZER);
		}
	}

	@Override
	public Long get(final K key) {

		final CounterQuery<K, String> counter = new ThriftCounterColumnQuery<K, String>(_keyspace, _serializer_k, STRING_SERIALIZER);
		counter.setColumnFamily(_cf_name).setKey(key).setName(COLUMN_NAME_AS_STRING);
		final HCounterColumn<String> c = counter.execute().get();

		if (c == null) {
			if (_default_value != null) {
				return _default_value;
			} else {
				return null;
			}
		} else {
			return c.getValue();
		}
	}

	@Override
	public void increment(final K key, final Long delta) {
		createMutator(_keyspace, _serializer_k).incrementCounter(key, _cf_name, COLUMN_NAME_AS_STRING, delta);
	}

	@Override
	public void set(final K key, final Long value) {
		createMutator(_keyspace, _serializer_k).insertCounter(key, _cf_name, createCounterColumn(COLUMN_NAME_AS_STRING, value));
	}
	
	@Override
	public void createRequiredSchemaEntities() throws DataAccessLayerException {
		final String keyspaceName = _keyspace.getKeyspaceName();
		KeyspaceDefinition ksdef = _factory.getCluster().describeKeyspace(keyspaceName);

		if (!hasColumnFamily(ksdef, _cf_name)) {
			ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, _cf_name);
			cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
			cfDef.setComparatorType(ComparatorType.UTF8TYPE);
			cfDef.setCompactionStrategy("LeveledCompactionStrategy");
			cfDef.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName());
			cfDef.setColumnType(ColumnType.STANDARD);
			_factory.getCluster().addColumnFamily(cfDef, true);
		}

		setDefaultValue(-1L);
	}
}