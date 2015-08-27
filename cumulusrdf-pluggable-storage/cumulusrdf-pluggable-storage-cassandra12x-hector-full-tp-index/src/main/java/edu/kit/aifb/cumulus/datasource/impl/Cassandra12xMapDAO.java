package edu.kit.aifb.cumulus.datasource.impl;

import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.BYTE_SERIALIZER;
import static edu.kit.aifb.cumulus.datasource.impl.Utils.hasColumnFamily;
import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static me.prettyprint.hector.api.factory.HFactory.createColumnQuery;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.service.KeyIterator.Builder;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.datasource.MapDAO;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * Cassandra 1.2.x implementation of {@link MapDAO}.
 * 
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 *
 * @param <K> the key kind / type managed by the underlying map structure.
 * @param <V> the value kind / type managed by the underlying map structure.
 */
public class Cassandra12xMapDAO<K, V> implements MapDAO<K, V> {

	protected Log _log = new Log(LoggerFactory.getLogger(Cassandra12xMapDAO.class));

	protected static final byte[] COLUMN_NAME = new byte[] { 0x1 };
	protected final Serializer<K> _serializer_k;
	protected final Serializer<V> _serializer_v;

	protected final Keyspace _keyspace;
	protected final String _cf_name;
	protected final boolean _isBidirectional;

	protected V _default_value;
	
	protected final CumulusDataAccessLayerFactory _factory;

	/**
	 * Builds a new data access object with the given data.
	 * 
	 * @param factory the data access layer factory.
	 * @param keyClass the key class.
	 * @param valueClass the value class.
	 * @param isBidirectional a flag indicating if the map shoulbe be accessed both by value and by key.
	 * @param columnFamilyName the column family name.
	 */
	Cassandra12xMapDAO(
			final DataAccessLayerFactory factory, 
			final Class<K> keyClass, 
			final Class<V> valueClass, 
			final boolean isBidirectional, 
			final String columnFamilyName) {
		_factory = (CumulusDataAccessLayerFactory) factory;
		_keyspace = _factory.getKeyspace();
		
		_serializer_k = SerializerTypeInferer.getSerializer(keyClass);
		_serializer_v = SerializerTypeInferer.getSerializer(valueClass);
		
		_isBidirectional = isBidirectional;
		_cf_name = columnFamilyName;
	}

	@Override
	public boolean contains(final K key) {
		final ColumnQuery<K, byte[], byte[]> q = createColumnQuery(_keyspace, _serializer_k, BYTE_SERIALIZER, BYTE_SERIALIZER);
		final QueryResult<HColumn<byte[], byte[]>> r = q.setKey(key).setName(COLUMN_NAME).setColumnFamily(_cf_name).execute();
		return r.get() != null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void delete(final K... keys) {

		if (keys == null || keys.length == 0) {
			return;
		}

		final Mutator<K> m = createMutator(_keyspace, _serializer_k);

		for (final K key : keys) {
			m.addDeletion(key, _cf_name, COLUMN_NAME, BYTE_SERIALIZER);
		}

		m.execute();
	}
	
	@Override
	public K getKey(final V value) throws DataAccessLayerException { 
		RangeSlicesQuery<K, byte[], V> rq = HFactory.createRangeSlicesQuery(
				_keyspace, 
				_serializer_k, 
				BYTE_SERIALIZER, 
				_serializer_v);
		rq.addEqualsExpression(COLUMN_NAME, value).setReturnKeysOnly()
				.setColumnFamily(_cf_name)
				.setColumnNames(COLUMN_NAME).setRowCount(1);
		final List<Row<K, byte[], V>> rows = rq.execute().get().getList();

		return rows.isEmpty() ? null : _serializer_k.fromBytes((byte[])rows.get(0).getKey());
	}
	
	@Override
	public V get(final K key) throws DataAccessLayerException {

		final ColumnQuery<K, byte[], V> q = createColumnQuery(_keyspace, _serializer_k, BYTE_SERIALIZER, _serializer_v);
		final QueryResult<HColumn<byte[], V>> r = q.setKey(key).setName(COLUMN_NAME).setColumnFamily(_cf_name).execute();
		final HColumn<byte[], V> c = r.get();

		if (c == null) {
			return (_default_value != null) ? _default_value : null;
		} else {
			return c.getValue();
		}
	}

	@Override
	public Iterator<K> keyIterator() {
		return new Builder<K>(_keyspace, _cf_name, _serializer_k).build().iterator();
	}

	@Override
	public Set<K> keySet() {

		final Set<K> keys = new HashSet<K>();

		for (final Iterator<K> iter = keyIterator(); iter.hasNext();) {
			keys.add(iter.next());
		}

		return keys;
	}

	@Override
	public void set(final K key, final V value) {
		createMutator(
				_keyspace, 
				_serializer_k).insert(
						key, 
						_cf_name, 
						createColumn(COLUMN_NAME, value, BYTE_SERIALIZER, _serializer_v));
	}

	@Override
	public void setAll(final Map<K, V> pairs) {
		final Mutator<K> mutator = createMutator(_keyspace, _serializer_k);

		for (final K key : pairs.keySet()) {
			mutator.addInsertion(key, _cf_name, createColumn(COLUMN_NAME, pairs.get(key), BYTE_SERIALIZER, _serializer_v));
		}

		try {
			mutator.execute();
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00057_ADD_FAILURE, exception);
		}
	}

	@Override
	public void setDefaultValue(final V defaultValue) {
		_default_value = defaultValue;
	}

	@Override
	public void createRequiredSchemaEntities() throws DataAccessLayerException {
		final String keyspace_name = _keyspace.getKeyspaceName();
		final KeyspaceDefinition ksdef = _factory.getCluster().describeKeyspace(keyspace_name);

		if (!hasColumnFamily(ksdef, _cf_name)) {

			final ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace_name, _cf_name);
			final Map<String, String> compressionOptions = new HashMap<String, String>();
			compressionOptions.put("sstable_compression", "SnappyCompressor");
			cfDef.setCompressionOptions(compressionOptions);
			
			cfDef.setColumnType(ColumnType.STANDARD);
			cfDef.setKeyValidationClass(ComparatorType.BYTESTYPE.getClassName());
			cfDef.setDefaultValidationClass(ComparatorType.BYTESTYPE.getClassName());
			cfDef.setCompactionStrategy("LeveledCompactionStrategy");

			if (_isBidirectional) {

				BasicColumnDefinition colDef = new BasicColumnDefinition();
				colDef.setName(BytesArraySerializer.get().toByteBuffer(COLUMN_NAME));
				colDef.setValidationClass(ComparatorType.BYTESTYPE.getClassName());
				colDef.setIndexType(ColumnIndexType.KEYS);
				colDef.setIndexName(_cf_name + "_val_idx");

				cfDef.addColumnDefinition(colDef);
			}

			_factory.getCluster().addColumnFamily(new ThriftCfDef(cfDef), true);
		}
	}
}