package edu.kit.aifb.cumulus.datasource.impl;

import static edu.kit.aifb.cumulus.datasource.ColumnFamily.O_SPC;
import static edu.kit.aifb.cumulus.datasource.ColumnFamily.PO_SC;
import static edu.kit.aifb.cumulus.datasource.ColumnFamily.RDT_PO_S;
import static edu.kit.aifb.cumulus.datasource.ColumnFamily.RDT_SP_O;
import static edu.kit.aifb.cumulus.datasource.ColumnFamily.RN_PO_S;
import static edu.kit.aifb.cumulus.datasource.ColumnFamily.RN_SP_O;
import static edu.kit.aifb.cumulus.datasource.ColumnFamily.S_POC;
import static edu.kit.aifb.cumulus.datasource.LocalUtility.reorderQuadReverse;
import static edu.kit.aifb.cumulus.datasource.LocalUtility.reorderTriple;
import static edu.kit.aifb.cumulus.datasource.LocalUtility.reorderTripleReverse;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.BYTE_SERIALIZER;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.COMPOSITE_SERIALIZER;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.DONT_INCLUDE_PREDICATE_COLUMN;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.DOUBLE_SERIALIZER;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.EMPTY_VAL;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.INCLUDE_ALL_COMPOSITE_HIGHER_BOUND;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.INCLUDE_ALL_COMPOSITE_LOWER_BOUND;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.LONG_SERIALIZER;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.P_COL;
import static edu.kit.aifb.cumulus.framework.Environment.DATETIME_RANGETYPES;
import static edu.kit.aifb.cumulus.framework.Environment.NUMERIC_RANGETYPES;
import static edu.kit.aifb.cumulus.framework.util.Utility.isVariable;
import static edu.kit.aifb.cumulus.framework.util.Utility.parseXMLSchemaDateTimeAsMSecs;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.KeyIterator.Builder;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;

import edu.kit.aifb.cumulus.datasource.ColumnFamily;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.datasource.TripleIndexDAO;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * Cassandra 1.2.x implementation of {@link TripleIndexDAO}.
 * Note that this DAO is not fully stateless; that is, since it uses
 * Hector behind the scenes, it maintains a set of thread-scoped Mutators that 
 * must be flushed (see {@link #executePendingMutations()} in order to trigger the material execution of the pending mutations.
 * 
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
@SuppressWarnings("deprecation")
public class Cassandra12xTripleIndexDAO implements TripleIndexDAO {
	protected final Log _log = new Log(LoggerFactory.getLogger(getClass()));
	
	protected Map<String, int[]> _maps = new HashMap<String, int[]>();
	{
		_maps.put(S_POC, new int[] {0, 1, 2, 3});
		_maps.put(O_SPC, new int[] {2, 0, 1, 3});
		_maps.put(PO_SC, new int[] {1, 2, 0, 3});

		_maps.put(RN_SP_O, new int[] {0, 1, 2});
		_maps.put(RN_PO_S, new int[] {1, 2, 0});
		_maps.put(RDT_PO_S, new int[] {1, 2, 0});
		_maps.put(RDT_SP_O, new int[] {0, 1, 2});
	}
	
	private final Map<String, String> _compressionOptions = new HashMap<String, String>();
	{
		_compressionOptions.put("sstable_compression", "SnappyCompressor");
	}

	protected final ThreadLocal<Mutator<byte[]>> _mutators = new ThreadLocal<Mutator<byte[]>>() {
		@Override
		protected Mutator<byte[]> initialValue() {
			return HFactory.createMutator(_dataAccessLayerFactory.getKeyspace(), BYTE_SERIALIZER);
		}
	};
	
	protected final CumulusDataAccessLayerFactory _dataAccessLayerFactory;
	protected final ITopLevelDictionary _dictionary;

	/**
	 * Builds a new triple index DAO with the given data.
	 * 
	 * @param factory the data access layer factory.
	 * @param dictionary the dictionary currently in use.
	 */
	Cassandra12xTripleIndexDAO(final DataAccessLayerFactory factory, final ITopLevelDictionary dictionary) {
		_dataAccessLayerFactory = (CumulusDataAccessLayerFactory)factory;
		_dictionary = dictionary;
	}
	
	@Override
	public void clear() {
		final Cluster cluster = _dataAccessLayerFactory.getCluster();
		final String keyspaceName = _dataAccessLayerFactory.getKeyspaceName();
		cluster.truncate(keyspaceName, S_POC);
		cluster.truncate(keyspaceName, O_SPC);
		cluster.truncate(keyspaceName, PO_SC);
		cluster.truncate(keyspaceName, RN_SP_O);
		cluster.truncate(keyspaceName, RN_PO_S);
		cluster.truncate(keyspaceName, RDT_PO_S);		
		cluster.truncate(keyspaceName, RDT_SP_O);		
	}
	
	@Override
	public void close() throws DataAccessLayerException {
		DAOManager.getInstance().daoWasClosed(_dataAccessLayerFactory.getCluster());
	}	
	
	/**
	 * Creates a column family definition.
	 * 
	 * @param colName the column family name.
	 * @param validationClass the validation class.
	 * @param indexName the index name.
	 * @return the column family definition.
	 */
	protected ColumnDefinition createCDef(
			final byte[] colName,
			final String validationClass,
			final String indexName) {

		final BasicColumnDefinition colDef = new BasicColumnDefinition();
		colDef.setName(ByteBuffer.wrap(colName));
		colDef.setValidationClass(validationClass);
		colDef.setIndexType(ColumnIndexType.KEYS);
		colDef.setIndexName(indexName);
		return colDef;
	}	
	
	/**
	 * Creates a column family definition.
	 * 
	 * @param cfName the column family name.
	 * @param indexedCols names of columns that will be indexed.
	 * @param keyComp the key comparator.
	 * @param valueValidationClass the value validation class.
	 * @param compositeCol a flag that indicates if columns are composite.
	 * @return the column family definition.
	 */
	protected ColumnFamilyDefinition createCF(
			final String cfName,
			final List<byte[]> indexedCols,
			final ComparatorType keyComp,
			final ComparatorType valueValidationClass,
			final boolean compositeCol) {

		final ColumnFamilyDefinition cfdef = HFactory.createColumnFamilyDefinition(
				_dataAccessLayerFactory.getKeyspaceName(), 
				cfName, 
				compositeCol
					? ComparatorType.COMPOSITETYPE
					: ComparatorType.BYTESTYPE);
		cfdef.setKeyspaceName(_dataAccessLayerFactory.getKeyspaceName());
		cfdef.setColumnType(ColumnType.STANDARD);
		cfdef.setCompactionStrategy("LeveledCompactionStrategy");

		if (compositeCol) {
			cfdef.setComparatorTypeAlias("(BytesType, BytesType, BytesType)");
		}

		for (byte[] col : indexedCols) {
			final String indexColumnFamilyName = "index_" + cfName + "_" + Arrays.hashCode(col);
			try {
				_dataAccessLayerFactory.getCluster().dropColumnFamily(
						_dataAccessLayerFactory.getKeyspaceName(),
						indexColumnFamilyName,
						true);
			} catch (HInvalidRequestException ignore) {
				// Nothing to be done here...
			}
			cfdef.addColumnDefinition(createCDef(col, valueValidationClass.getClassName(), indexColumnFamilyName));
		}

		cfdef.setKeyValidationClass(keyComp.getClassName());
		cfdef.setDefaultValidationClass(valueValidationClass.getClassName());
		cfdef.setCompressionOptions(_compressionOptions);

		return new ThriftCfDef(cfdef);
	}
	
	/**
	 * Creates POS? column family.
	 * 
	 * @param cfName the column family name.
	 * @return the POS? column family definition.
	 */
	protected ColumnFamilyDefinition createCF_PO_Sx(final String cfName) {

		final ColumnFamilyDefinition cfdef = HFactory.createColumnFamilyDefinition(_dataAccessLayerFactory.getKeyspaceName(), cfName, ComparatorType.COMPOSITETYPE);
		cfdef.setColumnType(ColumnType.STANDARD);
		cfdef.setKeyValidationClass(ComparatorType.BYTESTYPE.getClassName());
		cfdef.setDefaultValidationClass(ComparatorType.BYTESTYPE.getClassName());
		cfdef.setComparatorTypeAlias("(DoubleType, BytesType)");
		cfdef.setCompactionStrategy("LeveledCompactionStrategy");
		
		cfdef.setCompressionOptions(_compressionOptions);
		return new ThriftCfDef(cfdef);
	}
	
	/**
	 * Creates POS? column family used for date range queries.
	 * 
	 * @param cfName the column family name.
	 * @return the POS? column family definition.
	 */
	protected ColumnFamilyDefinition createCF_RDT_PO_S(final String cfName) {
		final ColumnFamilyDefinition cfdef = HFactory.createColumnFamilyDefinition(_dataAccessLayerFactory.getKeyspaceName(), cfName, ComparatorType.COMPOSITETYPE);
		cfdef.setColumnType(ColumnType.STANDARD);
		cfdef.setKeyValidationClass(ComparatorType.BYTESTYPE.getClassName());
		cfdef.setDefaultValidationClass(ComparatorType.BYTESTYPE.getClassName());
		cfdef.setComparatorTypeAlias("(LongType, BytesType)");
		cfdef.setCompactionStrategy("LeveledCompactionStrategy");

		cfdef.setCompressionOptions(_compressionOptions);
		return new ThriftCfDef(cfdef);
	}	
	
	/**
	 * Creates SPO? column family used for date range queries.
	 * 
	 * @param cfName the column family name.
	 * @return the SPO? column family definition.
	 */
	protected ColumnFamilyDefinition createCF_RDT_SP_O(final String cfName) {
		final ColumnFamilyDefinition cfdef = HFactory.createColumnFamilyDefinition(_dataAccessLayerFactory.getKeyspaceName(), cfName, ComparatorType.LONGTYPE);
		cfdef.setColumnType(ColumnType.STANDARD);
		cfdef.setKeyValidationClass(ComparatorType.BYTESTYPE.getClassName());
		cfdef.setDefaultValidationClass(ComparatorType.BYTESTYPE.getClassName());
		cfdef.setCompactionStrategy("LeveledCompactionStrategy");

		cfdef.setCompressionOptions(_compressionOptions);
		return new ThriftCfDef(cfdef);
	}
	
	/**
	 * Creates SPO? column family.
	 * 
	 * @param cfName the column family name.
	 * @return the SPO? column family definition.
	 */
	protected ColumnFamilyDefinition createCF_SP_Ox(final String cfName) {
		final BasicColumnFamilyDefinition cfdef = new BasicColumnFamilyDefinition();
		cfdef.setKeyspaceName(_dataAccessLayerFactory.getKeyspaceName()); 
		cfdef.setName(cfName);
		cfdef.setColumnType(ColumnType.STANDARD);
		cfdef.setComparatorType(ComparatorType.getByClassName("org.apache.cassandra.db.marshal.DoubleType"));
		cfdef.setCompactionStrategy("LeveledCompactionStrategy");

		cfdef.setKeyValidationClass(ComparatorType.BYTESTYPE.getClassName());
		cfdef.setDefaultValidationClass(ComparatorType.BYTESTYPE.getClassName());

		cfdef.setCompressionOptions(_compressionOptions);
		return new ThriftCfDef(cfdef);
	}
	
	/**
	 * Creates required column families according with the concrete store layout.
	 * 
	 * @return a list of column family definitions.
	 */
	@SuppressWarnings("unchecked")
	protected List<ColumnFamilyDefinition> createColumnFamiliyDefinitions() {

		final ColumnFamilyDefinition spoc = createCF(S_POC, Collections.EMPTY_LIST, ComparatorType.BYTESTYPE, ComparatorType.BYTESTYPE, true);
		final ColumnFamilyDefinition ospc = createCF(O_SPC, Collections.EMPTY_LIST, ComparatorType.BYTESTYPE, ComparatorType.BYTESTYPE, true);
		final ColumnFamilyDefinition posc = createCF(PO_SC, Arrays.asList(COMPOSITE_SERIALIZER.toBytes(P_COL)), ComparatorType.BYTESTYPE, ComparatorType.BYTESTYPE, true);
		
		final ColumnFamilyDefinition spox = createCF_SP_Ox(RN_SP_O);
		final ColumnFamilyDefinition psox = createCF_PO_Sx(RN_PO_S);
		final ColumnFamilyDefinition psodtx = createCF_RDT_PO_S(RDT_PO_S);
		final ColumnFamilyDefinition spodtx = createCF_RDT_SP_O(RDT_SP_O);

		List<ColumnFamilyDefinition> defs = new ArrayList<ColumnFamilyDefinition>();
		defs.addAll(Arrays.asList(spoc, ospc, posc, spox, psox, psodtx, spodtx));
		return defs;
	}
	
	/**
	 * Creates the CumulusRDF keyspace.
	 * 
	 * @param keyspaceName the keyspace name.
	 * @return the keyspace definition.
	 */
	protected KeyspaceDefinition createKeyspaceDefinition(final String keyspaceName) {
		return HFactory.createKeyspaceDefinition(
				keyspaceName,
				"org.apache.cassandra.locator.SimpleStrategy", 
				_dataAccessLayerFactory.getReplication(),
				createColumnFamiliyDefinitions());
	}

	@Override
	public Iterator<byte[][]> dateRangeQuery(
			final Value[] query, 
			final long lowerBound, 
			final boolean equalsLower,
			final long upperBound, 
			final boolean equalsUpper,
			final boolean reverse, 
			final int limit) throws DataAccessLayerException {
		final Keyspace keyspace = _dataAccessLayerFactory.getKeyspace();
		final byte[][] query_ids = new byte[][] {
				_dictionary.getID(query[0], false), 
				_dictionary.getID(query[1], true)};
		
		// subject is variable
		if (isVariable(query[0])) {

			final Composite start = new Composite(), end = new Composite();

			if (!reverse) {
				start.addComponent(0, lowerBound, Composite.ComponentEquality.EQUAL);
				end.addComponent(0, upperBound, Composite.ComponentEquality.GREATER_THAN_EQUAL);
			} else {
				start.addComponent(0, upperBound, Composite.ComponentEquality.EQUAL);
				end.addComponent(0, lowerBound, Composite.ComponentEquality.GREATER_THAN_EQUAL);
			}

			final SliceQuery<byte[], Composite, byte[]> sq = HFactory.createSliceQuery(keyspace, BYTE_SERIALIZER, COMPOSITE_SERIALIZER, BYTE_SERIALIZER)
				.setColumnFamily(RDT_PO_S)
				.setKey(query_ids[1])
				.setRange(start, end, reverse, 100);

			final ColumnSliceIterator<byte[], Composite, byte[]> iterator = new ColumnSliceIterator<byte[], Composite, byte[]>(sq, start, end, reverse, 100);

			return new AbstractIterator<byte[][]>() {

				private int _returned = 0;

				@Override
				protected byte[][] computeNext() {
					try {

						if (!iterator.hasNext() || (_returned >= limit)) {
							return endOfData();
						}

						HColumn<Composite, byte[]> next = iterator.next();

						long object_value = next.getName().get(0, LONG_SERIALIZER);

						if ((object_value == lowerBound) && !equalsLower) {

							if (!iterator.hasNext()) {
								return endOfData();
							} else {
								next = iterator.next();
								object_value = next.getName().get(0, LONG_SERIALIZER);
							}
						}

						if ((object_value == upperBound) && !equalsUpper) {
							return endOfData();
						}

						byte[] subject = next.getName().get(1, BYTE_SERIALIZER);
						byte[] object = next.getValue();

						_returned++;
						return new byte[][] {subject, query_ids[1], object};

					} catch (final Exception exception) {

						_log.error(
								MessageCatalog._00062_QUERY_EVALUATION_FAILURE,
								exception,
								Arrays.toString(query),
								lowerBound,
								upperBound);
						return endOfData();
					}
				}
			};
		} else {
			// subject is constant

			final SliceQuery<byte[], Long, byte[]> sq = HFactory.createSliceQuery(keyspace, BYTE_SERIALIZER, LONG_SERIALIZER, BYTE_SERIALIZER)
				.setColumnFamily(RDT_SP_O)
				.setKey(_dictionary.compose(query_ids[0], query_ids[1]));

			final ColumnSliceIterator<byte[], Long, byte[]> iterator = new ColumnSliceIterator<byte[], Long, byte[]>(sq, reverse ? upperBound
					: lowerBound, reverse ? lowerBound : upperBound, reverse, 100);

			return new AbstractIterator<byte[][]>() {

				private int _returned = 0;

				@Override
				protected byte[][] computeNext() {

					try {

						if (!iterator.hasNext() || (_returned >= limit)) {
							return endOfData();
						}

						HColumn<Long, byte[]> next = iterator.next();

						long object_value = next.getName();

						if ((object_value == lowerBound) && !equalsLower) {

							if (!iterator.hasNext()) {

								return endOfData();
							} else {
								next = iterator.next();
							}
						}

						if ((object_value == upperBound) && !equalsUpper) {
							return endOfData();
						}

						byte[] object = next.getValue();

						_returned++;
						return new byte[][] {query_ids[0], query_ids[1], object};

					} catch (final Exception exception) {
						_log.error(
								MessageCatalog._00062_QUERY_EVALUATION_FAILURE,
								exception,
								Arrays.toString(query),
								lowerBound,
								upperBound);
						return endOfData();
					}
				}
			};
		}		
	}

	/**
	 * Deletes in OPSC index.
	 * 
	 * @param rowKey the row key.
	 * @param ids the triple identifiers
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void deleteInOSPC(final byte[] rowKey, final byte[][]ids) throws DataAccessLayerException {
		final Composite spc_col = new Composite();

		spc_col.addComponent(ids[0], BYTE_SERIALIZER);
		spc_col.addComponent(ids[1], BYTE_SERIALIZER);

		if (ids.length == 4) {
			spc_col.addComponent(ids[3], BYTE_SERIALIZER);
		}

		_mutators.get().addDeletion(rowKey, O_SPC, spc_col, COMPOSITE_SERIALIZER);		
	}
	
	/**
	 * Deletes in POSC index.
	 * 
	 * @param rowKey the row key.
	 * @param ids the triple identifiers
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void deleteInPOSC(final byte[] rowKey, final byte[][]ids) throws DataAccessLayerException {
		final Composite sc_col = new Composite();
		// subject
		sc_col.addComponent(ids[0], BYTE_SERIALIZER);

		if (ids.length == 4) {
			// context
			sc_col.addComponent(ids[3], BYTE_SERIALIZER);
		}

		_mutators.get().addDeletion(rowKey, ColumnFamily.PO_SC, sc_col, COMPOSITE_SERIALIZER);
	}
	
	/**
	 * Deletes a value in RDT_POS index.
	 * 
	 * @param rowKey the row key.
	 * @param value the value.
	 * @param ids the triple identifiers.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void deleteInRDT_POS(final byte [] rowKey, final byte [][] ids, final long value) throws DataAccessLayerException {
		final Composite os_col = new Composite();
		os_col.addComponent(value, LONG_SERIALIZER);
		os_col.addComponent(ids[0], BYTE_SERIALIZER);
		_mutators.get().addDeletion(rowKey, RDT_PO_S, os_col, COMPOSITE_SERIALIZER);
	}
	
	/**
	 * Deletes a value in RDT_SPO index.
	 * 
	 * @param rowKey the row key.
	 * @param value the value.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void deleteInRDT_SPO(final byte [] rowKey, final long value) throws DataAccessLayerException {
		_mutators.get().addDeletion(rowKey, RDT_SP_O, value, LONG_SERIALIZER);
	}
	
	/**
	 * Deletes a value in RN_SPO index.
	 * 
	 * @param rowKey the row key.
	 * @param value the value.
	 * @param ids the triple identifiers.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void deleteInRN_POS(final byte [] rowKey, final byte [][] ids, final double value) throws DataAccessLayerException {
		final Composite os_col = new Composite();
		os_col.addComponent(value, DOUBLE_SERIALIZER);
		os_col.addComponent(ids[0], BYTE_SERIALIZER);
		_mutators.get().addDeletion(rowKey, RN_PO_S, os_col, COMPOSITE_SERIALIZER);
	}	

	/**
	 * Deletes a value in RN_SPO index.
	 * 
	 * @param rowKey the row key.
	 * @param value the value.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void deleteInRN_SPO(final byte [] rowKey, final double value) throws DataAccessLayerException {
		_mutators.get().addDeletion(rowKey, RN_SP_O, value, DOUBLE_SERIALIZER);
	}
	
	/**
	 * Deletes in SPOC index.
	 * 
	 * @param rowKey the row key.
	 * @param ids the triple identifiers
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void deleteInSPOC(final byte[] rowKey, final byte[][]ids) throws DataAccessLayerException {
		final Composite poc_col = new Composite();

		// predicate
		poc_col.addComponent(ids[1], BYTE_SERIALIZER);
		// object
		poc_col.addComponent(ids[2], BYTE_SERIALIZER);

		if (ids.length == 4) {
			// context
			poc_col.addComponent(ids[3], BYTE_SERIALIZER);
		}

		_mutators.get().addDeletion(rowKey, S_POC, poc_col, COMPOSITE_SERIALIZER);	
	}	
	
	@Override
	public List<byte[][]> deleteTriples(
			final Iterator<byte[][]> nodes, 
			final int batchSize, 
			final boolean rangesEnabled) throws DataAccessLayerException {
		
		final List<byte[][]> deleted = new ArrayList<byte[][]>(batchSize);
		final SecondaryIndexDeletionBuffer secondayIndexDeletionBuffer = new SecondaryIndexDeletionBuffer(batchSize);

		while (nodes.hasNext()) {

			for (int i = 0; (i < batchSize) && nodes.hasNext(); i++) {

				byte[][] ids = nodes.next();

				if ((ids == null) || (ids.length < 3)) {
					continue;
				}
				
				internalDelete(ids, secondayIndexDeletionBuffer, rangesEnabled);
				
				deleted.add(ids);
			}

			_mutators.get().execute();
			
			if (nodes.hasNext()) {
				secondayIndexDeletionBuffer.flushIfFull(_mutators.get(), PO_SC, P_COL, COMPOSITE_SERIALIZER, this);
			} else {
				secondayIndexDeletionBuffer.flush(_mutators.get(), PO_SC, P_COL, COMPOSITE_SERIALIZER, this);
			}
			
			_mutators.get().execute();
		}
		return deleted;
	}

	@Override
	public void executePendingMutations() throws DataAccessLayerException {
		_mutators.get().execute();
	}
	
	@Override 
	public void initialiseRdfIndex() throws DataAccessLayerException {
		final String keyspaceName = _dataAccessLayerFactory.getKeyspaceName();
		final Cluster cluster = _dataAccessLayerFactory.getCluster();
		
		final KeyspaceDefinition ksdef_rdf = cluster.describeKeyspace(keyspaceName);

		if (ksdef_rdf == null) {
			cluster.addKeyspace(createKeyspaceDefinition(keyspaceName));
		}

		_log.info(MessageCatalog._00050_REPLICATION_FACTOR, _dataAccessLayerFactory.getReplication());
		
		final HConsistencyLevel read = 
				HConsistencyLevel.valueOf(_dataAccessLayerFactory.getReadConsistencyLevel()) == null 
					? HConsistencyLevel.ONE 
					: HConsistencyLevel.valueOf(_dataAccessLayerFactory.getReadConsistencyLevel());
		final HConsistencyLevel write = 
				HConsistencyLevel.valueOf(_dataAccessLayerFactory.getWriteConsistencyLevel()) == null 
					? HConsistencyLevel.ONE 
					: HConsistencyLevel.valueOf(_dataAccessLayerFactory.getWriteConsistencyLevel());

		final Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster, new ConsistencyLevelPolicy() {

			@Override
			public HConsistencyLevel get(final OperationType type) {

				if ((type == OperationType.READ) || (type == OperationType.META_READ)) {
					return read;
				} else {
					return write;
				}
			}

			@Override
			public HConsistencyLevel get(final OperationType type, final String columnFamilyName) {
				if ((type == OperationType.READ) || (type == OperationType.META_READ)) {
					return read;
				} else {
					return write;
				}
			}
		});	
		
		_dataAccessLayerFactory.setKeyspace(keyspace);
		
		_log.info(MessageCatalog._00051_CONSISTENCY_LEVEL, read, write);
	}
	
	/**
	 * Insert a triple in OPSC index.
	 * 
	 * @param rowKey the row key.
	 * @param ids the triple identifiers
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void insertInOPSC(final byte [] rowKey, final byte[][]ids) throws DataAccessLayerException {
		final Composite spc_col = new Composite();
		spc_col.addComponent(ids[0], BYTE_SERIALIZER);
		spc_col.addComponent(ids[1], BYTE_SERIALIZER);
		if (ids.length == 4) {
			spc_col.addComponent(ids[3], BYTE_SERIALIZER);
		}

		_mutators.get().addInsertion(rowKey, O_SPC, HFactory.createColumn(spc_col, EMPTY_VAL, COMPOSITE_SERIALIZER, BYTE_SERIALIZER));
	}
	
	/**
	 * Insert a triple in POSC index.
	 * 
	 * @param rowKey the row key.
	 * @param ids the triple identifiers
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void insertInPOSC(final byte [] rowKey, final byte[][]ids) throws DataAccessLayerException {
	
		final Composite sc_col = new Composite();
		sc_col.addComponent(ids[0], BYTE_SERIALIZER);

		if (ids.length == 4) {
			sc_col.addComponent(ids[3], BYTE_SERIALIZER);
		}

		// subject col
		_mutators.get()
			.addInsertion(rowKey, PO_SC, HFactory.createColumn(sc_col, EMPTY_VAL, COMPOSITE_SERIALIZER, BYTE_SERIALIZER))
			.addInsertion(rowKey, PO_SC, HFactory.createColumn(P_COL, ids[1], COMPOSITE_SERIALIZER, BYTE_SERIALIZER));
	}
		
	/**
	 * Insert a value in RDT_POS index.
	 * 
	 * @param rowKey the row key.
	 * @param value the value.
	 * @param ids the triple identifiers
	 * @throws DataAccessLayerException in case of data access failure.
	 */	
	void insertInRDT_POS(final byte[] rowKey, final byte[][] ids, final long value) throws DataAccessLayerException {
		final Composite os_col = new Composite();
		os_col.addComponent(value, LONG_SERIALIZER);
		os_col.addComponent(ids[0], BYTE_SERIALIZER);

		_mutators.get().addInsertion(rowKey, RDT_PO_S, HFactory.createColumn(os_col, ids[2], COMPOSITE_SERIALIZER, BYTE_SERIALIZER));		
	}
	
	/**
	 * Insert a value in RDT_SPO index.
	 * 
	 * @param rowKey the row key.
	 * @param value the value.
	 * @param ids the triple identifiers
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void insertInRDT_SPO(final byte[] rowKey, final byte[][] ids, final long value) throws DataAccessLayerException {
		_mutators.get().addInsertion(rowKey, RDT_SP_O, HFactory.createColumn(value, ids[2], LONG_SERIALIZER, BYTE_SERIALIZER));
	}
	
	/**
	 * Insert a value in RN_POS index.
	 * 
	 * @param rowKey the row key.
	 * @param value the value.
	 * @param ids the triple identifiers
	 * @throws DataAccessLayerException in case of data access failure.
	 */	
	void insertInRN_POS(final byte[] rowKey, final byte[][] ids, final double value) throws DataAccessLayerException {
		final Composite os_col = new Composite();
		os_col.addComponent(value, DOUBLE_SERIALIZER);
		os_col.addComponent(ids[0], BYTE_SERIALIZER);

		_mutators.get().addInsertion(rowKey, RN_PO_S, HFactory.createColumn(os_col, ids[2], COMPOSITE_SERIALIZER, BYTE_SERIALIZER));
	}	
	
	/**
	 * Insert a value in RN_SPO index.
	 * 
	 * @param rowKey the row key.
	 * @param value the value.
	 * @param ids the triple identifiers
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void insertInRN_SPO(final byte[] rowKey, final byte[][] ids, final double value) throws DataAccessLayerException {
		_mutators.get().addInsertion(rowKey, RN_SP_O, HFactory.createColumn(value, ids[2], DOUBLE_SERIALIZER, BYTE_SERIALIZER));
	}	
	
	/**
	 * Insert a triple in SPOC index.
	 * 
	 * @param rowKey the row key.
	 * @param ids the triple identifiers
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void insertInSPOC(final byte [] rowKey, final byte[][]ids) throws DataAccessLayerException {
		final Composite poc_col = new Composite();
		poc_col.addComponent(ids[1], BYTE_SERIALIZER);
		poc_col.addComponent(ids[2], BYTE_SERIALIZER);
		if (ids.length == 4) {
			poc_col.addComponent(ids[3], BYTE_SERIALIZER);
		}

		_mutators.get().addInsertion(rowKey, S_POC, HFactory.createColumn(poc_col, EMPTY_VAL, COMPOSITE_SERIALIZER, BYTE_SERIALIZER));
	}
		
	@Override
	public void insertRanges(final byte[][] ids, final double value) throws DataAccessLayerException {
		insertInRN_SPO(_dictionary.compose(ids[0], ids[1]), ids, value);
		insertInRN_POS(ids[1], ids, value);
	}	
	
	@Override
	public void insertRanges(final byte[][] ids, final long value) throws DataAccessLayerException {
		insertInRDT_POS(ids[1], ids, value);
		insertInRDT_SPO(_dictionary.compose(ids[0], ids[1]), ids, value);					
	}	
	
	@Override
	public void insertTriple(final byte[][] ids) throws DataAccessLayerException {
		byte[] po_row = _dictionary.compose(ids[1], ids[2]);
		
		insertInPOSC(po_row, ids);
		insertInSPOC(ids[0], ids);
		insertInOPSC(ids[2], ids);
	}

	/**
	 * Internal method used for reuse delete stuff.
	 * 
	 * @param ids the triple identifiers.
	 * @param secondayIndexDeletionBuffer the secondary index deletion buffer.
	 * @param rangesEnabled if ranges have been enabled on the current store.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	protected void internalDelete(
			final byte [][]ids, 
			final SecondaryIndexDeletionBuffer secondayIndexDeletionBuffer, 
			final boolean rangesEnabled) throws DataAccessLayerException {
		byte[] po_row = _dictionary.compose(ids[1], ids[2]);
		deleteInPOSC(po_row, ids);
		
		if (ids.length == 4) {
			secondayIndexDeletionBuffer.add(new SecondaryIndexDeletionCandidate(po_row, new byte[][] {null, ids[1], ids[2], null}));
		} else {
			secondayIndexDeletionBuffer.add(new SecondaryIndexDeletionCandidate(po_row, new byte[][] {null, ids[1], ids[2]}));
		}

		deleteInSPOC(ids[0], ids);
		deleteInOSPC(ids[2], ids);

		if (rangesEnabled && _dictionary.isLiteral(ids[2])) {
			final Literal lit = (Literal) _dictionary.getValue(ids[2], false);
			final URI dt = lit.getDatatype();

			if (NUMERIC_RANGETYPES.contains(dt)) {

				final double value = Double.parseDouble(lit.getLabel());

				deleteInRN_SPO(_dictionary.compose(ids[0], ids[1]), value);
				deleteInRN_POS(ids[1], ids, value);
			} else if (DATETIME_RANGETYPES.contains(dt)) {

				final long value = parseXMLSchemaDateTimeAsMSecs(lit);

				deleteInRDT_POS(ids[1], ids, value);
				deleteInRDT_SPO(_dictionary.compose(ids[0], ids[1]), value);
			}
		}		
	}

	@Override
	public Iterator<byte[][]> numericRangeQuery(
			final Value[] query, 
			final double lowerBound, 
			final boolean equalsLower,
			final double upperBound, 
			final boolean equalsUpper,
			final boolean reverse, 
			final int limit) throws DataAccessLayerException {
		final Keyspace keyspace = _dataAccessLayerFactory.getKeyspace();
		
		final byte[][] query_ids = new byte[][] {
				_dictionary.getID(query[0], false), 
				_dictionary.getID(query[1], true)};
		
		if (isVariable(query[0])) {
			final Composite start = new Composite(), end = new Composite();
			if (!reverse) {
				start.addComponent(0, lowerBound, Composite.ComponentEquality.EQUAL);
				end.addComponent(0, upperBound, Composite.ComponentEquality.GREATER_THAN_EQUAL);
			} else {
				/*
				 * This is correct, since Hector expects to give upperBound as
				 * first Parameter when using reversed!
				 */
				start.addComponent(0, upperBound, Composite.ComponentEquality.EQUAL);
				end.addComponent(0, lowerBound, Composite.ComponentEquality.GREATER_THAN_EQUAL);
			}

			final SliceQuery<byte[], Composite, byte[]> sq = HFactory.createSliceQuery(keyspace, BYTE_SERIALIZER, COMPOSITE_SERIALIZER, BYTE_SERIALIZER);
			sq.setColumnFamily(RN_PO_S);
			sq.setKey(query_ids[1]);
			sq.setRange(start, end, reverse, 100);
			
			final ColumnSliceIterator<byte[], Composite, byte[]> iterator = new ColumnSliceIterator<byte[], Composite, byte[]>(sq, start, end, reverse, 100);

			return new AbstractIterator<byte[][]>() {

				private int _returned = 0;

				@Override
				protected byte[][] computeNext() {
					try {

						if (!iterator.hasNext() || (_returned >= limit)) {
							return endOfData();
						}

						HColumn<Composite, byte[]> next = iterator.next();

						double object_value = next.getName().get(0, DOUBLE_SERIALIZER);

						if ((object_value == lowerBound) && !equalsLower) {

							if (!iterator.hasNext()) {
								return endOfData();
							} else {
								next = iterator.next();
								object_value = next.getName().get(0, DOUBLE_SERIALIZER);
							}
						}

						if ((object_value == upperBound) && !equalsUpper) {
							return endOfData();
						}

						byte[] subject = next.getName().get(1, BYTE_SERIALIZER);
						byte[] object = next.getValue();

						_returned++;
						return new byte[][] {subject, query_ids[1], object};

					} catch (final Exception exception) {

						_log.error(
								MessageCatalog._00062_QUERY_EVALUATION_FAILURE,
								exception,
								Arrays.toString(query),
								lowerBound,
								upperBound);
						return endOfData();
					}
				}
			};
		} else {
			// subject is constant
			
			final double start = reverse ? upperBound : lowerBound, end = reverse ? lowerBound : upperBound;

			final SliceQuery<byte[], Double, byte[]> sq = HFactory.createSliceQuery(keyspace, BYTE_SERIALIZER, DOUBLE_SERIALIZER, BYTE_SERIALIZER);
			sq.setColumnFamily(RN_SP_O);
			sq.setKey(_dictionary.compose(query_ids[0], query_ids[1]));
			sq.setRange(start, end, reverse, 100);

			return new AbstractIterator<byte[][]>() {
				final ColumnSliceIterator<byte[], Double, byte[]> _iterator 
					= new ColumnSliceIterator<byte[], Double, byte[]>(sq, start, end, reverse, 100);

				private int _returned = 0;

				@Override
				protected byte[][] computeNext() {

					try {

						if (!_iterator.hasNext() || (_returned >= limit)) {
							return endOfData();
						}

						HColumn<Double, byte[]> next = _iterator.next();

						double object_value = next.getName();

						if ((object_value == lowerBound) && !equalsLower) {

							if (!_iterator.hasNext()) {
								return endOfData();
							} else {
								next = _iterator.next();
								object_value = next.getName();
							}
						}

						if ((object_value == upperBound) && !equalsUpper) {
							return endOfData();
						}

						final byte[] object = next.getValue();

						_returned++;
						return new byte[][] {query_ids[0], query_ids[1], object};

					} catch (final Exception exception) {
						_log.error(
								MessageCatalog._00062_QUERY_EVALUATION_FAILURE,
								exception,
								Arrays.toString(query),
								lowerBound,
								upperBound);
						return endOfData();
					}
				}
			};
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<byte[][]> query(final byte[][] query, final int limit) throws DataAccessLayerException {
		final Keyspace keyspace = _dataAccessLayerFactory.getKeyspace();
			
		if (isVariable(query[0]) && isVariable(query[1]) && isVariable(query[2])) {
			return querySPOC(query, limit);
		} else {
			final String columnFamily = tripleStoreColumnFamily(query);
			if (columnFamily.equals(PO_SC)) {

				if (isVariable(query[2])) {

					// we use a secondary index for P only when no other
					// constant is given
					IndexedSlicesQuery<byte[], Composite, byte[]> isq = 
							HFactory.createIndexedSlicesQuery(keyspace, BYTE_SERIALIZER, COMPOSITE_SERIALIZER, BYTE_SERIALIZER)
								.setColumnFamily(PO_SC)
								.addEqualsExpression(P_COL, query[1])
								.setReturnKeysOnly();

					return new POSSlicesQueryIterator(_dictionary, isq, limit, PO_SC, keyspace);
				} else {
					/*
					 * Here we always have a PO lookup, POS (=SPO) is handled by
					 * OSP or SPO
					 */
					final byte[] key = _dictionary.compose(query[1], query[2]);

					final SliceQuery<byte[], Composite, byte[]> sq = 
							HFactory.createSliceQuery(keyspace, BYTE_SERIALIZER, COMPOSITE_SERIALIZER, BYTE_SERIALIZER)
								.setColumnFamily(columnFamily)
								.setRange(INCLUDE_ALL_COMPOSITE_LOWER_BOUND, INCLUDE_ALL_COMPOSITE_HIGHER_BOUND, false, 100).setKey(key);

					final ColumnSliceIterator<byte[], Composite, byte[]> iter = 
							new ColumnSliceIterator<byte[], Composite, byte[]>(
									sq,
									INCLUDE_ALL_COMPOSITE_LOWER_BOUND,
									INCLUDE_ALL_COMPOSITE_HIGHER_BOUND,
									false)
								.setFilter(DONT_INCLUDE_PREDICATE_COLUMN);

					return new AbstractIterator<byte[][]>() {

						private int _returned = 0;

						@Override
						protected byte[][] computeNext() {

							if (!iter.hasNext() || (_returned >= limit)) {
								return endOfData();
							}

							_returned++;

							final HColumn<Composite, byte[]> column = iter.next();

							if (column.getName().size() == 2) {
								return new byte[][] { 
										BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.getName().get(0)), 
										query[1], 
										query[2],
										BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.getName().get(1)) };
							} else {
								return new byte[][] { 
										BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.getName().get(0)), 
										query[1], 
										query[2] };
							}
						}
					};
				}
			} else {
				// S_POC + O_SPC
				final int[] map = _maps.get(columnFamily);
				final byte[][] reordered_query = reorderTriple(query, map);

				final Composite start = new Composite(), end = new Composite();

				if (!isVariable(reordered_query[1])) {

					start.addComponent(0, reordered_query[1], ComponentEquality.EQUAL);
					end.addComponent(0, reordered_query[1], isVariable(reordered_query[2]) ? ComponentEquality.GREATER_THAN_EQUAL
							: ComponentEquality.EQUAL);

					if (!isVariable(reordered_query[2])) {
						start.addComponent(1, reordered_query[2], ComponentEquality.EQUAL);
						end.addComponent(1, reordered_query[2], query.length == 4 ? ComponentEquality.GREATER_THAN_EQUAL : ComponentEquality.EQUAL);
					}
				}

				final SliceQuery<byte[], Composite, byte[]> sq = HFactory.createSliceQuery(keyspace, BYTE_SERIALIZER, COMPOSITE_SERIALIZER, BYTE_SERIALIZER)
						.setColumnFamily(columnFamily)
						.setKey(reordered_query[0])
						.setRange(start, end, false, 100);

				final ColumnSliceIterator<byte[], Composite, byte[]> iter = new ColumnSliceIterator<byte[], Composite, byte[]>(sq, start, end, false)
						.setFilter(DONT_INCLUDE_PREDICATE_COLUMN);

				return new AbstractIterator<byte[][]>() {

					private int _returned = 0;

					@Override
					protected byte[][] computeNext() {

						try {

							if (!iter.hasNext() || (_returned >= limit)) {
								return endOfData();
							}

							final HColumn<Composite, byte[]> next_col = iter.next();
							byte[][] ids;

							_returned++;

							final Composite column = next_col.getName();
							if (column.size() == 3) {

								ids = new byte[][] {
										reordered_query[0],
										BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.get(0)),
										BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.get(1)),
										BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.get(2)) };

								return reorderQuadReverse(ids, map);

							} else {

								ids = new byte[][] {
										reordered_query[0],
										BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.get(0)),
										BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.get(1)) };

								return reorderTripleReverse(ids, map);
							}
						} catch (final Exception exception) {
							_log.error(MessageCatalog._00061_QUERY_EVALUATION_FAILURE, exception, Arrays.toString(query));
							return endOfData();
						}
					}
				};
			}
		}		
	}	
	
	/**
	 * Query over SPOC index.
	 * 
	 * @param query the query (as term identifiers).
	 * @param limit max number of triples.
	 * @return an iterator over the query results.
	 * @throws DataAccessLayerException in case of data access layer.
	 */
	private Iterator<byte[][]> querySPOC(final byte[][] query, final int limit) throws DataAccessLayerException {
		return new AbstractIterator<byte[][]>() {

			private int _returned = 0;

			private Iterator<byte[]> _rowIter = new Builder<byte[]>(_dataAccessLayerFactory.getKeyspace(), S_POC, BYTE_SERIALIZER).build().iterator();
			private byte[] _lastRowkey = null;
			private ColumnSliceIterator<byte[], Composite, byte[]> _colIter = null;

			@Override
			protected byte[][] computeNext() {

				try {
					if (_returned >= limit) {
						return endOfData();
					}

					// there is still data in this row
					if ((_colIter != null) && (_lastRowkey != null) && _colIter.hasNext()) {

						HColumn<Composite, byte[]> col = _colIter.next();
						_returned++;

						if (col.getName().size() == 3) {
							return new byte[][] { 
									_lastRowkey, 
									BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) col.getName().get(0)),
									BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) col.getName().get(1)),
									BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) col.getName().get(2)) };
						} else {
							return new byte[][] { 
									_lastRowkey, 
									BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) col.getName().get(0)),
									BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) col.getName().get(1)) };
						}
					}

					// if there is a next row -> move ...
					if (_rowIter.hasNext()) {

						_lastRowkey = _rowIter.next();

						final SliceQuery<byte[], Composite, byte[]> sq = 
								HFactory.createSliceQuery(
										_dataAccessLayerFactory.getKeyspace(), 
										BYTE_SERIALIZER, 
										COMPOSITE_SERIALIZER, 
										BYTE_SERIALIZER)
									.setColumnFamily(S_POC)
									.setKey(_lastRowkey);
						_colIter = new ColumnSliceIterator<byte[], Composite, byte[]>(
								sq, 
								INCLUDE_ALL_COMPOSITE_LOWER_BOUND, 
								INCLUDE_ALL_COMPOSITE_HIGHER_BOUND, 
								false);
						_colIter.hasNext(); 
						
						final HColumn<Composite, byte[]> col = _colIter.next();
						_returned++;

						if (col.getName().size() == 3) {
							return new byte[][] { _lastRowkey, BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) col.getName().get(0)),
									BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) col.getName().get(1)),
									BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) col.getName().get(2)) };
						} else {
							return new byte[][] { _lastRowkey, BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) col.getName().get(0)),
									BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) col.getName().get(1)) };
						}
					} else {
						return endOfData();
					}
				} catch (final Exception exception) {
					_log.error(MessageCatalog._00061_QUERY_EVALUATION_FAILURE, exception, Arrays.toString(query));
					return endOfData();
				}
			}
		};		
	}
	
	/**
	 * Gets the column family to use for the given triple pattern query.
	 * 
	 * @param q The query.
	 * @return The column family to use for handling the query.
	 */
	String tripleStoreColumnFamily(final byte[][] q) {

		if (!isVariable(q[0])) {
			if (!isVariable(q[2])) {
				return O_SPC; // SO
			} else {
				return S_POC; // S, SP
			}
		}

		// here S is variable
		if (!isVariable(q[2])) {
			if (!isVariable(q[1])) {
				return PO_SC; // PO
			} else {
				return O_SPC; // O
			}
		}

		// here S,O are variables
		if (!isVariable(q[1])) {
			return PO_SC; // P
		}

		// for pattern with no constants, use SPO by default
		return S_POC;
	}
}