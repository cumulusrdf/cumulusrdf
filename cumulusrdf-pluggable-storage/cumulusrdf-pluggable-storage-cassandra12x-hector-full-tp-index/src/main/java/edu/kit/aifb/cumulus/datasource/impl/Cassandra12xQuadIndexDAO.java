package edu.kit.aifb.cumulus.datasource.impl;

import static edu.kit.aifb.cumulus.datasource.ColumnFamily.OC_PS;
import static edu.kit.aifb.cumulus.datasource.ColumnFamily.PO_SC;
import static edu.kit.aifb.cumulus.datasource.ColumnFamily.SC_OP;
import static edu.kit.aifb.cumulus.datasource.ColumnFamily.SPC_O;
import static edu.kit.aifb.cumulus.framework.util.Utility.isVariable;
import static edu.kit.aifb.cumulus.datasource.LocalUtility.reorderQuad;
import static edu.kit.aifb.cumulus.datasource.LocalUtility.reorderQuadReverse;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.BYTE_SERIALIZER;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.COMPOSITE_SERIALIZER;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.C_COL;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.DONT_INCLUDE_PREDICATE_COLUMN;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.EMPTY_VAL;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.INCLUDE_ALL_COMPOSITE_HIGHER_BOUND;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.INCLUDE_ALL_COMPOSITE_LOWER_BOUND;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.PC_COL;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra12xHectorConstants.P_COL;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;

import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.datasource.QuadIndexDAO;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.log.Log;


/**
 * Cassandra 1.2.x implementation of {@link QuadIndexDAO}.
 * Note that this DAO is not fully stateless; that is, since it uses
 * Hector behind the scenes, it maintains a set of thread-scoped Mutators that 
 * must be flushed (see {@link #executePendingMutations()} in order to trigger the material execution of the pending mutations.
 * 
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class Cassandra12xQuadIndexDAO extends Cassandra12xTripleIndexDAO implements QuadIndexDAO {
	protected final Log _log = new Log(LoggerFactory.getLogger(Cassandra12xQuadIndexDAO.class));

	{
		_maps.put(OC_PS, new int[] {2, 3, 1, 0});
		_maps.put(SC_OP, new int[] {0, 3, 2, 1});
		_maps.put(SPC_O, new int[] {0, 1, 3, 2});
	}
	
	/**
	 * Builds a new dao with the given data.
	 * 
	 * @param factory the data access layer factory.
	 * @param dictionary the top level dictionary in use.
	 */
	Cassandra12xQuadIndexDAO(final DataAccessLayerFactory factory, final ITopLevelDictionary dictionary) {
		super(factory, dictionary);
	}
	
	@Override
	public void clear() {
		super.clear();

		final Cluster cluster = _dataAccessLayerFactory.getCluster();
		final String keyspaceName = _dataAccessLayerFactory.getKeyspaceName();
		cluster.truncate(keyspaceName, OC_PS);
		cluster.truncate(keyspaceName, SC_OP);
		cluster.truncate(keyspaceName, SPC_O);	
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected List<ColumnFamilyDefinition> createColumnFamiliyDefinitions() {
		List<ColumnFamilyDefinition> defs = super.createColumnFamiliyDefinitions();

		defs.add(createCF(OC_PS, Arrays.asList(COMPOSITE_SERIALIZER.toBytes(C_COL)), ComparatorType.BYTESTYPE, ComparatorType.BYTESTYPE, true));
		defs.add(createCF(SC_OP, Collections.EMPTY_LIST, ComparatorType.BYTESTYPE, ComparatorType.BYTESTYPE, true));
		defs.add(createCF(SPC_O, Arrays.asList(COMPOSITE_SERIALIZER.toBytes(PC_COL)), ComparatorType.BYTESTYPE, ComparatorType.BYTESTYPE, true));

		return defs;
	}
	
	@Override
	public List<byte[][]> deleteTriples(
			final Iterator<byte[][]> nodes,
			final int batchSize, 
			final boolean rangesEnabled) throws DataAccessLayerException {
		
		final List<byte[][]> deleted = new ArrayList<byte[][]>(batchSize);
		
		final SecondaryIndexDeletionBuffer cColDeletionTest = new SecondaryIndexDeletionBuffer(batchSize);
		final SecondaryIndexDeletionBuffer pcColDeletionTest = new SecondaryIndexDeletionBuffer(batchSize);
		final SecondaryIndexDeletionBuffer pColDeletionTest = new SecondaryIndexDeletionBuffer(batchSize);

		while (nodes.hasNext()) {

			for (int i = 0; (i < batchSize) && nodes.hasNext(); i++) {

				byte[][] ids = nodes.next();

				// check if valid triple or quad
				if ((ids == null) || (ids.length < 3)) {
					continue;
				}

				// Triple indexes.
				internalDelete(ids, pColDeletionTest, rangesEnabled);

				// Delete from SC_OP index
				byte[] sc_row = _dictionary.compose(ids[0], ids[3]);

				final Composite op_col = new Composite();
				op_col.addComponent(ids[2], BYTE_SERIALIZER);
				op_col.addComponent(ids[1], BYTE_SERIALIZER);

				_mutators.get().addDeletion(sc_row, SC_OP, op_col, COMPOSITE_SERIALIZER);

				// Delete from OC_PS index
				byte[] oc_row = _dictionary.compose(ids[2], ids[3]);
				final Composite sp_col = new Composite();
				sp_col.addComponent(ids[1], BYTE_SERIALIZER);
				sp_col.addComponent(ids[0], BYTE_SERIALIZER);

				// subject + predicate col
				_mutators.get().addDeletion(oc_row, OC_PS, sp_col, COMPOSITE_SERIALIZER);
				cColDeletionTest.add(new SecondaryIndexDeletionCandidate(oc_row, new byte[][] {null, null, ids[2], ids[3]}));

				// Delete from SPC_O
				byte[] spc_row = _dictionary.compose(ids[0], ids[1], ids[3]);
				final Composite o_col = new Composite();
				o_col.addComponent(ids[2], BYTE_SERIALIZER);

				// spc col
				_mutators.get().addDeletion(spc_row, SPC_O, o_col, COMPOSITE_SERIALIZER);
				pcColDeletionTest.add(new SecondaryIndexDeletionCandidate(spc_row, new byte[][] {ids[0], ids[1], null, ids[3]}));
				
				deleted.add(ids);
			}
			
			_mutators.get().execute();
			
			// Flush buffer only if this is the last chance or it is full.
			if (nodes.hasNext()) {
				cColDeletionTest.flushIfFull(_mutators.get(), OC_PS, C_COL, COMPOSITE_SERIALIZER, this);
				pcColDeletionTest.flushIfFull(_mutators.get(), SPC_O, PC_COL, COMPOSITE_SERIALIZER, this);
				pColDeletionTest.flushIfFull(_mutators.get(), PO_SC, P_COL, COMPOSITE_SERIALIZER, this);
			} else {
				cColDeletionTest.flush(_mutators.get(), OC_PS, C_COL, COMPOSITE_SERIALIZER, this);
				pcColDeletionTest.flush(_mutators.get(), SPC_O, PC_COL, COMPOSITE_SERIALIZER, this);
				pColDeletionTest.flush(_mutators.get(), PO_SC, P_COL, COMPOSITE_SERIALIZER, this);
			}

			_mutators.get().execute();
		}
		
		return deleted;
	}	
	
	@Override
	public void insertTriple(final byte[][] ids) throws DataAccessLayerException {
		super.insertTriple(ids);
		// Insert in OC_PS
		// Key: O + C
		byte[] oc_row = _dictionary.compose(ids[2], ids[3]);
		
		final Composite sp_col = new Composite();
		sp_col.addComponent(ids[1], BYTE_SERIALIZER);
		sp_col.addComponent(ids[0], BYTE_SERIALIZER);

		// subject + predicate col
		_mutators.get().addInsertion(oc_row, OC_PS, HFactory.createColumn(sp_col, EMPTY_VAL, COMPOSITE_SERIALIZER, BYTE_SERIALIZER));

		// context col
		_mutators.get().addInsertion(oc_row, OC_PS, HFactory.createColumn(C_COL, ids[3], COMPOSITE_SERIALIZER, BYTE_SERIALIZER));

		// Insert in SC_OP
		// Key: S + C
		final byte[] sc_row = _dictionary.compose(ids[0], ids[3]);
		
		final Composite op_col = new Composite();
		op_col.addComponent(ids[2], BYTE_SERIALIZER);
		op_col.addComponent(ids[1], BYTE_SERIALIZER);

		_mutators.get().addInsertion(sc_row, SC_OP, HFactory.createColumn(op_col, EMPTY_VAL, COMPOSITE_SERIALIZER, BYTE_SERIALIZER));

		// Insert in SPC_O
		// Key: S + P + C
		byte[] spc_row = _dictionary.compose(ids[0], ids[1], ids[3]);
		byte[] pc_val = _dictionary.compose(ids[1], ids[3]);
		
		final Composite o_col = new Composite();
		o_col.addComponent(ids[2], BYTE_SERIALIZER);

		// object col
		_mutators.get().addInsertion(spc_row, SPC_O, HFactory.createColumn(o_col, EMPTY_VAL, COMPOSITE_SERIALIZER, BYTE_SERIALIZER));
		// predicate + context col
		_mutators.get().addInsertion(spc_row, SPC_O, HFactory.createColumn(PC_COL, pc_val, COMPOSITE_SERIALIZER, BYTE_SERIALIZER));		
	}
	
	/**
	 * Returns the column family that handles the given query.
	 * 
	 * @param query The query.
	 * @return The column family that handles the given query.
	 */
	private String quadStoreColumnFamily(final byte[][] query) {
		if (isVariable(query[2]) && !isVariable(query[1])) {
			return SPC_O;
		}

		if (!isVariable(query[0]) && isVariable(query[1])) {
			return SC_OP;
		} else {
			return OC_PS;
		}
	}
	
	@Override
	public Iterator<byte[][]> query(final byte[][] query, final int limit) throws DataAccessLayerException {
		// Use triple-indexes if context is a variable.
		if ((query.length == 3) || isVariable(query[3])) {
			return super.query(query, limit);
		}
		
		// Only context is given (C)
		if (isVariable(query[0]) && isVariable(query[1]) && isVariable(query[2])) {
			return secundaryOCPSQuery(query, limit);
		}

		final String columnFamily = quadStoreColumnFamily(query);

		// PC or SPC
		if (columnFamily.equals(SPC_O)) {
			if (isVariable(query[0])) {
				return secundarySPCOQuery(query, limit);
			} else {
				return spcQuery(query, limit);
			}
		}

		final byte[][] reordered_query = reorderQuad(query, _maps.get(columnFamily));

		// Anything else. (POC, OC, SOC, SC, SPOC)
		// Working with the reordered query now to handle both indexes at once.
		Composite start = new Composite();
		Composite end = new Composite();

		if (!isVariable(reordered_query[2])) {
			start.addComponent(0, reordered_query[2], ComponentEquality.EQUAL);
			end.addComponent(0, reordered_query[2], isVariable(reordered_query[3]) ? ComponentEquality.GREATER_THAN_EQUAL : ComponentEquality.EQUAL);

			if (!isVariable(reordered_query[3])) {
				start.addComponent(1, reordered_query[3], ComponentEquality.EQUAL);
				end.addComponent(1, reordered_query[3], ComponentEquality.EQUAL);
			}
		}

		byte[] key = _dictionary.compose(reordered_query[0], reordered_query[1]);
		final SliceQuery<byte[], Composite, byte[]> sq = HFactory.createSliceQuery(
					_dataAccessLayerFactory.getKeyspace(), 
					BYTE_SERIALIZER, 
					COMPOSITE_SERIALIZER, 
					BYTE_SERIALIZER)
				.setColumnFamily(columnFamily)
				.setKey(key)
				.setRange(start, end, false, limit);

		final ColumnSliceIterator<byte[], Composite, byte[]> results = new ColumnSliceIterator<byte[], Composite, byte[]>(sq, start, end, false);
		results.setFilter(DONT_INCLUDE_PREDICATE_COLUMN);

		return new AbstractIterator<byte[][]>() {
			@Override
			protected byte[][] computeNext() {
				if (!results.hasNext()) {
					return endOfData();
				}

				final HColumn<Composite, byte[]> column = results.next();
				final byte[][] result = new byte[][] { 
						reordered_query[0], 
						reordered_query[1], 
						BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.getName().get(0)),
						BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.getName().get(1)) };

				return reorderQuadReverse(result, _maps.get(columnFamily));
			}
		};		
	}	
	
	/**
	 * A C query that should be handled by the OCSP index.
	 * 
	 * @param query The query.
	 * @param limit The maximum amount of results to return.
	 * @return An iterator iterating over the query results.
	 */
	private Iterator<byte[][]> secundaryOCPSQuery(final byte[][] query, final int limit) {
		return new CAndPCSlicesQueryIterator(
				HFactory.createRangeSlicesQuery(_dataAccessLayerFactory.getKeyspace(), BYTE_SERIALIZER, COMPOSITE_SERIALIZER, BYTE_SERIALIZER)
					.setColumnFamily(OC_PS)
					.addEqualsExpression(C_COL, query[3])
					.setRange(INCLUDE_ALL_COMPOSITE_LOWER_BOUND, INCLUDE_ALL_COMPOSITE_HIGHER_BOUND, false, limit)
					.setReturnKeysOnly(),
				limit, 
				OC_PS, 
				_dataAccessLayerFactory.getKeyspace(), 
				_dictionary, 
				false);
	}

	/**
	 * A PC query that should be handled by the SCPO index.
	 * 
	 * @param query The query.
	 * @param limit The maximum amount of results to return.
	 * @return An iterator iterating over the query results.
	 */
	private Iterator<byte[][]> secundarySPCOQuery(final byte[][] query, final int limit) {
		return new CAndPCSlicesQueryIterator(
				HFactory.createRangeSlicesQuery(_dataAccessLayerFactory.getKeyspace(), BYTE_SERIALIZER, COMPOSITE_SERIALIZER, BYTE_SERIALIZER)
					.setColumnFamily(SPC_O)
					.addEqualsExpression(PC_COL, _dictionary.compose(query[1], query[3]))
					.setRange(INCLUDE_ALL_COMPOSITE_LOWER_BOUND, INCLUDE_ALL_COMPOSITE_HIGHER_BOUND, false, limit)
					.setReturnKeysOnly(),				
				limit, 
				SPC_O, 
				_dataAccessLayerFactory.getKeyspace(), 
				_dictionary, 
				true);
	}

	/**
	 * A SPC query.
	 * 
	 * @param query The query.
	 * @param limit The maximum amount of results to return.
	 * @return An iterator iterating over the query results.
	 */
	private Iterator<byte[][]> spcQuery(final byte[][] query, final int limit) {
		final ColumnSliceIterator<byte[], Composite, byte[]> results = new ColumnSliceIterator<byte[], Composite, byte[]>(
				HFactory.createSliceQuery(_dataAccessLayerFactory.getKeyspace(), BYTE_SERIALIZER, COMPOSITE_SERIALIZER, BYTE_SERIALIZER)
					.setColumnFamily(SPC_O)
					.setKey(_dictionary.compose(query[0], query[1], query[3]))
					.setRange(INCLUDE_ALL_COMPOSITE_LOWER_BOUND, INCLUDE_ALL_COMPOSITE_HIGHER_BOUND, false, limit),
				INCLUDE_ALL_COMPOSITE_LOWER_BOUND,
				INCLUDE_ALL_COMPOSITE_HIGHER_BOUND, 
				false);
		results.setFilter(DONT_INCLUDE_PREDICATE_COLUMN);
		
		return new AbstractIterator<byte[][]>() {
			@Override
			protected byte[][] computeNext() {
				if (!results.hasNext()) {
					return endOfData();
				}

				return new byte[][] { 
						query[0], 
						query[1], 
						BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) results.next().getName().get(0)), 
						query[3] };
			}
		};
	}	
}