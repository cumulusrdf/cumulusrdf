package edu.kit.aifb.cumulus.datasource.impl;

import static edu.kit.aifb.cumulus.datasource.Table.TABLE_O_SPC;
import static edu.kit.aifb.cumulus.datasource.Table.TABLE_PO_SC;
import static edu.kit.aifb.cumulus.datasource.Table.TABLE_PO_SC_INDEX_P;
import static edu.kit.aifb.cumulus.datasource.Table.TABLE_RDT_P_OS;
import static edu.kit.aifb.cumulus.datasource.Table.TABLE_RDT_SP_O;
import static edu.kit.aifb.cumulus.datasource.Table.TABLE_RN_P_OS;
import static edu.kit.aifb.cumulus.datasource.Table.TABLE_RN_SP_O;
import static edu.kit.aifb.cumulus.datasource.Table.TABLE_S_POC;
import static edu.kit.aifb.cumulus.datasource.impl.Cassandra2xConstants.EMPTY_VAL;
import static edu.kit.aifb.cumulus.framework.util.Utility.isVariable;
import static edu.kit.aifb.cumulus.framework.util.Utility.parseXMLSchemaDateTimeAsMSecs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import edu.kit.aifb.cumulus.datasource.serializer.Serializer;
import edu.kit.aifb.cumulus.framework.Environment;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.TripleIndexDAO;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
/**
 * Cassandra 2x (CQL-based) implementation of {@link TripleIndexDAO}.
 * 
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class Cassandra2xTripleIndexDAO implements TripleIndexDAO {
	protected static final Serializer<byte[]> ID_SERIALIZER = Serializer.BYTE_ARRAY_SERIALIZER;
	
	protected final Session _session;
	protected final CumulusDataAccessLayerFactory _factory;
	
	protected final ThreadLocal<BatchStatement> _batchStatements = new ThreadLocal<BatchStatement>() {
		protected BatchStatement initialValue() {
			return new BatchStatement();
		}
	};
	
	private PreparedStatement _insertSPOCStatement;
	private PreparedStatement _insertOSPCStatement;
	private PreparedStatement _insertPOSCStatement;
	private PreparedStatement _insertNSPOStatement;
	private PreparedStatement _insertNPOSStatement;
	private PreparedStatement _insertDSPOStatement;
	private PreparedStatement _insertDPOSStatement;

	private PreparedStatement _deleteSPOCStatement;
	private PreparedStatement _deleteOSPCStatement;
	private PreparedStatement _deletePOSCStatement;
	private PreparedStatement _deleteNSPOStatement;
	private PreparedStatement _deleteNPOSStatement;
	private PreparedStatement _deleteDSPOStatement;
	private PreparedStatement _deleteDPOSStatement;

	private PreparedStatement _clearSPOCStatement;
	private PreparedStatement _clearOSPCStatement;
	private PreparedStatement _clearPOSCStatement;
	private PreparedStatement _clearNSPOStatement;
	private PreparedStatement _clearNPOSStatement;
	private PreparedStatement _clearDSPOStatement;
	private PreparedStatement _clearDPOSStatement;

	// Filled with the 8 query types used for the triple store.
	// To calculate the position of a query in the array, convert the triple pattern to binary.
	// If S is variable, add 4, if P is variable, add 2, if O is variable, add 1.
	private PreparedStatement[] _queries;

	// Filled with the 32 possible different range queries.
	// To calculate the position of a query int the array, use a binary conversation like this:
	// If result should be reversed, add 16, if S is variable, add 8, if type is double, add 4, if upper bound is open, add 2, if lower bound is open, add 1.
	protected PreparedStatement[] _rangeQueries;
	
	protected final ITopLevelDictionary _dictionary;
	
	/**
	 * Buils a new dao with the given data.
	 * 
	 * @param factory the data access layer factory.
	 * @param dictionary the dictionary currently used in the owning store instance.
	 */
	Cassandra2xTripleIndexDAO(final CumulusDataAccessLayerFactory factory, final ITopLevelDictionary dictionary) {
		_factory = factory;
		_session = factory.getSession(); 
		_dictionary = dictionary;
	}

	@Override
	public void initialiseRdfIndex() throws DataAccessLayerException {
		final String keyspaceName = _factory.getKeyspaceName();
		_session.execute("CREATE KEYSPACE IF NOT EXISTS "
				+ keyspaceName
				+ " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': "
				+ _factory.getReplicationFactor() + "}");
		_session.execute("USE " + keyspaceName);

		createTables();
		prepareStatements();
	}

	@Override
	public void insertTriple(final byte[][] ids) throws DataAccessLayerException {
		// insert in CF_PO_SC
		final BoundStatement poscStatement = _insertPOSCStatement.bind();

		poscStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[1]));
		poscStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[2]));
		poscStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[0]));

		if (ids.length == 4) {
			poscStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(ids[3]));
		} else {
			poscStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(EMPTY_VAL));
		}

		poscStatement.setBytesUnsafe(4, ID_SERIALIZER.serialize(ids[1]));

		_batchStatements.get().add(poscStatement);

		// insert in CF_S_POC
		BoundStatement spocStatement = _insertSPOCStatement.bind();

		spocStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[0]));
		spocStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[1]));
		spocStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[2]));

		if (ids.length == 4) {
			spocStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(ids[3]));
		} else {
			spocStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(EMPTY_VAL));
		}

		_batchStatements.get().add(spocStatement);

		// insert in CF_O_SPC
		final BoundStatement ospcStatement = _insertOSPCStatement.bind();

		ospcStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[2]));
		ospcStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[0]));
		ospcStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[1]));

		if (ids.length == 4) {
			ospcStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(ids[3]));
		} else {
			ospcStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(EMPTY_VAL));
		}
		
		_batchStatements.get().add(ospcStatement);
	}

	@Override
	public void insertRanges(final byte[][] ids, final double value) throws DataAccessLayerException {
		/*
		 * insert in CF_RN_SP_O
		 */
		final BoundStatement nspoStatement = _insertNSPOStatement.bind();
		nspoStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[0]));
		nspoStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[1]));
		nspoStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[2]));
		nspoStatement.setDouble(3, value);
		_batchStatements.get().add(nspoStatement);

		/*
		 * insert in CF_RN_P_OS
		 */
		final BoundStatement nposStatement = _insertNPOSStatement.bind();
		nposStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[1]));
		nposStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[2]));
		nposStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[0]));
		nposStatement.setDouble(3, value);
		_batchStatements.get().add(nposStatement);
	}

	@Override
	public void insertRanges(final byte[][] ids, final long value) throws DataAccessLayerException {
		/*
		 * insert in: CF_RDT_SP_O
		 */
		// row key: subject + predicate
		final BoundStatement dspoStatement = _insertDSPOStatement.bind();
		dspoStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[0]));
		dspoStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[1]));
		dspoStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[2]));
		dspoStatement.setLong(3, value);
		_batchStatements.get().add(dspoStatement);

		/*
		 * insert in: CF_RDT_P_OS
		 */
		final BoundStatement dposStatement = _insertDPOSStatement.bind();
		dposStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[1]));
		dposStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[2]));
		dposStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[0]));
		dposStatement.setLong(3, value);
		_batchStatements.get().add(dposStatement);

	}

	@Override
	public List<byte[][]> deleteTriples(
			final Iterator<byte[][]> nodes, 
			final int batchSize, 
			final boolean rangesEnabled) throws DataAccessLayerException {

		final List<byte[][]> deleted = new ArrayList<byte[][]>(batchSize);
		
		while (nodes.hasNext()) {
			for (int i = 0; i < batchSize && nodes.hasNext(); i++) {

				byte[][] ids = nodes.next();

				// check if valid triple or quad
				if (ids == null || ids.length < 3) {
					continue;
				}

				internalDelete(ids, rangesEnabled);
				
				deleted.add(ids);
				executePendingMutations();
			}
		}
		
		return deleted;
	}

	@Override
	public void executePendingMutations() throws DataAccessLayerException {
		try {
			_session.execute(_batchStatements.get());
			_batchStatements.get().clear();
		} catch (final Exception exception) {
			throw new DataAccessLayerException(exception);
		}
	}

	@Override
	public Iterator<byte[][]> query(final byte[][] query, final int limit) throws DataAccessLayerException {
		final BoundStatement statement = _queries[getQueryIndex(query)].bind();

		// Fill the query
		int queryVariableIndex = 0;
		for (int i = 0; i < 3; i++) {
			if (!isVariable(query[i])) {
				statement.setBytesUnsafe(queryVariableIndex++, ID_SERIALIZER.serialize(query[i]));
			}
		}

		// Set the limit, it is always the last variable
		statement.setInt(queryVariableIndex, limit);

		// Execute query and convert result set to ids
		return new SPOCResultIterator(_session.executeAsync(statement), true);
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
		
		final byte[] s = _dictionary.getID(query[0], false);
		final byte[] p = _dictionary.getID(query[1], true);

		final boolean subjectIsVariable = isVariable(s);
		final BoundStatement statement = _rangeQueries[getRangeQueryIndex(reverse, subjectIsVariable, true, !equalsUpper, !equalsLower)].bind();
		int queryParameterIndex = 0;

		if (!subjectIsVariable) {
			statement.setBytesUnsafe(queryParameterIndex++, ID_SERIALIZER.serialize(s));
		}

		statement.setBytesUnsafe(queryParameterIndex++, ID_SERIALIZER.serialize(p));
		statement.setDouble(queryParameterIndex++, lowerBound);
		statement.setDouble(queryParameterIndex++, upperBound);
		statement.setInt(queryParameterIndex, limit);

		return new SPOCResultIterator(_session.executeAsync(statement), false);
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
		final byte[] s = _dictionary.getID(query[0], false);
		final byte[] p = _dictionary.getID(query[1], true);
		final boolean subjectIsVariable = isVariable(s);
		
		final BoundStatement statement = _rangeQueries[getRangeQueryIndex(reverse, subjectIsVariable, false, !equalsUpper, !equalsLower)].bind();

		int queryParameterIndex = 0;

		if (!subjectIsVariable) {
			statement.setBytesUnsafe(queryParameterIndex++, ID_SERIALIZER.serialize(s));
		}

		statement.setBytesUnsafe(queryParameterIndex++, ID_SERIALIZER.serialize(p));
		statement.setDouble(queryParameterIndex++, lowerBound);
		statement.setDouble(queryParameterIndex++, upperBound);
		statement.setInt(queryParameterIndex, limit);

		return new SPOCResultIterator(_session.executeAsync(statement), false);
	}

	@Override
	public void close() throws DataAccessLayerException {
		_factory.daoWasClosed();
	}
	
	@Override
	public void clear() {
		_session.execute(_clearSPOCStatement.bind());
		_session.execute(_clearOSPCStatement.bind());
		_session.execute(_clearPOSCStatement.bind());
		_session.execute(_clearNSPOStatement.bind());
		_session.execute(_clearNPOSStatement.bind());
		_session.execute(_clearDSPOStatement.bind());
		_session.execute(_clearDPOSStatement.bind());
	}
	
	/**
	 * Lazy creation of needed persistent entities.
	 */
	protected void createTables() {
		_session.execute("CREATE TABLE IF NOT EXISTS " + TABLE_S_POC + "(s BLOB, p BLOB, o BLOB, c BLOB, PRIMARY KEY (s, p, o, c))");
		_session.execute("CREATE TABLE IF NOT EXISTS " + TABLE_O_SPC + "(o BLOB, s BLOB, p BLOB, c BLOB, PRIMARY KEY (o, s, p, c))");
		_session.execute("CREATE TABLE IF NOT EXISTS " + TABLE_PO_SC + "(p BLOB, o BLOB, s BLOB, c BLOB, p_index BLOB, PRIMARY KEY ((p, o), s, c))");
		_session.execute("CREATE INDEX IF NOT EXISTS " + TABLE_PO_SC_INDEX_P + " ON " + TABLE_PO_SC + "(p_index)");

		_session.execute("CREATE TABLE IF NOT EXISTS " + TABLE_RN_SP_O + "(s BLOB, p BLOB, o DOUBLE, o_id BLOB, PRIMARY KEY ((s, p), o))");
		_session.execute("CREATE TABLE IF NOT EXISTS " + TABLE_RN_P_OS + "(p BLOB, o DOUBLE, s BLOB, o_id BLOB, PRIMARY KEY (p, o, s))");
		_session.execute("CREATE TABLE IF NOT EXISTS " + TABLE_RDT_SP_O + "(s BLOB, p BLOB, o BIGINT, o_id BLOB, PRIMARY KEY ((s, p), o))");
		_session.execute("CREATE TABLE IF NOT EXISTS " + TABLE_RDT_P_OS + "(p BLOB, o BIGINT, s BLOB, o_id BLOB, PRIMARY KEY (p, o, s))");
	}
	
	/**
	 * Prepares statements.
	 */
	protected void prepareStatements() {
		final int ttl = _factory.getTtl();
		
		// Inserting
		if (ttl != -1) {
			_insertSPOCStatement = _session.prepare("INSERT INTO " + TABLE_S_POC + "(s, p, o, c) VALUES (?, ?, ?, ?) USING TTL " + ttl);
			_insertOSPCStatement = _session.prepare("INSERT INTO " + TABLE_O_SPC + "(o, s, p, c) VALUES (?, ?, ?, ?) USING TTL " + ttl);
			_insertPOSCStatement = _session.prepare("INSERT INTO " + TABLE_PO_SC + "(p, o, s, c, p_index) VALUES (?, ?, ?, ?, ?) USING TTL " + ttl);
			_insertNSPOStatement = _session.prepare("INSERT INTO " + TABLE_RN_SP_O + "(s, p, o_id, o) VALUES (?, ?, ?, ?) USING TTL " + ttl);
			_insertNPOSStatement = _session.prepare("INSERT INTO " + TABLE_RN_P_OS + "(p, o_id, s, o) VALUES (?, ?, ?, ?) USING TTL " + ttl);
			_insertDSPOStatement = _session.prepare("INSERT INTO " + TABLE_RDT_SP_O + "(s, p, o_id, o) VALUES (?, ?, ?, ?) USING TTL " + ttl);
			_insertDPOSStatement = _session.prepare("INSERT INTO " + TABLE_RDT_P_OS + "(p, o_id, s, o) VALUES (?, ?, ?, ?) USING TTL " + ttl);
		} else {
			_insertSPOCStatement = _session.prepare("INSERT INTO " + TABLE_S_POC + "(s, p, o, c) VALUES (?, ?, ?, ?)");
			_insertOSPCStatement = _session.prepare("INSERT INTO " + TABLE_O_SPC + "(o, s, p, c) VALUES (?, ?, ?, ?)");
			_insertPOSCStatement = _session.prepare("INSERT INTO " + TABLE_PO_SC + "(p, o, s, c, p_index) VALUES (?, ?, ?, ?, ?)");
			_insertNSPOStatement = _session.prepare("INSERT INTO " + TABLE_RN_SP_O + "(s, p, o_id, o) VALUES (?, ?, ?, ?)");
			_insertNPOSStatement = _session.prepare("INSERT INTO " + TABLE_RN_P_OS + "(p, o_id, s, o) VALUES (?, ?, ?, ?)");
			_insertDSPOStatement = _session.prepare("INSERT INTO " + TABLE_RDT_SP_O + "(s, p, o_id, o) VALUES (?, ?, ?, ?)");
			_insertDPOSStatement = _session.prepare("INSERT INTO " + TABLE_RDT_P_OS + "(p, o_id, s, o) VALUES (?, ?, ?, ?)");
		}

		// Deleting
		_deleteSPOCStatement = _session.prepare("DELETE FROM " + TABLE_S_POC + " WHERE s = ? AND p = ? AND o = ? AND c = ?");
		_deleteOSPCStatement = _session.prepare("DELETE FROM " + TABLE_O_SPC + " WHERE o = ? AND s = ? AND p = ? AND c = ?");
		_deletePOSCStatement = _session.prepare("DELETE FROM " + TABLE_PO_SC + " WHERE p = ? AND o = ? AND s = ? AND c = ?");
		_deleteNSPOStatement = _session.prepare("DELETE FROM " + TABLE_RN_SP_O + " WHERE s = ? AND p = ? AND o = ?");
		_deleteNPOSStatement = _session.prepare("DELETE FROM " + TABLE_RN_P_OS + " WHERE p = ? AND o = ? AND s = ?");
		_deleteDSPOStatement = _session.prepare("DELETE FROM " + TABLE_RDT_SP_O + " WHERE s = ? AND p = ? AND o = ?");
		_deleteDPOSStatement = _session.prepare("DELETE FROM " + TABLE_RDT_P_OS + " WHERE p = ? AND o = ? AND s = ?");

		// Clearing
		_clearSPOCStatement = _session.prepare("TRUNCATE " + TABLE_S_POC);
		_clearOSPCStatement = _session.prepare("TRUNCATE " + TABLE_O_SPC);
		_clearPOSCStatement = _session.prepare("TRUNCATE " + TABLE_PO_SC);
		_clearNSPOStatement = _session.prepare("TRUNCATE " + TABLE_RN_SP_O);
		_clearNPOSStatement = _session.prepare("TRUNCATE " + TABLE_RN_P_OS);
		_clearDSPOStatement = _session.prepare("TRUNCATE " + TABLE_RDT_SP_O);
		_clearDPOSStatement = _session.prepare("TRUNCATE " + TABLE_RDT_P_OS);

		// Querying
		_queries = new PreparedStatement[8];
		_queries[0] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_S_POC + " WHERE s = ? AND p = ? AND o = ? LIMIT ?"); // (s, p, o)
		_queries[1] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_S_POC + " WHERE s = ? AND p = ?           LIMIT ?"); // (s, p, ?)
		_queries[2] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_O_SPC + " WHERE s = ?           AND o = ? LIMIT ?"); // (s, ?, o)
		_queries[3] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_S_POC + " WHERE s = ?                     LIMIT ?"); // (s, ?, ?)
		_queries[4] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_PO_SC + " WHERE           p = ? AND o = ? LIMIT ?"); // (?, p, o)
		_queries[5] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_PO_SC + " WHERE           p_index = ?     LIMIT ?"); // (?, p, ?)
		_queries[6] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_O_SPC + " WHERE                     o = ? LIMIT ?"); // (?, ?, o)
		_queries[7] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_S_POC + "                                 LIMIT ?"); // (?, ?, ?)

		_rangeQueries = new PreparedStatement[32];
		_rangeQueries[0] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_SP_O + " WHERE s = ? AND p = ? AND o >= ? AND o <= ? LIMIT ?");
		_rangeQueries[1] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_SP_O + " WHERE s = ? AND p = ? AND o > ? AND o <= ? LIMIT ?");
		_rangeQueries[2] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_SP_O + " WHERE s = ? AND p = ? AND o >= ? AND o < ? LIMIT ?");
		_rangeQueries[3] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_SP_O + " WHERE s = ? AND p = ? AND o > ? AND o < ? LIMIT ?");
		_rangeQueries[4] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_SP_O + " WHERE s = ? AND p = ? AND o >= ? AND o <= ? LIMIT ?");
		_rangeQueries[5] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_SP_O + " WHERE s = ? AND p = ? AND o > ? AND o <= ? LIMIT ?");
		_rangeQueries[6] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_SP_O + " WHERE s = ? AND p = ? AND o >= ? AND o < ? LIMIT ?");
		_rangeQueries[7] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_SP_O + " WHERE s = ? AND p = ? AND o > ? AND o < ? LIMIT ?");
		_rangeQueries[8] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_P_OS + " WHERE p = ? AND o >= ? AND o <= ? LIMIT ?");
		_rangeQueries[9] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_P_OS + " WHERE p = ? AND o > ? AND o <= ? LIMIT ?");
		_rangeQueries[10] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_P_OS + " WHERE p = ? AND o >= ? AND o < ? LIMIT ?");
		_rangeQueries[11] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_P_OS + " WHERE p = ? AND o > ? AND o < ? LIMIT ?");
		_rangeQueries[12] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_P_OS + " WHERE p = ? AND o >= ? AND o <= ? LIMIT ?");
		_rangeQueries[13] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_P_OS + " WHERE p = ? AND o > ? AND o <= ? LIMIT ?");
		_rangeQueries[14] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_P_OS + " WHERE p = ? AND o >= ? AND o < ? LIMIT ?");
		_rangeQueries[15] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_P_OS + " WHERE p = ? AND o > ? AND o < ? LIMIT ?");
		_rangeQueries[16] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_SP_O + " WHERE s = ? AND p = ? AND o >= ? AND o <= ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[17] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_SP_O + " WHERE s = ? AND p = ? AND o > ? AND o <= ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[18] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_SP_O + " WHERE s = ? AND p = ? AND o >= ? AND o < ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[19] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_SP_O + " WHERE s = ? AND p = ? AND o > ? AND o < ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[20] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_SP_O + " WHERE s = ? AND p = ? AND o >= ? AND o <= ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[21] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_SP_O + " WHERE s = ? AND p = ? AND o > ? AND o <= ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[22] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_SP_O + " WHERE s = ? AND p = ? AND o >= ? AND o < ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[23] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_SP_O + " WHERE s = ? AND p = ? AND o > ? AND o < ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[24] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_P_OS + " WHERE p = ? AND o >= ? AND o <= ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[25] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_P_OS + " WHERE p = ? AND o > ? AND o <= ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[26] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_P_OS + " WHERE p = ? AND o >= ? AND o < ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[27] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RDT_P_OS + " WHERE p = ? AND o > ? AND o < ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[28] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_P_OS + " WHERE p = ? AND o >= ? AND o <= ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[29] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_P_OS + " WHERE p = ? AND o > ? AND o <= ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[30] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_P_OS + " WHERE p = ? AND o >= ? AND o < ? ORDER BY o DESC LIMIT ?");
		_rangeQueries[31] = _session.prepare("SELECT s, p, o_id, o FROM " + TABLE_RN_P_OS + " WHERE p = ? AND o > ? AND o < ? ORDER BY o DESC LIMIT ?");
	}	
	
	/**
	 * Returns the index of the prepared statement to handle a given triple pattern query.
	 * 
	 * @param triplePattern The triple pattern query.
	 * @return The index of the prepared statement to handle a given triple pattern query.
	 */
	int getQueryIndex(final byte[][] triplePattern) {
		int index = 0;

		if (isVariable(triplePattern[0])) {
			index += 4;
		}

		if (isVariable(triplePattern[1])) {
			index += 2;
		}

		if (isVariable(triplePattern[2])) {
			index += 1;
		}

		return index;
	}	
	
	/**
	 * Returns the index of the prepared statement to handle the range query with the given parameters.
	 * 
	 * @param reverse True if the result should be returned reversed, false if it should be returned normally.
	 * @param subjectIsVariable True if the subject of the query is variable, false if it is set.
	 * @param typeIsDouble True if the type of the range is double, false if it is date.
	 * @param upperBoundIsOpen True if the upper bound is smaller-than relation, false if it is a smaller-than-or-equal-to relation.
	 * @param lowerBoundIsOpen True if the lower bound is greater-than relation, false if it is a greater-than-or-equal-to relation.
	 * @return The index of the prepared statement.
	 */
	int getRangeQueryIndex(
			final boolean reverse, 
			final boolean subjectIsVariable, 
			final boolean typeIsDouble, 
			final boolean upperBoundIsOpen,
			final boolean lowerBoundIsOpen) {
		int index = 0;

		if (reverse) {
			index += 16;
		}

		if (subjectIsVariable) {
			index += 8;
		}

		if (typeIsDouble) {
			index += 4;
		}

		if (upperBoundIsOpen) {
			index += 2;
		}

		if (lowerBoundIsOpen) {
			index += 1;
		}

		return index;
	}	
	
	/**
	 * Internal method used for reuse delete stuff.
	 * 
	 * @param ids the triple identifiers.
	 * @param rangesEnabled if ranges have been enabled on the current store.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void internalDelete(final byte [][]ids, final boolean rangesEnabled) throws DataAccessLayerException {
		// delete in CF_PO_SC
		final BoundStatement poscStatement = _deletePOSCStatement.bind();
		poscStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[1]));
		poscStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[2]));
		poscStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[0]));

		if (ids.length == 4) {
			poscStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(ids[3]));
		} else {
			poscStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(EMPTY_VAL));
		}

		_batchStatements.get().add(poscStatement);

		// delete in CF_S_POC
		final BoundStatement spocStatement = _deleteSPOCStatement.bind();
		spocStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[0]));
		spocStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[1]));
		spocStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[2]));

		if (ids.length == 4) {
			spocStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(ids[3]));
		} else {
			spocStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(EMPTY_VAL));
		}

		_batchStatements.get().add(spocStatement);

		// delete in CF_O_SPC
		final BoundStatement ospcStatement = _deleteOSPCStatement.bind();
		ospcStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[2]));
		ospcStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[0]));
		ospcStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[1]));

		if (ids.length == 4) {
			ospcStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(ids[3]));
		} else {
			ospcStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(EMPTY_VAL));
		}

		_batchStatements.get().add(ospcStatement);

		/*
		 * delete in: CF_RN_SP_O + CF_RN_PO_S
		 */
		if (rangesEnabled && _dictionary.isLiteral(ids[2])) {
			final Literal lit = (Literal) _dictionary.getValue(ids[2], false);
			final URI dt = lit.getDatatype();

			if (Environment.NUMERIC_RANGETYPES.contains(dt)) {

				double number = Double.parseDouble(lit.getLabel());

				// delete in CF_RN_SP_O
				final BoundStatement nspoStatement = _deleteNSPOStatement.bind();
				nspoStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[0]));
				nspoStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[1]));
				nspoStatement.setDouble(2, number);

				_batchStatements.get().add(nspoStatement);

				// delete in CF_RN_PO_S
				final BoundStatement nposStatement = _deleteNPOSStatement.bind();
				nposStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[1]));
				nposStatement.setDouble(1, number);
				nposStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[0]));

				_batchStatements.get().add(nposStatement);

			} else if (Environment.DATETIME_RANGETYPES.contains(dt)) {

				long ms = parseXMLSchemaDateTimeAsMSecs(lit);

				// delete in CF_RN_SP_O
				final BoundStatement dspoStatement = _deleteDSPOStatement.bind();
				dspoStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[0]));
				dspoStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[1]));
				dspoStatement.setLong(2, ms);

				_batchStatements.get().add(dspoStatement);

				// delete in CF_RN_PO_S
				final BoundStatement dposStatement = _deleteDPOSStatement.bind();
				dposStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[1]));
				dposStatement.setLong(1, ms);
				dposStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[0]));

				_batchStatements.get().add(dposStatement);
			}		
		}
	}
}