package edu.kit.aifb.cumulus.datasource.impl;

import static edu.kit.aifb.cumulus.datasource.Table.TABLE_OC_PS;
import static edu.kit.aifb.cumulus.datasource.Table.TABLE_OC_PS_INDEX_C;
import static edu.kit.aifb.cumulus.datasource.Table.TABLE_SC_OP;
import static edu.kit.aifb.cumulus.datasource.Table.TABLE_SPC_O;
import static edu.kit.aifb.cumulus.datasource.Table.TABLE_SPC_O_INDEX_PC;
import static edu.kit.aifb.cumulus.framework.util.Utility.isVariable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.QuadIndexDAO;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;

/**
 * Cassandra 2.x implementation of {@link QuadIndexDAO}.
 * 
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class Cassandra2xQuadIndexDAO extends Cassandra2xTripleIndexDAO implements QuadIndexDAO {

	private PreparedStatement _insertOCPSStatement;
	private PreparedStatement _insertSCOPStatement;
	private PreparedStatement _insertSPCOStatement;

	private PreparedStatement _deleteOCPSStatement;
	private PreparedStatement _deleteSCOPStatement;
	private PreparedStatement _deleteSPCOStatement;

	private PreparedStatement _clearOCPSStatement;
	private PreparedStatement _clearSCOPStatement;
	private PreparedStatement _clearSPCOStatement;

	private PreparedStatement[] _queries;

	/**
	 * Buils a new dao with the given data.
	 * 
	 * @param factory the data access layer factory.
	 * @param dictionary the dictionary currently used in the owning store instance.
	 */
	Cassandra2xQuadIndexDAO(final CumulusDataAccessLayerFactory factory, final ITopLevelDictionary dictionary) {
		super(factory, dictionary);
	}

	@Override
	public void insertTriple(final byte[][] ids) throws DataAccessLayerException {
		super.insertTriple(ids);

		// Insert in OC_PS
		final BoundStatement ocpsStatement = _insertOCPSStatement.bind();
		ocpsStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[2]));
		ocpsStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[3]));
		ocpsStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[1]));
		ocpsStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(ids[0]));
		ocpsStatement.setBytesUnsafe(4, ID_SERIALIZER.serialize(ids[3]));

		_batchStatements.get().add(ocpsStatement);

		// Insert in SC_OP
		final BoundStatement scopStatement = _insertSCOPStatement.bind();
		scopStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[0]));
		scopStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[3]));
		scopStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[2]));
		scopStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(ids[1]));

		_batchStatements.get().add(scopStatement);

		// Insert in SPC_O
		final BoundStatement spcoStatement = _insertSPCOStatement.bind();
		spcoStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[0]));
		spcoStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[1]));
		spcoStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[3]));
		spcoStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(ids[2]));
		spcoStatement.setBytesUnsafe(4, ID_SERIALIZER.serialize(_dictionary.compose(ids[1], ids[3])));

		_batchStatements.get().add(spcoStatement);
	}

	@Override
	public List<byte[][]> deleteTriples(
			final Iterator<byte[][]> nodes,
			final int batchSize,
			final boolean rangesEnabled) throws DataAccessLayerException {
		final List<byte[][]> deleted = new ArrayList<byte[][]>(batchSize);

		while (nodes.hasNext()) {
			for (int i = 0; i < batchSize && nodes.hasNext(); i++) {

				final byte[][] ids = nodes.next();

				// check if valid triple or quad
				if (ids == null || ids.length < 3) {
					continue;
				}

				internalDelete(ids, rangesEnabled);

				// Delete from OC_PS index
				final BoundStatement ocpsStatement = _deleteOCPSStatement.bind();
				ocpsStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[2]));
				ocpsStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[3]));
				ocpsStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[1]));
				ocpsStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(ids[0]));

				_batchStatements.get().add(ocpsStatement);

				// Delete from SC_OP index
				final BoundStatement scopStatement = _deleteSCOPStatement.bind();
				scopStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[0]));
				scopStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[3]));
				scopStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[2]));
				scopStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(ids[1]));

				_batchStatements.get().add(scopStatement);

				// Delete from SPC_O
				final BoundStatement spcoStatement = _deleteSPCOStatement.bind();
				spcoStatement.setBytesUnsafe(0, ID_SERIALIZER.serialize(ids[0]));
				spcoStatement.setBytesUnsafe(1, ID_SERIALIZER.serialize(ids[1]));
				spcoStatement.setBytesUnsafe(2, ID_SERIALIZER.serialize(ids[3]));
				spcoStatement.setBytesUnsafe(3, ID_SERIALIZER.serialize(ids[2]));

				_batchStatements.get().add(spcoStatement);
				deleted.add(ids);

				executePendingMutations();
			}
		}

		return deleted;
	}

	@Override
	public Iterator<byte[][]> query(
			final byte[][] query,
			final int limit) throws DataAccessLayerException {

		// Use triple-indexes if context is a variable
		if (query.length == 3 || isVariable(query[3])) {
			return super.query(query, limit);
		}

		final int queryIndex = getQueryIndex(query);
		final BoundStatement statement = _queries[queryIndex].bind();

		// Fill the query
		int queryVariableIndex = 0;

		if (queryIndex == 5) {
			// The pc query is an exception, it has a manually composed column
			statement.setBytesUnsafe(queryVariableIndex++, ID_SERIALIZER.serialize(_dictionary.compose(query[1], query[3])));
		} else {
			for (int i = 0; i < 4; i++) {
				if (!isVariable(query[i])) {
					statement.setBytesUnsafe(queryVariableIndex++, ID_SERIALIZER.serialize(query[i]));
				}
			}
		}

		// Set the limit, it is always the last variable
		statement.setInt(queryVariableIndex, limit);

		return new SPOCResultIterator(_session.executeAsync(statement), true);
	}

	@Override
	protected void createTables() {
		super.createTables();

		_session.execute("CREATE TABLE IF NOT EXISTS " + TABLE_OC_PS + "(o BLOB, c BLOB, p BLOB, s BLOB, c_index BLOB, PRIMARY KEY ((o, c), p, s))");
		_session.execute("CREATE INDEX IF NOT EXISTS " + TABLE_OC_PS_INDEX_C + " ON " + TABLE_OC_PS + "(c_index)");
		_session.execute("CREATE TABLE IF NOT EXISTS " + TABLE_SC_OP + "(s BLOB, c BLOB, o BLOB, p BLOB, PRIMARY KEY ((s, c), o, p))");

		// Need a manually composed column (pc_index) here, because secondary indexes can only be created over one column
		_session.execute("CREATE TABLE IF NOT EXISTS " + TABLE_SPC_O + "(s BLOB, p BLOB, c BLOB, o BLOB, pc_index BLOB, PRIMARY KEY ((s, p, c), o))");
		_session.execute("CREATE INDEX IF NOT EXISTS " + TABLE_SPC_O_INDEX_PC + " ON " + TABLE_SPC_O + "(pc_index)");
	}

	@Override
	protected void prepareStatements() {
		super.prepareStatements();

		final int ttl = _factory.getTtl();
		// Inserting
		if (ttl != -1) {
			_insertOCPSStatement = _session.prepare("INSERT INTO " + TABLE_OC_PS + "(o, c, p, s, c_index) VALUES (?, ?, ?, ?, ?) USING TTL " + ttl);
			_insertSCOPStatement = _session.prepare("INSERT INTO " + TABLE_SC_OP + "(s, c, o, p) VALUES (?, ?, ?, ?) USING TTL " + ttl);
			_insertSPCOStatement = _session.prepare("INSERT INTO " + TABLE_SPC_O + "(s, p, c, o, pc_index) VALUES (?, ?, ?, ?, ?) USING TTL " + ttl);
		} else {
			_insertOCPSStatement = _session.prepare("INSERT INTO " + TABLE_OC_PS + "(o, c, p, s, c_index) VALUES (?, ?, ?, ?, ?)");
			_insertSCOPStatement = _session.prepare("INSERT INTO " + TABLE_SC_OP + "(s, c, o, p) VALUES (?, ?, ?, ?)");
			_insertSPCOStatement = _session.prepare("INSERT INTO " + TABLE_SPC_O + "(s, p, c, o, pc_index) VALUES (?, ?, ?, ?, ?)");
		}

		// Deleting
		_deleteOCPSStatement = _session.prepare("DELETE FROM " + TABLE_OC_PS + " WHERE o = ? AND c = ? AND p = ? and s = ?");
		_deleteSCOPStatement = _session.prepare("DELETE FROM " + TABLE_SC_OP + " WHERE s = ? AND c = ? AND o = ? and p = ?");
		_deleteSPCOStatement = _session.prepare("DELETE FROM " + TABLE_SPC_O + " WHERE s = ? AND p = ? AND c = ? and o = ?");

		// Clearing
		_clearOCPSStatement = _session.prepare("TRUNCATE " + TABLE_OC_PS);
		_clearSCOPStatement = _session.prepare("TRUNCATE " + TABLE_SC_OP);
		_clearSPCOStatement = _session.prepare("TRUNCATE " + TABLE_SPC_O);

		// Querying
		_queries = new PreparedStatement[8];
		_queries[0] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_SC_OP + " WHERE s = ? AND p = ? AND o = ? AND c = ? LIMIT ?"); // (s, p, o, c)
		_queries[1] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_SPC_O + " WHERE s = ? AND p = ?           AND c = ? LIMIT ?"); // (s, p, ?, c)
		_queries[2] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_SC_OP + " WHERE s = ?           AND o = ? AND c = ? LIMIT ?"); // (s, ?, o, c)
		_queries[3] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_SC_OP + " WHERE s = ?                     AND c = ? LIMIT ?"); // (s, ?, ?, c)
		_queries[4] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_OC_PS + " WHERE           p = ? AND o = ? AND c = ? LIMIT ?"); // (?, p, o, c)
		_queries[5] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_SPC_O + " WHERE           pc_index = ?              LIMIT ?"); // (?, p, ?, c)
		_queries[6] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_OC_PS + " WHERE                     o = ? AND c = ? LIMIT ?"); // (?, ?, o, c)
		_queries[7] = _session.prepare("SELECT s, p, o, c FROM " + TABLE_OC_PS + " WHERE                         c_index = ? LIMIT ?"); // (?, ?, ?, c)
	}

	@Override
	int getQueryIndex(final byte[][] quadPattern) {
		int index = 0;

		if (isVariable(quadPattern[0])) {
			index += 4;
		}

		if (isVariable(quadPattern[1])) {
			index += 2;
		}

		if (isVariable(quadPattern[2])) {
			index += 1;
		}

		return index;
	}

	@Override
	public void clear() {
		super.clear();

		_session.execute(_clearOCPSStatement.bind());
		_session.execute(_clearSCOPStatement.bind());
		_session.execute(_clearSPCOStatement.bind());
	}
}
