package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.util.Util.isVariable;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;

import com.google.common.collect.Iterators;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.StorageLayout;
import edu.kit.aifb.cumulus.framework.datasource.TripleIndexDAO;
import edu.kit.aifb.cumulus.framework.domain.configuration.Configuration;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.events.RemoveTriplesEvent;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFSesameUtil;
import edu.kit.aifb.cumulus.util.Util;

/**
 * CumulusRDF Triple Store.
 * 
 * A triplestore is a purpose-built database for the storage and retrieval of triples,
 * a triple being a data entity composed of subject-predicate-object, like "Bob is 35" or "Bob knows Fred" (Wikipedia).
 * 
 * @see http://en.wikipedia.org/wiki/Triplestore
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class TripleStore extends Store {

	static final BigDecimal MIN_LOWER_BOUND = BigDecimal.valueOf(Long.MIN_VALUE);
	static final BigDecimal MAX_UPPER_BOUND = BigDecimal.valueOf(Long.MAX_VALUE);

	protected PersistentMap<String, String> _prefix2namespace;

	/**
	 * Builds a new triple store.
	 */
	public TripleStore() {
		super();
	}

	/**
	 * Builds a new triple store.
	 * 
	 * @param id the store identifier.
	 */
	public TripleStore(final String id) {
		super(id);
	}

	@Override
	public void addData(final Iterator<Statement> iterator) throws CumulusStoreException {
		try {
			notifyListeners(_startChangesEvent);
			batchInsert(iterator, _batchLimit);
			notifyListeners(_finishedChangesEvent);
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
			throw new CumulusStoreException(exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}
	}

	@Override
	public void addData(final Statement data) throws CumulusStoreException {
		try {
			notifyListeners(_startChangesEvent);
			batchInsert(Iterators.singletonIterator(data), _batchLimit);
			notifyListeners(_finishedChangesEvent);
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
			throw new CumulusStoreException(exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}
	}

	@Override
	public Set<Value> getClasses() {
		return _schema.getClasses();
	}

	@Override
	public Set<Value> getDataProperties() {
		return _schema.getDatatypeProperties();
	}

	@Override
	public Set<Value> getObjectProperties() {
		return _schema.getObjectProperties();
	}

	@Override
	public PersistentMap<String, String> getPrefix2Namespaces() {
		return _prefix2namespace;
	}

	@Override
	public String getStatus() {
		return "";
	}

	@Override
	public boolean isClass(final Value n) {
		return getClasses().contains(n);
	}

	@Override
	public boolean isDataProperty(final Value n) {
		return getDataProperties().contains(n);
	}

	@Override
	public boolean isObjectProperty(final Value n) {
		return getObjectProperties().contains(n);
	}

	@Override
	protected void openInternal() throws CumulusStoreException {
		try {
			_schema = new Schema(_dictionary);
			_schema.initialise(_factory);
		} catch (final InitialisationException exception) {
			_log.error(MessageCatalog._00094_SCHEMA_INIT_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}
		
		try {
			_prefix2namespace = new PersistentMap<String, String>(
					String.class,
					String.class,
					"PREFIX_TO_NS",
					false,
					null);
			_prefix2namespace.initialise(_factory);
		} catch (final InitialisationException exception) {
			_log.error(MessageCatalog._00096_PREFIX_2_NS_INIT_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}

		_changeListeners.add(_schema);
	}

	@Override
	public Iterator<Statement> query(final Value[] query) throws CumulusStoreException {
		return query(query, Integer.MAX_VALUE);
	}

	@Override
	public Iterator<Statement> query(final Value[] query, final int limit) throws CumulusStoreException {
		return _dictionary.toValueTripleIterator(queryAsIDs(query, limit));
	}

	@Override
	protected Iterator<byte[][]> queryWithIDs(final Value[] query) throws CumulusStoreException {
		return queryAsIDs(query, Integer.MAX_VALUE);
	}

	@Override
	protected Iterator<byte[][]> queryAsIDs(final Value[] query, final int limit) throws CumulusStoreException {
		try {
			return queryWithIDs(_dictionary.getIDs(query[0], query[1], query[2]), limit);
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}
	}

	@Override
	public Iterator<byte[][]> queryWithIDs(final byte[][] query) throws CumulusStoreException {
		return queryWithIDs(query, Integer.MAX_VALUE);
	}

	@Override
	protected Iterator<byte[][]> queryWithIDs(final byte[][] query, final int limit) throws CumulusStoreException {
		try {
			if ((query == null || query.length < 3)) {
				return Iterators.emptyIterator();
			}

			return _rdfIndexDAO.query(query, limit);
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
			throw new CumulusStoreException(exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}
	}

	@Override
	public Iterator<Statement> range(final Value[] query, final Literal lower, final boolean equalsLower, final Literal upper, final boolean equalsUpper,
			final boolean reverse, final int limit) throws CumulusStoreException {
		try {
			return _dictionary.toValueTripleIterator(rangeAsIDs(query, lower, equalsLower, upper, equalsUpper, reverse, limit));
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
			throw new CumulusStoreException(exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}
	}

	@Override
	public Iterator<byte[][]> rangeAsIDs(
			final Value[] query,
			final Literal lower,
			final boolean equalsLower,
			final Literal upper,
			final boolean equalsUpper,
			final boolean reverse,
			final int limit) throws DataAccessLayerException {
		if (query == null || query.length != 2 || isVariable(query[1])) {
			return Iterators.emptyIterator();
		}

		final double lowerBound = asDecimal(lower, MIN_LOWER_BOUND).doubleValue();
		final double upperBound = asDecimal(upper, MAX_UPPER_BOUND).doubleValue();

		return _rdfIndexDAO.numericRangeQuery(query, lowerBound, equalsLower, upperBound, equalsUpper, reverse, limit);
	}

	@Override
	public Iterator<Statement> rangeDateTime(
			final Value[] query,
			final Literal lower,
			final boolean equalsLower,
			final Literal upper,
			final boolean equalsUpper,
			final boolean reverse,
			final int limit) throws CumulusStoreException {
		try {
			return _dictionary.toValueTripleIterator(rangeDateTimeAsIDs(query, lower, equalsLower, upper, equalsUpper, reverse, limit));
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
			throw new CumulusStoreException(exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}
	}

	@Override
	public Iterator<byte[][]> rangeDateTimeAsIDs(
			final Value[] query,
			final Literal lower,
			final boolean equalsLower,
			final Literal upper,
			final boolean equalsUpper,
			final boolean reverse,
			final int limit) throws DataAccessLayerException {

		if (query == null || query.length != 2 || isVariable(query[1])) {
			return Iterators.emptyIterator();
		}

		final long lowerBound = lower == null ? Long.MIN_VALUE : Util.parseXMLSchemaDateTimeAsMSecs(lower), upperBound = upper == null ? Long.MAX_VALUE
				: Util.parseXMLSchemaDateTimeAsMSecs(upper);

		return _rdfIndexDAO.dateRangeQuery(query, lowerBound, equalsLower, upperBound, equalsUpper, reverse, limit);
	}

	@Override
	public void removeData(final Iterator<Statement> iterator) throws CumulusStoreException {
		try {
			notifyListeners(_startChangesEvent);
			batchDeleteWithIDs(_dictionary.toIDTripleIterator(iterator), _batchLimit);
			notifyListeners(_finishedChangesEvent);
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
			throw new CumulusStoreException(exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}
	}

	@Override
	public void removeData(final Value[] pattern) throws CumulusStoreException {
		try {
			if (Util.ALL_CONSTANTS.apply(pattern)) {

				notifyListeners(_startChangesEvent);
				batchDeleteWithIDs(_dictionary.toIDTripleIterator(Iterators.singletonIterator(CumulusRDFSesameUtil.valuesToStatement(pattern))), _batchLimit);
				notifyListeners(_finishedChangesEvent);
			} else if (Util.ALL_VARS.apply(pattern)) {
				clear();
			} else {
				removeDataWithIDs(queryWithIDs(pattern));
			}
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
			throw new CumulusStoreException(exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}
	}

	@Override
	protected void removeDataWithIDs(final byte[][] pattern) throws CumulusStoreException {
		try {
			if (Util.ALL_CONSTANTS_WITH_IDS.apply(pattern)) {

				notifyListeners(_startChangesEvent);
				batchDeleteWithIDs(Iterators.singletonIterator(pattern), _batchLimit);
				notifyListeners(_finishedChangesEvent);

			} else {
				removeDataWithIDs(queryWithIDs(pattern));
			}
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
			throw new CumulusStoreException(exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}
	}

	@Override
	protected void removeDataWithIDs(final Iterator<byte[][]> triples) throws DataAccessLayerException {
		notifyListeners(_startChangesEvent);
		batchDeleteWithIDs(triples, _batchLimit);
		notifyListeners(_finishedChangesEvent);
	}



	@Override
	public void accept(final Configuration<Map<String, Object>> configuration) {
		if (!isRangeIndexesSupportEnabled()) {
			_idxRanges = configuration.getAttribute("storage-index-ranges", Boolean.FALSE);
		}
	}

	/**
	 * Returns the decimal representation of a given literal.
	 * If the literal cannot be converted in a decimal number (e.g. null or NaN) then a default value is returned.
	 * 
	 * @param literal the literal.
	 * @param defaultValue the default value that will be returned in case of errors.
	 * @return the decimal representation of a given literal.
	 */
	BigDecimal asDecimal(final Literal literal, final BigDecimal defaultValue) {
		try {
			return literal.decimalValue();
		} catch (final Exception exception) {
			return defaultValue;
		}
	}

	/**
	 * Deletes the given triples/quads from the store.
	 * The triples/quads are represented as ids.
	 * 
	 * @param nodes an iterator iterating over the triples/quads to delete.
	 * @param batchSize the maximum size of a batch query.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void batchDeleteWithIDs(final Iterator<byte[][]> nodes, final int batchSize) throws DataAccessLayerException {
		List<byte[][]> deleted = null;
		try {
			deleted = _rdfIndexDAO.deleteTriples(nodes, batchSize, _idxRanges);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00055_DELETION_FAILURE, exception);
		}

		if (deleted != null && !deleted.isEmpty()) {
			notifyListeners(new RemoveTriplesEvent(this, deleted));
		}
	}


	@Override
	TripleIndexDAO getRdfIndexDAO() {
		return _factory.getTripleIndexDAO(_dictionary);
	}

	@Override
	protected StorageLayout storageLayout() {
		return StorageLayout.TRIPLE;
	}
}