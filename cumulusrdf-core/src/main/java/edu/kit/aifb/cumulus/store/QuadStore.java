package edu.kit.aifb.cumulus.store;

import static edu.kit.aifb.cumulus.store.sesame.CumulusRDFSesameUtil.valuesToStatement;

import java.util.Arrays;
import java.util.Iterator;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.StorageLayout;
import edu.kit.aifb.cumulus.framework.datasource.TripleIndexDAO;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.util.Util;

/**
 * CumulusRDF Quad Store.
 * A Quad Store extends the underlying concept of Triple Store by introducing a fourth 
 * variable called "context", which allows named graphs, that is, qualification of datasets according 
 * to a given context. 
 * 
 * @see http://en.wikipedia.org/wiki/Named_graph
 * @see http://en.wikipedia.org/wiki/Triplestore
 * @author Sebastian Schmidt
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class QuadStore extends TripleStore {

	@SuppressWarnings("unused")
	private static final Function<Value[], Value[]> CUT_OFF_CONTEXT = new Function<Value[], Value[]>() {
		@Override
		public Value[] apply(final Value[] values) {
			return Arrays.copyOfRange(values, 0, 3);
		}
	};
	
	/**
	 * Builds a new quad store.
	 */
	public QuadStore() {
		super();
	}

	/**
	 * Builds a new quad store.
	 * 
	 * @param id the store identifier.
	 */
	public QuadStore(final String id) {
		super(id);
	}

	@Override
	public Iterator<Statement> query(final Value[] query, final int limit) throws CumulusStoreException {
		return _dictionary.toValueQuadIterator(queryAsIDs(query, limit));
	}
	
	@Override
	public void removeData(final Iterator<Statement> iterator) throws CumulusStoreException {
		try {
			notifyListeners(_startChangesEvent);
			batchDeleteWithIDs(_dictionary.toIDQuadIterator(iterator), _batchLimit);
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
				batchDeleteWithIDs(_dictionary.toIDQuadIterator(Iterators.singletonIterator(valuesToStatement(pattern))), _batchLimit);
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
	protected Iterator<byte[][]> queryAsIDs(final Value[] query, final int limit) throws CumulusStoreException {
		try {
			return queryWithIDs(_dictionary.getIDs(query[0], query[1], query[2], query.length > 3 ? query[3] : null), limit);
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
			throw new CumulusStoreException(exception);
		}	
	}
	
	@Override
	public Iterator<byte[][]> queryWithIDs(final byte[][] query, final int limit) throws CumulusStoreException {
		try {
			// check if query is valid
			if (query == null || (query.length > 4 || query.length < 3)) {
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
	TripleIndexDAO getRdfIndexDAO() {
		return _factory.getQuadIndexDAO(_dictionary);
	}	

	@Override
	protected StorageLayout storageLayout() {
		return StorageLayout.QUAD;
	}
}