package edu.kit.aifb.cumulus.store;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.openrdf.model.Value;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;

import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * A Set implementation around the dictionary of the store. 
 * 
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @since 1.0
 */
public class DictionarySetDecorator extends AbstractSet<Value> implements Set<Value> {

	protected static final Log LOGGER = new Log(LoggerFactory.getLogger(DictionarySetDecorator.class));
	
	private final PersistentSet<byte[]> _ids;
	private final ITopLevelDictionary _dict;
	private final boolean _p;

	/**
	 * Builds a new Set with the given data.
	 * 
	 * @param ids a set of identifiers.
	 * @param dict the CumulusRDF dictionary implementation.
	 * @param p a flag indicating if the dictionary would serve predicates or subjects / objects.
	 */
	public DictionarySetDecorator(final PersistentSet<byte[]> ids, final ITopLevelDictionary dict, final boolean p) {
		_ids = ids;
		_dict = dict;
		_p = p;
	}

	/**
	 * Operation not supported on this set implementation.
	 * 
	 * @param value the value to be added.
	 * @return always false, as the invocation has no effect.
	 */
	@Override
	public boolean add(final Value value) {
		return false;
	}

	/**
	 * Operation not supported on this set implementation.
	 * 
	 * @param values the values to be added.
	 * @return always false, as the invocation has no effect.
	 */
	@Override
	public boolean addAll(final Collection<? extends Value> values) {
		return false;
	}

	/**
	 * Operation not supported on this set implementation.
	 * That is, the invocation has no effect on this set.
	 */
	@Override
	public void clear() {
		// Nothing to do here.
	}

	@Override
	public boolean contains(final Object o) {
		try {
			return (o instanceof Value && _ids.contains(_dict.getID((Value) o, _p)));
		} catch (DataAccessLayerException exception) {
			throw new RuntimeException(exception);
		}
	}

	/**
	 * Returns the value associated with the given identifier.
	 * 
	 * @param id the identifier.
	 * @return the value associated with the given identifier, null in case no value is found.
	 * @throws DataAccessLayerException in case of data access failure. 
	 */
	private Value getValue(final byte[] id) throws DataAccessLayerException {
		return _dict.getValue(id, _p);
	}

	@Override
	public Iterator<Value> iterator() {

		final Iterator<byte[]> iterator = _ids.iterator();

		return new AbstractIterator<Value>() {
			@Override
			protected Value computeNext() {
				if (!iterator.hasNext()) {
					return endOfData();
				} else {
					try {
						final Value n = getValue(iterator.next());
						return (n != null) ? n : endOfData();
					} catch (final DataAccessLayerException exception) {
						LOGGER.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
						return endOfData();
					}
				}
			}
		};
	}

	/**
	 * Operation not supported on this set implementation.
	 * 
	 * @param value the value to be removed.
	 * @return always false, as the invocation has no effect.
	 */
	@Override
	public boolean remove(final Object value) {
		return false;
	}

	/**
	 * Operation not supported on this set implementation.
	 * 
	 * @param values the values to be removed.
	 * @return always false, as the invocation has no effect.
	 */
	@Override
	public boolean removeAll(final Collection<?> values) {
		return false;
	}

	/**
	 * Operation not supported on this set implementation.
	 * 
	 * @param values the values to be retained.
	 * @return always false, as the invocation has no effect.
	 */
	@Override
	public boolean retainAll(final Collection<?> values) {
		return false;
	}

	@Override
	public int size() {
		try {
			return _ids.size();			
		} catch (final DataAccessLayerException exception) {
			throw new RuntimeException(exception);
		}
	}
}