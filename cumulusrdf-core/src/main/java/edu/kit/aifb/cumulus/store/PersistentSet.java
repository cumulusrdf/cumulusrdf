package edu.kit.aifb.cumulus.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import edu.kit.aifb.cumulus.framework.Initialisable;
import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;

/**
 * CumulusRDF persistent set.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.1.0
 * @param <E> the element kind managed by this set.
 */
public class PersistentSet<E> implements Iterable<E>, Initialisable {

	private static final byte[] PRESENT = new byte[] { 0x1 };

	private PersistentMap<E, byte[]> _map;

	/**
	 * Builds a new set with a given element class and identified by a given name.
	 * 
	 * @param elementClass the element class.
	 * @param name the set (identity) name.
	 */
	public PersistentSet(final Class<E> elementClass, final String name) {
		_map = new PersistentMap<E, byte[]>(elementClass, byte[].class, name, false, null);
	}

	@Override
	public void initialise(final DataAccessLayerFactory factory) throws InitialisationException {
		_map.initialise(factory);
	}
	
	/**
	 * Adds a new element to this set.
	 * 
	 * @param e the element.
	 * @return true if the add succeeds. 
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public boolean add(final E e) throws DataAccessLayerException {
		return e == null ? false : (_map.put(e, PRESENT) == null);
	}

	/**
	 * Adds all elements within a given collection to this set.
	 * 
	 * @param collect the input collection.
	 * @return true if the add succeeds.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public boolean addAll(final Collection<? extends E> collect) throws DataAccessLayerException {

		if (collect == null || collect.isEmpty()) {
			return false;
		}

		boolean changed = false;

		for (final E obj : collect) {
			changed = changed | add(obj);
		}

		return changed;
	}

	/**
	 * Removes all elements from this set.
	 * 
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public void clear() throws DataAccessLayerException {
		_map.clear();
	}

	/**
	 * Returns true if a given element is found within this set.
	 * 
	 * @param o the element.
	 * @return true if a given element is found within this set, false otherwise.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public boolean contains(final E o) throws DataAccessLayerException {
		return o == null ? false : _map.containsKey(o);
	}

	/**
	 * Returns true if all given elements are found within this set.
	 * 
	 * @param collect the element collection.
	 * @return true if all given elements are found within this set, false otherwise.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public boolean containsAll(final Collection<? extends E> collect) throws DataAccessLayerException {

		for (final E obj : collect) {
			if (!contains(obj)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Returns true if this set is empty.
	 * 
	 * @return true if this set is empty.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public boolean isEmpty() throws DataAccessLayerException {
		return _map.isEmpty();
	}

	/**
	 * Returns an iterator over the elements of this set.
	 * 
	 * @return an iterator over the elements of this set.
	 */
	public Iterator<E> iterator() {
		try {
			return _map.keyIterator();
		} catch (DataAccessLayerException exception) {
			throw new RuntimeException(exception);
		}
	}

	/**
	 * Removes the given element from this set.
	 * 
	 * @param o the element to be removed.
	 * @return true if the removal succeeds.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public boolean remove(final E o) throws DataAccessLayerException {
		return o == null ? false : (Arrays.equals(_map.remove(o), PRESENT));
	}

	/**
	 * Removes a set of given elements from this set.
	 * 
	 * @param collect the element set to be removed.
	 * @return true if the removal succeeds.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public boolean removeAll(final Collection<? extends E> collect) throws DataAccessLayerException {

		boolean changed = false;

		for (E obj : collect) {
			changed = changed | _map.remove(obj) == null ? false : true;
		}

		return changed;
	}

	/**
	 * Removes from this set all elements that are not within a given set of elements.
	 * 
	 * @param collect the set of elements to be retained.
	 * @return true if at least one element has been retained.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public boolean retainAll(final Collection<? extends E> collect) throws DataAccessLayerException {

		if (collect == null || collect.isEmpty()) {
			return false;
		}

		boolean changed = false;
		final Set<E> keys = new HashSet<E>(_map.keySet());

		for (final E key : keys) {

			if (!collect.contains(key)) {
				changed = changed | remove(key);
			}
		}

		return changed;
	}

	/**
	 * Returns the size of this set.
	 * 
	 * @return the size of this set.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	public int size() throws DataAccessLayerException {
		return _map.size();
	}

	/**
	 * Returns an array containing all elements of this set.
	 * 
	 * @param a the destination array.
	 * @return an array containing all elements of this set.
	 * @throws DataAccessLayerException in case of data access failure.
	 * @param <E> the element kind.
	 */
	@SuppressWarnings("hiding")
	public <E> E[] toArray(final E[] a) throws DataAccessLayerException {
		return _map.keySet().toArray(a);
	}
}