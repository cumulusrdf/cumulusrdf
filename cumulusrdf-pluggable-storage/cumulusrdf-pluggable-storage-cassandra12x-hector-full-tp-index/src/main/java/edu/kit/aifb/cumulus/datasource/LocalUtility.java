package edu.kit.aifb.cumulus.datasource;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Booch utility for shared functions.
 * 
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class LocalUtility {
	/**
	 * Reorders <i>nodes</i>, an array in SPOC order, to the target order specified by <i>map</i>.
	 * 
	 * @param nodes the array that will be reordered.
	 * @param map the order criteria.
	 * @return a new array reordered according with requested criteria.
	 */
	public static byte[][] reorderQuad(final byte[][] nodes, final int[] map) {
		byte[][] reordered = new byte[4][];
		reordered[0] = nodes[map[0]];
		reordered[1] = nodes[map[1]];
		reordered[2] = nodes[map[2]];
		reordered[3] = nodes[map[3]];

		return reordered;
	}

	/**
	 * Reorders <i>nodes</i> from the order specified by <i>map</i> to SPOC order.
	 * 
	 * @param in the array that will be reordered.
	 * @param map the order criteria.
	 * @return a new array reordered according with requested criteria.
	 */
	public static byte[][] reorderQuadReverse(final byte[][] in, final int[] map) {
		byte[][] reordered = new byte[4][];
		reordered[map[0]] = in[0];
		reordered[map[1]] = in[1];
		reordered[map[2]] = in[2];
		reordered[map[3]] = in[3];

		return reordered;
	}

	/**
	 * Reorders <i>nodes</i>, an array in SPO order, to the target order
	 * specified by <i>map</i>.
	 * 
	 * @param nodes the triple identifiers.
	 * @param map the map that specifies the order criteria.
	 * @return a new array reordered according with the given criteria.
	 */
	public static byte[][] reorderTriple(final byte[][] nodes, final int[] map) {

		byte[][] reordered = new byte[3][];
		reordered[0] = nodes[map[0]];
		reordered[1] = nodes[map[1]];
		reordered[2] = nodes[map[2]];

		return reordered;
	}

	/**
	 * Reorders <i>nodes</i> from the order specified by <i>map</i> to SPO order.
	 * 
	 * @param in the input byte array.
	 * @param map the map containing reorder criteria.
	 * @return a new input array reordered according with input criteria.
	 */
	public static byte[][] reorderTripleReverse(final byte[][] in, final int[] map) {

		final byte[][] reordered = new byte[3][];
		reordered[map[0]] = in[0];
		reordered[map[1]] = in[1];
		reordered[map[2]] = in[2];

		return reordered;
	}

	/**
	 * Returns a singleton iterator with a single item.
	 * 
	 * @param <T> the item kind.
	 * @param item the item.
	 * @return a singleton iterator with a single item.
	 */
	public static <T> Iterator<T> singletonIterator(final T item) {

		return new Iterator<T>() {

			private T _item = item;
			private boolean _hasItem = false;

			@Override
			public boolean hasNext() {
				return !_hasItem;
			}

			@Override
			public T next() {
				if (_hasItem) {
					throw new NoSuchElementException();
				}
				_hasItem = true;
				return _item;
			}

			@Override
			public void remove() {
				if (!_hasItem) {
					_hasItem = true;
				} else {
					throw new NoSuchElementException();
				}
			}
		};
	}
}