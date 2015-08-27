package edu.kit.aifb.cumulus.store.sesame;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.LookAheadIteration;

/**
 * A closeable iterator that concats multiple closeable iterators.
 * 
 * @author Sebastian Schmidt
 * @since 1.0
 * @param <E> The type of the iteration.
 * @param <X> An exception that might be thrown when trying to get the next
 *            element.
 */
public class CloseableMultiIterator<E, X extends Exception> extends LookAheadIteration<E, X> {
	private final CloseableIteration<E, X>[] _iterators;
	private int _index = 0;

	/**
	 * Creates a new closeable iteration that concats the goven closeable iterations.
	 * 
	 * @param iterators the iterators to concat.
	 */
	public CloseableMultiIterator(final CloseableIteration<E, X>... iterators) {
		// This assignment is safe, since nothing is every assigned to iterators. 
		_iterators = iterators;
	}

	@Override
	protected E getNextElement() throws X {
		if (_index >= _iterators.length) {			
			return null;
		} else if (!_iterators[_index].hasNext()) {			
			_index++;
			return getNextElement();
		} else {
			return _iterators[_index].next();
		}
	}

	/**
	 * Closes every single iterator that was passed in the constructor.
	 * 
	 * @throws X in case of close failure.
	 */
	@Override
	protected void handleClose() throws X {
		super.handleClose();

		for (final CloseableIteration<E, X> iterator : _iterators) {
			iterator.close();
		}
	}
}