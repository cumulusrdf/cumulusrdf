package edu.kit.aifb.cumulus.store.sesame;

import java.util.Arrays;




import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;
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
	private final static Log LOGGER = new Log(LoggerFactory.getLogger(LookAheadIteration.class));

	private final CloseableIteration<E, X>[] _iterators;
	private int _index;

	/**
	 * Creates a new closeable iteration that concats the given closeable iterations.
	 * 
	 * @param iterators the iterators to concat.
	 */
	@SafeVarargs
	public CloseableMultiIterator(final CloseableIteration<E, X>... iterators) {
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
		Arrays.stream(_iterators).forEach(iterator -> {
			try {
				iterator.close();
			} catch (final Exception exception) {
				LOGGER.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE, exception);
			}
		});
	}
}