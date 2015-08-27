package edu.kit.aifb.cumulus.store;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.Initialisable;
import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.CounterDAO;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * CumulusRDF counter factory.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public final class CounterFactory implements Initialisable {
	private CounterDAO<String> _dao;
	
	/**
	 * CumulusRDF counter.
	 * 
	 * @author Andreas Wagner
	 * @since 1.0
	 */
	public class Counter {

		private String _id;
		private long _data;

		/**
		 * Builds a new counter with a given identifier.
		 * 
		 * @param id the counter identifier.
		 */
		public Counter(final String id) {
			_id = id;
			try {
				sync();
				if (_data == -1L) {
					_data = 0;

					_dao.set(_id, 0L);
					LOGGER.info(MessageCatalog._00080_INIT_COUNTER, id, _data);
				}
			} catch (final DataAccessLayerException exception) {
				LOGGER.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
				throw new IllegalStateException(exception);
			}
		}

		/**
		 * Returns the current value of the counter.
		 * @return the current value of the counter.
		 * 
		 * @throws DataAccessLayerException if the counter value can not be read.
		 */ 
		public long current() throws DataAccessLayerException {
			sync();
			return _data;
		}

		/**
		 * Decreases the counter value.
		 * 
		 * @param decrement the decrement.
		 * @return the new value of the counter.
		 * @throws DataAccessLayerException if the counter can not be decremented.
		 */
		public synchronized long decrement(final long decrement) throws DataAccessLayerException {
			_dao.decrement(_id, decrement);
			sync();
			return _data;
		}

		@Override
		public boolean equals(final Object obj) {
			return obj instanceof Counter && ((Counter)obj)._id.equals(_id);
		}

		@Override
		public int hashCode() {
			return _id.hashCode();
		}

		/**
		 * Increases the counter value.
		 * 
		 * @param increment the increment.
		 * @return the new value of the counter.
		 * @throws DataAccessLayerException if the counter can not be incremented.
		 */
		public synchronized long increment(final long increment) throws DataAccessLayerException {

			_dao.increment(_id, increment);
			sync();

			if (_data < 0) {
				LOGGER.error(MessageCatalog._00081_COUNTER_OVERFLOW, _id, _data);
			}

			return _data;
		}

		/**
		 * Increments the counter.
		 * 
		 * @return the new value of the counter
		 * @throws DataAccessLayerException if the counter can not be incremented.
		 */
		public synchronized long next() throws DataAccessLayerException {
			_dao.increment(_id, 1L);
			sync();
			return _data;
		}

		/**
		 * Sets the counter to the given value.
		 * 
		 * @param val the target counter value. 
		 * @throws DataAccessLayerException if the counter cannot be set.
		 */
		public synchronized void set(final long val) throws DataAccessLayerException {
			_dao.set(_id, val);
			sync();
		}

		/**
		 * Reads the current counter value from the DAO.
		 * 
		 * @throws DataAccessLayerException if the counter value can not be read.
		 */
		private void sync() throws DataAccessLayerException {
			_data = _dao.get(_id);
		}

		@Override
		public String toString() {
			return "counter: " + _id;
		}
	}

	private static final Log LOGGER = new Log(LoggerFactory.getLogger(CounterFactory.class));
	private static final CounterFactory INSTANCE = new CounterFactory();

	private ConcurrentMap<String, Counter> _id2counter = new ConcurrentHashMap<String, Counter>(1);

	/**
	 * Returns the singleton instance of this factory.
	 * 
	 * @return the singleton instance of this factory.
	 */
	public static CounterFactory getInstance() {
		return INSTANCE;
	}

	/**
	 * Builds a new factory.
	 */
	private CounterFactory() {
	}

	/**
	 * Closes and clears all open counters.
	 */
	public synchronized void close() {
		_id2counter.clear();
	}

	/**
	 * Releases all resources retained by this factory.
	 */
	public synchronized void destroy() {
		for (final String id : _id2counter.keySet()) {
			removeCounter(id);
		}
	}

	/**
	 * Returns a counter associated with a given identifier.
	 * 
	 * @param id the identifier.
	 * @return a counter associated with a given identifier.
	 */
	public synchronized Counter getCounter(final String id) {

		Counter counter = _id2counter.get(id);

		if (counter == null) {
			counter = new Counter(id);
			_id2counter.put(id, counter);
		}

		return counter;
	}

	/**
	 * Removes the counter associated with a given identifier.
	 * 
	 * @param id the counter identifier.
	 */
	public synchronized void removeCounter(final String id) {
		if (_id2counter.containsKey(id)) {
			_id2counter.remove(id);
		}
	}

	@Override
	public void initialise(final DataAccessLayerFactory factory) throws InitialisationException {
		try {
			_dao = factory.getCounterDAO(String.class, "COUNTER");
			_dao.setDefaultValue(-1L);
			_dao.createRequiredSchemaEntities();
		} catch (DataAccessLayerException exception) {
			throw new InitialisationException(exception);
		}
	}
}