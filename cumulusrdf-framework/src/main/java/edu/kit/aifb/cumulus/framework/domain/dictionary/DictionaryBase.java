package edu.kit.aifb.cumulus.framework.domain.dictionary;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.mx.ManageableDictionary;
import edu.kit.aifb.cumulus.framework.mx.ManagementRegistrar;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * Supertype layer for all dictionaries.
 * Makes a wide use of template method pattern in order to enforce some common behaviour like:
 * 
 * <ul>
 * 	<li>Basic metrics count</li>
 * 	<li>MBean registration / unregistration</li>
 * </ul>
 * 
 * Although is possible to build a dictionary from scratch, in case you need 
 * an additional dictionary implementation, it is strongly recommended to derive from 
 * this class.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 * @param <V> the concrete value kind managed by this dictionary.
 */
public abstract class DictionaryBase<V> implements IDictionary<V>, ManageableDictionary  {
	protected final Log _log = new Log(LoggerFactory.getLogger(getClass()));
	
	protected final String _id;
	protected final AtomicLong _idLookupsCount = new AtomicLong();
	protected final AtomicLong _valueLookupsCount = new AtomicLong();

	/**
	 * Builds a new dictionary with the given identifier.
	 * 
	 * @param id the dictionary identifier.
	 */
	public DictionaryBase(final String id) {
		_id = id;
	}
	
	/**
	 * Returns the identifier associated with this dictionary.
	 * 
	 * @return the identifier associated with this dictionary.
	 */
	public String getId() {
		return _id;
	}
	
	@Override
	public final byte[] getID(final V value, final boolean p) throws DataAccessLayerException {
		_idLookupsCount.incrementAndGet();

		if (value == null) {
			return null;
		}
		
		return getIdInternal(value, p);
	};
	
	@Override
	public final V getValue(final byte[] id, final boolean p) throws DataAccessLayerException {
		_valueLookupsCount.incrementAndGet();
		if (id == null) {
			return null;
		}
		
		return getValueInternal(id, p);
	}
	
	@Override
	public long getValueLookupsCount() {
		return _valueLookupsCount.get();
	}

	@Override
	public long getIdLookupsCount() {
		return _idLookupsCount.get();
	}	
	
	@Override
	public final void close() {
		ManagementRegistrar.unregisterDictionary(this);
		closeInternal();
	}
	
	@Override
	public final void initialise(final DataAccessLayerFactory factory) throws InitialisationException {
		initialiseInternal(factory);
		
		try {
			ManagementRegistrar.registerDictionary(this);
		} catch (Exception exception) {
			_log.error(MessageCatalog._00111_MBEAN_ALREADY_REGISTERED, _id);
			throw new InitialisationException(exception);
		}
	}

	/**
	 * Internal method where each concrete implementor must define its own shutdown procedure.
	 */
	protected abstract void closeInternal();	
	
	/**
	 * Internal method where each concrete implementor must define its own initialisation procedure.
	 * 
	 * @param factory the data access layer factory.
	 * @throws InitialisationException in case of initialisation failure.
	 */
	protected abstract void initialiseInternal(DataAccessLayerFactory factory) throws InitialisationException;
	
	/**
	 * Internal method where each concrete implementor must define for retrieving identifiers.
	 * 
	 * @param value the value.
	 * @param p the predicate flag.
	 * @return the identifier associated with the given value.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	protected abstract byte[] getIdInternal(V value, boolean p) throws DataAccessLayerException;
	
	/**
	 * Internal method where each concrete implementor must define for retrieving values.
	 * 
	 * @param id the identifier.
	 * @param p the predicate flag.
	 * @return the value associated with the given identifier.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	protected abstract V getValueInternal(byte[] id, boolean p) throws DataAccessLayerException;	
}