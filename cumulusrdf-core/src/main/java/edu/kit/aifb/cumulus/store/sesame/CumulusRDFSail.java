package edu.kit.aifb.cumulus.store.sesame;

import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailBase;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.CumulusStoreException;
import edu.kit.aifb.cumulus.store.Store;

/**
 * A Sail that uses CumulusRDF for storing RDF data.
 * 
 * @author Andreas Wagner
 * @since 1.0
 */
//TODO support transactions using locking (!) 
public class CumulusRDFSail extends NotifyingSailBase {
	private final static Log LOGGER = new Log(LoggerFactory.getLogger(CumulusRDFSail.class));

	private final Store _store;
	private final CumulusRDFValueFactory _valueFactory;

	/**
	 * Builds a new {@link CumulusRDFSail} with the given {@link Store}.
	 * 
	 * @param crdf the CumulusRDF {@link Store}.
	 */
	public CumulusRDFSail(final Store crdf) {
		_store = crdf;
		_valueFactory = new CumulusRDFValueFactory(_store.getDictionary());
	}

	@Override
	protected NotifyingSailConnection getConnectionInternal() throws SailException {
		return new CumulusRDFSailConnection(this);
	}
	
	/**
	 * Returns the store currently in use.
	 * 
	 * @return the store currently in use.
	 */
	protected Store getStore() {
		return _store;
	}

	@Override
	public CumulusRDFValueFactory getValueFactory() {
		return _valueFactory;
	}
	
	@Override
	protected void initializeInternal() throws SailException {
		try {
			_store.open();			
		} catch (final CumulusStoreException exception) {
			LOGGER.error(MessageCatalog._00025_CUMULUS_SYSTEM_INTERNAL_FAILURE, exception);
			throw new SailException(exception);
		}
	}

	@Override
	protected boolean isInitialized() {
		return _store.isOpen();
	}

	@Override
	public boolean isWritable() throws SailException {
		return _store.isOpen();
	}

	@Override
	protected void shutDownInternal() throws SailException {
		if (_store != null) {
			_store.close();
		}
	}
}