package edu.kit.aifb.cumulus.store.sesame;

import static edu.kit.aifb.cumulus.framework.Environment.DATETIME_RANGETYPES_AS_STRING;
import static edu.kit.aifb.cumulus.framework.Environment.NUMERIC_RANGETYPES_AS_STRING;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailBase;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.CumulusStoreException;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.sesame.model.INativeCumulusValue;

/**
 * A Sail that uses CumulusRDF for storing RDF data.
 * 
 * @author Andreas Wagner
 * @since 1.0
 */
//TODO support transactions using locking (!) 
public class CumulusRDFSail extends NotifyingSailBase {

	private Store _crdf;
	private ITopLevelDictionary _dict;
	private CumulusRDFValueFactory _valueFactory;

	private Log _log = new Log(LoggerFactory.getLogger(getClass()));

	/**
	 * Builds a new {@link CumulusRDFSail} with the given {@link Store}.
	 * 
	 * @param crdf the CumulusRDF {@link Store}.
	 */
	public CumulusRDFSail(final Store crdf) {
		_crdf = crdf;
		_valueFactory = new CumulusRDFValueFactory(this);
	}

	protected <X extends Exception> CloseableIteration<Statement, X> createRangeStatementIterator(Resource subj, URI pred, Literal lowerBound,
			boolean lower_equals, Literal upperBound, boolean upper_equals, Literal equals, boolean reverse) throws SailException {

		if (equals != null) {
			return createStatementIterator(subj, pred, equals);
		}

		Value[] nx = new Value[2];

		if (subj == null) {
			nx[0] = null;
		} else {
			nx[0] = subj;
		}
		if (pred == null) {
			nx[1] = null;
		} else {
			nx[1] = pred;
		}

		// at least predicate must be set!
		if ((nx[0] == null) && (nx[1] == null)) {
			return new EmptyIteration<Statement, X>();
		}

		URI datatype_lower = lowerBound == null ? null : lowerBound.getDatatype(), datatype_upper = upperBound == null ? null : upperBound
				.getDatatype(), datatype = null;

		// both datatypes are set
		if ((upperBound != null) && (lowerBound != null)) {

			if (!datatype_lower.equals(datatype_upper)) {
				_log.warning(MessageCatalog._00072_DATATYPE_BOUNDS_MISMATCH, datatype_lower, datatype_upper);
				return new EmptyIteration<Statement, X>();
			} else {
				datatype = datatype_lower;
			}
		} else {
			// only one or no datatype is set

			if ((datatype_lower == null) && (datatype_upper == null)) {
				_log.warning(MessageCatalog._00073_DATATYPE_BOUNDS_NULL);
				return new EmptyIteration<Statement, X>();
			} else {
				datatype = datatype_lower == null ? datatype_upper : datatype_lower;
			}
		}

		try {

			Literal lower_lit = lowerBound;
			Literal upper_lit = upperBound;

			if (NUMERIC_RANGETYPES_AS_STRING.contains(datatype.stringValue())) {
				return new CumulusRDFIterator<X>(_crdf.rangeAsIDs(nx, lower_lit, lower_equals, upper_lit, upper_equals, reverse, Integer.MAX_VALUE),
						this);
			} else if (DATETIME_RANGETYPES_AS_STRING.contains(datatype.stringValue())) {
				return new CumulusRDFIterator<X>(_crdf.rangeDateTimeAsIDs(nx, lower_lit, lower_equals, upper_lit, upper_equals, reverse,
						Integer.MAX_VALUE), this);
			}

		} catch (final ClassCastException exception) {
			_log.error(MessageCatalog._00074_BOUND_NOT_LITERAL, exception);
		} catch (final DataAccessLayerException exception) {			
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
		}

		return new EmptyIteration<Statement, X>();
	}

	protected <X extends Exception> CloseableIteration<Statement, X> createStatementIterator(Resource subj, URI pred, Value obj, Resource... contexts) throws SailException {
		try {
			byte[][] ids;
			
			if ((contexts == null) || (contexts.length == 0)) {
				ids = new byte[3][];
			} else {
				// quads
				ids = new byte[4][];
			}
	
			if (subj == null) {
	
				ids[0] = null;
	
			} else if (subj instanceof INativeCumulusValue) {
	
				INativeCumulusValue native_val = (INativeCumulusValue) subj;
	
				if (native_val.hasInternalID()) {
					ids[0] = native_val.getInternalID();
				} else {
					Value node = subj;
					ids[0] = getDictionary().getID(node, false);
					native_val.setInternalID(ids[0]);
				}
	
			} else {
				// this should not happen ...
				Value node = subj;
				try {
					ids[0] = getDictionary().getID(node, false);
				} catch (DataAccessLayerException exception) {
					_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
				}
			}
	
			if (pred == null) {
	
				ids[1] = null;
	
			} else if (pred instanceof INativeCumulusValue) {
	
				INativeCumulusValue native_val = (INativeCumulusValue) pred;
	
				if (native_val.hasInternalID()) {
					ids[1] = native_val.getInternalID();
				} else {
					Value node = pred;
					ids[1] = getDictionary().getID(node, true);
					native_val.setInternalID(ids[1]);
				}
	
			} else {
				// this should not happen ...
				Value node = pred;
				ids[1] = getDictionary().getID(node, true);
			}
	
			if (obj == null) {
	
				ids[2] = null;
	
			} else if (obj instanceof INativeCumulusValue) {
	
				INativeCumulusValue native_val = (INativeCumulusValue) obj;
	
				if (native_val.hasInternalID()) {
					ids[2] = native_val.getInternalID();
				} else {
					Value node = obj;
					ids[2] = getDictionary().getID(node, false);
					native_val.setInternalID(ids[2]);
				}
	
			} else {
				// this should not happen ...
				Value node = obj;
				ids[2] = getDictionary().getID(node, false);
			}
			
			if (ids.length == 4) {
				// Only use first entry, ignore anything else.
				if (contexts[0] == null) {
	
					ids[3] = null;
	
				} else if (contexts[0] instanceof INativeCumulusValue) {
	
					INativeCumulusValue native_val = (INativeCumulusValue) contexts[0];
	
					if (native_val.hasInternalID()) {
						ids[3] = native_val.getInternalID();
					} else {
						Value node = contexts[0];
						ids[3] = getDictionary().getID(node, false);
						native_val.setInternalID(ids[3]);
					}
	
				} else {
					// this should not happen ...
					Value node = contexts[0];
					ids[3] = getDictionary().getID(node, false);
				}
			}


			return new CumulusRDFIterator<X>(_crdf.queryWithIDs(ids), this);
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
			return new EmptyIteration<Statement, X>();
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
			return new EmptyIteration<Statement, X>();
		}			
	}

	@Override
	protected NotifyingSailConnection getConnectionInternal() throws SailException {
		return new CumulusRDFSailConnection(this);
	}

	/**
	 * Returns the dictionary currently in use.
	 * 
	 * @return the dictionary currently in use.
	 */
	protected ITopLevelDictionary getDictionary() {

		if (_dict == null) {
			_dict = _crdf.getDictionary();
		}

		return _dict;
	}

	/**
	 * Returns the store currently in use.
	 * 
	 * @return the store currently in use.
	 */
	protected Store getStore() {
		return _crdf;
	}

	@Override
	public CumulusRDFValueFactory getValueFactory() {
		return _valueFactory;
	}
	
	@Override
	protected void initializeInternal() throws SailException {

		try {
			_crdf.open();			
		} catch (CumulusStoreException e) {
			e.printStackTrace();
		}
		
		super.initializeInternal();
	}

	@Override
	protected boolean isInitialized() {
		return _crdf.isOpen();
	}

	@Override
	public boolean isWritable() throws SailException {
		return _crdf.isOpen();
	}

	@Override
	protected void shutDownInternal() throws SailException {

		if (_crdf != null) {
			_crdf.close();
		}
	}
}