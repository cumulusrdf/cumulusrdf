package edu.kit.aifb.cumulus.store.sesame.model;

import java.util.Arrays;

import org.openrdf.model.BNode;
import org.openrdf.model.impl.BNodeImpl;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * CumulusRDF blank node.
 * 
 * @author Andreas Wagner
 * @since 1.0
 */
public class NativeCumulusBNode extends BNodeImpl implements INativeCumulusResource {
	private static final long serialVersionUID = 1L;

	private final Log _log = new Log(LoggerFactory.getLogger(getClass()));

	private volatile byte[] _internalID = INativeCumulusValue.UNKNOWN_ID;
	private ITopLevelDictionary _dict;
	private int _hash;
	private boolean _has_data;

	/**
	 * Builds a new blank node with the given data.
	 * 
	 * @param internalID the internal identifier.
	 * @param dict the dictionary in use.
	 */
	public NativeCumulusBNode(final byte[] internalID, final ITopLevelDictionary dict) {

		super();

		_internalID = internalID;
		_hash = Arrays.hashCode(_internalID);
		_dict = dict;
		_has_data = false;
	}

	/**
	 * Builds a new blank node with given (string) identifier.
	 * Note that that is not the CumulusRDF internal identifier.
	 * 
	 * @param id the blank node identifier.
	 */
	public NativeCumulusBNode(final String id) {

		super(id);

		_hash = super.hashCode();
		_has_data = true;
	}

	@Override
	public boolean equals(final Object object) {

		if (this == object) {
			return true;
		}

		if (!(object instanceof BNode)) {
			return false;
		}

		if (object instanceof NativeCumulusBNode && !Arrays.equals(_internalID, INativeCumulusValue.UNKNOWN_ID)) {

			NativeCumulusBNode other = (NativeCumulusBNode) object;

			if (!Arrays.equals(other._internalID, INativeCumulusValue.UNKNOWN_ID)) {
				return Arrays.equals(_internalID, other._internalID);
			}
		}

		// this should not happen -> _internalID should be used only!
		if (!_has_data) {
			load();
		}

		return super.equals(object);
	}

	@Override
	public String getID() {

		if (!_has_data) {
			load();
		}

		return super.getID();
	}

	@Override
	public byte[] getInternalID() {
		return _internalID;
	}

	@Override
	public int hashCode() {
		return _hash;
	}

	@Override
	public boolean hasInternalID() {
		return _internalID.length != 0;
	}

	/**
	 * Loads data associated with this blank node.
	 */
	private synchronized void load() {

		if (_has_data) {
			return;
		}

		// load data ...
		try {

			final BNode bnode = (BNode) _dict.getValue(_internalID, false);
			super.setID(bnode.getID());

		} catch (final Exception exception) {
			_log.error(MessageCatalog._00075_COULDNT_LOAD_NODE, exception, Arrays.toString(_internalID));
			super.setID("cumulus/internal/" + Arrays.toString(_internalID));
		}

		_has_data = true;
	}

	@Override
	public void setInternalID(final byte[] internalID) {
		_internalID = internalID;
		_hash = Arrays.hashCode(internalID);
	}

	@Override
	public String stringValue() {

		if (!_has_data) {
			load();
		}

		return super.stringValue();
	}

	@Override
	public String toString() {

		if (!_has_data) {
			load();
		}

		return super.toString();
	}
}