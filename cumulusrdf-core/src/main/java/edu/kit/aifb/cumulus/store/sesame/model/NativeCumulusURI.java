package edu.kit.aifb.cumulus.store.sesame.model;

import java.util.Arrays;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * CumulusRDF URI resource representation.
 * 
 * @author Andreas Wagner
 * @since 1.0
 */
public class NativeCumulusURI extends URIImpl implements INativeCumulusResource {
	private static final long serialVersionUID = 1L;

	private final Log _log = new Log(LoggerFactory.getLogger(getClass()));

	private volatile byte[] _internalID = INativeCumulusValue.UNKNOWN_ID;
	private ITopLevelDictionary _dict;
	private int _hash;
	private boolean _has_data, _p;

	/**
	 * Builds a new URI with the given data.
	 * 
	 * @param internalID the internal identifier.
	 * @param dict the dictionary.
	 * @param p the predicate flag.
	 */
	public NativeCumulusURI(final byte[] internalID, final ITopLevelDictionary dict, final boolean p) {

		super();

		_internalID = internalID;
		_hash = Arrays.hashCode(internalID);
		_dict = dict;
		_p = p;
		_has_data = false;
	}

	/**
	 * Builds a new URI from the given string representation.
	 * 
	 * @param uri the string representation of the URI.
	 */
	public NativeCumulusURI(final String uri) {

		super(uri);

		_hash = super.hashCode();
		_has_data = true;
	}

	@Override
	public boolean equals(final Object object) {

		if (this == object) {
			return true;
		}

		if (!(object instanceof URI)) {
			return false;
		}

		if (object instanceof NativeCumulusURI && !Arrays.equals(_internalID, INativeCumulusValue.UNKNOWN_ID)) {

			NativeCumulusURI other = (NativeCumulusURI) object;

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
	public byte[] getInternalID() {
		return _internalID;
	}

	@Override
	public String getLocalName() {

		if (!_has_data) {
			load();
		}

		return super.getLocalName();
	}

	@Override
	public String getNamespace() {

		if (!_has_data) {
			load();
		}

		return super.getNamespace();
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
	 * Loads data associated with this URI.
	 */
	private synchronized void load() {

		if (_has_data) {
			return;
		}

		// load data ...
		try {

			URI uri = (URI) _dict.getValue(_internalID, _p);
			super.setURIString(uri.stringValue());

		} catch (final Exception exception) {
			_log.error(MessageCatalog._00075_COULDNT_LOAD_NODE, exception, Arrays.toString(_internalID));
			super.setURIString("http://cumulus/internal/" + Arrays.toString(_internalID));
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