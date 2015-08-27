package edu.kit.aifb.cumulus.store.sesame.model;

import java.util.Arrays;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * CumulusRDF literal.
 * 
 * @author Andreas Wagner
 * @since 1.0
 */
public class NativeCumulusLiteral extends LiteralImpl implements INativeCumulusValue {

	private final Log _log = new Log(LoggerFactory.getLogger(getClass()));
	private static final long serialVersionUID = 1L;

	private volatile byte[] _internalID = INativeCumulusValue.UNKNOWN_ID;
	private ITopLevelDictionary _dict;
	private int _hash;
	private boolean _has_data;

	/**
	 * Builds a new CumulusRDF literal with the given data.
	 * 
	 * @param internalID the internal identifier.
	 * @param dict the dictionary in use.
	 */
	public NativeCumulusLiteral(final byte[] internalID, final ITopLevelDictionary dict) {

		super();

		_internalID = internalID;
		_hash = Arrays.hashCode(_internalID);
		_dict = dict;
		_has_data = false;
	}

	/**
	 * Builds a new CumulusRDF literal from the given string literal.
	 * 
	 * @param label the literal content.
	 */
	public NativeCumulusLiteral(final String label) {

		super(label);

		_hash = super.hashCode();
		_has_data = true;
	}

	/**
	 * Builds a new CumulusRDF literal from the given data.
	 * 
	 * @param label the literal content.
	 * @param language the language code.
	 */
	public NativeCumulusLiteral(final String label, final String language) {

		super(label, language);

		_hash = super.hashCode();
		_has_data = true;
	}

	/**
	 * Builds a new CumulusRDF literal from the given data.
	 * 
	 * @param label the literal content.
	 * @param datatype the datatype URI.
	 */
	public NativeCumulusLiteral(final String label, final URI datatype) {

		super(label, datatype);

		_hash = super.hashCode();
		_has_data = true;
	}

	@Override
	public boolean equals(final Object object) {

		if (this == object) {
			return true;
		}

		if (!(object instanceof Literal)) {
			return false;
		}

		if (object instanceof NativeCumulusLiteral && !Arrays.equals(_internalID, INativeCumulusValue.UNKNOWN_ID)) {

			NativeCumulusLiteral other = (NativeCumulusLiteral) object;

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
	public URI getDatatype() {

		if (!_has_data) {
			load();
		}

		return super.getDatatype();
	}

	@Override
	public byte[] getInternalID() {
		return _internalID;
	}

	@Override
	public String getLabel() {

		if (!_has_data) {
			load();
		}

		return super.getLabel();
	}

	@Override
	public String getLanguage() {

		if (!_has_data) {
			load();
		}

		return super.getLanguage();
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
	 * Loads data associated with this literal.
	 */
	private synchronized void load() {

		if (_has_data) {
			return;
		}

		// load data ...
		try {

			Literal lit = (Literal) _dict.getValue(_internalID, false);

			super.setDatatype(lit.getDatatype());
			super.setLabel(lit.getLabel());
			super.setLanguage(lit.getLanguage());

		} catch (final Exception exception) {
			_log.error(MessageCatalog._00075_COULDNT_LOAD_NODE, exception, Arrays.toString(_internalID));
			super.setLabel("cumulus/internal/" + Arrays.toString(_internalID));
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