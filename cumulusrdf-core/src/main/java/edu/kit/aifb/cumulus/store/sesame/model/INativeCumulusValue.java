package edu.kit.aifb.cumulus.store.sesame.model;

/**
 * CumulusRDF value interface.
 * 
 * @author Andreas Wagner.
 * @since 1.0
 */
public interface INativeCumulusValue {

	byte[] UNKNOWN_ID = new byte[0];

	/**
	 * Injects the internal identifier of this value.
	 * 
	 * @param id the identifier.
	 */
	void setInternalID(byte[] id);

	/**
	 * Returns the identifier associated with this URI.
	 * 
	 * @return the identifier associated with this URI.
	 */
	byte[] getInternalID();
	
	/**
	 * Returns true if an identifier has been associated with this value.
	 * 
	 * @return true if an identifier has been associated with this value, false otherwise.
	 */
	boolean hasInternalID();
}