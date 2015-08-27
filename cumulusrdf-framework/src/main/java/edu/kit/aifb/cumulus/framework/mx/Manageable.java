package edu.kit.aifb.cumulus.framework.mx;


/**
 * Marker interface which defines a CumulusRDF manageable object.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public interface Manageable {
	/**
	 * Returns a mnemonic code that identifies this manageable instance.
	 * 
	 * @return a mnemonic code that identifies this manageable instance.
	 */
	String getId();
}