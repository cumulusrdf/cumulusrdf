package edu.kit.aifb.cumulus.store;

/**
 * An exception that is thrown if an error occurs during storing, deleting or
 * querying RDF data.
 * 
 * @author Andreas Harth
 * @since 1.0
 */
public class CumulusStoreException extends Exception {
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new exception.
	 * 
	 * @param cause The cause.
	 */
	public CumulusStoreException(final Exception cause) {
		super(cause);
	}
}
