package edu.kit.aifb.cumulus.framework.datasource;

/**
 * CumulusRDF data access layer exception.
 * Wraps specific exception cause thrown in case of data access failure.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class DataAccessLayerException extends Exception {

	private static final long serialVersionUID = -8321710035824020707L;

	/**
	 * Builds a new exception with the given underlying cause.
	 * 
	 * @param cause the exception cause.
	 */
	public DataAccessLayerException(final Throwable cause) {
		super(cause);
	}
}
