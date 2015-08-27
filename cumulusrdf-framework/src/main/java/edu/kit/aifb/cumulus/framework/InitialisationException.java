package edu.kit.aifb.cumulus.framework;

/**
 * Marker exception thrown in case a dictionary fails its initialisation.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class InitialisationException extends Exception {

	private static final long serialVersionUID = 5191331244334257980L;

	/**
	 * Builds a new exception with the given message.
	 * 
	 * @param message the exception message.
	 */
	public InitialisationException(final String message) {
		super(message);
	}
	
	/**
	 * Builds a new exception with the given cause.
	 * 
	 * @param cause the underlying cause.
	 */
	public InitialisationException(final Throwable cause) {
		super(cause);
	}	
}