package edu.kit.aifb.cumulus.log;

/**
 * Simple Booch utility for creating messages by concatenation.
 * 
 * @author Andrea Gazzarini
 * @since 1.0.1
 */
public abstract class MessageFactory {
	/**
	 * Creates a new message by replacing the given set of params.
	 * 
	 * @param message the message (with placeholders).
	 * @param params the parameters.
	 * @return a new message with placeholders replaced.
	 */
	public static String createMessage(final String message, final Object ... params) {
		return String.format(message, params);
	}
}