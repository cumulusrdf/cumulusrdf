package edu.kit.aifb.cumulus.store.events;

import java.util.EventObject;

/**
 * Marker {@link EventObject} for indicating that a change is going to start.
 * 
 * @author Andreas Wagner.
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class StartChangesEvent extends EventObject {

	private static final long serialVersionUID = 1L;

	/**
	 * Builds a new event with the given source.
	 * 
	 * @param source the event source.
	 */
	StartChangesEvent(final Object source) {
		super(source);
	}

	/**
	 * Factory method for creating new events.
	 * 
	 * @param source the event source.
	 * @return a new (start) event object.
	 */
	public static StartChangesEvent newEvent(final Object source) {
		return new StartChangesEvent(source);
	}
}