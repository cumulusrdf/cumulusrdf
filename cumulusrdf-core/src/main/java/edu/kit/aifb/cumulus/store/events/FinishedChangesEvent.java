package edu.kit.aifb.cumulus.store.events;

import java.util.EventObject;

/**
 * Marker {@link EventObject} for indicating that a change has been completed.
 * 
 * @author Andreas Wagner.
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class FinishedChangesEvent extends EventObject {

	private static final long serialVersionUID = 1L;

	/**
	 * Builds a new event with the given source.
	 * 
	 * @param source the event source.
	 */
	FinishedChangesEvent(final Object source) {
		super(source);
	}

	/**
	 * Factory method for creating new events.
	 * 
	 * @param source the event source.
	 * @return a new (finish) event object.
	 */
	public static FinishedChangesEvent newEvent(final Object source) {
		return new FinishedChangesEvent(source);
	}
}