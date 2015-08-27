package edu.kit.aifb.cumulus.framework.events;

import java.util.EventObject;
import java.util.List;

/**
 * {@link EventObject} for indicating that one or more triples have been changed.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class TriplesChangedEvent extends EventObject {

	private static final long serialVersionUID = 1L;
	private List<byte[][]> _triples;

	/**
	 * Builds a new event with the given source.
	 * 
	 * @param source the event source.
	 * @param triples the affected triples.
	 */
	public TriplesChangedEvent(final Object source, final List<byte[][]> triples) {
		super(source);
		_triples = triples;
	}

	/**
	 * Returns the triples that were affected by the current change.
	 * 
	 * @return the triples that were affected by the current change.
	 */
	public List<byte[][]> getChangedTriples() {
		return _triples;
	}

	/**
	 * Returns the total number of changes associated with this event.
	 * 
	 * @return the total number of changes associated with this event.
	 */
	public abstract int numOfChanges();
}