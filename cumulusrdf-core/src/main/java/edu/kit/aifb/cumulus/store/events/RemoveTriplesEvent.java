package edu.kit.aifb.cumulus.store.events;

import java.util.Collections;
import java.util.List;

import edu.kit.aifb.cumulus.framework.events.TriplesChangedEvent;

/**
 * Event object for indicating that one or more triples have been removed.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class RemoveTriplesEvent extends TriplesChangedEvent {

	private int _changes;
	private static final long serialVersionUID = 1L;

	/**
	 * Builds a new event with the given source and removed triple.
	 * 
	 * @param source the event source.
	 * @param triple the removed triple.
	 */
	public RemoveTriplesEvent(final Object source, final byte[][] triple) {
		super(source, Collections.singletonList(triple));
		_changes = 1;
	}

	/**
	 * Builds a new event with the given source and removed triples.
	 * 
	 * @param source the event source.
	 * @param triples the removed triples.
	 */
	public RemoveTriplesEvent(final Object source, final List<byte[][]> triples) {
		super(source, triples);
		_changes = triples.size();
	}

	@Override
	public int numOfChanges() {
		return _changes;
	}
}