package edu.kit.aifb.cumulus.store.events;

import java.util.Collections;
import java.util.List;

import edu.kit.aifb.cumulus.framework.events.TriplesChangedEvent;

/**
 * An event object for indicating that one or more triples have been added.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public class AddTripleEvent extends TriplesChangedEvent {

	private int _changes;
	private static final long serialVersionUID = 1L;

	/**
	 * Builds a new event with the given source and triple.
	 * 
	 * @param source the event source.
	 * @param triples the affected triple.
	 */
	public AddTripleEvent(final Object source, final byte[][] triples) {
		super(source, Collections.singletonList(triples));
		_changes = 1;
	}

	/**
	 * Builds a new event with the given source and triples.
	 * 
	 * @param source the event source.
	 * @param triples the affected triples.
	 */
	public AddTripleEvent(final Object source, final List<byte[][]> triples) {
		super(source, triples);
		_changes = triples.size();
	}

	@Override
	public int numOfChanges() {
		return _changes;
	}
}