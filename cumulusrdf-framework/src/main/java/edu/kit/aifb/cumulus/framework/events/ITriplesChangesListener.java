package edu.kit.aifb.cumulus.framework.events;

import java.util.EventListener;
import java.util.EventObject;

/**
 * A listener interested in triple(s) changes.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface ITriplesChangesListener extends EventListener {
	
	/**
	 * A change in one or more triples has been detected.
	 *  
	 * @param event the event object.
	 */
	void update(EventObject event);
}