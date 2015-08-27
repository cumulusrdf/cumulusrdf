package edu.kit.aifb.cumulus.framework;

import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;

/**
 * An interface that adds "initialisation" behaviour to a CumulusRDF domain object.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public interface Initialisable {
	
	/** 
	 * Initializes this dictionary.
	 * This is a callback method that the owning store instance uses for inform 
	 * the domain object about its (startup) status.
	 * 
	 * @param factory the data access layer (abstract) factory.
	 * @throws InitialisationException in case of initialisaton failure.
	 */
	void initialise(DataAccessLayerFactory factory) throws InitialisationException;
}
