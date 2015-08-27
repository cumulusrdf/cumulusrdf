package edu.kit.aifb.cumulus.framework.domain.dictionary;

import edu.kit.aifb.cumulus.framework.Initialisable;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;


/**
 * CumulusRDF dictionary interface.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 * 
 * @param <V> the kind of resource managed by this dictionary.
 */
public interface IDictionary<V> extends Initialisable {

	byte[] NOT_SET = { 1 }; // 0000 0001
	byte RESOURCE_BYTE_FLAG = 8; // 0000 1000
	byte BNODE_BYTE_FLAG = 16; // 0001 0000
	byte LITERAL_BYTE_FLAG = 32; // 0010 0000
	
	/**
	 * Closes this dictionary and releases resources.
	 */
	void close();

	/**
	 * Returns the identifier of the given resource.
	 * 
	 * @param node the resource.
	 * @param p a flag indicating if the resource is a predicate.
	 * @return the identifier of the given resource.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	byte[] getID(V node, boolean p) throws DataAccessLayerException;
	
	/**
	 * Returns the value associated with the given identifier.
	 * 
	 * @param id the identifier.
	 * @param p a flag indicating if the id corresponds to a predicate.
	 * @return the value associated with the given identifier.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	V getValue(byte[] id, boolean p) throws DataAccessLayerException;

	/**
	 * Removes a given value from this dictionary.
	 *  
	 * @param value the value to be removed.
	 * @param p a flag indicating if the value is a predicate.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	void removeValue(V value, boolean p) throws DataAccessLayerException;
	
	/**
	 * Returns a mnemonic code that identifies this dictionary.
	 * 
	 * @return a mnemonic code that identifies this dictionary.
	 */
	String getId();
}