package edu.kit.aifb.cumulus.framework.domain.dictionary;

import java.util.Iterator;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;

import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;

/**
 * CumulusRDF Dictionary interface.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public interface ITopLevelDictionary extends IDictionary<Value> {	

	/**
	 * Returns the identifiers of the given resources.
	 * 
	 * @param s the subject.
	 * @param p the predicate.
	 * @param o the object.
	 * @return the identifiers of the given resources.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	byte[][] getIDs(Value s, Value p, Value o) throws DataAccessLayerException;

	/**
	 * Returns the identifiers of the given resources.
	 * 
	 * @param s the subject.
	 * @param p the predicate.
	 * @param o the object.
	 * @param c the context.
	 * @return the identifiers of the given resources.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	byte[][] getIDs(Value s, Value p, Value o, Value c) throws DataAccessLayerException;
	
	/**
	 * Converts the given identifiers in a statement.
	 * 
	 * @param s the subject identifier.
	 * @param p the predicate identifier.
	 * @param o the object identifier.
	 * @return a {@link Statement} with values associated with input identifiers.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	Statement getValues(byte[] s, byte[] p, byte[] o) throws DataAccessLayerException;

	/**
	 * Converts the given identifiers in a statement.
	 * 
	 * @param s the subject identifier.
	 * @param p the predicate identifier.
	 * @param o the object identifier.
	 * @param c the context identifier.
	 * @return a {@link Statement} with values associated with input identifiers.
	 * @throws DataAccessLayerException in case of data access failure.
	 */
	Statement getValues(byte[] s, byte[] p, byte[] o, byte[] c) throws DataAccessLayerException;

	/**
	 * Returns true if a given identifiers maps to a BNode.
	 * 
	 * @param id the identifier.
	 * @return true if a given identifiers maps to a BNode.
	 */
	boolean isBNode(byte[] id);

	/**
	 * Returns true if a given identifiers maps to a Literal.
	 * 
	 * @param id the identifier.
	 * @return true if a given identifiers maps to a Literal..
	 */
	boolean isLiteral(byte[] id);

	/**
	 * Returns true if a given identifiers maps to a Resource.
	 * 
	 * @param id the identifier.
	 * @return true if a given identifiers maps to a Resource.
	 */
	boolean isResource(byte[] id);

	/**
	 * Converts a given iterator of statements in an iterator of identifiers.
	 * 
	 * @param statements the iterator of statements.
	 * @return the corresponding iterator of identifiers.
	 */
	Iterator<byte[][]> toIDTripleIterator(Iterator<Statement> statements);

	/**
	 * Converts the incoming statements iterator in a ids (byte[][]) iterator.
	 * Each statement in the incoming iterator is converted into the corresponding byte[][].
	 * 
	 * @param quads the iterator containing quads statements.
	 * @return an iterator containing corresponding ids.
	 */
	Iterator<byte[][]> toIDQuadIterator(final Iterator<Statement> quads);
	
	/**
	 * Converts a given iterator of identifiers in an iterator of statements.
	 * 
	 * @param identifiers the iterator of identifiers.
	 * @return the corresponding iterator of statements.
	 */
	Iterator<Statement> toValueTripleIterator(Iterator<byte[][]> identifiers);

	/**
	 * Converts the incoming ids iterator in a statement iterator.
	 * Each id in the incoming iterator is converted into the corresponding statement.
	 * 
	 * @param quads the iterator containing quads ids.
	 * @return an iterator containing corresponding statements.
	 */
	Iterator<Statement> toValueQuadIterator(final Iterator<byte[][]> quads);
	
	/**
	 * Given a set of identifiers this method returns the corresponding composite identifier.
	 * This is actually needed for generating composite primary keys because all CF row keys are byte [] but
	 * some of them are composed by just one term (e.g. s, p, o) while some others are actually composites (e.g. so, po, sp, spc).
	 * 
	 * When the identifier is a composite, its internal encoding depends on how the dictionary generates each identifier. 
	 * For example
	 * 
	 * - A dictionary (e.g. ReadOptimizeDictionary) could generate fixed length identifiers;
	 * - A dictionary (e.g. WriteOptimizeDictionary) could generate variable lenght identifiers;
	 * 
	 * While in the first case the deserialization is trivial, in the second scenario there should be some mechanism for correctly 
	 * get each sub-identifier.
	 * At the end, instead of putting that codec logic somewhere, the natural and cohesive place is the dictionary itself.
	 * 
	 * @param id1 the first identifier.
	 * @param id2 the second identifier.
	 * @return a composite identifier containing sub-identifiers associated with input values. 
	 */
	byte[] compose(byte[] id1, final byte [] id2);

	/**
	 * Given a set of identifiers this method returns the corresponding composite identifier.
	 * This is actually needed for generating composite primary keys because all CF row keys are byte [] but
	 * some of them are composed by just one term (e.g. s, p, o) while some others are actually composites (e.g. so, po, sp, spc).
	 * 
	 * When the identifier is a composite, its internal encoding depends on how the dictionary generates each identifier. 
	 * For example
	 * 
	 * - A dictionary (e.g. ReadOptimizeDictionary) could generate fixed length identifiers;
	 * - A dictionary (e.g. WriteOptimizeDictionary) could generate variable lenght identifiers;
	 * 
	 * While in the first case the deserialization is trivial, in the second scenario there should be some mechanism for correctly 
	 * get each sub-identifier.
	 * At the end, instead of putting that codec logic somewhere, the natural and cohesive place is the dictionary itself.
	 * 
	 * @param id1 the first identifier.
	 * @param id2 the second identifier.
	 * @param id3 the third identifier.
	 * @return a composite identifier containing sub-identifiers associated with input values. 
	 */
	byte[] compose(byte[] id1, final byte [] id2, byte[] id3);

	/**
	 * Decomposes a composite id returning an array of all compounding identifiers.
	 * 
	 * @param compositeId the composite identifiers.
	 * @return an array containing all compounding identifiers.
	 */
	byte[][] decompose(byte[] compositeId);
}