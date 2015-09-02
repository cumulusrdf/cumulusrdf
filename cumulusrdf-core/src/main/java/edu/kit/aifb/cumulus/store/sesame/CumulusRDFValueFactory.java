package edu.kit.aifb.cumulus.store.sesame;

import java.util.Arrays;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryBase;

import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.store.sesame.model.INativeCumulusValue;
import edu.kit.aifb.cumulus.store.sesame.model.NativeCumulusBNode;
import edu.kit.aifb.cumulus.store.sesame.model.NativeCumulusLiteral;
import edu.kit.aifb.cumulus.store.sesame.model.NativeCumulusURI;

/**
 * CumulusRDF factory for creating URIs, blank nodes, literals and statements.
 * 
 * @author Andreas Wagner
 * @since 1.0
 */
public class CumulusRDFValueFactory extends ValueFactoryBase {

	private final ITopLevelDictionary dictionary;
	
	/**
	 * Creates a CumulusRDF native value from a given {@link Value}.
	 * 
	 * @param value the incoming {@link Value}.
	 * @return a CumulusRDF native value.
	 */
	public static Value makeNativeValue(final Value value) {
		if (value == null || value instanceof INativeCumulusValue) {
			return value;
		}

		if (value instanceof URI) {
			return new NativeCumulusURI(value.stringValue());
		} else if (value instanceof Literal) {
			
			final Literal lit = (Literal) value;
			final String label = lit.getLabel(), language = lit.getLanguage();
			final URI datatype = lit.getDatatype();

			if (language != null) {
				return new NativeCumulusLiteral(label, language);
			} else if (datatype != null) {
				return new NativeCumulusLiteral(label, datatype);
			} else {
				return new NativeCumulusLiteral(label);
			}
			
		} else if (value instanceof BNode) {
			return new NativeCumulusBNode(value.stringValue());
		}

		return value;
	}

	/**
	 * Builds a new value factory with the given store.
	 * 
	 * @param dictionary the {@link ITopLevelDictionary} currently in use.
	 */
	public CumulusRDFValueFactory(final ITopLevelDictionary dictionary) {
		this.dictionary = dictionary;
	}

	public BNode createBNode(final BNode bNode) {
		return createBNode(bNode.getID());
	}

	@Override
	public BNode createBNode(String nodeID) {
		return new NativeCumulusBNode(nodeID);
	}

	@Override
	public Literal createLiteral(String label) {
		return new NativeCumulusLiteral(label);
	}

	@Override
	public Literal createLiteral(String label, String language) {
		return new NativeCumulusLiteral(label, language);
	}

	@Override
	public Literal createLiteral(String label, URI datatype) {
		return new NativeCumulusLiteral(label, datatype);
	}

	public Value[] createNodes(final Value ... vs) {
		return vs;
	}

	public Statement createStatement(final byte[][] nodes) {
		return (nodes.length == 3) 
				? createStatement((Resource) createValue(nodes[0], false), (URI) createValue(nodes[1], true), createValue(nodes[2], false))
				: createStatement((Resource) createValue(nodes[0], false), (URI) createValue(nodes[1], true), createValue(nodes[2], false), (Resource) createValue(nodes[3], false));
	}

	@Override
	public Statement createStatement(final Resource subject, final URI predicate, final Value object) {
		return new StatementImpl(subject, predicate, object);
	}

	@Override
	public Statement createStatement(final Resource subject, final URI predicate, final Value object, final Resource context) {
		return new ContextStatementImpl(subject, predicate, object, context);
	}

	@Override
	public URI createURI(final String uri) {
		return new NativeCumulusURI(uri);
	}

	@Override
	public URI createURI(final String namespace, final String localName) {
		return createURI(namespace + localName);
	}

	Value createValue(final byte[] id, final boolean isPredicate) {
		if (dictionary.isBNode(id)) {
			return new NativeCumulusBNode(id, dictionary);
		} else if (dictionary.isLiteral(id)) {
			return new NativeCumulusLiteral(id, dictionary);
		} else if (dictionary.isResource(id)) {
			return new NativeCumulusURI(id, dictionary, isPredicate);
		} else {
			throw new IllegalArgumentException("Could not create sesame value from node ID " + Arrays.toString(id));
		}
	}
}