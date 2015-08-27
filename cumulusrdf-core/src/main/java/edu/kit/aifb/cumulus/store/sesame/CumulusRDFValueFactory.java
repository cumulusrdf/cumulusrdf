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

	private CumulusRDFSail _store;

	public CumulusRDFValueFactory(final CumulusRDFSail store) {
		_store = store;
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

	public Value[] createNodes(Statement stmt) {

		Value[] values = new Value[stmt.getContext() == null ? 3 : 4];
		values[0] = stmt.getSubject();
		values[1] = stmt.getPredicate();
		values[2] = stmt.getObject();

		if (stmt.getContext() != null) {
			values[3] = stmt.getContext();
		}

		return values;
	}

	public Value[] createNodes(Value... vs) {
		return vs;
	}

	public Statement createStatement(byte[][] nodes) throws IllegalArgumentException {

		if (nodes.length == 3) {

			try {
				return createStatement((Resource) createValue(nodes[0], false), (URI) createValue(nodes[1], true), createValue(nodes[2], false));
			} catch (ClassCastException e) {
				throw new IllegalArgumentException("node had a wrong type", e);
			}
		}

		if (nodes.length == 4) {

			try {
				return createStatement((Resource) createValue(nodes[0], false), (URI) createValue(nodes[1], true), createValue(nodes[2], false),
						(Resource) createValue(nodes[3], false));
			} catch (ClassCastException e) {
				throw new IllegalArgumentException("node had a wrong type", e);
			}
		}

		throw new IllegalArgumentException("argument should have length 3 or 4");
	}

	@Override
	public Statement createStatement(Resource subject, URI predicate, Value object) {
		return new StatementImpl(subject, predicate, object);
	}

	@Override
	public Statement createStatement(Resource subject, URI predicate, Value object, Resource context) {
		return new ContextStatementImpl(subject, predicate, object, context);
	}

	@Override
	public URI createURI(String uri) {
		return new NativeCumulusURI(uri);
	}

	@Override
	public URI createURI(String namespace, String localName) {
		return createURI(namespace + localName);
	}

	public Value createValue(byte[] id, boolean p) throws IllegalArgumentException {

		if (_store.getDictionary().isBNode(id)) {
			return new NativeCumulusBNode(id, _store.getDictionary());
		} else if (_store.getDictionary().isLiteral(id)) {
			return new NativeCumulusLiteral(id, _store.getDictionary());
		} else if (_store.getDictionary().isResource(id)) {
			return new NativeCumulusURI(id, _store.getDictionary(), p);
		} else {
			throw new IllegalArgumentException("could not create sesame value from node ID " + Arrays.toString(id));
		}
	}
}