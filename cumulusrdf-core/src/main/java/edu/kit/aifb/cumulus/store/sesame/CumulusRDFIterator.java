package edu.kit.aifb.cumulus.store.sesame;

import info.aduna.iteration.LookAheadIteration;

import java.util.Iterator;

import org.openrdf.model.Statement;

/**
 * CumulusRDF implementation of {@link LookAheadIteration}.
 * 
 * @since 1.0
 * @param <X> the exception kind that will be eventually raised during the iteration.
 */
public class CumulusRDFIterator<X extends Exception> extends LookAheadIteration<Statement, X> {
	private final Iterator<byte[][]> _iterator;
	private final CumulusRDFValueFactory _valueFactory;

	/**
	 * Builds a new iterator with the given data.
	 * 
	 * @param res the underlying resource iterator.
	 * @param sail the CumulusRDF Sail.
	 */
	public CumulusRDFIterator(final Iterator<byte[][]> res, final CumulusRDFSail sail) {
		_iterator = res;
		_valueFactory = sail.getValueFactory();
	}

	@Override
	protected Statement getNextElement() throws X {
		return (_iterator.hasNext()) 
			? _valueFactory.createStatement(_iterator.next())
			: null;
	}
}