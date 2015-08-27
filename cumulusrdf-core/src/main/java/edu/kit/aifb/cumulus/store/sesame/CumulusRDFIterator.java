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
	
	private Iterator<byte[][]> _it;
	private CumulusRDFSail _sail;

	/**
	 * Builds a new iterator with the given data.
	 * 
	 * @param res the underlying resource iterator.
	 * @param sail the CumulusRDF Sail.
	 */
	public CumulusRDFIterator(final Iterator<byte[][]> res, final CumulusRDFSail sail) {
		_it = res;
		_sail = sail;
	}

	@Override
	protected Statement getNextElement() throws X {
		if (_it.hasNext()) {
			return _sail.getValueFactory().createStatement(_it.next());
		}
		return null;
	}
}