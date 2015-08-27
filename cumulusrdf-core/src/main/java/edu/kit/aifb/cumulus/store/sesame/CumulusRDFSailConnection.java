package edu.kit.aifb.cumulus.store.sesame;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.CloseableIterationBase;

import java.util.Iterator;
import java.util.Map.Entry;

import org.openrdf.model.Literal;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.BindingAssigner;
import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.ConstantOptimizer;
import org.openrdf.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.openrdf.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryJoinOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelTreePrinter;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailConnectionBase;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.store.CumulusStoreException;
import edu.kit.aifb.cumulus.store.PersistentMap;
import edu.kit.aifb.cumulus.store.QuadStore;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.sel.AbstractSelectivityEstimator;

/**
 * A SailConnection that connects to a CumulusRDFSail.
 * 
 * @author Andreas Wagner
 * @since 1.0
 */
public class CumulusRDFSailConnection extends NotifyingSailConnectionBase {

	protected class CumulusRDFTripleSource implements TripleSource {
		@Override
		public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
				final Resource subj,
				final URI pred,
				final Value obj,
				final Resource... contexts) throws QueryEvaluationException {

			try {

				if (contexts != null && contexts.length > 0) {
					@SuppressWarnings("unchecked")
					CloseableIteration<Statement, QueryEvaluationException>[] iterations = new CloseableIteration[contexts.length];

					for (int i = 0; i < contexts.length; i++) {
						iterations[i] = _sail.createStatementIterator(subj, pred, obj, contexts[i]);
					}

					return new CloseableMultiIterator<Statement, QueryEvaluationException>(iterations);
				} else {
					return _sail.createStatementIterator(subj, pred, obj, contexts);
				}
			} catch (SailException e) {
				e.printStackTrace();
				throw new QueryEvaluationException(e);
			}
		}

		public CloseableIteration<? extends Statement, QueryEvaluationException> getRangeStatements(Resource subj, URI pred, Literal lowerBound,
				boolean lower_equals, Literal upperBound, boolean upper_equals, Literal equals, boolean reverse) throws QueryEvaluationException {
			try {
				return _sail.createRangeStatementIterator(subj, pred, lowerBound, lower_equals, upperBound, upper_equals, equals, reverse);
			} catch (SailException e) {
				e.printStackTrace();
				throw new QueryEvaluationException(e);
			}
		}

		@Override
		public ValueFactory getValueFactory() {
			return _factory;
		}
	}

	private CumulusRDFSail _sail;
	private Store _crdf;
	private CumulusRDFValueFactory _factory;
	private AbstractSelectivityEstimator _select_est;

	private final boolean _quad;
	private Log _log = new Log(LoggerFactory.getLogger(getClass()));

	/**
	 * Builds a new connection with a given Sail.
	 * 
	 * @param sail the CumulusRDF sail instance.
	 */
	public CumulusRDFSailConnection(final CumulusRDFSail sail) {
		super(sail);
		_sail = sail;
		_crdf = sail.getStore();
		_factory = sail.getValueFactory();
		_select_est = (AbstractSelectivityEstimator) _crdf.getSelectivityEstimator();

		_quad = _crdf instanceof QuadStore;
	}

	@Override
	protected void closeInternal() throws SailException {

	}

	@Override
	protected CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr, Dataset dataset,
			BindingSet bindings, boolean includeInferred) throws SailException {
		// Lock stLock = _sail.getStatementsReadLock();
		// Clone the tuple expression to allow for more aggressive optimizations
		tupleExpr = tupleExpr.clone();

		if (!(tupleExpr instanceof QueryRoot)) {
			// Add a dummy root node to the tuple expressions to allow the
			// optimizers to modify the actual root node
			tupleExpr = new QueryRoot(tupleExpr);
		}

		TripleSource tripleSource = new CumulusRDFTripleSource();
		EvaluationStrategy strategy = new RangeEvaluationStrategy(tripleSource, dataset);

		new BindingAssigner().optimize(tupleExpr, dataset, bindings);
		new ConstantOptimizer(strategy).optimize(tupleExpr, dataset, bindings);
		new CompareOptimizer().optimize(tupleExpr, dataset, bindings);
		new ConjunctiveConstraintSplitter().optimize(tupleExpr, dataset, bindings);
		new DisjunctiveConstraintOptimizer().optimize(tupleExpr, dataset, bindings);
		new SameTermFilterOptimizer().optimize(tupleExpr, dataset, bindings);
		new QueryModelNormalizer().optimize(tupleExpr, dataset, bindings);

		new CumulusQueryOptimizer(_crdf.isRangeIndexesSupportEnabled()).optimize(tupleExpr, dataset, bindings);
		new QueryJoinOptimizer(_select_est).optimize(tupleExpr, dataset, bindings); //		

		new FilterOptimizer().optimize(tupleExpr, dataset, bindings);
		new IterativeEvaluationOptimizer().optimize(tupleExpr, dataset, bindings);
		new OrderLimitOptimizer().optimize(tupleExpr, dataset, bindings);

		if (_log.isDebugEnabled()) {
			_log.debug(QueryModelTreePrinter.printTree(tupleExpr));
		}
		try {
			return strategy.evaluate(tupleExpr, EmptyBindingSet.getInstance());
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
			throw new SailException(e);
		}
	}

	@Override
	protected CloseableIteration<? extends Resource, SailException> getContextIDsInternal() throws SailException {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	protected CloseableIteration<? extends Statement, SailException> getStatementsInternal(
			final Resource subj,
			final URI pred,
			final Value obj,
			final boolean includeInferred,
			final Resource... contexts) throws SailException {

		if (_quad && (contexts == null || contexts.length == 0)) {
			throw new IllegalArgumentException("A quadstore always needs a context.");
		}

		if (contexts != null && contexts.length > 0) {
			@SuppressWarnings("unchecked")
			CloseableIteration<Statement, SailException>[] iterations = new CloseableIteration[contexts.length];

			for (int i = 0; i < contexts.length; i++) {
				iterations[i] = _sail.createStatementIterator(subj, pred, obj, contexts[i]);
			}

			return new CloseableMultiIterator<Statement, SailException>(iterations);
		} else {
			return _sail.createStatementIterator(subj, pred, obj, contexts);
		}
	}

	@Override
	protected long sizeInternal(final Resource... contexts) throws SailException {
		return _crdf.triplesCount();
	}

	@Override
	protected void startTransactionInternal() throws SailException {
	}

	@Override
	protected void commitInternal() throws SailException {
	}

	@Override
	protected void rollbackInternal() throws SailException {
	}

	@Override
	protected void addStatementInternal(
			final Resource subj,
			final URI pred,
			final Value obj,
			final Resource... contexts) throws SailException {

		try {
			if (_quad) {
				if (contexts == null || contexts.length == 0) {
					throw new IllegalArgumentException("A quadstore always needs a context.");
				}

				for (int i = 0; i < contexts.length; i++) {
					_crdf.addData(_factory.createStatement(subj, pred, obj, contexts[i]));
				}
			} else {
				_crdf.addData(_factory.createStatement(subj, pred, obj));
			}
		} catch (CumulusStoreException e) {
			e.printStackTrace();
			throw new SailException(e);
		}
	}

	@Override
	protected void removeStatementsInternal(
			final Resource subj,
			final URI pred,
			final Value obj,
			final Resource... contexts) throws SailException {

		try {
			if (_quad) {
				if (contexts == null || contexts.length == 0) {
					throw new IllegalArgumentException("A quadstore always needs a context.");
				}

				for (int i = 0; i < contexts.length; i++) {
					_crdf.removeData(_factory.createNodes(subj, pred, obj, contexts[i]));
				}
			} else {
				_crdf.removeData(_factory.createNodes(subj, pred, obj));
			}
		} catch (CumulusStoreException e) {
			e.printStackTrace();
			throw new SailException(e);
		}
	}

	@Override
	protected void clearInternal(final Resource... contexts) throws SailException {

		if (contexts == null || contexts.length == 0) {
			
			_crdf.clear();
			
		} else {
			
			for (Resource context : contexts) {
				try {
					((QuadStore) _crdf).removeData(new Value[] { null, null, null, context });
				} catch (CumulusStoreException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal() throws SailException {

		try {

			final Iterator<Entry<String, String>> prefix2ns = _crdf.getPrefix2Namespaces().entrySet().iterator();

			CloseableIteration<? extends Namespace, SailException> iter = new CloseableIterationBase<Namespace, SailException>() {

				private Entry<String, String> _current;

				@Override
				public boolean hasNext() throws SailException {
					return prefix2ns.hasNext();
				}

				@Override
				public Namespace next() throws SailException {
					_current = prefix2ns.next();
					return _current == null ? null : new NamespaceImpl(_current.getKey(), _current.getValue());
				}

				@Override
				public void remove() throws SailException {
					if (_current != null) {
						try {
							_crdf.getPrefix2Namespaces().remove(_current.getKey());
						} catch (DataAccessLayerException exception) {
							throw new SailException(exception);
						}
					}
				}
			};

			return iter;
		} catch (DataAccessLayerException exception) {
			throw new SailException(exception);
		}
	}

	@Override
	protected String getNamespaceInternal(final String prefix) throws SailException {

		if (prefix == null || prefix.isEmpty()) {
			return prefix;
		}

		try {
			return _crdf.getPrefix2Namespaces().get(prefix);
		} catch (DataAccessLayerException exception) {
			throw new SailException(exception);
		}
	}

	@Override
	protected void setNamespaceInternal(final String prefix, final String name) throws SailException {
		try {
			_crdf.getPrefix2Namespaces().put(prefix, name);
		} catch (DataAccessLayerException exception) {
			throw new SailException(exception);
		}
	}

	@Override
	protected void removeNamespaceInternal(final String prefix) throws SailException {
		try {
			_crdf.getPrefix2Namespaces().remove(prefix);
		} catch (DataAccessLayerException exception) {
			throw new SailException(exception);
		}
	}

	@Override
	protected void clearNamespacesInternal() throws SailException {
		try {
			PersistentMap<String, String> prefix2ns = _crdf.getPrefix2Namespaces();

			for (String prefix : prefix2ns.keySet()) {
				prefix2ns.remove(prefix);
			}
		} catch (DataAccessLayerException exception) {
			throw new SailException(exception);
		}
	}
}