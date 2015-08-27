package edu.kit.aifb.cumulus.store.sesame;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.ConvertingIteration;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;

public class RangeEvaluationStrategy extends EvaluationStrategyImpl {

	public RangeEvaluationStrategy(TripleSource tripleSource, Dataset dataset) {
		super(tripleSource, dataset);
	}

	public RangeEvaluationStrategy(TripleSource tripleSource) {
		super(tripleSource);
	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(RangeStatementPattern sp, final BindingSet bindings)
			throws QueryEvaluationException {

		CloseableIteration<? extends Statement, QueryEvaluationException> stIter = null;

		if (tripleSource instanceof CumulusRDFSailConnection.CumulusRDFTripleSource) {

			final Var subjVar = sp.getSubjectVar();
			final Var predVar = sp.getPredicateVar();
			final Var objVar = sp.getObjectVar();
			final Var conVar = sp.getContextVar();

			final boolean upper_equals = sp.getUpperBoundEquals(), lower_equals = sp.getLowerBoundEquals();

			final Value subjValue = getVarValue(subjVar, bindings);
			final Value predValue = getVarValue(predVar, bindings);

			final boolean reverse = !sp.isAscending();

			stIter = ((CumulusRDFSailConnection.CumulusRDFTripleSource) tripleSource).getRangeStatements((Resource) subjValue, (URI) predValue,
					sp.getLowerBound(), lower_equals, sp.getUpperBound(), upper_equals, sp.getEquals(), reverse);

			return new ConvertingIteration<Statement, BindingSet, QueryEvaluationException>(stIter) {

				@Override
				protected BindingSet convert(Statement st) {
					QueryBindingSet result = new QueryBindingSet(bindings);

					if (subjVar != null && !result.hasBinding(subjVar.getName())) {
						result.addBinding(subjVar.getName(), st.getSubject());
					}
					if (predVar != null && !result.hasBinding(predVar.getName())) {
						result.addBinding(predVar.getName(), st.getPredicate());
					}
					if (objVar != null && !result.hasBinding(objVar.getName())) {
						result.addBinding(objVar.getName(), st.getObject());
					}
					if (conVar != null && !result.hasBinding(conVar.getName()) && st.getContext() != null) {
						result.addBinding(conVar.getName(), st.getContext());
					}

					return result;
				}
			};

		} else {
			throw new UnsupportedOperationException("RangeEvaluationStrategy can only be used with CumulusRdfStore!");
		}

	}

	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(StatementPattern sp, final BindingSet bindings)
			throws QueryEvaluationException {
		if (sp instanceof RangeStatementPattern) {
			return evaluate((RangeStatementPattern) sp, bindings);
		} else {
			return super.evaluate(sp, bindings);
		}
	}
}
