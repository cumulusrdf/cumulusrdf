package edu.kit.aifb.cumulus.store.sesame;

import static edu.kit.aifb.cumulus.framework.Environment.DATETIME_RANGETYPES_AS_STRING;
import static edu.kit.aifb.cumulus.framework.Environment.NUMERIC_RANGETYPES_AS_STRING;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.OrderElem;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

public class CumulusQueryOptimizer implements QueryOptimizer {

	private final boolean _ranges_indexed;

	/**
	 * Builds a new query optimizer.
	 * 
	 * @param rangesIndexed a flag indicating if range indexes have been enabled.
	 */
	public CumulusQueryOptimizer(final boolean rangesIndexed) {
		_ranges_indexed = rangesIndexed;
	}

	@Override
	public void optimize(final TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings) {

		// use native support for range queries
		if (_ranges_indexed) {
			tupleExpr.visit(new RangeQueryVisitor(tupleExpr));
			tupleExpr.visit(new OrderByVisitor(tupleExpr));
		}

		// use native cumulus model
		tupleExpr.visit(new CumulusNativeModelVisitor());
	}

	protected static class CumulusNativeModelVisitor extends QueryModelVisitorBase<RuntimeException> {

		@Override
		public void meet(final StatementPattern node) {

			Value subject_value = node.getSubjectVar().getValue();
			Value predicate_value = node.getPredicateVar().getValue();
			Value object_value = node.getObjectVar().getValue();
			Value context_value = node.getContextVar() != null ? node.getContextVar().getValue() : null;

			if (subject_value != null) {
				node.getSubjectVar().setValue(CumulusRDFValueFactory.makeNativeValue(subject_value));
			}

			if (predicate_value != null) {
				node.getPredicateVar().setValue(CumulusRDFValueFactory.makeNativeValue(predicate_value));
			}

			if (object_value != null) {
				node.getObjectVar().setValue(CumulusRDFValueFactory.makeNativeValue(object_value));
			}

			if (context_value != null) {
				node.getObjectVar().setValue(CumulusRDFValueFactory.makeNativeValue(context_value));
			}
		}
	}

	protected static class OrderByVisitor extends QueryModelVisitorBase<RuntimeException> {
		protected final TupleExpr _tupleExpr;

		public OrderByVisitor(final TupleExpr tupleExpr) {
			_tupleExpr = tupleExpr;
		}

		@Override
		public void meet(final Order order) {
			super.meet(order);
			_tupleExpr.visit(new OrderByModifier(_tupleExpr, order));
		}

		protected class OrderByModifier extends QueryModelVisitorBase<RuntimeException> {
			protected final TupleExpr _tupleExpr;
			protected Order _order;
			protected Map<Var, Boolean> _variables;

			public OrderByModifier(final TupleExpr tupleExpr, final Order order) {

				_tupleExpr = tupleExpr;
				_order = order;
				_variables = new HashMap<Var, Boolean>();

				List<OrderElem> elems = order.getElements();
				Iterator<OrderElem> iter = elems.iterator();

				while (iter.hasNext()) {

					OrderElem ele = iter.next();
					boolean ascending = ele.isAscending();
					ValueExpr ex = ele.getExpr();

					if (ex instanceof Var) {
						_variables.put((Var) ex, new Boolean(ascending));
					}
				}
			}

			@Override
			public void meet(final StatementPattern sp) {
				if (sp instanceof RangeStatementPattern) {
					if (_variables.containsKey(sp.getObjectVar())) {
						((RangeStatementPattern) sp).setAscending(_variables.get(sp.getObjectVar()));
					}
				}
			}
		}
	}

	protected static class RangeQueryVisitor extends QueryModelVisitorBase<RuntimeException> {

		protected final TupleExpr _tupleExpr;

		/**
		 * Builds a new range query visitor.
		 * 
		 * @param tupleExpr the current tuple expression.
		 */
		public RangeQueryVisitor(final TupleExpr tupleExpr) {
			_tupleExpr = tupleExpr;
		}

		@Override
		public void meet(final Filter filter) {

			super.meet(filter);
			ValueExpr condition = filter.getCondition();

			if (condition instanceof Compare) {

				Literal bound = null;
				Var var = null;
				CompareOp comp = null;

				boolean replaceFilter = true;
				comp = ((Compare) condition).getOperator();
				ValueExpr left = ((Compare) condition).getLeftArg();
				ValueExpr right = ((Compare) condition).getRightArg();

				if (comp.equals(CompareOp.NE)) {
					replaceFilter = false;
				}

				if (left instanceof Var) {
					var = (Var) left;
				} else {
					replaceFilter = false;
				}

				if (right instanceof ValueConstant && replaceFilter) {
					Value v = ((ValueConstant) right).getValue();
					if (v instanceof Literal && ((Literal) v).getDatatype() != null) {
						bound = (Literal) v;
						String dts = ((Literal) v).getDatatype().stringValue();
						replaceFilter = NUMERIC_RANGETYPES_AS_STRING.contains(dts)
								|| DATETIME_RANGETYPES_AS_STRING.contains(dts)
								|| (comp.equals(CompareOp.EQ) && dts.equals(XMLSchema.STRING));
					} else if (v instanceof Literal && ((Literal) v).getDatatype() == null) {
						replaceFilter = comp.equals(CompareOp.EQ);
					} else {
						replaceFilter = false;
					}
				} else {
					replaceFilter = false;
				}

				if (replaceFilter) {
					_tupleExpr.visit(new RangeStatementModifier(_tupleExpr, comp, var, bound));
					filter.replaceWith(filter.getArg());
				}
			}
		}
	}

	protected static class RangeStatementModifier extends QueryModelVisitorBase<RuntimeException> {
		protected final TupleExpr _tupleExpr;
		CompareOp _comp;
		Var _var;
		Literal _bound;

		public RangeStatementModifier(final TupleExpr tupleExpr, final CompareOp comp, final Var var, final Literal bound) {
			_tupleExpr = tupleExpr;
			_comp = comp;
			_var = var;
			_bound = bound;
		}

		@Override
		public void meet(final StatementPattern statement) {

			if (statement instanceof RangeStatementPattern) {

				if (statement.getObjectVar().equals(_var)) {

					boolean equals_lower = ((RangeStatementPattern) statement).getLowerBoundEquals();
					boolean equals_upper = ((RangeStatementPattern) statement).getUpperBoundEquals();

					Literal lower = ((RangeStatementPattern) statement).getLowerBound();
					Literal upper = ((RangeStatementPattern) statement).getUpperBound();
					Literal equal = ((RangeStatementPattern) statement).getEquals();

					if (_comp.equals(CompareOp.GE) || _comp.equals(CompareOp.GT)) {
						if (lower == null) {
							lower = _bound;
							equals_lower = _comp.equals(CompareOp.GE);
						} else {
							double currentLower = Double.parseDouble(lower.getLabel());
							double newLower = Double.parseDouble(_bound.getLabel());
							if (newLower > currentLower) {
								lower = _bound;
								equals_lower = _comp.equals(CompareOp.GE);
							}
						}
					} else if (_comp.equals(CompareOp.LE) || _comp.equals(CompareOp.LT)) {
						if (upper == null) {
							upper = _bound;
							equals_upper = _comp.equals(CompareOp.LE);
						} else {
							double currentUpper = Double.parseDouble(upper.getLabel());
							double newUpper = Double.parseDouble(_bound.getLabel());
							if (newUpper < currentUpper) {
								upper = _bound;
								equals_upper = _comp.equals(CompareOp.LE);
							}
						}
					} else if (_comp.equals(CompareOp.EQ)) {
						equal = _bound;
					}

					((RangeStatementPattern) statement).setLowerBound(lower);
					((RangeStatementPattern) statement).setUpperBound(upper);

					((RangeStatementPattern) statement).setUpperBoundEquals(equals_upper);
					((RangeStatementPattern) statement).setLowerBoundEquals(equals_lower);

					((RangeStatementPattern) statement).setEquals(equal);
				}
			} else {

				if (statement.getObjectVar().equals(_var)) {

					boolean equals_lower = false, equals_upper = false;
					boolean replace = false;

					Literal upper = null, lower = null, equals = null;

					if (_comp.equals(CompareOp.GE) || _comp.equals(CompareOp.GT)) {
						lower = _bound;
						equals_lower = _comp.equals(CompareOp.GE);
						replace = true;
					} else if (_comp.equals(CompareOp.EQ)) {
						equals = _bound;
						replace = true;
					} else if (_comp.equals(CompareOp.LE) || _comp.equals(CompareOp.LT)) {
						upper = _bound;
						equals_upper = _comp.equals(CompareOp.LE);
						replace = true;
					}

					if (replace) {
						RangeStatementPattern newP = new RangeStatementPattern(statement.getSubjectVar(), statement.getPredicateVar(),
								statement.getObjectVar(), lower, equals_lower, upper, equals_upper, equals);
						statement.replaceWith(newP);
					}
				}
			}
		}
	}
}