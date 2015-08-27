package edu.kit.aifb.cumulus.store.sesame;

import org.openrdf.model.Literal;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

public class RangeStatementPattern extends StatementPattern {

	private Literal _lowerBound, _upperBound, _equals;
	private boolean _ascending = true, _equal_lower, _equal_upper;

	/**
	 * Creates a statement pattern that matches a subject-, predicate- and
	 * object variable against statements from all contexts.
	 */
	public RangeStatementPattern(Var subject, Var predicate, Var object, Literal lowerBound, boolean equal_lower, Literal upperBound,
			boolean equal_upper, Literal equals) {

		super(Scope.DEFAULT_CONTEXTS, subject, predicate, object);

		_lowerBound = lowerBound;
		_upperBound = upperBound;

		_equals = equals;

		_equal_lower = equal_lower;
		_equal_upper = equal_upper;
	}

	public Literal getEquals() {
		return _equals;
	}

	public Literal getLowerBound() {
		return _lowerBound;
	}

	public boolean getLowerBoundEquals() {
		return _equal_lower;
	}

	@Override
	public String getSignature() {
		StringBuilder sb = new StringBuilder(128);
		sb.append(super.getSignature());
		sb.append(" lower: " + _lowerBound);
		sb.append(" lower equals: " + _equal_lower);
		sb.append(" upper: " + _upperBound);
		sb.append(" upper equals: " + _equal_upper);
		sb.append(" lower: " + _lowerBound);
		sb.append(" equals: " + _equals);
		return sb.toString();
	}

	public Literal getUpperBound() {
		return _upperBound;
	}

	public boolean getUpperBoundEquals() {
		return _equal_upper;
	}

	public boolean isAscending() {
		return _ascending;
	}

	public void setAscending(boolean ascending) {
		_ascending = ascending;
	}

	public void setEquals(Literal equals) {
		_equals = equals;
	}

	public void setLowerBound(Literal lower) {
		_lowerBound = lower;
	}

	public void setLowerBoundEquals(boolean equal_lower) {
		_equal_lower = equal_lower;
	}

	public void setUpperBound(Literal upper) {
		_upperBound = upper;
	}

	public void setUpperBoundEquals(boolean equal_upper) {
		_equal_upper = equal_upper;
	}
}