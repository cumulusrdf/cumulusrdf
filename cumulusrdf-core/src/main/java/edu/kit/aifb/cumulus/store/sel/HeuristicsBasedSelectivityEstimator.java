package edu.kit.aifb.cumulus.store.sel;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.store.CounterFactory;
import edu.kit.aifb.cumulus.store.sesame.RangeStatementPattern;

/**
 * Selectivity estimation based on simple heuristics.
 * 
 * @author Andreas Wagner
 * @since 1.0
 */
public class HeuristicsBasedSelectivityEstimator extends AbstractSelectivityEstimator {

	private static final Double CARD_1000M = 1000d * 1000d * 1000d;

	public HeuristicsBasedSelectivityEstimator(final CounterFactory factory) {
		super(factory);
	}

	@Override
	public synchronized void close() {
		// Nothing to be done here...
	}

	@Override
	public double equiJoinCardinality(TupleExpr expr) {
		// TODO: create meaningful estimates for simple equi-joins
		return 0;
	}

	@Override
	public synchronized double getCardinality(TupleExpr expr) {

		if (expr instanceof RangeStatementPattern) {
			return rangePatternCardinality(expr);
		}
		if (expr instanceof StatementPattern) {
			return triplePatternCardinality(expr);
		}

		return super.getCardinality(expr);
	}

	@Override
	// TODO: create meaningful estimates/statistics for range queries ...
	public double rangePatternCardinality(TupleExpr expr) {

		RangeStatementPattern pattern = (RangeStatementPattern) expr;
		Value subject = pattern.getSubjectVar().getValue();

		// subject is bound
		if (subject != null) {
			return 1d;
		}
		// range contained '=' comparison, i.e., no range but exact value
		else if (pattern.getEquals() != null) {
			return 1d;
		}
		// only one bound is set, i.e., either [a, infty[ or ]-infty, a]
		else if (pattern.getLowerBound() == null || pattern.getUpperBound() == null) {
			return CARD_1000M;
		}
		// both bounds are set, i.e., range is something like [a,b]
		else {
			return CARD_1000M;
		}
	}

	@Override
	public double triplePatternCardinality(TupleExpr expr) {

		StatementPattern pattern = (StatementPattern) expr;
		Value subject = pattern.getSubjectVar().getValue(), predicate = pattern.getPredicateVar().getValue(), object = pattern.getObjectVar()
				.getValue();

		/*
		 * some RDF/OWL schema heuristics
		 */
		// pattern: ?x type ?class/class1
		if (subject == null && predicate != null && predicate.equals(org.openrdf.model.vocabulary.RDF.TYPE)) {
			return CARD_1000M;
		}
		// pattern: x1 type class1/?class
		else if (subject != null && predicate != null && predicate.equals(org.openrdf.model.vocabulary.RDF.TYPE)) {
			return 1d;
		}
		// pattern: ?x/x1 subclassof class1/?class
		else if (predicate != null && predicate.equals(RDFS.SUBCLASSOF)) {
			return 1d;
		}
		// pattern: ?x/x1 subpropertyof class1/?class
		else if (predicate != null && predicate.equals(RDFS.SUBPROPERTYOF)) {
			return 1d;
		}
		// pattern: ?x/x1 domain class1/?class
		else if (predicate != null && predicate.equals(RDFS.DOMAIN)) {
			return 1d;
		}
		// pattern: ?x/x1 range class1/?class
		else if (predicate != null && predicate.equals(RDFS.RANGE)) {
			return 1d;
		}

		/*
		 * known expensive patterns
		 */

		// pattern: ?x seeAlso ?y
		else if (subject == null && predicate != null && predicate.equals(RDFS.SEEALSO) && object == null) {
			return CARD_1000M;
		}
		// pattern: ?x sameAs ?y
		else if (subject == null && predicate != null && predicate.equals(OWL.SAMEAS) && object == null) {
			return CARD_1000M;
		}
		// pattern: ?x label ?y
		else if (subject == null && predicate != null && predicate.equals(RDFS.LABEL) && object == null) {
			return CARD_1000M;
		}
		// pattern: ?x comment ?y
		else if (subject == null && predicate != null && predicate.equals(RDFS.COMMENT) && object == null) {
			return CARD_1000M;
		}
		// pattern: ?x isDefindedBy ?y
		else if (subject == null && predicate != null && predicate.equals(RDFS.ISDEFINEDBY) && object == null) {
			return CARD_1000M;
		}

		/*
		 * known cheap patterns
		 */
		// pattern: ?x/x1 label ?y/y1
		else if ((subject != null || object != null) && predicate != null && predicate.equals(RDFS.LABEL)) {
			return 1;
		}
		// pattern: ?x/x1 seeAlso ?y/y1
		else if ((subject != null || object != null) && predicate != null && predicate.equals(RDFS.SEEALSO)) {
			return 1;
		}
		// pattern: ?x/x1 sameAs ?y/y1
		else if ((subject != null || object != null) && predicate != null && predicate.equals(OWL.SAMEAS)) {
			return 1;
		}
		// pattern: ?x/x1 comment ?y/y1
		else if ((subject != null || object != null) && predicate != null && predicate.equals(RDFS.COMMENT)) {
			return 1;
		}
		// pattern: ?x/x1 isDefindedBy ?y/y1
		else if ((subject != null || object != null) && predicate != null && predicate.equals(RDFS.ISDEFINEDBY)) {
			return 1;
		}

		/*
		 * other heuristics
		 */
		// pattern: x1 ?p/p1 lit1
		else if (subject != null && object != null && object instanceof Literal) {
			return 1d;
		}
		// pattern: x1 ?p/p1 y1
		else if (subject != null && object != null && (object instanceof URI || object instanceof BNode)) {
			return 1d;
		}
		// pattern: x1 p1 ?o
		else if (subject != null && predicate != null && object == null) {
			return 20d;
		}
		// pattern: x1 ?p ?o
		else if (subject != null && predicate == null && object == null) {
			return 50d;
		}
		// pattern: ?x p1 y1
		else if (subject == null && predicate != null && object != null && (object instanceof URI || object instanceof BNode)) {
			return 20d;
		}
		// pattern: ?x ?p y1
		else if (subject == null && predicate == null && object != null && (object instanceof URI || object instanceof BNode)) {
			return 40d;
		}
		// pattern: ?x p1 lit1
		else if (subject == null && predicate != null && object != null && object instanceof Literal) {
			return 50d;
		}
		// pattern: ?x ?p lit1
		else if (subject == null && object != null && object instanceof Literal) {
			return 100d;
		}
		// pattern: ?x p1 ?o
		else if (subject == null && predicate != null && object == null) {
			return CARD_1000M;
		}
		// pattern: ?x ?p ?o
		else if (subject == null && predicate == null && object == null) {
			return CARD_1000M;
		} else {
			// this should not happen
			return CARD_1000M;
		}
	}

	@Override
	public void initialise(DataAccessLayerFactory factory) throws InitialisationException {
		// Nothing to be done here...
	}
}