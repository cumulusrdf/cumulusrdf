package edu.kit.aifb.cumulus.framework.domain.selectivity;

import org.openrdf.query.algebra.TupleExpr;

import edu.kit.aifb.cumulus.framework.Initialisable;
import edu.kit.aifb.cumulus.framework.events.ITriplesChangesListener;

public interface ISelectivityEstimator extends ITriplesChangesListener, Initialisable {

	void close();

	double triplePatternCardinality(TupleExpr expr);

	double rangePatternCardinality(TupleExpr expr);
	
	double equiJoinCardinality(TupleExpr expr);

	long triplesCount();
}