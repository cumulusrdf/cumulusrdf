package edu.kit.aifb.cumulus.store.sel;

import java.util.EventObject;

import org.openrdf.query.algebra.evaluation.impl.EvaluationStatistics;
import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.domain.selectivity.ISelectivityEstimator;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;
import edu.kit.aifb.cumulus.store.CounterFactory;
import edu.kit.aifb.cumulus.store.CounterFactory.Counter;
import edu.kit.aifb.cumulus.store.events.AddTripleEvent;
import edu.kit.aifb.cumulus.store.events.RemoveTriplesEvent;

/**
 * Supertype layer of selectivity estimators.
 * 
 * @author Andreas Wagner
 * @author 1.0
 */
public abstract class AbstractSelectivityEstimator extends EvaluationStatistics implements ISelectivityEstimator {

	protected Log _log = new Log(LoggerFactory.getLogger(getClass()));

	protected final Counter _triple_counter;

	/**
	 * Builds a new selectivity estimator with the given data.
	 * 
	 * @param counterFactory the counter factory.
	 */
	public AbstractSelectivityEstimator(final CounterFactory counterFactory) {
		_triple_counter = counterFactory.getCounter("TRIPLE_COUNTER");
	}

	@Override
	public long triplesCount() {
		try {
			return _triple_counter.current();
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
		}
		return -1;
	}
	
	@Override
	public void update(final EventObject event) {
		try {
			if (event instanceof AddTripleEvent) {
				_triple_counter.increment(((AddTripleEvent)event).numOfChanges());
			} else if (event instanceof RemoveTriplesEvent && _triple_counter.current() > 0) {
				_triple_counter.decrement(((RemoveTriplesEvent)event).numOfChanges());
			}
		} catch (final DataAccessLayerException exception) {
			_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
		} catch (final Exception exception) {
			_log.error(MessageCatalog._00026_NWS_SYSTEM_INTERNAL_FAILURE, exception);
		}
	}
}