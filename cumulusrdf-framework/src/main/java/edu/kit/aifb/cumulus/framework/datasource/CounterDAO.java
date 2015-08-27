package edu.kit.aifb.cumulus.framework.datasource;


/**
 * CumulusRDF CounterDAO interface.
 *
 * @param <K> the key class.
 * @author Andreas Wagner
 * @author Sebastian Schmidt
 * @since 1.0
 */
public interface CounterDAO<K> extends MapDAO<K, Long> {

	/**
	 * Decrements a counter.
	 * 
	 * @param key the key of the counter.
	 * @param delta the decrement.
	 */
	void decrement(K key, Long delta);

	/**
	 * Increments a counter.
	 * 
	 * @param key the key of the counter.
	 * @param delta the increment.
	 */
	void increment(K key, Long delta);
}