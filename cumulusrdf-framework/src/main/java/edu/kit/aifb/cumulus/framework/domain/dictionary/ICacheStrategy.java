package edu.kit.aifb.cumulus.framework.domain.dictionary;

import java.nio.ByteBuffer;

/**
 * Interface for defining cache strategies behaviour.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 * @param <V> the value kind managed by this strategy.
 */
public interface ICacheStrategy<V> {

	/**
	 * Caches a given identifier and the corresponding value.
	 * 
	 * @param id a value identifier.
	 * @param value the value.
	 */
	void cacheId(ByteBuffer id, V value);

	/**
	 * Caches a given value and its corresponding identifier.
	 * 
	 * @param value the value.
	 * @param id the value identifier.
	 */
	void cacheValue(V value, byte[] id);
}