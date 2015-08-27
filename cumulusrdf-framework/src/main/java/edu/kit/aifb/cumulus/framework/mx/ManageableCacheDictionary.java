package edu.kit.aifb.cumulus.framework.mx;

import javax.management.MXBean;

/**
 * Management interface of a Cache (String or Value) dictionary.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
@MXBean
public interface ManageableCacheDictionary extends ManageableDictionary {
	/**
	 * Returns the current id cache (max) size.
	 * 
	 * @return the current id cache (max) size.
	 */
	int getIdCacheMaxSize();
	
	/**
	 * Returns the current value cache (max) size.
	 * 
	 * @return the current value cache (max) size.
	 */
	int getValueCacheMaxSize();	
	
	/**
	 * Returns the total count of currently cached identifiers.
	 * 
	 * @return the total count of currently cached identifiers.
	 */
	int getCachedIdentifiersCount();
	
	/**
	 * Returns the total count of currently cached values.
	 * From this perspective a value could be a string (in case of string dictionary) or
	 * a value (in case of ValueDictionary).
	 * 
	 * @return the total count of currently cached values.
	 */	
	int getCachedValuesCount();
	
	/**
	 * Returns true if this cache is working as a cumulative cache, false in case of first level cache.
	 * 
	 * @return true if this cache is working as a cumulative cache, false in case of first level cache.
	 */
	boolean isCumulativeCache();

	/**
	 * The total number of identifier hits (i.e. lookups with a positive match) occurred since this cache has been created.
	 * 
	 * @return the total number of identifier hits occurred since this cache has been created.
	 */
	long getIdHitsCount();
	
	/**
	 * ID hits ratio (percentage of hits over total lookups).
	 * 
	 * @return the hits ratio.
	 */
	double getIdHitsRatio();
	
	/**
	 * The total number of id evictions.
	 * 
	 * @return the total number of id evictions.
	 */
	long getIdEvictionsCount();
	
	/**
	 * The total number of value hits (i.e. lookups with a positive match) occurred since this cache has been created.
	 * 
	 * @return the total number of value hits occurred since this cache has been created.
	 */
	long getValueHitsCount();
	
	/**
	 * Value hits ratio (percentage of hits over total lookups).
	 * 
	 * @return the hits ratio.
	 */
	double getValueHitsRatio();
	
	/**
	 * The total number of value evictions.
	 * 
	 * @return the total number of value evictions.
	 */
	long getValueEvictionsCount();	
}