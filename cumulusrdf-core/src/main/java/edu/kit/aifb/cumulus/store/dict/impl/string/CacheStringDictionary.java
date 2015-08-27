package edu.kit.aifb.cumulus.store.dict.impl.string;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.domain.dictionary.DictionaryRuntimeContext;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ICacheStrategy;
import edu.kit.aifb.cumulus.framework.domain.dictionary.IDictionary;
import edu.kit.aifb.cumulus.framework.mx.ManageableCacheDictionary;
import edu.kit.aifb.cumulus.log.MessageCatalog;


/**
 * A dictionary decorator that adds caching capability to another string dictionary. 
 * Note that although this class "is a" dictionary, due to its Decorator nature, 
 * is supposed to be used in conjunction with a concrete dictionary.
 * 
 * @see http://en.wikipedia.org/wiki/Decorator_pattern
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class CacheStringDictionary extends StringDictionaryBase implements ManageableCacheDictionary {
	/**
	 * First level strategy allows caching only for results that directly comes from the next decoratee in the chain.
	 * This is useful in case you have an articulated decorator chain and you want to activate several caches in the chain
	 * (with different configurations, for example).
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.1.0
	 */
	class FirstLevelCacheStrategy implements ICacheStrategy<String> {

		@Override
		public void cacheId(final ByteBuffer id, final String value) {
			final DictionaryRuntimeContext context = RUNTIME_CONTEXTS.get();
			if (context.isFirstLevelResult != null && context.isFirstLevelResult) {
				context.isFirstLevelResult = null;
				_id2node_cache.put(id, value);
			}
		}

		@Override
		public void cacheValue(final String value, final byte[] id) {
			final DictionaryRuntimeContext context = RUNTIME_CONTEXTS.get();
			if (context.isFirstLevelResult != null && context.isFirstLevelResult) {
				context.isFirstLevelResult = null;
				_node2id_cache.put(value, id);
			}
		}
	}

	/**
	 * Cumulative strategy basically caches everything, regardless the concrete decoratee that provided the value or the identifier.
	 * 
	 * @author Andrea Gazzarini
	 * @since 1.1.0
	 */
	class CumulativeCacheStrategy implements ICacheStrategy<String> {

		@Override
		public void cacheId(final ByteBuffer id, final String value) {
			_id2node_cache.put(id, value);
			RUNTIME_CONTEXTS.get().isFirstLevelResult = null;
		}

		@Override
		public void cacheValue(final String value, final byte[] id) {
			_node2id_cache.put(value, id);
			RUNTIME_CONTEXTS.get().isFirstLevelResult = null;
		}
	}

	static final int DEFAULT_CACHE_SIZE = 1000;
	
	final ConcurrentMap<ByteBuffer, String> _id2node_cache;
	final ConcurrentMap<String, byte[]> _node2id_cache;
	final ICacheStrategy<String> _cacheStrategy;

	private final IDictionary<String> _decoratee;

	private final int _idCacheMaxSize;
	private final int _valueCacheMaxSize;
	private final AtomicLong _idHitsCount = new AtomicLong();
	private final AtomicLong _valueHitsCount = new AtomicLong();
	private final AtomicLong _idEvictionsCount = new AtomicLong();
	private final AtomicLong _valueEvictionsCount = new AtomicLong();
	
	private final EvictionListener<ByteBuffer, String> _idEvictionListener = new EvictionListener<ByteBuffer, String>() {
		@Override
		public void onEviction(final ByteBuffer key, final String value) {
			_idEvictionsCount.incrementAndGet();
		}
	};

	private final EvictionListener<String, byte[]> _valueEvictionListener = new EvictionListener<String, byte[]>() {

		@Override
		public void onEviction(final String key, final byte[] value) {
			_valueEvictionsCount.incrementAndGet();
		}
	};

	/**
	 * Builds and initializes a cache capability on top of a given dictionary.
	 * 
	 * @param id the dictionary identifier.
	 * @param decoratee the decorated dictionary.
	 * @param idCacheSize the identifier cache size. In case <=0 It defaults to {@link #DEFAULT_CACHE_SIZE}
	 * @param valueCacheSize the value cache size. In case <=0 It defaults to {@link #DEFAULT_CACHE_SIZE}
	 * @param isFirstLevelCache a boolean that marks this cache as first level (or cumulative).
	 */
	public CacheStringDictionary(
			final String id, 
			final IDictionary<String> decoratee, 
			final int idCacheSize, 
			final int valueCacheSize, 			
			final boolean isFirstLevelCache) {
		super(id);

		if (decoratee == null) {
			throw new IllegalArgumentException(MessageCatalog._00091_NULL_DECORATEE_DICT);
		}

		_decoratee = decoratee;		
		_idCacheMaxSize = cacheSize(idCacheSize);
		_valueCacheMaxSize = cacheSize(valueCacheSize);
		_id2node_cache = new ConcurrentLinkedHashMap
				.Builder<ByteBuffer, String>()
				.maximumWeightedCapacity(_idCacheMaxSize)
				.listener(_idEvictionListener)
				.build();
		_node2id_cache = new ConcurrentLinkedHashMap
				.Builder<String, byte[]>()
				.maximumWeightedCapacity(_valueCacheMaxSize)
				.listener(_valueEvictionListener)
				.build();
		_cacheStrategy = isFirstLevelCache ? new FirstLevelCacheStrategy() : new CumulativeCacheStrategy();
	}

	@Override
	protected void initialiseInternal(final DataAccessLayerFactory factory) throws InitialisationException {
		_decoratee.initialise(factory);
	}

	@Override
	public void removeValue(final String value, final boolean p) throws DataAccessLayerException {
		_decoratee.removeValue(value, p);
	}

	@Override
	protected void closeInternal() {
		_id2node_cache.clear();
		_node2id_cache.clear();
		_decoratee.close();
	}

	@Override
	protected byte[] getIdInternal(final String value, final boolean p) throws DataAccessLayerException {
		byte[] id = _node2id_cache.get(value);

		if (id == null) {
			id = _decoratee.getID(value, p);
			_cacheStrategy.cacheValue(value, id);
		} else {
			_idHitsCount.incrementAndGet();
		}

		return id;
	}

	@Override
	protected String getValueInternal(final byte[] id, final boolean p) throws DataAccessLayerException {
		final ByteBuffer key = ByteBuffer.wrap(id);
		String value = _id2node_cache.get(key);

		if (value == null) {
			value = _decoratee.getValue(id, p);
			_cacheStrategy.cacheId(key, value);
		} else {
			_valueHitsCount.incrementAndGet();
		}
		return value;
	}
	
	/**
	 * Computes the cache size according with a given input.
	 * If input size is <=0 then default value for cache size will be used.
	 * 
	 * @param inputSize the input cache size.
	 * @return the cache size according with a given input.
	 */
	int cacheSize(final int inputSize) {
		return inputSize > 0 ? inputSize : DEFAULT_CACHE_SIZE;
	}

	@Override
	public int getIdCacheMaxSize() {
		return _idCacheMaxSize;
	}

	@Override
	public int getValueCacheMaxSize() {
		return _valueCacheMaxSize;
	}

	@Override
	public int getCachedIdentifiersCount() {
		return _id2node_cache.size();
	}

	@Override
	public int getCachedValuesCount() {
		return _node2id_cache.size();
	}

	@Override
	public boolean isCumulativeCache() {
		return _cacheStrategy instanceof CumulativeCacheStrategy;
	}

	@Override
	public long getIdHitsCount() {
		return _idHitsCount.get();
	}

	@Override
	public double getIdHitsRatio() {
		final double hitsCount = _idHitsCount.get();
		if (hitsCount != 0) {
			return (hitsCount / _idLookupsCount.get()) * 100;
		}
		return 0;
	}

	@Override
	public long getIdEvictionsCount() {
		return _idEvictionsCount.get();
	}

	@Override
	public long getValueHitsCount() {
		return _valueHitsCount.get();
	}

	@Override
	public double getValueHitsRatio() {
		final double hitsCount = _valueHitsCount.get();
		if (hitsCount != 0) {
			return (hitsCount / _valueLookupsCount.get()) * 100;
		}
		return 0;
	}

	@Override
	public long getValueEvictionsCount() {
		return _valueEvictionsCount.get();
	}
}