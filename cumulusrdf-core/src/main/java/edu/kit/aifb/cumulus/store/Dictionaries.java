package edu.kit.aifb.cumulus.store;

import java.util.Map;

import edu.kit.aifb.cumulus.framework.domain.configuration.Configuration;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.store.dict.impl.string.CacheStringDictionary;
import edu.kit.aifb.cumulus.store.dict.impl.string.PersistentStringDictionary;
import edu.kit.aifb.cumulus.store.dict.impl.string.TransientStringDictionary;
import edu.kit.aifb.cumulus.store.dict.impl.value.CacheValueDictionary;
import edu.kit.aifb.cumulus.store.dict.impl.value.KnownURIsDictionary;
import edu.kit.aifb.cumulus.store.dict.impl.value.PersistentValueDictionary;
import edu.kit.aifb.cumulus.store.dict.impl.value.ThreeTieredValueDictionary;
import edu.kit.aifb.cumulus.store.dict.impl.value.TransientValueDictionary;

/**
 * Predefined dictionaries catalog.
 * Note that this is not an enum of available dictionaries. Instead is more properly a factory so
 * each time a dictionary is requested, a new instance will be created. 
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public abstract class Dictionaries {

	/**
	 * Creates a new CumulusRDF default dictionary.
	 * 
	 * @param configuration the configuration of the owning store.
	 * @return a new instance of default dictionary.
	 */
	public static ITopLevelDictionary newDefaultDictionary(final Configuration<Map<String, Object>> configuration) {
		return new CacheValueDictionary(
				"TopLevelCacheDictionary",
				new KnownURIsDictionary(
						"KnownURIsDictionary",
						new ThreeTieredValueDictionary(
								"ThreeTieredDictionary",
								new CacheStringDictionary(
										"NamespacesCacheDictionary",
										new PersistentStringDictionary("NamespacesDictionary", "DICT_NAMESPACES"),
										configuration.getAttribute("namespaces-id-cache-size", Integer.valueOf(1000)),
										configuration.getAttribute("namespaces-value-cache-size", Integer.valueOf(1000)),
										false),
								new TransientStringDictionary("LocalNamesDictionary"),
								new CacheValueDictionary(
										"LiteralsAndBNodesCacheDictionary",
										new TransientValueDictionary(
												"LiteralAndBNodesDictionary",
												new PersistentValueDictionary("LongLiteralsDictionary"),
												configuration.getAttribute("long-literals-threshold", Integer.valueOf(1000))),
										configuration.getAttribute("literals-bnodes-id-cache-size", Integer.valueOf(50000)),
										configuration.getAttribute("literals-bnodes-value-cache-size", Integer.valueOf(50000)),										
										true))),
				configuration.getAttribute("known-uris-id-cache-size", Integer.valueOf(2000)),
				configuration.getAttribute("known-uris-value-cache-size", Integer.valueOf(2000)),
				true);
	}

	/**
	 * Creates a new instance of dictionary that was in use in 1.0.x version.
	 * 
	 * @return a new instance of dictionary that was in use in 1.0.x version.
	 */
	public static ITopLevelDictionary new10xDictionary() {
		return new CacheValueDictionary(
				"TopLevelCacheDictiobary",
				new PersistentValueDictionary("PersistentDictionary"),
				50000,
				50000,
				true);
	}
}
