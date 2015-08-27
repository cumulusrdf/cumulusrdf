package edu.kit.aifb.cumulus.framework.datasource;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.framework.domain.configuration.Configurable;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * An AbstractFactory for CumulusRDF storage access layer.
 * Since 1.1.0 CumulusRDF supports the so called modulare storage, so here, with this AbstractFactory, 
 * we formally declare which members should be part of a data access layer family, in order to plug in 
 * CumulusRDF a new kind of storage.
 * 
 * N.B: this class maintains a registry of instantiated factories, in order to make sure they will behave as singletons.
 * 
 * @see http://en.wikipedia.org/wiki/Abstract_factory_pattern
 * @see https://code.google.com/p/cumulusrdf/issues/detail?id=57
 * @see https://code.google.com/p/cumulusrdf/issues/detail?id=66
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public abstract class DataAccessLayerFactory implements Configurable<Map<String, Object>> {
	private static final Log LOGGER = new Log(LoggerFactory.getLogger(DataAccessLayerFactory.class));
	
	private static final String FACTORY_NAME_SYSTEM_PROPERTY = "cdrf.factory.fqdn";
	private static final String DEFAULT_FACTORY = "edu.kit.aifb.cumulus.datasource.impl.CumulusDataAccessLayerFactory";

	private static final Map<String, DataAccessLayerFactory> TRIPLE_REGISTRY = new HashMap<String, DataAccessLayerFactory>();
	private static final Map<String, DataAccessLayerFactory> QUAD_REGISTRY = new HashMap<String, DataAccessLayerFactory>();
	
	protected final StorageLayout _layout;

	/**
	 * Returns the concrete factory associated with a given storage.
	 * An exception is thrown in case of a not yet supported storage.
	 * 
	 * @param factoryClassName the FQDN of the factory class.
	 * @param layout the storage layout.
	 * @return the concrete factory associated with a given storage.
	 */
	public static DataAccessLayerFactory getDataAccessLayerFactory(final String factoryClassName, final StorageLayout layout) {
		return getDataAccessLayerFactory(
				factoryClassName,
				layout, 
				layout == StorageLayout.TRIPLE ? TRIPLE_REGISTRY : QUAD_REGISTRY);
	}
	
	/**
	 * Returns the default concrete factory defined on this branch of CumulusRDF.
	 * 
	 * @param layout the storage layout.
	 * @return the default concrete factory defined on this branch of CumulusRDF.
	 */
	public static DataAccessLayerFactory getDefaultDataAccessLayerFactory(final StorageLayout layout) {
		final String clazzName = System.getProperty(FACTORY_NAME_SYSTEM_PROPERTY);
		return (clazzName == null) 
				? getDataAccessLayerFactory(DEFAULT_FACTORY, layout)
				: getDataAccessLayerFactory(clazzName, layout);		
	}
	
	/**
	 * Builds a new factory with a given layout.
	 * 
	 * @param layout the storage layout.
	 */
	protected DataAccessLayerFactory(final StorageLayout layout) {
		this._layout = layout;
	}

	/**
	 * Returns the {@link MapDAO}.
	 * 
	 * @param <K> the key class.
	 * @param <V> the value class.
	 * @param keyClass the key class used in the returned {@link MapDAO}.
	 * @param valueClass the key class used in the returned {@link MapDAO}.
	 * @param isBidirectional a flag indicating if we want a bidirectional {@link MapDAO} instance.
	 * @param mapName the name that will identify the map.
	 * @return a new {@link MapDAO} instance.
	 */
	public abstract <K, V> MapDAO<K, V> getMapDAO(
			Class<K> keyClass, 
			Class<V> valueClass,
			boolean isBidirectional, 
			String mapName);
	
	/**
	 * Returns a {@link CounterDAO} instance.
	 * 
	 * @param <K> the key class.
	 * @param keyClass the key kind used in counter.
	 * @param counterName the name that identify the underlying counter.
	 * @return a new {@link CounterDAO} instance.
	 */
	public abstract <K> CounterDAO<K> getCounterDAO(Class<K> keyClass, String counterName);
	
	/**
	 * Creates the data access object needed for interacting with the underlying RDF index.
	 * 
	 * @param dictionary the dictionary currently in use.
	 * @return the data access object needed for interacting with the underlying RDF index.
	 */
	public abstract TripleIndexDAO getTripleIndexDAO(ITopLevelDictionary dictionary);

	/**
	 * Creates the data access object needed for interacting with the underlying RDF index.
	 * 
	 * @param dictionary the dictionary currently in use.
	 * @return the data access object needed for interacting with the underlying RDF index.
	 */
	public abstract QuadIndexDAO getQuadIndexDAO(ITopLevelDictionary dictionary);

	/**
	 * Returns the concrete factory associated with a given storage.
	 * An exception is thrown in case of a not yet supported storage.
	 * 
	 * @param factoryClassName the FQDN of the factory class.
	 * @param layout the storage layout.
	 * @param registry the data access layer factories registry.
	 * @return the concrete factory associated with a given storage.
	 */
	static DataAccessLayerFactory getDataAccessLayerFactory(
			final String factoryClassName,
			final StorageLayout layout,
			final Map<String, DataAccessLayerFactory> registry) {
		try {
			DataAccessLayerFactory factory = registry.get(factoryClassName);
			if (factory == null) {
				factory = (DataAccessLayerFactory) Class.forName(factoryClassName).getConstructor(StorageLayout.class).newInstance(layout);
				registry.put(factoryClassName, factory);
			}
			return factory;
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00098_UNABLE_TO_INSTANTIATE_DAL_FACTORY, exception, factoryClassName);
			throw new IllegalArgumentException("Unable to instantiate " + factoryClassName, exception);
		}
	}

	/**
	 * Returns an informational string that describes the underlying storage of this factory.
	 * Note that, other than for informational purposes, this is used as member part 
	 * of the JMX object name so it is better to avoid long strings.
	 * 
	 * @return an informational string that describes the underlying storage of this factory.
	 */
	public abstract String getUnderlyingStorageInfo();
}