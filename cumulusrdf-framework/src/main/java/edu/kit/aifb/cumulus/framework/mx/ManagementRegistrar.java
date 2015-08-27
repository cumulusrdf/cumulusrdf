package edu.kit.aifb.cumulus.framework.mx;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.LoggerFactory;

import edu.kit.aifb.cumulus.log.Log;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * Utility class for registering / unregistering MX beans.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public abstract class ManagementRegistrar {
	
	static final MBeanServer MX_SERVER = ManagementFactory.getPlatformMBeanServer();
	static final Log LOGGER = new Log(LoggerFactory.getLogger(ManagementRegistrar.class));
	static String DOMAIN = "CumulusRDF:";
	
	/**
	 * Registers a store management interface.
	 * 
	 * @param store the CumulusRDF store.
	 * @param layout the store layout.
	 * @param storage the storage (brief mnemonic description) of the underlying storage.
	 * @throws JMException in case of registration failure.
	 * @throws InstanceAlreadyExistsException in case the store has been already registered.
	 */
	public static void registerStore(
			final ManageableStore store, 
			final String layout, 
			final String storage) throws InstanceAlreadyExistsException, JMException {
		register(store, createStoreObjectName(layout, store.getId(), storage));
	}

	/**
	 * Registers a value dictionary management interface.
	 * 
	 * @param dictionary the dictionary.
	 * @throws JMException in case of registration failure.
	 * @throws InstanceAlreadyExistsException in case the dictionary has been already registered.
	 */
	public static void registerDictionary(final ManageableDictionary dictionary) throws InstanceAlreadyExistsException, JMException {
		register(dictionary, createDictionaryObjectName(dictionary.getId()));
	}

	/**
	 * General purposes registration method.
	 * Note that we usually prefer specific registration methods.
	 * 
	 * @param manageable the manageable instance to be registered.
	 * @param name the name of the management bean.
	 * @throws JMException in case of registration failure.
	 */
	public static void register(final Manageable manageable, final ObjectName name) throws InstanceAlreadyExistsException, JMException {
		if (MX_SERVER.isRegistered(name))
		{
			throw new InstanceAlreadyExistsException();
		}
		
		MX_SERVER.registerMBean(manageable, name);
		LOGGER.info(MessageCatalog._00110_MBEAN_REGISTERED, manageable.getId());
	}
	
	public static void unregisterStore(
			final ManageableStore store, 
			final String layout, 
			final String storage) {
		unregister(createStoreObjectName(layout, store.getId(), storage));
	}
	
	/**
	 * Unregisters a dictionary management interface.
	 * 
	 * @param dictionary the dictionary.
	 */
	public static void unregisterDictionary(final ManageableDictionary dictionary) {
		unregister(createDictionaryObjectName(dictionary.getId()));
	}
	/**
	 * General purposes unregistration method.
	 * Note that we usually prefer specific registration methods.
	 * 
	 * @param name the name of the management bean.
	 */
	public static void unregister(final ObjectName name) {
		try {
			if (MX_SERVER.isRegistered(name))
			{
				MX_SERVER.unregisterMBean(name);
			}

			LOGGER.info(MessageCatalog._00112_MBEAN_UNREGISTERED, name);
		} catch (final Exception exception) {
			LOGGER.error(MessageCatalog._00113_UNABLE_TO_UNREGISTER_MBEAN, name, exception);
		}
	}
	
	/**
	 * ObjectNames (i.e. management names) factory for dictionaries.
	 * 
	 * @param id the dictionary identifier.
	 * @return the {@link ObjectName} associated with the given identifier. 
	 */
	static ObjectName createDictionaryObjectName(final String id) {
		try {
			return new ObjectName(DOMAIN + "Type=Dictionary,ID=" + id);
		} catch (final Exception exception) {
			throw new RuntimeException(exception);
		}
	}
	
	/**
	 * ObjectNames (i.e. management names) factory for stores.
	 * 
	 * @param layout the store layout.
	 * @param id the store identifier.
	 * @param storage the storage (brief mnemonic description) of the underlying storage.
	 * @return the {@link ObjectName} associated with the given identifier. 
	 */
	static ObjectName createStoreObjectName(final String layout, final String id, final String storage) {
		try {
			return new ObjectName(DOMAIN + "Type=" + layout + ",Storage=" + storage + ",ID=" + id);
		} catch (final Exception exception) {
			throw new RuntimeException(exception);
		}
	}	
}