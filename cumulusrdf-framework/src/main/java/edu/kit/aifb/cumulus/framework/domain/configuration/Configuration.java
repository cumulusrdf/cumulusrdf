package edu.kit.aifb.cumulus.framework.domain.configuration;

import java.util.Set;


/**
 * CumulusRDF configuration / configurator interface.
 * Implements a Visitor pattern in order to have an implementor decoupled from the concrete configurable objects.
 * 
 * @param <E> the configuration content.
 * 
 * @see 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public interface Configuration<E> {

	/**
	 * Configures a given {@link Configurable}.
	 * 
	 * @param configurable the domain object we need to configure.
	 */
	void configure(Configurable<E> configurable);

	/**
	 * Returns the value associated with a given attribute.
	 * 
	 * @param <T> the attribute value kind.
	 * @param name the attribute name.
	 * @param defaultValue the default value that will be returned in case the requested attribute is not found.
	 * @return the value associated with a given attribute, or a default value in case the attribute is not found.
	 */
	<T> T getAttribute(String name, T defaultValue);
	
	/**
	 * Returns a set containing all identifiers of stores declared in this configuration.
	 * 
	 * @return a set containing all identifiers of stores declared in this configuration.
	 */
	Set<String> getDeclaredIdentifiers();

	/**
	 * Returns the identifier of the store owning this configuration.
	 * 
	 * @return the identifier of the store owning this configuration.
	 */
	String getOwningStoreIdentifier();
}