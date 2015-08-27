package edu.kit.aifb.cumulus.framework.domain.configuration;

/**
 * CumulusRDF framework interface for configurable objects.
 * 
 * @param <E> the configuration content.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public interface Configurable<E> {
	/**
	 * Informs this configurable about an available configuration.
	 * 
	 * @param configuration the configuraton.
	 */
	void accept(Configuration<E> configuration);
}