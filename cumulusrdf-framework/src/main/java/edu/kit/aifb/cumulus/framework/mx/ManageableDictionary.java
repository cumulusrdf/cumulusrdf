package edu.kit.aifb.cumulus.framework.mx;

import javax.management.MXBean;

/**
 * Dictiobary common management interface.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
@MXBean
public interface ManageableDictionary extends Manageable {
	/**
	 * The total number of identifier lookups occurred since this dictionary has been created.
	 * 
	 * @return the total number of identifier lookups occurred since this dictionary has been created.
	 */
	long getIdLookupsCount();
	
	/**
	 * The total number of value lookups occurred since this dictionary has been created.
	 * 
	 * @return the total number of value lookups occurred since this dictionary has been created.
	 */
	long getValueLookupsCount();	
}