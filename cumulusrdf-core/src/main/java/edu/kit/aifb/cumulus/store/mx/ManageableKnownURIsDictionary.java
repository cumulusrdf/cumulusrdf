package edu.kit.aifb.cumulus.store.mx;

import javax.management.MXBean;

import edu.kit.aifb.cumulus.framework.mx.ManageableDictionary;

@MXBean
public interface ManageableKnownURIsDictionary extends ManageableDictionary {
	
	/**
	 * How many getID requests referred to a know URIs.
	 * Note that if this dictionary is wrapped (i.e. decorated) within a cache dictionary, 
	 * this metric will report a hit for *each* well known URIs. That is, if n requests 
	 * have been executed with the same well known URI, then this attribute will have 1 as value.
	 * 
	 * @return how many requests referred to a know URIs.
	 */
	long getIdKnownURIsHitsCount();
	
	/**
	 * How many getValue requests referred to a know URIs.
	 * Note that if this dictionary is wrapped (i.e. decorated) within a cache dictionary, 
	 * this metric will report a hit for *each* well known URIs. That is, if n requests 
	 * have been executed with the same well known URI, then this attribute will have 1 as value.
	 * 
	 * @return how many requests referred to a know URIs.
	 */
	long getValueKnownURIsHitsCount();	
}