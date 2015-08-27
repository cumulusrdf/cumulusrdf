package edu.kit.aifb.cumulus.framework.datasource;

/**
 * Enumeration of all available storage layouts.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public enum StorageLayout {
	TRIPLE {
		@Override
		public String toString() {
			return "TripleStore";
		}
	},
	QUAD {
		@Override
		public String toString() {
			return "QuadStore";
		}
	}
}