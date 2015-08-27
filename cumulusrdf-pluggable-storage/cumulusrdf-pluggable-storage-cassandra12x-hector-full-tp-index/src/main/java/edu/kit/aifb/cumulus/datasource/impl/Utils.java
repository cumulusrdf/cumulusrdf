package edu.kit.aifb.cumulus.datasource.impl;

import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

/**
 * Booch utility for Cassandra / Hector specific implementation.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public abstract class Utils {
	/**
	 * Returns the {@link Serializer} associated with a given class.
	 * 
	 * @param clazz the class.
	 * @param <T> the class kind.
	 * @return the {@link Serializer} associated with a given class.
	 */
	public static <T> Serializer<T> guessSerializer(final Class<T> clazz) {
		return SerializerTypeInferer.getSerializer(clazz);
	}

	/**
	 * Returns true if a given keyspace has a column family.
	 * 
	 * @param ksdef the keyspace.
	 * @param cfname the column family name.
	 * @return true if the keyspace has the column family, false otherwise.
	 */
	public static boolean hasColumnFamily(final KeyspaceDefinition ksdef, final String cfname) {
		for (final ColumnFamilyDefinition cfdef : ksdef.getCfDefs()) {
			if (cfdef.getName().equals(cfname)) {
				return true;
			}
		}
		return false;
	}
}
