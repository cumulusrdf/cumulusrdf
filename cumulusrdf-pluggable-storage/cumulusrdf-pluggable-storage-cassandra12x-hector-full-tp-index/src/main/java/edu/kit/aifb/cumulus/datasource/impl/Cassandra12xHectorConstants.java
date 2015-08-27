package edu.kit.aifb.cumulus.datasource.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.SliceFilter;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;

/**
 * Shared constants between classes.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
interface Cassandra12xHectorConstants {
	Composite INCLUDE_ALL_COMPOSITE_LOWER_BOUND = new Composite();
	Composite INCLUDE_ALL_COMPOSITE_HIGHER_BOUND = new Composite();
	
	LongSerializer LONG_SERIALIZER = LongSerializer.get();
	DoubleSerializer DOUBLE_SERIALIZER = DoubleSerializer.get();
	BytesArraySerializer BYTE_SERIALIZER = BytesArraySerializer.get();
	CompositeSerializer COMPOSITE_SERIALIZER = CompositeSerializer.get();
	StringSerializer STRING_SERIALIZER = StringSerializer.get();
			
	Composite C_COL = new Composite(new byte[] {3});
	Composite PC_COL = C_COL;
	
	byte[] EMPTY_VAL = new byte[0];
	byte[] P_COL_NAME = { 3 };
	Composite P_COL = new Composite(P_COL_NAME);
	
	SliceFilter<HColumn<Composite, byte[]>> DONT_INCLUDE_PREDICATE_COLUMN = new SliceFilter<HColumn<Composite, byte[]>>() {
		@Override
		public boolean accept(final HColumn<Composite, byte[]> column) {
			return !Arrays.equals(BYTE_SERIALIZER.fromByteBuffer((ByteBuffer) column.getName().get(0)), P_COL_NAME);
		}
	};
}
