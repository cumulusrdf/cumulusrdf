package edu.kit.aifb.cumulus.framework.util;

import java.util.Arrays;

/**
 * Booch utility for bytes manipulation.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @author Sebastian Schmidt
 * @since 1.0.1
 */
public abstract class Bytes {

	public static final int SIZE_OF_LONG = 8;

	/**
	 * Returns a subarray of a given input array.
	 * 
	 * @param in the input array.
	 * @param from the start offset (inclusive) in the input array.
	 * @param length the length of the resulting subarray.
	 * @return a subarray of a given input array.
	 */
	public static byte[] subarray(final byte[] in, final int from, final int length) {
		return Arrays.copyOfRange(in, from, from + length);
	}

	/**
	 * Copies an array into another array, at a given offset.
	 * 
	 * @param dest the destination array.
	 * @param destPos the destination offset (inclusive).
	 * @param src the source array.
	 */
	public static void fillIn(final byte[] dest, final int destPos, final byte[] src) {
		System.arraycopy(src, 0, dest, destPos, src.length);
	}
	
	/**
	 * Copies an array into another array, at a given offset.
	 * 
	 * @param dest the destination array.
	 * @param destPos the destination offset (inclusive).
	 * @param src the source array.
	 * @param length how many bytes of the source array will be copied.
	 */
	public static void fillIn(final byte[] dest, final int destPos, final byte[] src, final int length) {
		System.arraycopy(src, 0, dest, destPos, length);
	}
	
	/**
	 * Copies an array into another array, at a given offset.
	 * 
	 * @param dest the destination array.
	 * @param destPos the destination offset (inclusive).
	 * @param srcPos the source offset (inclusive).
	 * @param src the source array.
	 * @param length how many bytes of the source array will be copied.
	 */
	public static void fillIn(final byte[] dest, final int destPos, final byte[] src, final int srcPos, final int length) {
		System.arraycopy(src, srcPos, dest, destPos, length);
	}
	
	/**
	 * Returns the short value of the 2 bytes starting from a given offset in the input array.
	 * 
	 * @param data the input array.
	 * @param offset the offset, that is the index of the first byte that will be used for decoding.
	 * @return the short value computed following the above rule.
	 */
	public static short decodeShort(final byte[] data, final int offset) {
        return (short) ((data[0 + offset] << 8) + (data[1 + offset] & 255) << 0);		
	}
	
	/**
	 * Encodes a short value into the input array, at a given position.
	 * 
	 * @param value the short value that will be encoded.
	 * @param dest the destination array.
	 * @param offset the offset, that is the index of the first byte that will be used for decoding.
	 */
	public static void encode(final int value, final byte[] dest, final int offset) {
		dest [offset] =  (byte)((value >>> 8) & 0xFF);
		dest [offset + 1] =  (byte)((value >>> 0) & 0xFF);
	}	

	/**
	 * Returns the long value of the 8 bytes starting from a given offset in the input array.
	 * 
	 * @param data the input array.
	 * @param offset the offset, that is the index of the first byte that will be used for decoding.
	 * @return the long value computed following the above rule.
	 */
	public static long decodeLong(final byte[] data, final int offset) {
		return ((data[0 + offset] & 0xFFL) << 56)
				+ ((data[1 + offset] & 0xFFL) << 48)
				+ ((data[2 + offset] & 0xFFL) << 40)
				+ ((data[3 + offset] & 0xFFL) << 32)
				+ ((data[4 + offset] & 0xFFL) << 24)
				+ ((data[5 + offset] & 0xFFL) << 16)
				+ ((data[6 + offset] & 0xFFL) << 8)
				+ ((data[7 + offset] & 0xFFL) << 0);
	}

	/**
	 * Encodes a long value into the input array, at a given position.
	 * 
	 * @param value the short value that will be encoded.
	 * @param dest the destination array.
	 * @param offset the offset, that is the index of the first byte that will be used for decoding.
	 */
	public static void encode(final long value, final byte[] dest, final int offset) {
		dest[offset + 0] = (byte) (value >>> 56);
		dest[offset + 1] = (byte) (value >>> 48);
		dest[offset + 2] = (byte) (value >>> 40);
		dest[offset + 3] = (byte) (value >>> 32);
		dest[offset + 4] = (byte) (value >>> 24);
		dest[offset + 5] = (byte) (value >>> 16);
		dest[offset + 6] = (byte) (value >>> 8);
		dest[offset + 7] = (byte) (value >>> 0);
	}
	
	/**
	 * Returns the values from each provided array combined into a single array.
	 * 
	 * @param marker a special byte that will be inserted at the very beginning of the resulting array.
	 * @param a the first array.
	 * @param b the second array.
	 * @return a single array containing all the values from the source arrays, in order.
	 */
	public static byte[] concat(final byte marker, final byte[] a, final byte[] b) {
		byte[] result = new byte[1 + a.length + b.length];
		result[0] = marker;
		System.arraycopy(a, 0, result, 1, a.length);
		System.arraycopy(b, 0, result, a.length + 1, b.length);
		return result;
	}
}