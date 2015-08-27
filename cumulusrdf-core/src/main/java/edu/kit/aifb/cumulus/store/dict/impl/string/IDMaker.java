package edu.kit.aifb.cumulus.store.dict.impl.string;

import static edu.kit.aifb.cumulus.framework.util.Bytes.SIZE_OF_LONG;
import static edu.kit.aifb.cumulus.framework.util.Bytes.encode;

import java.net.InetAddress;
/**
 * A very simple UID maker monotonically incremented.
 *
 * @author Andrea Gazzarini.
 * @since 1.0
 */
abstract class IDMaker {
	private static long currentID;

	static 
	{
		StringBuffer sb = new StringBuffer();
		String ipAddress = "127.0.0.1";
		try {
			InetAddress ip = InetAddress.getLocalHost();
			ipAddress = ip.getHostAddress();
			sb.append(ipAddress.substring(ipAddress.lastIndexOf('.') + 1));
		} catch (final Exception ignore) {
			// Nothing to do here
		} 

		sb.append(System.currentTimeMillis());
		currentID = Long.parseLong(sb.toString());
	}

	/**
	 * Obtain the next available ID.
	 *
	 * @return long the next available ID.
	 */
	public static synchronized byte[] nextID() {
		byte[] result = new byte[SIZE_OF_LONG];
		encode(currentID++, result, 0);
		return result;
	}
}