package edu.kit.aifb.cumulus.framework;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * CumulusRDF shared constants. 
 * 
 * @author Andreas Wagner
 * @author Sebastian Schmidt 
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class Environment {

	/**
	 * Configuration parameters.
	 * 
	 * @author Andreas Wagner
	 * @author Sebastian Schmidt 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	public interface ConfigParams {
		static final String LAYOUT = "storage-layout";
		static final String DEFAULT_STORE = "default-store-id";
		static final String ERROR = "error", STORE = "store", SESAME_REPO = "sesame-repo";
		static final String INTERNAL_BASE_URI = "internal-base-URI", EXTERNAL_BASE_URI = "external-base-URI";
	}

	/**
	 * Configuration values.
	 * 
	 * @author Andreas Wagner
	 * @author Sebastian Schmidt 
	 * @author Andrea Gazzarini
	 * @since 1.0
	 */
	public interface ConfigValues {
		String STORE_LAYOUT_TRIPLE = "triple", STORE_LAYOUT_QUAD = "quad";
	}

	public static final String NEW_LINE = System.getProperty("line.separator");
	public static final Resource XML_SCHEMA_DATE_TIME = ValueFactoryImpl.getInstance().createURI(
			"http://www.w3.org/2001/XMLSchema#dateTime");
	public static final Resource XML_SCHEMA_DATE = ValueFactoryImpl.getInstance().createURI("http://www.w3.org/2001/XMLSchema#date");
	public static final Resource XML_SCHEMA_TIME = ValueFactoryImpl.getInstance().createURI("http://www.w3.org/2001/XMLSchema#time");
	public static final Resource XML_SCHEMA_DOUBLE = ValueFactoryImpl.getInstance().createURI("http://www.w3.org/2001/XMLSchema#double");
	public static final Resource XML_SCHEMA_FLOAT = ValueFactoryImpl.getInstance().createURI("http://www.w3.org/2001/XMLSchema#float");
	public static final Resource XML_SCHEMA_INTEGER = ValueFactoryImpl.getInstance().createURI("http://www.w3.org/2001/XMLSchema#integer");

	public static final Resource XML_SCHEMA_LONG = ValueFactoryImpl.getInstance().createURI("http://www.w3.org/2001/XMLSchema#long");
	public static final Resource XML_SCHEMA_INT = ValueFactoryImpl.getInstance().createURI("http://www.w3.org/2001/XMLSchema#int");
	public static final Resource XML_SCHEMA_SHORT = ValueFactoryImpl.getInstance().createURI("http://www.w3.org/2001/XMLSchema#short");
	public static final Resource XML_SCHEMA_DECIMAL = ValueFactoryImpl.getInstance().createURI("http://www.w3.org/2001/XMLSchema#decimal");
	public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

	public static final Set<Resource> NUMERIC_RANGETYPES;

	public static final Set<String> NUMERIC_RANGETYPES_AS_STRING;

	public static final Set<Resource> DATETIME_RANGETYPES;

	public static final Set<String> DATETIME_RANGETYPES_AS_STRING;

	public static final String BASE_URI = "http://cumulus#";

	static {

		NUMERIC_RANGETYPES = new HashSet<Resource>();
		NUMERIC_RANGETYPES.add(XML_SCHEMA_DOUBLE);
		NUMERIC_RANGETYPES.add(XML_SCHEMA_FLOAT);
		NUMERIC_RANGETYPES.add(XML_SCHEMA_INTEGER);
		NUMERIC_RANGETYPES.add(XML_SCHEMA_LONG);
		NUMERIC_RANGETYPES.add(XML_SCHEMA_INT);
		NUMERIC_RANGETYPES.add(XML_SCHEMA_SHORT);
		NUMERIC_RANGETYPES.add(XML_SCHEMA_DECIMAL);

		NUMERIC_RANGETYPES_AS_STRING = new HashSet<String>();

		for (Resource res : NUMERIC_RANGETYPES) {
			NUMERIC_RANGETYPES_AS_STRING.add(res.toString());
		}

		DATETIME_RANGETYPES = new HashSet<Resource>();
		DATETIME_RANGETYPES.add(XML_SCHEMA_DATE_TIME);
		DATETIME_RANGETYPES.add(XML_SCHEMA_DATE);
		DATETIME_RANGETYPES.add(XML_SCHEMA_TIME);

		DATETIME_RANGETYPES_AS_STRING = new HashSet<String>();

		for (Resource res : DATETIME_RANGETYPES) {
			DATETIME_RANGETYPES_AS_STRING.add(res.toString());
		}
	}
}