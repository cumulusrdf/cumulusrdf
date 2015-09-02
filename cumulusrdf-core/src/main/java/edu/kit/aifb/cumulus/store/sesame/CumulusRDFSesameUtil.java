package edu.kit.aifb.cumulus.store.sesame;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

public abstract class CumulusRDFSesameUtil {

	public static final ValueFactory SESAME_VALUE_FACTORY = ValueFactoryImpl.getInstance();

	public static Statement valuesToQuadStatement(final Value[] next) {
		return SESAME_VALUE_FACTORY.createStatement((Resource) next[0], (URI) next[1], next[2], (Resource) next[3]);
	}

	public static Statement valuesToTripleStatement(final Value[] next) {
		return SESAME_VALUE_FACTORY.createStatement((Resource) next[0], (URI) next[1], next[2]);
	}
	
	public static Statement valuesToStatement(final Value[] next) {
		if (next == null || next.length < 3 || next.length > 4) {
			return null;
		}

		return (next.length == 3)
			? valuesToTripleStatement(next)
			: valuesToQuadStatement(next);
	}	
}