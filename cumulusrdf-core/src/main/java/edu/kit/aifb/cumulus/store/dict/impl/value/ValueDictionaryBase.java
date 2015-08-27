package edu.kit.aifb.cumulus.store.dict.impl.value;

import static edu.kit.aifb.cumulus.framework.util.Bytes.decodeShort;
import static edu.kit.aifb.cumulus.framework.util.Bytes.encode;
import static edu.kit.aifb.cumulus.framework.util.Bytes.fillIn;
import static edu.kit.aifb.cumulus.store.sesame.CumulusRDFSesameUtil.SESAME_VALUE_FACTORY;

import java.util.Iterator;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.google.common.collect.AbstractIterator;

import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.domain.dictionary.DictionaryBase;
import edu.kit.aifb.cumulus.framework.domain.dictionary.DictionaryRuntimeContext;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * Base for dictionary implementations.
 * Provides shared and common behaviour for concrete dictionary implementors.
 * 
 * @author Andreas Wagner
 * @author Andrea Gazzarini
 * @since 1.0
 */
public abstract class ValueDictionaryBase extends DictionaryBase<Value> implements ITopLevelDictionary {

	protected static final ThreadLocal<DictionaryRuntimeContext> RUNTIME_CONTEXTS = new ThreadLocal<DictionaryRuntimeContext>() {
		protected DictionaryRuntimeContext initialValue() {
			return new DictionaryRuntimeContext();
		};
	};

	/**
	 * Builds a new dictionary.
	 * 
	 * @param id the dictionary identifier.
	 */
	public ValueDictionaryBase(final String id) {
		super(id);
	}
	
	@Override
	public byte[][] getIDs(final Value s, final Value p, final Value o) throws DataAccessLayerException {
		return new byte[][] {
				getID(s, false),
				getID(p, true),
				getID(o, false) };
	}

	@Override
	public byte[][] getIDs(final Value s, final Value p, final Value o, final Value c) throws DataAccessLayerException {
		return new byte[][] {
				getID(s, false),
				getID(p, true),
				getID(o, false),
				getID(c, false) };
	}

	@Override
	public Statement getValues(final byte[] s, final byte[] p, final byte[] o) throws DataAccessLayerException {
		return SESAME_VALUE_FACTORY.createStatement(
				(Resource) getValue(s, false),
				(URI) getValue(p, true),
				getValue(o, false));
	}

	@Override
	public Statement getValues(final byte[] s, final byte[] p, final byte[] o, final byte[] c) throws DataAccessLayerException {
		return SESAME_VALUE_FACTORY.createStatement(
				(Resource)getValue(s, false), 
				(URI)getValue(p, true), 
				getValue(o, false), 
				(Resource)getValue(c, false));
	}

	@Override
	public Iterator<byte[][]> toIDQuadIterator(final Iterator<Statement> quads) {
		return new AbstractIterator<byte[][]>() {

			@Override
			protected byte[][] computeNext() {

				if (!quads.hasNext()) {
					return endOfData();
				}

				final Statement statement = quads.next();

				try {
					return new byte[][] {
						getID(statement.getSubject(), false),
						getID(statement.getPredicate(), true),
						getID(statement.getObject(), false),
						getID(statement.getContext(), false) };
				} catch (DataAccessLayerException exception) {
					_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
					return endOfData();
				}
			}
		};
	}

	@Override
	public Iterator<byte[][]> toIDTripleIterator(final Iterator<Statement> triples) {

		return new AbstractIterator<byte[][]>() {

			@Override
			protected byte[][] computeNext() {

				if (!triples.hasNext()) {
					return endOfData();
				}

				final Statement statement = triples.next();

				try {
					return new byte[][] {
						getID(statement.getSubject(), false),
						getID(statement.getPredicate(), true),
						getID(statement.getObject(), false) };
				} catch (final DataAccessLayerException exception) {
					_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
					return endOfData();
				}
			}
		};
	}

	@Override
	public Iterator<Statement> toValueQuadIterator(final Iterator<byte[][]> quads) {
		return new AbstractIterator<Statement>() {

			@Override
			protected Statement computeNext() {

				while (quads.hasNext()) {

					final byte[][] ids = quads.next();

					try {
						return getValues(ids[0], ids[1], ids[2], ids[3]);
					} catch (DataAccessLayerException exception) {
						_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
						return endOfData();
					}
				}

				return endOfData();
			}
		};
	}

	@Override
	public Iterator<Statement> toValueTripleIterator(final Iterator<byte[][]> triples) {

		return new AbstractIterator<Statement>() {

			@Override
			protected Statement computeNext() {

				while (triples.hasNext()) {

					final byte[][] ids = triples.next();
					try {
						return getValues(ids[0], ids[1], ids[2]);
					} catch (DataAccessLayerException exception) {
						_log.error(MessageCatalog._00093_DATA_ACCESS_LAYER_FAILURE, exception);
						return endOfData();
					}
				}

				return endOfData();
			}
		};
	}

	@Override
	public byte[][] decompose(final byte[] compositeId) {
		final short howManySubIdentifiers = decodeShort(compositeId, 0);
		byte[][] tuple = new byte[howManySubIdentifiers][];
		int offset = 2;
		for (int i = 0; i < howManySubIdentifiers; i++) {
			final int length = decodeShort(compositeId, offset);
			offset += 2;

			byte[] id = new byte[length];
			fillIn(id, 0, compositeId, offset, length);
			offset += length;

			tuple[i] = id;
		}
		return tuple;
	}

	@Override
	public byte[] compose(final byte[] id1, final byte[] id2) {
		if (id1 == null || id2 == null) {
			throw new IllegalArgumentException("Both identifiers must be not null.");
		}
		byte[] result = new byte[2 + 2 + id1.length + 2 + id2.length];
		encode(2, result, 0);
		encode(id1.length, result, 2);
		fillIn(result, 2 + 2, id1, id1.length);
		encode(id2.length, result, 2 + 2 + id1.length);
		fillIn(result, 2 + 2 + id1.length + 2, id2, id2.length);
		return result;
	}

	@Override
	public byte[] compose(final byte[] id1, final byte[] id2, final byte[] id3) {
		if (id1 == null || id2 == null) {
			throw new IllegalArgumentException("All identifiers must be not null.");
		}
		byte[] result = new byte[2 + 2 + id1.length + 2 + id2.length + 2 + id3.length];
		encode(3, result, 0);
		encode(id1.length, result, 2);
		fillIn(result, 2 + 2, id1, id1.length);
		encode(id2.length, result, 2 + 2 + id1.length);
		fillIn(result, 2 + 2 + id1.length + 2, id2, id2.length);
		encode(id3.length, result, 2 + 2 + id1.length + 2 + id2.length);
		fillIn(result, 2 + 2 + id1.length + 2 + id2.length + 2, id3, id3.length);
		return result;
	}
}