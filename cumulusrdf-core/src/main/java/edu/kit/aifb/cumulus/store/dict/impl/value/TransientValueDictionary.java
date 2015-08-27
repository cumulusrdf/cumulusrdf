package edu.kit.aifb.cumulus.store.dict.impl.value;

import static edu.kit.aifb.cumulus.framework.Environment.CHARSET_UTF8;
import static edu.kit.aifb.cumulus.framework.util.Bytes.fillIn;
import static edu.kit.aifb.cumulus.framework.util.Bytes.subarray;
import static edu.kit.aifb.cumulus.store.sesame.CumulusRDFSesameUtil.SESAME_VALUE_FACTORY;

import java.util.UUID;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.rio.ntriples.NTriplesUtil;

import edu.kit.aifb.cumulus.framework.InitialisationException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerException;
import edu.kit.aifb.cumulus.framework.datasource.DataAccessLayerFactory;
import edu.kit.aifb.cumulus.framework.domain.dictionary.ITopLevelDictionary;
import edu.kit.aifb.cumulus.log.MessageCatalog;

/**
 * Dictionary that uses strings encoding for generating variable-length identifiers.
 * 
 * Identifiers can be simple or compound. In case of a simple id then its value is the byte array
 * representation of the N3 value (String.getBytes(UTF-8)).
 * In case of compound id, the resulting byte array is composed in the following way
 * 
 * <ul>
 * 	<li>2 bytes indicating how many components we have in the identifier;</li>
 *  <li>2 bytes indicating the length of the sub-identifier;</li>
 *  <li>sub-identifier;</li>
 * </ul>
 * 
 * Note that 2nd and 3rd point are repeated for each sub-identifier.
 * 
 * In addition, this dictionary decorates and delegates identifier management to another dictionary in 
 * case the input value is a literal with a size exceeding a predefined length.
 * 
 * @author Andrea Gazzarini
 * @since 1.1.0
 */
public class TransientValueDictionary extends ValueDictionaryBase {

	static final int DEFAULT_THRESHOLD = 1000; // 1K
	
	static final byte THRESHOLD_EXCEEDED = 1;
	static final byte THRESHOLD_NOT_EXCEEDED = 2;
			
	final int _threshold; 
	ITopLevelDictionary _longLiteralsDictionary;

	/**
	 * Builds a new write optimized dictionary with the given data.
	 * 
	 * @param id the dictionary identifier.
	 * @param longLiteralsDictionary the dictionary that will be used for long literals.
	 * @param literalLengthThresold 
	 * 				the minimum literal length that will trigger involvment of the wrapped dictionary.
	 * 				0 disables the wrapped dictionary, -1 uses default {@link #DEFAULT_THRESHOLD}.
	 */
	public TransientValueDictionary(
			final String id, 
			final ITopLevelDictionary longLiteralsDictionary, 
			final int literalLengthThresold) {
		super(id);
		
		if (longLiteralsDictionary == null) {
			throw new IllegalArgumentException(MessageCatalog._00091_NULL_DECORATEE_DICT);
		}
		
		_longLiteralsDictionary = longLiteralsDictionary;
		if (literalLengthThresold < 0) {
			_threshold = DEFAULT_THRESHOLD;
		} else if (literalLengthThresold == 0) {
			_threshold = Integer.MAX_VALUE;
		} else {
			_threshold = literalLengthThresold;
		}
	}
	
	/**
	 * Builds a new write optimized dictionary with default values.
	 * Specifically 
	 * 
	 * <ul>
	 * 	<li>Long literals threshold length is 1K;</li>
	 * 	<li>Long literals dictionary is {@link PersistentValueDictionary}</li>
	 * </ul>
	 * 
	 * @param id the dictionary identifier.
	 */
	public TransientValueDictionary(final String id) {
		this(id, new PersistentValueDictionary(UUID.randomUUID().toString()), DEFAULT_THRESHOLD);
	}
	
	@Override
	protected void initialiseInternal(final DataAccessLayerFactory factory) throws InitialisationException {
		_longLiteralsDictionary.initialise(factory);
	}
	
	@Override
	protected void closeInternal() {
		_longLiteralsDictionary.close();
	}

	@Override
	public Value getValueInternal(final byte[] id, final boolean p) throws DataAccessLayerException {
		switch (id[0]) {
		case THRESHOLD_EXCEEDED:
			RUNTIME_CONTEXTS.get().isFirstLevelResult = false;
			return _longLiteralsDictionary.getValue(subarray(id, 1, id.length - 1), p);
		default:
			RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
			final String n3 = new String(id, 2, id.length - 2, CHARSET_UTF8);
			if (id[1] == RESOURCE_BYTE_FLAG) {
				return NTriplesUtil.parseResource(n3, SESAME_VALUE_FACTORY);
			} else if (id[1] == LITERAL_BYTE_FLAG) {
				return NTriplesUtil.parseLiteral(n3, SESAME_VALUE_FACTORY);
			} else {
				return NTriplesUtil.parseBNode(n3, SESAME_VALUE_FACTORY);
			}
		}
	}

	@Override
	public void removeValue(final Value value, final boolean p) throws DataAccessLayerException {
		final byte[] id = getID(value, p);
		if (id[0] == THRESHOLD_EXCEEDED) {
			_longLiteralsDictionary.removeValue(value, p);
		}
	}

	@Override
	protected byte[] getIdInternal(final Value value, final boolean p) throws DataAccessLayerException {		
		if (value instanceof Literal) {
			final Literal literal = (Literal) value;
			if (literal.getLabel().length() > _threshold) {
				final byte[] idFromEmbeddedDictionary = _longLiteralsDictionary.getID(value, p);
				final byte [] result = new byte[idFromEmbeddedDictionary.length + 1];
				result[0] = THRESHOLD_EXCEEDED;
				fillIn(result, 1, idFromEmbeddedDictionary);
				RUNTIME_CONTEXTS.get().isFirstLevelResult = false;
				return result;
			}
		} 
		 
		final String n3 = NTriplesUtil.toNTriplesString(value);

		final byte[] n3b = n3.getBytes(CHARSET_UTF8);
		final byte[] id = new byte[n3b.length + 2];
		
		if (value instanceof Literal) {
			id[0] = THRESHOLD_NOT_EXCEEDED;
			id[1] = LITERAL_BYTE_FLAG;
		} else if (value instanceof BNode) {
			id[0] = THRESHOLD_NOT_EXCEEDED;
			id[1] = BNODE_BYTE_FLAG;
		} else {
			id[0] = THRESHOLD_NOT_EXCEEDED;
			id[1] = RESOURCE_BYTE_FLAG;
		}		
		
		fillIn(id, 2, n3b);
		RUNTIME_CONTEXTS.get().isFirstLevelResult = true;
		return id;			
	}

	@Override
	public boolean isBNode(final byte[] id) {
		return id != null && id[0] == THRESHOLD_NOT_EXCEEDED && id[1] == BNODE_BYTE_FLAG;
	}

	@Override
	public boolean isLiteral(final byte[] id) {
		return id != null && (id[0] == THRESHOLD_EXCEEDED || (id[0] == THRESHOLD_NOT_EXCEEDED && id[1] == LITERAL_BYTE_FLAG));
	}

	@Override
	public boolean isResource(final byte[] id) {
		return id != null && id[0] == THRESHOLD_NOT_EXCEEDED && id[1] == RESOURCE_BYTE_FLAG;
	}
}