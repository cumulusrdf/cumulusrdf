package edu.kit.aifb.cumulus;

import org.openrdf.model.Value;
import static edu.kit.aifb.cumulus.TestUtils.VALUE_FACTORY;
/**
 * Test Data.
 * A collection of test data shared between test cases.
 * 
 * Each test could also have an additional set of local data.
 * 
 * @author Andrea Gazzarini
 * @since 1.1
 */
public interface TestData {
	Value ACTOR = VALUE_FACTORY.createURI("http://gridpedia.org/id/Actor"), 
		DEVICE = VALUE_FACTORY.createURI("http://gridpedia.org/id/Device"),
		MACHINE = VALUE_FACTORY.createURI("http://gridpedia.org/id/Machine"), 
		SWIVT_PAGE = VALUE_FACTORY.createURI("http://semantic-mediawiki.org/swivt/1.0#page"),
		STRING = VALUE_FACTORY.createLiteral("Actor"),
		DATE	= VALUE_FACTORY.createLiteral("2012-01-31T10:52:19Z", VALUE_FACTORY.createURI("http://www.w3.org/2001/XMLSchema#dateTime")),
		SWIVT_MODIFICATION = VALUE_FACTORY.createURI("http://semantic-mediawiki.org/swivt/1.0#wikiPageModificationDate"), 
		DBPEDIA_MACHINE = VALUE_FACTORY.createURI("http://dbpedia.org/resource/Machine"), 
		MEDIA = VALUE_FACTORY.createURI("http://gridpedia.org/id/Media");
}