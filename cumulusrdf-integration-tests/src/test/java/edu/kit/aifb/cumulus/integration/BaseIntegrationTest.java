package edu.kit.aifb.cumulus.integration;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;

/**
 * 
 * Base class for integration tests.
 * 
 * @author Andreas Wagner
 * @since 1.1
 */
public class BaseIntegrationTest {

	/**
	 * MIME types for RDF serializations
	 */
	protected static final String RDF_XML = "application/rdf+xml",
			BINARY = "application/x-binary-rdf",
			N_TRIPLES = "text/plain", //application/n-triples
			N_QUADS = "text/x-nquads", //application/n-quads
			JSON_LD = "application/ld+json",
			RDF_JSON = "application/rdf+json",
			TRIG = "application/x-trig",
			TRIX = "application/trix",
			TURTLE = "text/turtle",
			TURTLE_ALT = "application/x-turtle",
			N3 = "text/n3",
			N3_ALT = "text/rdf+n3",
			TEXT_PLAIN = "text/plain",
			TEXT_HTML = "text/html";

	protected static final String[] RDF_SERIALIZATIONS = new String[] { RDF_XML, BINARY, N_TRIPLES, N_QUADS, JSON_LD, RDF_JSON, TRIG, TRIX, TURTLE, TURTLE_ALT, N3, N3_ALT,
			TEXT_PLAIN };

	protected static HttpClient _client = new HttpClient();

	protected static List<String> URIS, TRIPLES_NT,
				TRIPLES_INVALID_NT, URIS_INVALID;

	static {

		URIS = Arrays.asList("http://localhost:8080/cumulusrdf-web-module/resource/Actor",
				"http://localhost:8080/cumulusrdf-web-module/resource/Thing");
		URIS_INVALID = Arrays
				.asList("http://localhost:8080/cumulusrdf-web-module/resource/Actor2", "http://localhost:8080/cumulusrdf-web-module/resource/Thing2");

		/*
		 * test data for HTTP requests on CumulusRDF, which is deployed on the localhost
		 */
		TRIPLES_NT = Arrays
				.asList("<http://localhost:8080/cumulusrdf-web-module/resource/Actor> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Class> .\n "
						+ " <http://localhost:8080/cumulusrdf-web-module/resource/Actor> <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Actor> .\n <http://localhost:8080/cumulusrdf-web-module/resource/Actor>"
						+ " <http://semantic-mediawiki.org/swivt/1.0#wikiPageModificationDate> \"2012-02-01T09:53:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n "
						+ " <http://localhost:8080/cumulusrdf-web-module/resource/Actor> <http://www.w3.org/2000/01/rdf-schema#comment> \"Actors have the capability to make decisions and exchange "
						+ " information with other actors through interfaces. Actors my be devices, computer systems, or software programs and/or the organizations that own them. "
						+ " An actor may also comprise other actors. Source: NIST Framework and Roadmap for Smart Grid Interoperability Standards, National Institute of Standards"
						+ " and Technology (2010).\"^^<http://www.w3.org/2001/XMLSchema#string> .\n <http://localhost:8080/cumulusrdf-web-module/resource/Actor> <http://www.w3.org/2000/01/rdf-schema#comment> "
						+ " \"Netznutzer: nat\u00FCrliche oder juristische Personen, die Energie in ein Elektrizit\u00E4ts- oder Gasversorgungsnetz einspeisen oder daraus beziehen "
						+ " (http://bundesrecht.juris.de/enwg_2005/__3.html). Quelle: MeRegioMobil http://meregiomobil.forschung.kit.edu/.\"^^<http://www.w3.org/2001/XMLSchema#string> .\n"
						+ " <http://localhost:8080/cumulusrdf-web-module/resource/Actor> <http://www.w3.org/2000/01/rdf-schema#comment> \"Ein Agent ist eine (nat\u00FCrliche oder juristische) Person,"
						+ " welche eine Transaktion auf dem Markt im Auftrag seines Kunden oder seines Arbeitgebers ausf\u00FChrt. Es kann erforderlich sein, dass f\u00FCr gewisse "
						+ " Transaktionen ein Personenbezug hergestellt werden kann. Regelungen des Wertpapierhandelsgesetztes sind gegebenenfalls zu ber\u00FCcksichtigen, da der "
						+ " Marktplatz b\u00F6rsen\u00E4hnlich aufgebaut ist.Quelle: MeRegioMobil http://meregiomobil.forschung.kit.edu/.\"^^<http://www.w3.org/2001/XMLSchema#string> .\n"
						+ " <http://localhost:8080/cumulusrdf-web-module/resource/Actor> <http://www.w3.org/2000/01/rdf-schema#isDefinedBy> <http://gridpedia.org/wiki/Actor> .\n <http://localhost:8080/cumulusrdf-web-module/resource/Actor> "
						+ " <http://www.w3.org/2000/01/rdf-schema#label> \"Actor\" .\n <http://localhost:8080/cumulusrdf-web-module/resource/Actor> <http://www.w3.org/2000/01/rdf-schema#seeAlso> "
						+ " <http://dbpedia.org/resource/Actant> .\n <http://localhost:8080/cumulusrdf-web-module/resource/Actor> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://localhost:8080/cumulusrdf-web-module/resource/Thing> .\n",
						" <http://localhost:8080/cumulusrdf-web-module/resource/Thing> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#Class> .\n"
								+ " <http://localhost:8080/cumulusrdf-web-module/resource/Thing> <http://semantic-mediawiki.org/swivt/1.0#page> <http://gridpedia.org/wiki/Thing> .");
		/*
		 * invalid data ...
		 */
		TRIPLES_INVALID_NT = Arrays
				.asList("<http://localhost:8080/cumulusrdf-web-module/resource/Actor> <http://www.w3.org/1999/02/22-rd");

	}
}
