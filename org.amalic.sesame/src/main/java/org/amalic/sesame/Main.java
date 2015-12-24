package org.amalic.sesame;

import java.io.File;

import org.amalic.sesame.store.SesameStore;
import org.amalic.sesame.store.StoreType;
import org.openrdf.model.IRI;
import org.openrdf.rio.RDFFormat;

public class Main {

	public static void main(String[] args) {
		SesameStore sesameStore = new SesameStore(StoreType.MEMORY_STORE, "http://amalic.org/");
		try {
			sesameStore.initRepository();

			createFlintstoneData(sesameStore);
			sesameStore.loadFromFile(new File("ebola.ttl"), RDFFormat.TURTLE);
			sesameStore.loadFromFile(new File("sneeair.rdf"), RDFFormat.RDFXML);

			queryStatistics(sesameStore);
			queryFirstNTriples(sesameStore, 10);
			queryGraphs(sesameStore);
			queryNamespaces(sesameStore);
			querySelfReferencingTriples(sesameStore);

			queryFlightsWithSpecificFlightTime(sesameStore, "0:53");
			queryFlightTimes(sesameStore, 10);

			queryForSpouses(sesameStore);
			queryForGrandChildOfFred(sesameStore);
			queryPathLengthExample(sesameStore);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		} finally {
			sesameStore.shutdownRepository();
		}
	}

	private static void createFlintstoneData(SesameStore sesameStore) {
		IRI hasSpouse = sesameStore.createIRI("hasSpouse");
		IRI hasChild = sesameStore.createIRI("hasChild");
		IRI fred = sesameStore.createIRI("fred");
		IRI wilma = sesameStore.createIRI("wilma");
		IRI pebbles = sesameStore.createIRI("pebbles");
		IRI bammBamm = sesameStore.createIRI("bamm-bamm");
		IRI roxy = sesameStore.createIRI("roxy");
		IRI chip = sesameStore.createIRI("chip");
		sesameStore.storeTriple(fred, hasSpouse, wilma);
		sesameStore.storeTriple(fred, hasChild, pebbles);
		sesameStore.storeTriple(wilma, hasChild, pebbles);
		sesameStore.storeTriple(pebbles, hasSpouse, bammBamm);
		sesameStore.storeTriple(pebbles, hasChild, roxy);
		sesameStore.storeTriple(pebbles, hasChild, chip);
	}

	static void queryStatistics(SesameStore sesameStore) {
		sesameStore.printQueryResult(
				"SELECT (count(distinct ?s) as ?subjects)"
					+ " (count(distinct ?p) as ?predicates)"
					+ " (count(distinct ?o) as ?objects)"
					+ " (count(?p) as ?Triples)"
				+ " WHERE { ?s ?p ?o. }");
	}

	static void queryFirstNTriples(SesameStore sesameStore, int limit) {
		sesameStore.printQueryResult(
				"SELECT ?s ?p ?o"
				+ " WHERE { ?s ?p ?o . }"
				+ " ORDER BY ?s ?p ?o"
				+ " LIMIT " + limit);
	}

	static void queryGraphs(SesameStore sesameStore) {
		sesameStore.printQueryResult(
				"SELECT distinct ?graph ?label"
				+ " WHERE { GRAPH ?graph { ?s ?p ?o . }"
				+ " OPTIONAL { ?graph rdfs:label ?label . } }");
	}

	static void queryNamespaces(SesameStore sesameStore) {
		sesameStore.printQueryResult(
				"SELECT DISTINCT ?namespace"
				+ " WHERE { { ?uri ?p ?o . } UNION { ?sub ?p ?uri . FILTER(isIRI(?uri)) }"
				+ " BIND (str(?uri) as ?s) FILTER (STRSTARTS(?s, \"http://\"))"
				+ " BIND (IRI(CONCAT(\"http://\", STRBEFORE(SUBSTR(?s, 8), \"/\"))) AS ?namespace) }"
				+ " ORDER BY ?namespace");
	}

	static void querySelfReferencingTriples(SesameStore sesameStore) {
		sesameStore.printQueryResult(
				"SELECT ?c ?p ?o"
				+ " WHERE { ?c ?p ?o . filter(?c = ?o) }");
	}

	static void queryForSpouses(SesameStore sesameStore) {
		sesameStore.printQueryResult(
				"SELECT ?s ?o"
				+ " WHERE { ?s :hasSpouse ?o. }");
	}

	static void queryForGrandChildOfFred(SesameStore sesameStore) {
		sesameStore.printQueryResult(
				"SELECT ?child ?grandChild"
				+ " WHERE { :fred :hasChild ?child."
					+ " ?child :hasChild ?grandChild. }");
	}

	static void queryFlightsWithSpecificFlightTime(SesameStore sesameStore, String flightTime) {
		sesameStore.printQueryResult(
				"PREFIX io: <http://www.daml.org/2001/06/itinerary/itinerary-ont#>"
				+ " PREFIX fl: <http://www.snee.com/ns/flights#>"
				+ " SELECT ?flightnumber ?departure ?destination"
				+ " WHERE {?flight io:flight ?flightnumber ."
                	+ " ?flight fl:flightFromCityName ?departure ."
                	+ " ?flight fl:flightToCityName ?destination ."
                	+ " ?flight io:duration \"" + flightTime + "\" . }");
	}

	static void queryFlightTimes(SesameStore sesameStore, int limit) {
		sesameStore.printQueryResult(
				"PREFIX io: <http://www.daml.org/2001/06/itinerary/itinerary-ont#>"
				+ " PREFIX fl: <http://www.snee.com/ns/flights#>"
				+ " SELECT ?flightTime (count(?flight) as ?flights)"
				+ " WHERE { ?flight io:duration ?flightTime . }"
				+ " GROUP BY ?flightTime"
				+ " ORDER BY desc(?flights)"
				+ " LIMIT " + limit);
	}

	static void queryPathLengthExample(SesameStore sesameStore) {
		sesameStore.printQueryResult(
				"SELECT ?start ?end (count(?mid) as ?pathlength)"
				+ " WHERE {"
					+ " ?start :hasChild+ ?mid ."
					+ " ?mid :hasChild* ?end . }"
				+ " GROUP BY ?start ?end ");
	}

}

