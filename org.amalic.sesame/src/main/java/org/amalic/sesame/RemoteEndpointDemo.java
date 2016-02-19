package org.amalic.sesame;

import java.io.BufferedOutputStream;

import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.resultio.text.tsv.SPARQLResultsTSVWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sparql.SPARQLRepository;

public class RemoteEndpointDemo {
	public static void main(String[] args) {
		Repository repository = new SPARQLRepository("http://lodlaundromat.org/sparql/");
		repository.initialize();
		RepositoryConnection conn = null;
		try {
			conn = repository.getConnection();
			printQueryResult(conn, "select * where {[] a ?Concept} LIMIT 100");
			printQueryResult(conn, "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 100");
			printQueryResult(conn, "SELECT DISTINCT ?properties ?classes WHERE { {[] a ?classes} UNION {[] ?properties ?x}} LIMIT 100");
			conn.close();
		} finally {
			if(conn!= null && conn.isOpen())
				conn.close();
		}
	}
	
	public static void printQueryResult(RepositoryConnection conn, String sparql) {
		System.err.println(prettify(sparql));
		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql);
		tupleQuery.evaluate(new SPARQLResultsTSVWriter(new BufferedOutputStream(System.out)));
		System.out.println("\n");
	}
	
	private static String prettify(String sparql) {
		try {
			return OpAsQuery.asQuery(Algebra.compile(QueryFactory.create(sparql))).serialize();
		} catch (Throwable t) {
			return sparql + "\n";
		}
	}
}
