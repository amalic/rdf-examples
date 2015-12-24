package org.amalic.sesame.store;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.openrdf.model.IRI;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.resultio.text.tsv.SPARQLResultsTSVWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;
import org.openrdf.sail.nativerdf.NativeStore;

import info.aduna.iteration.Iterations;

public class SesameStore {
	private String namespace;
	private StoreType storeType;
	private Repository repository;
	private RepositoryConnection conn;
	private ValueFactory valueFactory;

	public SesameStore(StoreType storeType, String nameSpace) {
		this.storeType = storeType;
		this.namespace = nameSpace;
	}

	public void initRepository() {
		initRepository(storeType);
	}

	public void printQueryResult(String sparqlQuery) {
		String sparql = "prefix : <" + namespace + "> " + sparqlQuery;
		System.err.println(prettify(sparql));
		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparql);
		tupleQuery.evaluate(new SPARQLResultsTSVWriter(new BufferedOutputStream(System.out)));
		System.out.println("\n");
	}

	public void shutdownRepository() {
		conn.close();
		repository.shutDown();
	}

	public void printRepositoryContents() {
		RepositoryResult<Statement> statements =  conn.getStatements(null, null, null, true);
		Model model = Iterations.addAll(statements, new LinkedHashModel());
		model.setNamespace("", namespace);
		Rio.write(model, System.out, RDFFormat.TURTLE);
	}

	public IRI createIRI(String identifier) {
		return createIRI(this.namespace, identifier);
	}

	public IRI createIRI(String namespace, String identifier) {
		return valueFactory.createIRI(namespace, identifier);
	}

	public void storeTriple(IRI subject, IRI predicate, IRI object) {
		conn.add(subject, predicate, object);
	}

	public void loadFromFile(File file, RDFFormat format) {
		try {
			RDFParser parser = Rio.createParser(format);
			InputStream stream = new FileInputStream(file);
			Model model = new LinkedHashModel();
			StatementCollector collector = new StatementCollector(model);
			parser.setRDFHandler(collector);
			parser.parse(stream, "file://" + file.getName());
			conn.add(collector.getStatements());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void initRepository(StoreType storeType) {
		switch (storeType) {
		case NATIVE_STORE: initWithNativeStore(); break;
		case CACHED_MEMORY_STORE: initWithCachedMemoryStore(); break;
		default: initWithMemoryStore(); break;
		}
	}

	private void initWithNativeStore() {
		File file = new File("./data");
		initRepository(new NativeStore(file));
	}

	private void initWithCachedMemoryStore() {
		File file = new File("./cache");
		initRepository(new MemoryStore(file));
	}

	private void initWithMemoryStore() {
		initRepository(new MemoryStore());
	}

	private void initRepository(Sail store) {
		repository = new SailRepository(store);
		repository.initialize();
		conn = repository.getConnection();
		valueFactory = repository.getValueFactory();
	}

	private String prettify(String sparql) {
		try {
			return OpAsQuery.asQuery(Algebra.compile(QueryFactory.create(sparql, namespace))).serialize();
		} catch (Throwable t) {
			return sparql + "\n";
		}
	}
}
