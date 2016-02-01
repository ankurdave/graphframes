package core.query;
import java.util.Set;
import java.util.Vector;

import core.query.plan.QueryPlanTriple;

public class Query {
	private GraphQuery graphQuery;
	private Vector<QueryPlanTriple> triples = new Vector<QueryPlanTriple>();
	
	public Query(String query) {
		graphQuery = new GraphQuery(query);
	}
	
	public GraphQuery getGraphQuery() {
		return graphQuery;
	}

	public Vector<QueryPlanTriple> getPlanTriples() {
		return triples;
	}

	public void setPlanTriples(Vector<QueryPlanTriple> t) {
		triples = t;
	}
	
	public boolean hasPlanTriples() {
		return (triples.size() > 0);
	}
	
	public boolean hasOnlyOneHead() {
		return graphQuery.hasOnlyOneHead();
	} 

	public String firsrHead() {
		return graphQuery.firstHead();
	}
	
	public void addPlanTriple(QueryPlanTriple triple) {
		triples.add(triple);
	}
	
	public int planTripleCount() {
		return triples.size();
	}
	
	public String getSQLSchema() {
		return graphQuery.getSQLSchema(); 
	}
	
	public Set<String> getAllVertexes() {
		return graphQuery.getAllVertexes();
	}

	public int getIndexOfVertex(String vertex) {
		return graphQuery.getIndexOfVertex(vertex);
	}
}