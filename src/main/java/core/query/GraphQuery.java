package core.query;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import core.data.Edge;

public class GraphQuery {
	static final String DELIMITER = " ";
	static final String IDTYPE = "int";
	//static final String IDTYPE = "text";
	
	private Vector<Edge> edges = new Vector<Edge>();
	private int numberOfValidEdges = 0;
	private String sqlSchema = "";
	
	public GraphQuery(String query) {
		String[] splits = query.split("\n");
		
		for (String s : splits) {
			String[] array = s.split(DELIMITER);
			// Without a filter
			if (array.length == 3)
				addEdge(array[0], array[1], array[2]);
			// With a filter
			else if (array.length > 3) {
				String filter = "";
				for (int i = 3; i < array.length; i++)
					filter += " " + array[i];
				addEdge(array[0], array[1], array[2], filter);
			}
			else {
			}
		}
	}
	
	public GraphQuery() {
		//edges = new Vector<Edge>();
		//numberOfValidEdges = 0;
	}
	
	public Vector<Edge> getEdges() {
		return edges;
	}

	public void addEdge(String head, String edgeLable, String tail, String filter) {
		edges.add(new Edge(head, edgeLable, tail, filter));
		numberOfValidEdges++;
	}
	
	public void addEdge(String head, String edgeLable, String tail) {
		edges.add(new Edge(head, edgeLable, tail));
		numberOfValidEdges++;
	}
	
	public void validateAllEdges() {
		for (Edge e : edges)
			e.validateEdge();
		numberOfValidEdges = edges.size();
	}
	
	public Vector<Edge> getValidEdgesWithHead(String v) {
		Vector<Edge> result = new Vector<Edge>();
		
		for (Edge e : edges) {
			if (e.getValid() && e.getHead().equals(v))
				result.add(e);
		}
		
		if (result.size() == 0)
			return null;
		else
			return result;
	}
	
	public int getNumberOfValidEdges() {
		return numberOfValidEdges;
	}
	
	public void invalidateEdge(Edge e) {
		e.invalidateEdge();
		numberOfValidEdges--;
	}
	
	public String getHeadWithValidEdges() {
		for (Edge e : edges) {
			if (e.getValid())
				return e.getHead(); 
		}
		
		return null;
	}
	
	public Set<String> getHeadVertexes() {
		HashSet<String> result = new HashSet<String>();
		
		for (Edge e : edges)
			result.add(e.getHead());
		
		return result;
	}
	
	public boolean hasOnlyOneHead() {
		String head = null;
		
		for (Edge e : edges) {
			if (head == null) {
				head = e.getHead();
				continue;
			}
			
			if (!head.equals(e.getHead()))
				return false;
		}
		
		return true;
	}

	public String firstHead() {
		return edges.get(0).getHead();
	}
	
	public Set<String> getAllVertexes() {
		HashSet<String> result = new HashSet<String>();
		
		for (Edge e : edges) {
			result.add(e.getHead());
			result.add(e.getTail());
		}
		
		return result;		
	}
	
	public String getSQLSchema() {
		if (sqlSchema.length() != 0)
			return sqlSchema;
		
		Set<String> vertexes = getAllVertexes();
		String[] vertexesArray = vertexes.toArray(new String[0]);
		java.util.Arrays.sort(vertexesArray);
		
		sqlSchema += "CREATE TABLE tmp\n(\n";
		
		for (String v: vertexesArray) {
			sqlSchema += "\t" + v + " " + IDTYPE + ",\n";
		}
		
		sqlSchema = sqlSchema.substring(0, sqlSchema.length() - 2);
		
		sqlSchema += "\n)";
		
		return sqlSchema;
	}
	
	public static Set<String> findCommonVertexes(GraphQuery q1, GraphQuery q2) {
		Set<String> result;
		
		result = q1.getAllVertexes();
		result.retainAll(q2.getAllVertexes());
		
		return result;
	}
	
	public static Set<String> findCommonVertexes(GraphQuery q, Edge e) {
		Set<String> result;
		
		Set<String> tmp = new HashSet<String>();
		tmp.add(e.getHead());
		tmp.add(e.getTail());
		
		result = q.getAllVertexes();
		result.retainAll(tmp);
		
		return result;
	}
	
	public int getIndexOfVertex(String vertex) {
		Set<String> vertexes = getAllVertexes();
		String[] vertexesArray = vertexes.toArray(new String[0]);
		java.util.Arrays.sort(vertexesArray);

		for (int i = 0; i < vertexesArray.length; i++) {
			if (vertexesArray[i].equals(vertex))
				return i;
		}

		return -1; // could not find a match
	}
	
	public boolean equals(Object obj){
		GraphQuery q = (GraphQuery)obj;
		
		if(q.edges.size()!=edges.size())
			return false;
		
		for(int i=0; i<edges.size(); i++)
			if(!edges.get(i).equals(q.edges.get(i)))
				return false;
		
		return true;
	}
}