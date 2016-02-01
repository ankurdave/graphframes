package core.query.plan;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import core.data.CostEstimator;
import core.data.Edge;
import core.query.Query;

public class QueryPlanNode {
	private final String EDGE_TABLE_NAME = "edges";
	private final String ATTRIBUTE_TABLE_NAME = "attributes";	
	private final String CHILD_TABLE_NAME = "t";
	//public final String LEFTCHILDTABLENAME = "t1";
	//public final String RIGHTCHILDTABLENAME = "t2";
	
	private Vector<QueryPlanNode> children = new Vector<QueryPlanNode>();
	private Vector<Action> actions;
	private String stringQuery; 						// only used in collocated join
	private QueryPlanNodeType type;
	private String joinVertex;
	private Query query;
	private String nodeID;								// used by the plan executor as the name of intermediate files/tables  
	private String joinQuery;
	
	public QueryPlanNode(Query query, String stringQuery) {
		this.type = QueryPlanNodeType.LOCAL_EXECUTION;
		this.stringQuery = stringQuery;
		this.query = query;
	}
	
	public QueryPlanNode(Query query, QueryPlanNodeType type, String joinVertex, QueryPlanNode leftChild, QueryPlanNode rightChild) {
		this.type = type;
		this.joinVertex = joinVertex;
		children.add(leftChild);
		children.add(rightChild);
		this.query = query;
		actions = new Vector<Action>();
		Action action1 = null;
		Action action2 = null;
		
		switch(type) {
		    case COLLOCATED_JOIN:
				action1 = new Action(ActionType.STAY, null);
				action2 = new Action(ActionType.STAY, null);
				break;
			case DIRECTED_JOIN_Q1_TO_Q2:
				action1 = new Action(ActionType.HASH, joinVertex);
				action2 = new Action(ActionType.STAY, null);
				break;
			case DIRECTED_JOIN_Q2_TO_Q1:
				action1 = new Action(ActionType.STAY, null);
				action2 = new Action(ActionType.HASH, joinVertex);
				break;	
			case HASH_JOIN:
				action1 = new Action(ActionType.HASH, joinVertex);
				action2 = new Action(ActionType.HASH, joinVertex);
				break;		
			case BROADCAST_JOIN_Q1_TO_Q2:
				action1 = new Action(ActionType.BROADCAST, null);
				action2 = new Action(ActionType.STAY, null);
				break;
			case BROADCAST_JOIN_Q2_TO_Q1:
				action1 = new Action(ActionType.STAY, null);
				action2 = new Action(ActionType.BROADCAST, null);
				break;
			default:
		        System.out.println("QueryPlanNodeType " + type + " is not known.");
		}
		actions.add(action1);
		actions.add(action2);
	}

	// This is only use by BUSHYHASHJOIN
	public QueryPlanNode(QueryPlanNodeType type, Query query, String joinVertex, Vector<QueryPlanNode> children, Vector<Action> actions) {
		this.type = type;
		this.joinVertex = joinVertex;
		this.children = children;
		this.query = query;
		this.actions = actions;
	}

	/*
	// This is only use by BUSHYBROADCASTJOIN
	public QueryPlanNode(Query query, String joinVertex, Vector<QueryPlanNode> children, Vector<Action> actions) {
		this.type = QueryPlanNodeType.BUSHY_BROADCAST_JOIN;
		this.joinVertex = joinVertex;
		this.children = children;
		this.query = query; 
		this.childToStay = childToStay;
	}	
	*/
	
	// Hard code the join query
	public void setJoinQuery(String joinQuery){
		this.joinQuery = joinQuery;
	}
	
	public void setNodeID(String nodeID) {
		this.nodeID = nodeID;
	}
	
	public String getNodeID() {
		return nodeID;
	}	
	
	public Query getQuery() {
		return query;
	}
	
	public Vector<Action> getActions() {
		return actions;
	}
	
	public Set<String> getAllVertexes() {
		return query.getAllVertexes();
	}
	
	public Vector<QueryPlanNode> getAllChildren() {
		return new Vector<QueryPlanNode>(children);
	}
  
	public QueryPlanNodeType getType() {
		return type;
	}

	public int getIndexOfVertex(String vertex) {
		return query.getIndexOfVertex(vertex);
	}

	/*
	public String getJoinVertex() {
		return joinVertex;
	}
  	*/
	
	public String getSQLSchema() {
                //System.out.println("\n!!!!!!!!!!!!Schema---------------\n" + query.getSQLSchema() + "\n---------------------------\n");

		return query.getSQLSchema();
	}
	
	public void assignNodeID() {
		LinkedList<QueryPlanNode> queue = new LinkedList<QueryPlanNode>();
		int count = 0;
		queue.add(this);
		
		while (!queue.isEmpty()) {
			QueryPlanNode node = queue.remove();
			node.setNodeID("N" + count);
			count++;
			for (QueryPlanNode child : node.getAllChildren())
				queue.add(child);
		}
		
		//printNodeID();
	}
	
	@SuppressWarnings("unused")
	private void printNodeID() {		
		LinkedList<QueryPlanNode> queue = new LinkedList<QueryPlanNode>();
		queue.add(this);
		
		while (!queue.isEmpty()) {
			QueryPlanNode node = queue.remove();
			System.out.println(node.getNodeID());
			for (QueryPlanNode child : node.getAllChildren())
				queue.add(child);
		}
	}
/*	
	public boolean Q1StaysPut() {
		if (type == QueryPlanNodeType.COLLOCATED_JOIN || 
			type == QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1 ||
			type == QueryPlanNodeType.BROADCAST_JOIN_Q2_TO_Q1 )
			return true;
		
		return false;
	}

	public boolean Q2StaysPut() {
		if (type == QueryPlanNodeType.COLLOCATED_JOIN || 
			type == QueryPlanNodeType.DIRECTED_JOIN_Q1_TO_Q2 ||
			type == QueryPlanNodeType.BROADCAST_JOIN_Q1_TO_Q2 )
			return true;
		
		return false;
	}	
*/	
	public String getJoinQuery(Vector<String> inputTables) {
		// return the join query if it has been hard coded
		if (joinQuery != null)
			return joinQuery;
		
		String result = "";
		String projection = "";
		String fromClause = "";
		String selection = "";
		
		if (type == QueryPlanNodeType.LOCAL_EXECUTION) {
	
			Hashtable<String, String> vertexes = new Hashtable<String, String>();
			int attributeTableCount = 0;
			boolean hasFilter = false;
			
			if (query == null)
				System.out.println("query == null");
				
			Vector<Edge> edges = query.getGraphQuery().getEdges();
			
			// Construct the from clause and the selection
			for (int i = 0; i < edges.size(); i++) {
				Edge e = edges.get(i);
				
				String tableAlias = "e" + i;
				
				if (!fromClause.equals("")) 
					fromClause += ",\n";
				
				// Add an edge table for each edge in the graph
				fromClause += "\t" + EDGE_TABLE_NAME + " " + tableAlias;
				
				String head = e.getHead();
				String headColumn = vertexes.get(head);
				
				if (headColumn == null)
					vertexes.put(head, tableAlias + ".head");
				else {
					if (!selection.equals(""))
						selection += " AND\n";
					
					// Add a join between two columns representing the same vertex  
					selection += "\t" + headColumn + " = " + tableAlias + ".head";
				}

				String tail = e.getTail();
				String tailColumn = vertexes.get(tail);
				
				if (tailColumn == null)
					vertexes.put(tail, tableAlias + ".tail");
				else {
					if (!selection.equals(""))
						selection += " AND\n";
					
					// Add a join between two columns representing the same vertex  
					selection += "\t" + tailColumn + " = " + tableAlias + ".tail";
				}
				
				// When a filter exists				
				if (e.hasFilter()) {
					hasFilter = true;
					String filter = e.getFilter().trim();
					
					while(true) {
						Pattern pattern = Pattern.compile("[A-Z].attribute");
						Matcher matcher = pattern.matcher(filter);
						if (matcher.find()) {
							String attributeTableAlias = "a" + attributeTableCount;
							attributeTableCount++;
							String match = matcher.group(0);
							String vertex = match.substring(0, match.indexOf('.'));
							// Add the attribute table to the from clause
						    fromClause += ",\n\t" + ATTRIBUTE_TABLE_NAME + " " + attributeTableAlias;
						    
							if (!selection.equals(""))
								selection += " AND\n";
							// Update the filter with the attribute table
							filter = filter.replaceFirst(vertex + ".attribute", attributeTableAlias + ".attribute");
							filter = filter.replaceFirst(vertex + ".value", attributeTableAlias + ".value");
							// Add the attribute table to the selection clause
							selection += "\t" + attributeTableAlias + ".id = " + vertexes.get(vertex);						    
						} else
						    break;
					}
					
					// Add the filter to selection
					selection += " AND\n\t" + filter;
				}
			}
			
			String[] vertexesArray = vertexes.keySet().toArray(new String[0]);
			java.util.Arrays.sort(vertexesArray);
			
			// Construct the projection
			for (String v : vertexesArray) {
				String column = vertexes.get(v);
				
				if (!projection.equals(""))
					projection += ",\n";
				
				projection += "\t" + column + " AS " + v;				
			}
			
			if (hasFilter)
				projection = "DISTINCT " + projection;
				
			// To make sure that different vertexes are actually different
			for (int i = 0; i < vertexesArray.length; i++) {
			   for (int j = i + 1; j < vertexesArray.length; j++) {
			      if (!selection.equals(""))
			          selection += " AND\n";

			      selection += "\t" + vertexes.get(vertexesArray[i]) + " <> " + vertexes.get(vertexesArray[j]); 
			   }
			}   

			result = "SELECT\n" + projection + "\nFROM\n" + fromClause;
			
			if (!selection.equals(""))
				result += "\nWHERE\n" + selection;
			
			//System.out.println("\n---------------------------\n" + result + "\n---------------------------\n");
		}
		else {			
			Set<String> vertexes = new HashSet<String>();
			for (QueryPlanNode n : children)
				vertexes.addAll(n.getAllVertexes());
			
			String[] vertexesArray = vertexes.toArray(new String[0]);
			java.util.Arrays.sort(vertexesArray);

			for (String v : vertexesArray) {
				
				if (!projection.equals(""))
					projection += ",\n";
				
				projection += "\t" + v;				
			}
		
			for (int i = 0; i < vertexesArray.length; i++) {
			   for (int j = i + 1; j < vertexesArray.length; j++) {
			      if (!selection.equals(""))
			          selection += " AND\n";

			      selection += "\t" + vertexesArray[i] + " <> " + vertexesArray[j]; 
			   }
			}   
			
			if (inputTables == null) {
				for (int i = 0; i < children.size() - 1; i++) {
					fromClause += "\t" + CHILD_TABLE_NAME + (i + 1) + " NATURAL JOIN\n"; 
				}
				fromClause += "\t" + CHILD_TABLE_NAME + (children.size()); 
			} else {
				for (int i = 0; i < inputTables.size() - 1; i++) {
					fromClause += "\t" + inputTables.get(i) + " NATURAL JOIN\n"; 
				}
				fromClause += "\t" + inputTables.get(inputTables.size() - 1); 				
			}
			
			result = "ANALYZE; SELECT\n" + projection + "\nFROM\n" + fromClause + "\nWHERE\n" + selection;
		}
		
		return result;
	}
	
	public String toString(int indent, CostEstimator ce) {
		String result = "";

		String indentString = "";

		for (int i = 0; i < indent; i++)
			 indentString += " ";
		
		if (type == QueryPlanNodeType.LOCAL_EXECUTION) {
			//System.out.println(query);
			
		    //result.charAt(index);

			//getJoinQuery();
			//getSQLSchema();
		    
			result = indentString + stringQuery.replaceAll("\n", ";") + "\t\t\tsize:" + ce.sizeEstimate(query)  + "\n";
			return result;
		}
		
		int count = 0;
		//result += "\n";
		for (QueryPlanNode node : children) {
			//System.out.println("children");
			result += indentString + "child :" + count + "\n" + node.toString(indent + 4, ce);
			count++;
		}
		
		//if (type == QueryPlanNodeType.BUSHY_PLAN) {
			result += indentString + type + " on " + joinVertex + " (Actions:";
			
			for (Action a : actions)
				result += " " + a.getType();
			
			//result += ")" + "\t\t\tsize:" + ce.sizeEstimate(query)  + "\n";
			result += ")" + "\n";
		/*	
		} else {
			//result += indentString + type + " on " + joinVertex + "(Q1 stays put:" + Q1StaysPut() + " Q2 stays put:" + Q2StaysPut() + ")" + "\t\t\tsize:" + ce.sizeEstimate(query)  + "\n";
			result += indentString + type + " on " + joinVertex + "(Q1 stays put:" + Q1StaysPut() + " Q2 stays put:" + Q2StaysPut() + ")" + "\n";
		}
		*/
			
		getJoinQuery(null);
		getSQLSchema();
		
		return result;
	}
}
