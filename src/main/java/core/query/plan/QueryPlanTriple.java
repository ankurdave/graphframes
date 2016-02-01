package core.query.plan;
/*
 * QueryPlanTriple is a class that stores the information of 
 */

public class QueryPlanTriple {
	private String vertex;
	private double cost;
	private QueryPlanNode plan;
	
	public QueryPlanTriple(String vertex, double cost, QueryPlanNode plan) {
		this.vertex = vertex;
		this.cost = cost;
		this.plan = plan;
	}
	
	public String getVertex() {
		return vertex;
	}
	
	public double getCost() {
		return cost;
	}
	
	public QueryPlanNode getPlan() {
		return plan;
	}	
}
