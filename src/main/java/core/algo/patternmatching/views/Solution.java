package core.algo.patternmatching.views;

import core.data.CostEstimator;
import core.query.plan.QueryPlanNode;

public class Solution implements Comparable<Solution> {

	long partitioningKey;				// Abadi's paper has a partitioning by source vertex; but it could more general
	private double cost;				// the cost of this solution
	private QueryPlanNode plan;			// the query plan in this solution
	private CostEstimator estimator;	// the cost estimator for this solution
	
	public Solution(long partitioningKey, QueryPlanNode plan, CostEstimator estimator, double cost){
		//this.partitioning = partitioning;
		this.plan = plan;
		this.estimator = estimator;
		this.cost = cost;
	}

	public long getPartitioningKey() {
		return partitioningKey;
	}

	public double getCost() {
		return cost;
	}

	public QueryPlanNode getPlan() {
		return plan;
	}

	public CostEstimator getEstimator() {
		return estimator;
	}

	@Override
	public int compareTo(Solution obj) {
		if(cost < obj.cost)
			return -1;
		else if(cost > obj.cost)
			return 1;
		else
			return 0;
	}
}
