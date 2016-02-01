package core.algo.patternmatching.views;

import core.data.CostEstimator;
import core.query.plan.QueryPlanNode;

public class View extends Solution {

	public View(long partitioningKey, QueryPlanNode plan, CostEstimator estimator, double cost) {
		super(partitioningKey, plan, estimator, cost);
	}

}
