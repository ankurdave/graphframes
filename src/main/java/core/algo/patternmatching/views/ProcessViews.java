package core.algo.patternmatching.views;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import core.algo.patternmatching.BushyDecomposition;
import core.algo.patternmatching.LayeredComponent;
import core.algo.patternmatching.LinearDecomposition;
import core.algo.patternmatching.ProcessInput;
import core.data.CostEstimator;
import core.data.Partitioning;
import core.query.GraphQuery;
import core.query.Query;
import core.query.ViewQuery;
import core.query.plan.Action;
import core.query.plan.ActionType;
import core.query.plan.QueryPlanNode;
import core.query.plan.QueryPlanNodeType;

public class ProcessViews extends ProcessInput{

	public static Vector<Solution> candidateViews = new Vector<Solution>(); 
	
	// 1. we need to create a different CostEstimator for each view
	// 2. add a view id on the query plan triple
	
	public static void findPlan(String q, Hashtable<String, Query> ht, int numberOfMachines, CostEstimator baseCE, Vector<View> views, String approach, Partitioning p) {
    	ViewQuery query = getQuery(ht, q); 
    	Vector<Solution> planSolutions = new Vector<Solution>();
    	
    	if (query.planSolutionCount() > 0)
    		return;
    	
    	if (query.hasEdgesCopartitioned(p)) {
    		QueryPlanNode node = new QueryPlanNode(query, q);
    		
    		Solution baseSol = new Solution(p.getKey(query.firstEdge()), node, baseCE, 0);	// not sure why the cost is set to 0!
    		planSolutions.add(baseSol);
    		matchViews(baseSol, views, planSolutions);
    		query.setPlanSolutions(planSolutions);
    		
    		candidateViews.addAll(query.getPlanSolutions());
    		
    		return;
    	} 
    	
    	Vector<LinearDecomposition> vld = linearDecomposition(query.getGraphQuery());
    	
    	//System.out.println("linearDecomposition size " + vld.size() + "\n");
    	
    	for (LinearDecomposition ld : vld) {
    		findPlan(ld.getOneHeadSubgraph(), ht, numberOfMachines, baseCE, views, approach, p);
    		findPlan(ld.getTheRest(), ht, numberOfMachines, baseCE, views, approach, p);
    		Query q1 = ht.get(ld.getOneHeadSubgraph());
    		Query q2 = ht.get(ld.getTheRest());
    		Vector<Solution> linearSolutions = computeLinearViewCosts(query, (ViewQuery)q2, (ViewQuery)q1, numberOfMachines);
    		planSolutions.addAll(linearSolutions);
    		
    		matchViews(linearSolutions, views, planSolutions);
    	}
    	
    	/********************************START OF BUSHY PLANS************************************************/
    	// TODO: fix the bushy plan once we fix the linear one ...
    	
    	if (approach.equals("DYNAMICPROGRAMMINGBUSHY")) {
    		findBushyPlan(q, numberOfMachines, ht, baseCE, views, approach, planSolutions, p);
    	}
    	/********************************END OF BUSHY PLANS**************************************************/
    	// 
    	query.setPlanSolutions(EliminateNonMinViewCosts(planSolutions));
    	
    	candidateViews.addAll(query.getPlanSolutions());
    	
    	//System.out.println(q + "\n" + planTriples.size() + "-----------\n");
    }
	
	private static void matchViews(Solution currentSolution, Vector<View> views, Vector<Solution> planSolutions){
		for(Solution v: views){
			if(v.getPlan().getQuery().getGraphQuery().equals(
					currentSolution.getPlan().getQuery().getGraphQuery()
				)){
				planSolutions.add(v);
			}
				
		}
	}
	
	private static void matchViews(Vector<Solution> currentSolutions, Vector<View> views, Vector<Solution> planSolutions){
		for(Solution currS: currentSolutions)
			matchViews(currS, views, planSolutions);			
	}
	
	
	public static void findBushyPlan(String q, int numberOfMachines, Hashtable<String, Query> ht, CostEstimator baseCE, Vector<View> views, String approach, Vector<Solution> planSolutions, Partitioning p) {
    	GraphQuery g = new GraphQuery(q);
    	ViewQuery query = getQuery(ht, q);
    	
    	Vector<Vector<LayeredComponent>> vvlc = generateComponents(g);
    	
    	//System.out.println(q);
    	//System.out.println("------------------------------DAGs--------------------------------------------------------------------------");
    	//System.out.println("aaaa");
    	//system.out.println("")
    	Vector<LayeredComponent> dags = generateDAGs(vvlc);   
    	//System.out.println(dags.size());
    	dags = removeDuplicateDAGs(dags);
    	
    	// Print out all the LayerComponents generated
    	/*
    	for (LayeredComponent lc : dags)
    		lc.print();    	
    	*/
    	
    	for (LayeredComponent lc : dags) {
    		// decompose the query
    		//System.out.println("---BushyDecomposition---");
    		BushyDecomposition bd = lc.BushyDecomposition();
    		
    		if (bd == null)
    			continue;
    		
    		// If there is no more than 3 components, do not do bushy decomposition.
    		if (bd.size() <= 2)
    			continue;
    		
    		for (int i = 0; i < bd.size(); i++) {
    			findPlan(bd.getDecomposition(i), ht, numberOfMachines, baseCE, views, approach, p);
    		}
    		
    		// construct a multi-way parallel hash join
    		double totalCost = 0;
    		Vector<QueryPlanNode> children = new Vector<QueryPlanNode>(); // This will be shared with the broadcast join
    		Vector<Action> actions = new Vector<Action>(); 
    		CostEstimator[] estimators = new CostEstimator[bd.size()];    		
    		
    		for (int i = 0; i < bd.size(); i++) {
    			String tmpQ = bd.getDecomposition(i);
    			ViewQuery tmpQuery = (ViewQuery)ht.get(tmpQ);
    			Vector<Solution> solutions = tmpQuery.getPlanSolutions();
    			double minCost = -1;
    			Solution minCostSolution = null;
    			boolean toMove = true;
    			
    			for (int j = 0; j < solutions.size(); j++) {
    				Solution solution = solutions.get(j);
    				double cost;
    				boolean toMoveofTriple;
    				
    				if (solution.getPartitioningKey()==bd.getJoinVertex().hashCode()) {
    					cost = solution.getCost();
    					toMoveofTriple = false;
    				} else {
    					// add the cost to move the query result
    					cost = solution.getCost() + solution.getEstimator().sizeEstimate(tmpQuery);
    					toMoveofTriple = true;
    				}
    				
    				if (minCost < 0 || minCost > cost) {
    					minCost = cost;
    					minCostSolution = solution;
    					toMove = toMoveofTriple;
    				}	    				
    			}
    			
    			estimators[i] = minCostSolution.getEstimator();
    				    		
    			// vertex = join vertex
    			totalCost += minCost;
    			children.add(minCostSolution.getPlan());
    			Action action;
    			
    			if (toMove)
    				action = new Action(ActionType.HASH, bd.getJoinVertex());
    			else
    				action = new Action(ActionType.STAY, null);
    				
    			actions.add(action);    			
    		}
    
    		CostEstimator combinedEstimator = CostEstimator.combine(estimators);
    		totalCost += combinedEstimator.sizeEstimate(query); // WARNING: join cost is included
    		QueryPlanNode node = new QueryPlanNode(QueryPlanNodeType.BUSHY_HASH_JOIN, query, bd.getJoinVertex(), children, actions);
    		Solution solution = new Solution(bd.getJoinVertex().hashCode(), node, combinedEstimator, totalCost);
    		//QueryPlanTriple triple = new QueryPlanTriple(bd.getJoinVertex(), totalCost, node);
    		planSolutions.add(solution);
    		matchViews(solution, views, planSolutions);
    		
    		/*
    		System.out.println("construct a bushy join");
			System.out.println("cost:" + totalCost);
			System.out.println(node.toString(0, ce));
    		*/
    		
    		//System.out.println("# bd: " + bd.size());
    		//System.out.println("construct a bushy hash join");
    		
    		// construct a multi-way broadcast join for each subquery as the stay-put query
    		
    		// the sum of size of each subquery
    		double sumOfSizes = 0;
    		// the sum of cost of each subquery. min cost of each query is used here
    		double sumOfSubqueryMinCosts = 0;
    		// the triples of min cost of each subquery
    		Vector<Solution> minCostSolutions = new Vector<Solution>();
    		for (int i = 0; i < bd.size(); i++) {
    			String tmpQ = bd.getDecomposition(i);
    			Query tmpQuery = ht.get(tmpQ);
    			Solution tmp = findMinCostSolution(tmpQ, ht);
    			sumOfSizes += tmp.getEstimator().sizeEstimate(tmpQuery);
    			minCostSolutions.add(tmp); 
    			sumOfSubqueryMinCosts += tmp.getCost();
    		}
    		
    		for (int i = 0; i < bd.size(); i++) {
    			String tmpQ = bd.getDecomposition(i);
    			ViewQuery tmpQuery = (ViewQuery)ht.get(tmpQ);
    			// The sum of min cost of subqueries expect  
    			//double sumOfOtherSubqueryMinCosts = sumOfSubqueryMinCosts - findMinCostTriple(tmpQ, ht).getCost();
    					
    			Vector<Solution> solutions = tmpQuery.getPlanSolutions();
    			//System.out.println("#triples:" + triples.size());
    			
    			for (Solution s: solutions) {
    				// first subtract the min cost of current subquery from sumOfSubqueryMinCosts,
    				// and then add the cost of current triple
    				double costsOfAllSubqueries = sumOfSubqueryMinCosts - findMinCostSolution(tmpQ, ht).getCost() + s.getCost();
    				// The network cost of the broadcast join
    				double sumOfSizesOfOtherSubqueries = sumOfSizes - s.getEstimator().sizeEstimate(tmpQuery);
    				totalCost = costsOfAllSubqueries + sumOfSizesOfOtherSubqueries * numberOfMachines + s.getEstimator().sizeEstimate(query); // WARNING: join cost is included
    				//totalCost = costsOfAllSubqueries + sumOfSizesOfOtherSubqueries * numberOfMachines;
    				// i-th node in the children vector is the subquery that stays put in the broadcast join
    				actions = new Vector<Action>();
    				for (int j = 0; j < bd.size(); j++) {
    					Action action;
    					if (i == j)
    						action = new Action(ActionType.STAY, null);
    					else
    						action = new Action(ActionType.BROADCAST, null);
    					
    					actions.add(action);
    				}
    				
    				QueryPlanNode newNode = new QueryPlanNode(QueryPlanNodeType.BUSHY_BROADCAST_JOIN, query, bd.getJoinVertex(), children, actions);  
    				//QueryPlanTriple newTriple = new QueryPlanTriple(s.getVertex(), totalCost, newNode);
    				Solution newSolution = new Solution(s.getPartitioningKey(), newNode, s.getEstimator(), totalCost);
    				planSolutions.add(newSolution);
    				matchViews(newSolution, views, planSolutions);
    				
    				//newNode.toString(0);
    				/*
    				System.out.println("construct a broadcast join");
    				System.out.println("cost:" + totalCost);
    				System.out.println(newNode.toString(0));
    				*/
    			}
    		}
    		
    		
    	}    	
    }
	
	
	public static Vector<Solution> computeLinearViewCosts(ViewQuery query, ViewQuery q1, ViewQuery q2, int numberOfMachines) {
    	Vector<Solution> solns = new Vector<Solution>();
    	
    	Vector<Solution> solutions1 = q1.getPlanSolutions();
    	Vector<Solution> solutions2 = q2.getPlanSolutions();
    	
    	Hashtable<Long, Double> ht = new Hashtable<Long, Double>();
    	
    	for (Solution s1: solutions1) {
    		for (Solution s2: solutions2) {
    			CostEstimator qEstimator = CostEstimator.combine(s1.getEstimator(), s2.getEstimator());
    			Set<String> commonVertexes = GraphQuery.findCommonVertexes(q1.getGraphQuery(), q2.getGraphQuery());
    			for (String v: commonVertexes) {
    				double costOft1Andt2 = s1.getCost() + s2.getCost();
    				//double costOfJoinInDB = ce.sizeEstimate(query) + ce.sizeEstimate(q1) + ce.sizeEstimate(q2);
    				
    				// Directed Join: q1 to q2
    				if (s2.getPartitioningKey()==v.hashCode()) {
        				compareCost(query, 
        						    costOft1Andt2 + s1.getEstimator().sizeEstimate(q1) + qEstimator.sizeEstimate(query),   // WARNING: total size of the query is added and other joins
    								ht, 
    								v,
    								v.hashCode(),
    								s1.getPlan(), 
    								s2.getPlan(),
    								qEstimator,
    								solns, 
    								QueryPlanNodeType.DIRECTED_JOIN_Q1_TO_Q2);				
    				}
    				// Directed Join: q2 to q1
    				else if (s1.getPartitioningKey()==v.hashCode()) {
        				compareCost(query, costOft1Andt2 + s2.getEstimator().sizeEstimate(q2) + qEstimator.sizeEstimate(query),
    								ht, 
    								v,
    								v.hashCode(),
    								s1.getPlan(), 
    								s2.getPlan(),
    								qEstimator,
    								solns, 
    								QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1);				
    				}
    				// Only use Parallel Hash Join if Directed Join is not applicable    		
    				else {
        				compareCost(query, costOft1Andt2 + s1.getEstimator().sizeEstimate(q1) + s2.getEstimator().sizeEstimate(q2) + qEstimator.sizeEstimate(query),
        							ht, 
        							v,
        							v.hashCode(),
        							s1.getPlan(), 
        							s2.getPlan(), 
        							qEstimator,        							
        							solns, 
        							QueryPlanNodeType.HASH_JOIN);
    				}
    				
    				// Broadcast Join: q1 to q2
    				compareCost(query, costOft1Andt2 + s1.getEstimator().sizeEstimate(q1) * numberOfMachines + qEstimator.sizeEstimate(query),
								ht, 
								v, 
								s2.getPartitioningKey(),
								s1.getPlan(), 
								s2.getPlan(), 
								qEstimator,
								solns, 
								QueryPlanNodeType.BROADCAST_JOIN_Q1_TO_Q2);
    				
    				// Broadcast Join: q2 to q1
    				compareCost(query, costOft1Andt2 + s2.getEstimator().sizeEstimate(q2) * numberOfMachines + qEstimator.sizeEstimate(query),
								ht, 
								v, 
								s1.getPartitioningKey(),
								s1.getPlan(), 
								s2.getPlan(), 
								qEstimator,
								solns, 
								QueryPlanNodeType.BROADCAST_JOIN_Q2_TO_Q1);
    			}
    		}
    	}
    	
    	return solns;
    }
	
	public static void compareCost(Query query, double cost, Hashtable<Long, Double> ht, String joinVertex, long partitioningKey, QueryPlanNode n1, QueryPlanNode n2, CostEstimator ce, Vector<Solution> triples, QueryPlanNodeType type) {
		Double minCostForV = ht.get(partitioningKey);
		if (minCostForV == null || cost < minCostForV) {
			QueryPlanNode node = new QueryPlanNode(query, type, joinVertex, n1, n2);
			Solution solution = new Solution(
										partitioningKey, 
										node, 
										ce, 
										cost
									);
			triples.add(solution); 
			ht.put(partitioningKey, cost);
		}    	
    }
	
	
	public static Vector<Solution> EliminateNonMinViewCosts(Vector<Solution> solutions) {
    	HashSet<Long> pKeys = new HashSet<Long>();
    	Vector<Solution> result = new Vector<Solution>();
    	
    	for (Solution s: solutions)
    		pKeys.add(s.getPartitioningKey());
    	
    	for (Long v: pKeys) {
    		Solution minCostSolution = null;
    		
    		for (Solution s : solutions) {
    			if (v.equals(s.getPartitioningKey())) {
    				if (minCostSolution == null || s.getCost() < minCostSolution.getCost()) {
    					minCostSolution = s;
    				}
    			}
    		}
    		
    		result.add(minCostSolution);
    	}
    	
//    	for (Solution s : solutions)
//    		System.out.println("vertex:" + s.getPartitioningKey() + "\t\tcost:" + s.getCost() + "\t\tplan:" + s.getPlan().toString() + "\n-----------\n");
    	
    	return result;
//    	System.out.println("triples.size() after elimination: " + solutions.size() + "\n");
    }
	
	
	public static ViewQuery getQuery(Hashtable<String, Query> ht, String query) {
    	ViewQuery result = (ViewQuery)ht.get(query);
    	//System.out.print(query);
    	if (result == null) {
    		result = new ViewQuery(query);
    		ht.put(query, result);
    	}    	
    	return result;    	
    }
	
	public static Solution findMinCostSolution(String inputQuery, Hashtable<String, Query> ht) {
	    ViewQuery query = (ViewQuery)ht.get(inputQuery);
	    double minCost = -1;
        Solution minCostSolution = null;

  	    for (Solution s : query.getPlanSolutions()) {
  	    	if (minCost < 0 || minCost > s.getCost()) {
  	    		minCost = s.getCost();
  	    		minCostSolution = s;
  	    	}
  	    }
  	    
  	    return minCostSolution;    	
    }
}
