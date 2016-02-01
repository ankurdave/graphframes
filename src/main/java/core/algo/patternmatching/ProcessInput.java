package core.algo.patternmatching;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import core.data.CostEstimator;
import core.data.Edge;
import core.query.GraphQuery;
import core.query.Query;
import core.query.plan.Action;
import core.query.plan.ActionType;
import core.query.plan.QueryPlanNode;
import core.query.plan.QueryPlanNodeType;
import core.query.plan.QueryPlanTriple;
import core.query.plan.execute.PlanExecutor;

public class ProcessInput {
	
	static final String DELIMITER = " ";
	
    @SuppressWarnings({ "unused", "rawtypes", "unchecked" })
	public static void main(String[] args) {
    	String input = args[0];
    	int numberOfMachines = Integer.parseInt(args[1]);
        String optimizationApproach = args[2];
        String dataset = args[3]; // amazon,  youtube or something else
        String configuration = args[4];
        String planName = args[5];
        
        String hostFile = "../config/" + numberOfMachines + "-nodes.txt";
        String databaseName = dataset + "-" + numberOfMachines;
        String statsFile = "../stats/" + dataset + ".stats";
        
    	CostEstimator ce = new CostEstimator(statsFile);
    	
    	Hashtable<String, Query> ht = new Hashtable<String, Query>();
    	
    	String inputQuery = GenerateInputString(input);

    	/*
    	GraphQuery g = new GraphQuery(inputQuery);
    	
    	Vector<Vector<LayeredComponent>> vvlc = generateComponents(g);
    	
    	System.out.println("------------------------------DAGs--------------------------------------------------------------------------");
    	
    	Vector<LayeredComponent> dags = generateDAGs(vvlc);   
    	dags = removeDuplicateDAGs(dags);
    	
    	for (LayeredComponent lc : dags)
    		lc.print();    	
    	
    	for (LayeredComponent lc : dags) {
    		System.out.println("---BushyDecomposition---");
    		lc.BushyDecomposition();
    	}
    	*/
    	
    	double cost = 0;
    	QueryPlanNode plan = null;
 	
    	if (optimizationApproach.equals("GREEDYPLAN")) {
    		plan = findGreedyPlan(inputQuery, ht, numberOfMachines, ce);
    	} 
    	else if (optimizationApproach.equals("GREEDYPLANWITHGROUPING")) {
    	} 
    	else if (optimizationApproach.equals("DYNAMICPROGRAMMINGLINEAR") || optimizationApproach.equals("DYNAMICPROGRAMMINGBUSHY")) {
    	    findPlan(inputQuery, ht, numberOfMachines, ce, optimizationApproach);
    	    QueryPlanTriple triple = findMinCostTriple(inputQuery, ht);
    	    cost = triple.getCost();
    	    plan = triple.getPlan();
    	} 
        
        /*
        Vector<QueryPlanNode> children = plan.getAllChildren();
        for (QueryPlanNode n : children) {
        	System.out.println("-------A Child------");
        	System.out.println(n.getJoinQuery(null));
        	System.out.println(n.getSQLSchema());
        }
        */
        
        plan.assignNodeID();
        
        //plan = hardCodedQuery0(ht);
        //plan = hardCodedQuery1(ht);
        
        ProcessInput pi = new ProcessInput();
        Class piClass = pi.getClass();
        
        if (!planName.equals("null")){
        	try {
	            Method method = piClass.getMethod(planName, Hashtable.class);
	            Object obj = method.invoke(piClass, ht);
	            plan = (QueryPlanNode) obj;
            } catch (Exception e) {
	            e.printStackTrace();
            }
        } //
        
    	//System.out.println("cost:" + cost + "\n");
        if (planName.equals("null"))
        	System.out.println(plan.toString(0, ce));
        //System.out.println(plan.getSQLSchema());
        //System.out.println(plan.getJoinQuery(null));
        
        PlanExecutor executor = new PlanExecutor(numberOfMachines, hostFile, databaseName, configuration);
        executor.execute(plan);
    }
    
    // Return the triple with the min cost given a query. We assume that laborious plan generation has been performed by method findPlan 
    private static QueryPlanTriple findMinCostTriple(String inputQuery, Hashtable<String, Query> ht) {
	    Query query = ht.get(inputQuery);
	    double minCost = -1;
        QueryPlanTriple minCostPlanTriple = null;

  	    for (QueryPlanTriple t : query.getPlanTriples()) {
  	    	if (minCost < 0 || minCost > t.getCost()) {
  	    		minCost = t.getCost();
  	    		minCostPlanTriple = t;
  	    	}
  	    }
  	    
  	    return minCostPlanTriple;    	
    }
    
    public static QueryPlanNode findGreedyPlan(String q, Hashtable<String, Query> ht, int numberOfMachines, CostEstimator ce) {
    	String currentQuery = "";
    	String hashVertex = "";
    	
    	GraphQuery gq = new GraphQuery(q);
    	Vector<Edge> edges = gq.getEdges();
    	
    	Edge minSizeEdge = null;
    	double minSize = -1;
    	
    	for (Edge e : edges) {
    		double size = ce.sizeEstimate(getQuery(ht, e.toString()));
    		
    		if (minSizeEdge == null || size < minSize) {
    			minSizeEdge = e;
    			minSize = size;
    		}
    	}
    	
    	currentQuery = minSizeEdge.toString();
    	gq.invalidateEdge(minSizeEdge);
    
    	hashVertex = minSizeEdge.getHead();
    	QueryPlanNode currentQueryNode = new QueryPlanNode(getQuery(ht, currentQuery), currentQuery);
    	
    	while(gq.getNumberOfValidEdges() > 0) {
    		Edge lowestCostEdge = null;
    		double lowestCost = 0;
    		String lowestCostHashVertex = null;
    		String lowestCostJoinVertex = null;
    		QueryPlanNodeType lowestCostJoinType = null;
    		
    		edgeLoop:
    		for (Edge e : edges) {
    			if (!e.getValid())
    				continue;
    	        		
        		Query q1 = getQuery(ht, currentQuery);
        		Query q2 = getQuery(ht, e.toString());
    			
    			Set<String> commonVertexes = GraphQuery.findCommonVertexes(q1.getGraphQuery(), q2.getGraphQuery());
    			
    			for (String v : commonVertexes) {
    				if (hashVertex.equals(e.getHead())) {
    					lowestCostEdge = e;
    					lowestCostHashVertex = v;
    					lowestCostJoinVertex = v;
    					lowestCostJoinType = QueryPlanNodeType.COLLOCATED_JOIN;
    					break edgeLoop;
    				}

    				double joinCost = ce.sizeEstimate(q2) * numberOfMachines;
    				//System.out.println("joinCost:" + joinCost);
					if (lowestCostEdge == null || joinCost < lowestCost) {
    					lowestCostEdge = e;
    					lowestCostHashVertex = hashVertex;
    					lowestCostJoinVertex = v;
    					lowestCost = joinCost;
    					lowestCostJoinType = QueryPlanNodeType.BROADCAST_JOIN_Q2_TO_Q1;
					}
			
					joinCost = ce.sizeEstimate(q1) * numberOfMachines;
					if (joinCost < lowestCost) {
    					lowestCostEdge = e;
    					lowestCostHashVertex = e.getHead();
    					lowestCostJoinVertex = v;
    					lowestCost = joinCost;
    					lowestCostJoinType = QueryPlanNodeType.BROADCAST_JOIN_Q1_TO_Q2;
					} 
					
    				if (e.getHead().equals(v)) {
    					joinCost = ce.sizeEstimate(q1);
    					if (joinCost < lowestCost) {
        					lowestCostEdge = e;
        					lowestCostHashVertex = v;
        					lowestCostJoinVertex = v;
        					lowestCost = joinCost;
        					lowestCostJoinType = QueryPlanNodeType.DIRECTED_JOIN_Q1_TO_Q2;
    					}
    				}
    				else if (hashVertex.equals(v)) {
    					joinCost = ce.sizeEstimate(q2);
    					if (joinCost < lowestCost) {
        					lowestCostEdge = e;
        					lowestCostHashVertex = v;
        					lowestCostJoinVertex = v;
        					lowestCost = joinCost;
        					lowestCostJoinType = QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1; 
    					}    					
    				} 
    				else {
    					joinCost = ce.sizeEstimate(q2) + ce.sizeEstimate(q1);
    					if (joinCost < lowestCost) {
        					lowestCostEdge = e;
        					lowestCostHashVertex = v;
        					lowestCostJoinVertex = v;
        					lowestCost = joinCost; 
        					lowestCostJoinType = QueryPlanNodeType.HASH_JOIN;  
    					}        					
    				}	
    			}
    		}
    		
    		//System.out.println("totalCost:" + totalCost + "\t\tlowestCost:" + lowestCost);
    		hashVertex = lowestCostHashVertex;
    		currentQuery += lowestCostEdge.toString();
    		//currentQuery = currentQuery.substring(0, currentQuery.length() - 1);
    		
    		//System.out.println(currentQuery);
    		
    		QueryPlanNode node = new QueryPlanNode(getQuery(ht, lowestCostEdge.toString()), lowestCostEdge.toString());
    		currentQueryNode = new QueryPlanNode(getQuery(ht, currentQuery), lowestCostJoinType, lowestCostJoinVertex, currentQueryNode, node);
    		
    		gq.invalidateEdge(lowestCostEdge);
    	} 
    	
    	//System.out.println(currentQueryNode.getSQLSchema());
    	
    	//System.out.println("cost: " + totalCost + "\n" + currentQueryNode.toString(0));
       
      return currentQueryNode;
      //PlanExecutor executor = new PlanExecutor(currentQueryNode, numberOfMachines);
      //executor.execute();
    }
    
    // Return the query information from a hash table given a query in string
    public static Query getQuery(Hashtable<String, Query> ht, String query) {
    	Query result = ht.get(query);
    	//System.out.print(query);
    	if (result == null) {
    		result = new Query(query);
    		ht.put(query, result);
    	}
    	
    	return result;
    	
    } 
    
    public static void findPlan(String q, Hashtable<String, Query> ht, int numberOfMachines, CostEstimator ce, String approach) {
    	Query query = getQuery(ht, q); 
    	Vector<QueryPlanTriple> planTriples = new Vector<QueryPlanTriple>();
    	
    	if (query.planTripleCount() > 0)
    		return;
    	
    	if (query.hasOnlyOneHead()) {
    		QueryPlanNode node = new QueryPlanNode(query, q);
    		QueryPlanTriple triple = new QueryPlanTriple(query.firsrHead(), 0, node);
    		query.addPlanTriple(triple);
            
    		return;
    	} 
    	
    	Vector<LinearDecomposition> vld = linearDecomposition(query.getGraphQuery());
    	
    	//System.out.println("linearDecomposition size " + vld.size() + "\n");
    	
    	for (LinearDecomposition ld : vld) {
    		findPlan(ld.getOneHeadSubgraph(), ht, numberOfMachines, ce, approach);
    		findPlan(ld.getTheRest(), ht, numberOfMachines, ce, approach);
    		Query q1 = ht.get(ld.getOneHeadSubgraph());
    		Query q2 = ht.get(ld.getTheRest());
    		planTriples.addAll(computeLinearCosts(query, q2, q1, numberOfMachines, ce));
    	}
    	
    	/********************************START OF BUSHY PLANS************************************************/
    	if (approach.equals("DYNAMICPROGRAMMINGBUSHY")) {
    		findBushyPlan(q, numberOfMachines, ht, ce, approach, planTriples);
    	}
    	/********************************END OF BUSHY PLANS**************************************************/
    	// 
    	query.setPlanTriples(EliminateNonMinCosts(planTriples));
    	
    	//System.out.println(q + "\n" + planTriples.size() + "-----------\n");
    }
    
    public static void findBushyPlan(String q, int numberOfMachines, Hashtable<String, Query> ht, CostEstimator ce, String approach, Vector<QueryPlanTriple> planTriples) {
    	GraphQuery g = new GraphQuery(q);
    	Query query = getQuery(ht, q);
    	
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
    			findPlan(bd.getDecomposition(i), ht, numberOfMachines, ce, approach);
    		}
    		
    		// construct a multi-way parallel hash join
    		double totalCost = 0;
    		Vector<QueryPlanNode> children = new Vector<QueryPlanNode>(); // This will be shared with the broadcast join
    		Vector<Action> actions = new Vector<Action>(); 
    		
    		for (int i = 0; i < bd.size(); i++) {
    			String tmpQ = bd.getDecomposition(i);
    			Query tmpQuery = ht.get(tmpQ);
    			Vector<QueryPlanTriple> triples = tmpQuery.getPlanTriples();
    			double minCost = -1;
    			QueryPlanTriple minCostTriple = null;
    			boolean toMove = true;
    			
    			for (int j = 0; j < triples.size(); j++) {
    				QueryPlanTriple triple = triples.get(j);
    				double cost;
    				boolean toMoveofTriple;
    				
    				if (triple.getVertex().equals(bd.getJoinVertex())) {
    					cost = triple.getCost();
    					toMoveofTriple = false;
    				} else {
    					// add the cost to move the query result
    					cost = triple.getCost() + ce.sizeEstimate(tmpQuery);
    					toMoveofTriple = true;
    				}
    				
    				if (minCost < 0 || minCost > cost) {
    					minCost = cost;
    					minCostTriple = triple;
    					toMove = toMoveofTriple;
    				}	    				
    			}
    				    		
    			// vertex = join vertex
    			totalCost += minCost;
    			children.add(minCostTriple.getPlan());
    			Action action;
    			
    			if (toMove)
    				action = new Action(ActionType.HASH, bd.getJoinVertex());
    			else
    				action = new Action(ActionType.STAY, null);
    				
    			actions.add(action);
    		}
    
    		totalCost += ce.sizeEstimate(query); // WARNING: join cost is included
    		QueryPlanNode node = new QueryPlanNode(QueryPlanNodeType.BUSHY_HASH_JOIN, query, bd.getJoinVertex(), children, actions);
    		QueryPlanTriple triple = new QueryPlanTriple(bd.getJoinVertex(), totalCost, node);
    		planTriples.add(triple);
    		
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
    		Vector<QueryPlanTriple> minCostTriples = new Vector<QueryPlanTriple>();
    		for (int i = 0; i < bd.size(); i++) {
    			String tmpQ = bd.getDecomposition(i);
    			Query tmpQuery = ht.get(tmpQ);
    			sumOfSizes += ce.sizeEstimate(tmpQuery);
    			QueryPlanTriple tmp = findMinCostTriple(tmpQ, ht);
    			minCostTriples.add(tmp); 
    			sumOfSubqueryMinCosts += tmp.getCost();
    		}
    		
    		for (int i = 0; i < bd.size(); i++) {
    			String tmpQ = bd.getDecomposition(i);
    			Query tmpQuery = ht.get(tmpQ);
    			// The network cost of the broadcast join
    			double sumOfSizesOfOtherSubqueries = sumOfSizes - ce.sizeEstimate(tmpQuery);
    			// The sum of min cost of subqueries expect  
    			//double sumOfOtherSubqueryMinCosts = sumOfSubqueryMinCosts - findMinCostTriple(tmpQ, ht).getCost();
    					
    			Vector<QueryPlanTriple> triples = tmpQuery.getPlanTriples();
    			//System.out.println("#triples:" + triples.size());
    			
    			for (QueryPlanTriple t: triples) {
    				// first subtract the min cost of current subquery from sumOfSubqueryMinCosts,
    				// and then add the cost of current triple
    				double costsOfAllSubqueries = sumOfSubqueryMinCosts - findMinCostTriple(tmpQ, ht).getCost() + t.getCost();
    				totalCost = costsOfAllSubqueries + sumOfSizesOfOtherSubqueries * numberOfMachines + ce.sizeEstimate(query); // WARNING: join cost is included
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
    				QueryPlanTriple newTriple = new QueryPlanTriple(t.getVertex(), totalCost, newNode);
    				planTriples.add(newTriple);
    				
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
    
    public static Vector<QueryPlanTriple> EliminateNonMinCosts(Vector<QueryPlanTriple> triples) {
    	HashSet<String> vertexes = new HashSet<String>();
    	Vector<QueryPlanTriple> result = new Vector<QueryPlanTriple>();
    	
    	for (QueryPlanTriple t: triples)
    		vertexes.add(t.getVertex());
    	
    	for (String v: vertexes) {
    		QueryPlanTriple minCostTriple = null;
    		
    		for (QueryPlanTriple t : triples) {
    			if (v.equals(t.getVertex())) {
    				if (minCostTriple == null || t.getCost() < minCostTriple.getCost()) {
    					minCostTriple = t;
    				}
    			}
    		}
    		
    		result.add(minCostTriple);
    	}
    	
    	//for (QueryPlanTriple t : triples)
    	//	System.out.println("vertex:" + t.getVertex() + "\t\tcost:" + t.getCost() + "\t\tplan:" + t.getPlan().toString() + "\n-----------\n");
    	
    	return result;
    	//System.out.println("triples.size() after elimination: " + triples.size() + "\n");
    }
    
    public static Vector<QueryPlanTriple> computeLinearCosts(Query query, Query q1, Query q2, int numberOfMachines, CostEstimator ce) {
    	Vector<QueryPlanTriple> triples = new Vector<QueryPlanTriple>();
    	
    	Vector<QueryPlanTriple> triples1 = q1.getPlanTriples();
    	Vector<QueryPlanTriple> triples2 = q2.getPlanTriples();
    	
    	Hashtable<String, Double> ht = new Hashtable<String, Double>();
    	
    	for (QueryPlanTriple t1: triples1) {
    		for (QueryPlanTriple t2: triples2) {
    			Set<String> commonVertexes = GraphQuery.findCommonVertexes(q1.getGraphQuery(), q2.getGraphQuery());
    			for (String v: commonVertexes) {
    				double costOft1Andt2 = t1.getCost() + t2.getCost();
    				//double costOfJoinInDB = ce.sizeEstimate(query) + ce.sizeEstimate(q1) + ce.sizeEstimate(q2);
    				
    				// Directed Join: q1 to q2
    				if (t2.getVertex().equals(v)) {
        				compareCost(query, 
        						    costOft1Andt2 + ce.sizeEstimate(q1) + ce.sizeEstimate(query),   // WARNING: total size of the query is added and other joins
    								ht, 
    								v,
    								v,
    								t1.getPlan(), 
    								t2.getPlan(), 
    								triples, 
    								QueryPlanNodeType.DIRECTED_JOIN_Q1_TO_Q2);				
    				}
    				// Directed Join: q2 to q1
    				else if (t1.getVertex().equals(v)) {
        				compareCost(query, costOft1Andt2 + ce.sizeEstimate(q2) + ce.sizeEstimate(query),
    								ht, 
    								v,
    								v,
    								t1.getPlan(), 
    								t2.getPlan(), 
    								triples, 
    								QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1);				
    				}
    				// Only use Parallel Hash Join if Directed Join is not applicable    		
    				else {
        				compareCost(query, costOft1Andt2 + ce.sizeEstimate(q1) + ce.sizeEstimate(q2) + ce.sizeEstimate(query),
        							ht, 
        							v,
        							v,
        							t1.getPlan(), 
        							t2.getPlan(), 
        							triples, 
        							QueryPlanNodeType.HASH_JOIN);
    				}
    				
    				// Broadcast Join: q1 to q2
    				compareCost(query, costOft1Andt2 + ce.sizeEstimate(q1) * numberOfMachines + ce.sizeEstimate(query),
								ht, 
								v, 
								t2.getVertex(),
								t1.getPlan(), 
								t2.getPlan(), 
								triples, 
								QueryPlanNodeType.BROADCAST_JOIN_Q1_TO_Q2);
    				
    				// Broadcast Join: q2 to q1
    				compareCost(query, costOft1Andt2 + ce.sizeEstimate(q2) * numberOfMachines + ce.sizeEstimate(query),
								ht, 
								v, 
								t1.getVertex(),
								t1.getPlan(), 
								t2.getPlan(), 
								triples, 
								QueryPlanNodeType.BROADCAST_JOIN_Q2_TO_Q1);
    			}
    		}
    	}
    	
    	return triples;
    }
    
    // Given a cost for hashVertex, modify the min cost if the given cost is lower than the current min cost 
    public static void compareCost(Query query, double cost, Hashtable<String, Double> ht, String joinVertex, String hashVertex, QueryPlanNode n1, QueryPlanNode n2, Vector<QueryPlanTriple> triples, QueryPlanNodeType type) {
		Double minCostForV = ht.get(hashVertex);
		if (minCostForV == null || cost < minCostForV) {
			QueryPlanNode node = new QueryPlanNode(query, type, joinVertex, n1, n2);
			QueryPlanTriple t = new QueryPlanTriple(hashVertex, cost, node);
			triples.add(t); 
			ht.put(hashVertex, cost);
		}    	
    }
    
    public static Vector<LinearDecomposition> linearDecomposition(GraphQuery q) {
    	Vector<LinearDecomposition> vld = new Vector<LinearDecomposition>();
    	
    	Set<String> heads = q.getHeadVertexes();
    	Vector<Edge> edges = q.getEdges();
    	
    	for (String h : heads) {
    		String oneHeadSubquery = "";
    		String theRest = "";
    		
    		for (Edge e : edges) {
    			if (h.equals(e.getHead()))
    				oneHeadSubquery += e.toString();
    			else 
    				theRest += e.toString();
    		}
    		
    		vld.add(new LinearDecomposition(oneHeadSubquery, theRest));
    		
    		// If there are only two heads, the second decomposition will be the same
    		if (heads.size() == 2)
    			break;
    	}
    	
    	return vld;
    }

    public static String GenerateInputString(String input) {
    	try {
    		FileInputStream fstream = new FileInputStream(input);
    		DataInputStream in = new DataInputStream(fstream);
    		BufferedReader br = new BufferedReader(new InputStreamReader(in));
    		
    		String output = "";
    		
    		String strLine;
    		while ((strLine = br.readLine()) != null) {
    			output += strLine + "\n";
    		}

    		output = output.substring(0, output.length() - 1);
    		
    		in.close();
    	
    		//System.out.println(output);
    		
    		return output;
    	} catch (Exception e) {
    		System.err.println("Error: " + e.getMessage());
    	}
    	
    	return null; 
    }
    
    // Receives the output from generateComponents and returns a layered DAG for each starting vertex.
    public static Vector<LayeredComponent> generateDAGs(Vector<Vector<LayeredComponent>> vvlc) {
    	Vector<LayeredComponent> result = new Vector<LayeredComponent>();
    	
    	for (Vector<LayeredComponent> vlc : vvlc) {
    		//System.out.println("-------------------vlc---------------------");
    		LayeredComponent dag = LayeredComponent.mergeLayeredComponents(vlc);
    		if (dag != null)
    			result.add(dag);
    		//dag.print();
    	}
    	
    	return result;
    }
    
    // Given an input graph, return a vector of LayeredComponents for each suitable starting vertex.
    public static Vector<Vector<LayeredComponent>> generateComponents(GraphQuery g) {
    	Set<String> heads = g.getHeadVertexes();
    	Vector<Vector<LayeredComponent>> result = new Vector<Vector<LayeredComponent>>();
    	
    	for (String v : heads) {
    		//System.out.println("---------------------------------------------------------------components for vertex " + v);
    		Vector<LayeredComponent> lcs = generateComponents(v, g);
    		
    		/////////////////        DEBUGGING         ///////////////////
    		if (lcs.size() >= 2) {
    			Vector<ConnectionPoint> cps = lcs.get(0).findConnectionPoints(lcs.get(1));
    			for (ConnectionPoint cp : cps) {
    				//cp.print();
    				@SuppressWarnings("unused")
					LayeredComponent merged = LayeredComponent.mergeTwoLayeredComponents(cp, lcs.get(0), lcs.get(1));
    				//System.out.println("MMMMMMMMMMMMMMMMMMMMMMMMMMMM merged");
    				//merged.print();
    			}
    		}
    		////////////////////////////////////////////
    		
    		result.add(lcs);
    	}
    	
    	return result;
    }
    
    // Given a starting vertex v, return a vector of LayeredComponents.
    // V guarantees to be in the first layer in the first component. 
    // The first layers of other components are chosen from function getHeadWithValidEdges()
    public static Vector<LayeredComponent> generateComponents(String v, GraphQuery g) {
    	Vector<LayeredComponent> result = new Vector<LayeredComponent>();
    	
    	g.validateAllEdges();
    	String startingVertex = v; 
    	
    	while(g.getNumberOfValidEdges() > 0) { 
    		LayeredComponent lc = generateComponent(startingVertex, g);
    		
    		if (lc.hasEdges()) {
    			result.add(lc);
    			//lc.print();
    		}
    		
    		startingVertex = g.getHeadWithValidEdges();
    	}
    	
    	g.validateAllEdges();
    	
    	return result;
    }
    
    // Given a starting vertex and an input graph, return a LayeredComponent.
    public static LayeredComponent generateComponent(String v, GraphQuery g) {
    	LayeredComponent lc = new LayeredComponent();
    	int layerCount = 0;
    	lc.addVertex(v, layerCount); 
    	
    	while(g.getNumberOfValidEdges() > 0) {
    		boolean thisLayerHasEdges = false;
    		for (String vertex: lc.getLayer(layerCount)) {
    			Vector<Edge> edges = g.getValidEdgesWithHead(vertex); 
    			if (edges == null)
    				continue;
    			else {
    				thisLayerHasEdges = true;
    				for (Edge e: edges) {
    					lc.addEdge(e, layerCount);
    					lc.addVertex(e.getTail(), layerCount + 1);
    					g.invalidateEdge(e);
    				}
    			}
    		}
    		if (!thisLayerHasEdges)
    			break;
    		layerCount++;	
    	}
    		
    	return lc;
    }
    
    public static Vector<LayeredComponent> removeDuplicateDAGs(Vector<LayeredComponent> vlc) {
    	Vector<LayeredComponent> result = new Vector<LayeredComponent>();
    	
    	outerLoop:
    	for (LayeredComponent lc : vlc) {
    		int siezOfresult = result.size();
    		for (int i = 0; i < siezOfresult; i++) {
    			LayeredComponent tmp = result.get(i);
    			if (LayeredComponent.theSame(lc, tmp))
    				continue outerLoop;
    		}
    		result.add(lc);
    	}
    	
    	return result;
    }

    public static QueryPlanNode AB_BA_BD_BE_CB_CD_CE_LINEAR(Hashtable<String, Query> ht) {    	
    	QueryPlanNode n9 = new QueryPlanNode(getQuery(ht, "A 0 B\n"), "A 0 B\n"); 
    	n9.setNodeID("N9");
    	QueryPlanNode n10 = new QueryPlanNode(getQuery(ht, "B 0 A\n"), "B 0 A\n");
    	n10.setNodeID("N10");
    	QueryPlanNode n7 = new QueryPlanNode(getQuery(ht, "A 0 B\nB 0 A\n"), QueryPlanNodeType.DIRECTED_JOIN_Q1_TO_Q2, "B", n9, n10); 
    	n7.setNodeID("N7");

    	QueryPlanNode n8 = new QueryPlanNode(getQuery(ht, "B 0 D\n"), "B 0 D\n"); 
    	n8.setNodeID("N8");
    	QueryPlanNode n5 = new QueryPlanNode(getQuery(ht, "A 0 B\nB 0 A\nB 0 D\n"), QueryPlanNodeType.COLLOCATED_JOIN, "B", n7, n8); 
    	n5.setNodeID("N5");    	

    	QueryPlanNode n6 = new QueryPlanNode(getQuery(ht, "C 0 B\nC 0 D\n"), "C 0 B\nC 0 D\n"); 
    	n6.setNodeID("N6");
    	QueryPlanNode n3 = new QueryPlanNode(getQuery(ht, "A 0 B\nB 0 A\nB 0 D\nC 0 B\nC 0 D\n"), QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1, "B", n5, n6); 
    	n3.setNodeID("D3"); 

    	QueryPlanNode n4 = new QueryPlanNode(getQuery(ht, "B 0 E\n"), "B 0 E\n"); 
    	n4.setNodeID("N4");
    	QueryPlanNode n1 = new QueryPlanNode(getQuery(ht, "B 0 E\nC 0 E\n"), QueryPlanNodeType.COLLOCATED_JOIN, "B", n3, n4); 
    	n1.setNodeID("N1");
    	String query = "select b,c,e from n4 natural join (SELECT DISTINCT ON (b,c) b,c from d3) as tmp where b <> c and b <> e and c <> e;";
    	n1.setJoinQuery(query);
    	
    	QueryPlanNode n2 = new QueryPlanNode(getQuery(ht, "C 0 E\n"), "C 0 E\n"); 
    	n2.setNodeID("N2");    	
    	QueryPlanNode n0 = new QueryPlanNode(getQuery(ht, "B 0 E\nC 0 E\n"), QueryPlanNodeType.DIRECTED_JOIN_Q1_TO_Q2, "C", n1, n2); 
    	n0.setJoinQuery("select b,c,e from (select distinct on (b,c,e) b,c,e from n1) as tmp natural join n2 where b <> c and b <> e and c <> e");    	
    	n0.setNodeID("N0");    	

    	Vector<QueryPlanNode> children = new Vector<QueryPlanNode>();
    	children.add(n0); 
    	Vector<Action> actions = new Vector<Action>();
    	actions.add(new Action(ActionType.HASH, "B"));
    	
    	QueryPlanNode root = new QueryPlanNode(QueryPlanNodeType.BUSHY_HASH_JOIN, null, "B", children, actions);
    	root.setNodeID("root");    	
    	root.setJoinQuery("select a,b,c,d,e from n0 natural join d3 WHERE a<>e and b<>e and c<>e and d<>e");
    	
    	return root;
    }  
    
    public static QueryPlanNode AB_BA_CB_DB_Bushy_Filter(Hashtable<String, Query> ht) {    	
    	QueryPlanNode n1 = new QueryPlanNode(getQuery(ht, "B 0 A B.attribute = 'category' AND B.value LIKE '%Parenting & Families%'\n"), "B 0 A B.attribute = 'category' AND B.value LIKE '%Parenting & Families%'\n"); 
    	n1.setNodeID("N1");
    	QueryPlanNode n2 = new QueryPlanNode(getQuery(ht, "A 0 B A.attribute = 'category' AND A.value LIKE '%Health, Mind & Body%'\n"), "A 0 B A.attribute = 'category' AND A.value LIKE '%Health, Mind & Body%'\n");
    	n2.setNodeID("N2");
    	QueryPlanNode n3 = new QueryPlanNode(getQuery(ht, "C 0 B C.attribute = 'category' AND C.value LIKE '%Children%'\n"), "C 0 B C.attribute = 'category' AND C.value LIKE '%Children%'\n");
    	n3.setNodeID("N3");
    	QueryPlanNode n4 = new QueryPlanNode(getQuery(ht, "D 0 B D.attribute = 'category' AND D.value LIKE '%Home & Garden%'\n"), "D 0 B D.attribute = 'category' AND D.value LIKE '%Home & Garden%'\n");
    	n4.setNodeID("N4");
    	Vector<QueryPlanNode> children = new Vector<QueryPlanNode>();
    	children.add(n1); 
    	children.add(n2);
    	children.add(n3); 
    	children.add(n4);
    	Vector<Action> actions = new Vector<Action>();
    	actions.add(new Action(ActionType.STAY, null));
    	actions.add(new Action(ActionType.HASH, "B"));
    	actions.add(new Action(ActionType.HASH, "B"));
    	actions.add(new Action(ActionType.HASH, "B"));
    	String query = "SELECT A,B,C,D FROM n1 NATURAL JOIN n2 NATURAL JOIN n3 NATURAL JOIN n4 WHERE A <> B AND A <> C AND A <> D AND B <> C AND B <> D AND C <> D";
    	QueryPlanNode root = new QueryPlanNode(QueryPlanNodeType.BUSHY_HASH_JOIN, null, "B", children, actions);
    	root.setNodeID("N0");		
    	root.setJoinQuery(query);
    	
    	return root;
    }
    
    public static QueryPlanNode AB_BA_CB_DB_Bushy_Shared(Hashtable<String, Query> ht) {
    	QueryPlanNode placeHolder = new QueryPlanNode(null, ""); 
    	
    	QueryPlanNode n1 = new QueryPlanNode(getQuery(ht, "B 0 A\n"), "B 0 A\n"); 
    	n1.setNodeID("N1");
    	QueryPlanNode n2 = new QueryPlanNode(getQuery(ht, "A 0 B\n"), "A 0 B\n");
    	n2.setNodeID("N2");
    	Vector<QueryPlanNode> children = new Vector<QueryPlanNode>();
    	children.add(n1); 
    	children.add(n2);
    	children.add(placeHolder); // null will not work because getNodeID() is called for all tables; child0 only serves as a placeholder
    	children.add(placeHolder); // null will not work because getNodeID() is called for all tables; child0 only serves as a placeholder
    	Vector<Action> actions = new Vector<Action>();
    	actions.add(new Action(ActionType.STAY, null));
    	actions.add(new Action(ActionType.HASH, "B"));
    	actions.add(new Action(ActionType.NO, null));
    	actions.add(new Action(ActionType.NO, null));
    	String n3 = "(SELECT A AS C, B FROM n2) AS n3 ";
    	String n4 = "(SELECT A AS D, B FROM n2) AS n4 ";
    	String query = "SELECT " + "A,B,C,D " + "FROM n1 NATURAL JOIN n2 NATURAL JOIN " + n3 + " NATURAL JOIN " + n4 + "WHERE A <> B AND A <> C AND A <> D AND B <> C AND B <> D AND C <> D";
    	QueryPlanNode root = new QueryPlanNode(QueryPlanNodeType.BUSHY_HASH_JOIN, null, "B", children, actions);
    	root.setNodeID("N0");		
    	root.setJoinQuery(query);
    	
    	return root;
    }

    public static QueryPlanNode AB_BA_CB_DB_Linear_Shared(Hashtable<String, Query> ht) {
    	QueryPlanNode placeHolder = new QueryPlanNode(null, ""); 
    	
    	QueryPlanNode n5 = new QueryPlanNode(getQuery(ht, "B 0 A\n"), "B 0 A\n"); 
    	n5.setNodeID("D5");
    	QueryPlanNode n6 = new QueryPlanNode(getQuery(ht, "A 0 B\n"), "A 0 B\n");
    	n6.setNodeID("D6");
    	QueryPlanNode n3 = new QueryPlanNode(getQuery(ht, "B 0 A\nA 0 B\n"), QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1,  "B", n5, n6);
    	String query = "SELECT " + "A,B " + "FROM D5 NATURAL JOIN D6 " + "WHERE A <> B";
    	n3.setJoinQuery(query);
    	n3.setNodeID("D3");
 
    	Vector<QueryPlanNode> children = new Vector<QueryPlanNode>();
    	children.add(n3); 
    	children.add(placeHolder); // only a place holder
    	Vector<Action> actions = new Vector<Action>();
    	actions.add(new Action(ActionType.STAY, null));
    	actions.add(new Action(ActionType.NO, null));
    	QueryPlanNode n1 = new QueryPlanNode(QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1, getQuery(ht, "B 0 A\nA 0 B\nC 0 B\n"),  "B", children, actions);
    	n1.setNodeID("D1");
    	query = "SELECT " + "A,B,C " + "FROM d3 NATURAL JOIN " + "(SELECT A AS C, B FROM d6) AS tmp " + "WHERE A <> B AND A <> C AND B <> C";
    	n1.setJoinQuery(query);

    	children = new Vector<QueryPlanNode>();
    	children.add(n1); 
    	children.add(placeHolder); // only a place holder
    	actions = new Vector<Action>();
    	actions.add(new Action(ActionType.STAY, null));
    	actions.add(new Action(ActionType.NO, null));
    	QueryPlanNode n0 = new QueryPlanNode(QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1, null, "B", children, actions);
    	n0.setNodeID("N0");
    	query = "SELECT " + "A,B,C,D " + "FROM d1 NATURAL JOIN " + "(SELECT A AS D, B FROM d6) AS tmp " + "WHERE A <> B AND A <> C AND A <> D AND B <> C AND B <> D AND C <> D";
    	n0.setJoinQuery(query);
    	
    	return n0;
    }
    
    /*
     * child :0
    child :0
        child :0
            B 0 A;                      size:3577450.0
        child :1
            A 0 B;                      size:3577450.0
        DIRECTED_JOIN_Q2_TO_Q1 on B (Actions: STAY HASH)
    child :1
        C 0 B;                  size:3577450.0
    DIRECTED_JOIN_Q2_TO_Q1 on B (Actions: STAY HASH)
child :1
    D 0 B;                      size:3577450.0
DIRECTED_JOIN_Q2_TO_Q1 on B (Actions: STAY HASH)

     */
    
    public static QueryPlanNode AC_AD_BC_BD_Linear_Shared(Hashtable<String, Query> ht) {
    	QueryPlanNode child0 = new QueryPlanNode(getQuery(ht, "B 0 C\nB 0 D\n"), "B 0 C\nB 0 D\n"); 
    	child0.setNodeID("N1");
    	Vector<QueryPlanNode> children = new Vector<QueryPlanNode>();
    	children.add(child0);
    	children.add(child0); // null will not work because getNodeID() is called for all tables; child0 only serves as a placeholder
    	Vector<Action> actions = new Vector<Action>();
    	actions.add(new Action(ActionType.HASH, "D"));
    	actions.add(new Action(ActionType.NO, null));
    	String n2 = "(SELECT B AS A, C, D FROM n1) AS n2 ";
    	String query = "SELECT " + "A,B,C,D " + "FROM n1 NATURAL JOIN  " + n2 + "WHERE A <> B AND A <> C AND A <> D AND B <> C AND B <> D AND C <> D";
    	QueryPlanNode root = new QueryPlanNode(QueryPlanNodeType.BUSHY_HASH_JOIN, null, "B", children, actions);
    	root.setNodeID("N0");		
    	root.setJoinQuery(query);
    	
    	return root;
    }
    
    public static QueryPlanNode AB_CA_CB_DB_DE_ED_Cycle(Hashtable<String, Query> ht) {
    	QueryPlanNode n9 = new QueryPlanNode(getQuery(ht, "D 0 E\n"), "D 0 E\n"); 
    	n9.setNodeID("n9");
    	QueryPlanNode n10 = new QueryPlanNode(getQuery(ht, "E 0 D\n"), "E 0 D\n"); 
    	n10.setNodeID("n10");    	
    	QueryPlanNode n7 = new QueryPlanNode(getQuery(ht, "D 0 E\nE 0 D\n"), QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1, "D", n9, n10); 
    	n7.setNodeID("n7");
    	
    	QueryPlanNode n8 = new QueryPlanNode(getQuery(ht, "D 0 B\n"), "D 0 B\n"); 
    	n8.setNodeID("n8");
    	QueryPlanNode n5 = new QueryPlanNode(getQuery(ht, "D 0 B\nD 0 E\nE 0 D\n"), QueryPlanNodeType.COLLOCATED_JOIN, "D", n7, n8); 
    	n5.setNodeID("n5");
    	
    	QueryPlanNode n6 = new QueryPlanNode(getQuery(ht, "A 0 B\n"), "A 0 B\n"); 
    	n6.setNodeID("n6");
    	QueryPlanNode n3 = new QueryPlanNode(getQuery(ht, "A 0 B\nD 0 B\nD 0 E\nE 0 D\n"), QueryPlanNodeType.HASH_JOIN, "B", n5, n6); 
    	n3.setNodeID("n3");    	
    	
    	QueryPlanNode n4 = new QueryPlanNode(getQuery(ht, "C 0 A\nC 0 B\n"), "C 0 A\nC 0 B\n"); 
    	n4.setNodeID("n4");
    	QueryPlanNode n2 = new QueryPlanNode(getQuery(ht, "A 0 B\nC 0 A\nC 0 B\nD 0 B\nD 0 E\nE 0 D\n"), QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1, "B", n3, n4); 
    	n2.setNodeID("n2");
    	
    	return n2;
    }
    
    public static QueryPlanNode AB_CA_CB_DB_DE_ED_Cycle_2(Hashtable<String, Query> ht) {
    	QueryPlanNode n9 = new QueryPlanNode(getQuery(ht, "D 0 E\n"), "D 0 E\n"); 
    	n9.setNodeID("n9");
    	QueryPlanNode n10 = new QueryPlanNode(getQuery(ht, "E 0 D\n"), "E 0 D\n"); 
    	n10.setNodeID("n10");    	
    	QueryPlanNode n7 = new QueryPlanNode(getQuery(ht, "D 0 E\nE 0 D\n"), QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1, "D", n9, n10); 
    	n7.setNodeID("n7");
    	
    	QueryPlanNode n8 = new QueryPlanNode(getQuery(ht, "D 0 B\n"), "D 0 B\n"); 
    	n8.setNodeID("n8");
    	QueryPlanNode n5 = new QueryPlanNode(getQuery(ht, "D 0 B\nD 0 E\nE 0 D\n"), QueryPlanNodeType.COLLOCATED_JOIN, "D", n7, n8); 
    	n5.setNodeID("n5");
    	
    	QueryPlanNode n6 = new QueryPlanNode(getQuery(ht, "A 0 B\n"), "A 0 B\n"); 
    	n6.setNodeID("n6");
    	QueryPlanNode n3 = new QueryPlanNode(getQuery(ht, "A 0 B\nD 0 B\nD 0 E\nE 0 D\n"), QueryPlanNodeType.HASH_JOIN, "B", n5, n6); 
    	n3.setNodeID("n3");    	
    	
    	QueryPlanNode n4 = new QueryPlanNode(getQuery(ht, "C 0 B\n"), "C 0 B\n"); 
    	n4.setNodeID("n4");
    	QueryPlanNode n2 = new QueryPlanNode(getQuery(ht, "A 0 B\nC 0 B\nD 0 B\nD 0 E\nE 0 D\n"), QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1, "B", n3, n4); 
    	n2.setNodeID("n2");
    	
    	QueryPlanNode n1 = new QueryPlanNode(getQuery(ht, "C 0 A\n"), "C 0 A\n"); 
    	n1.setNodeID("n1");    	
    	QueryPlanNode n0 = new QueryPlanNode(getQuery(ht, "A 0 B\nC 0 A\nC 0 B\nD 0 B\nD 0 E\nE 0 D\n"), QueryPlanNodeType.HASH_JOIN, "C", n2, n1); 
    	n0.setNodeID("n0");
    	
    	return n0;
    }

    public static QueryPlanNode AB_CA_CB_DB_DE_ED_Cycle_3(Hashtable<String, Query> ht) {
    	QueryPlanNode n5 = new QueryPlanNode(getQuery(ht, "D 0 E\n"), "D 0 E\n"); 
    	n5.setNodeID("n5");
    	QueryPlanNode n6 = new QueryPlanNode(getQuery(ht, "E 0 D\n"), "E 0 D\n"); 
    	n6.setNodeID("n6");    	
    	QueryPlanNode n3 = new QueryPlanNode(getQuery(ht, "D 0 E\nE 0 D\n"), QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1, "D", n5, n6); 
    	n3.setNodeID("n3");
    	
    	QueryPlanNode n4 = new QueryPlanNode(getQuery(ht, "D 0 B\n"), "D 0 B\n"); 
    	n4.setNodeID("n4");
    	QueryPlanNode n1 = new QueryPlanNode(getQuery(ht, "D 0 B\nD 0 E\nE 0 D\n"), QueryPlanNodeType.COLLOCATED_JOIN, "D", n3, n4); 
    	n1.setNodeID("n1");
    	
    	QueryPlanNode n7 = new QueryPlanNode(getQuery(ht, "C 0 A\nC 0 B\n"), "C 0 A\nC 0 B\n"); 
    	n7.setNodeID("n7");
    	QueryPlanNode n8 = new QueryPlanNode(getQuery(ht, "A 0 B\n"), "A 0 B\n"); 
    	n8.setNodeID("n8");    	
    	QueryPlanNode n2 = new QueryPlanNode(getQuery(ht, "A 0 B\nC 0 A\nC 0 B\n"), QueryPlanNodeType.HASH_JOIN, "B", n7, n8); 
    	n2.setNodeID("n2");
    	
    	QueryPlanNode n0 = new QueryPlanNode(getQuery(ht, "A 0 B\nC 0 A\nC 0 B\nD 0 B\nD 0 E\nE 0 D\n"), QueryPlanNodeType.DIRECTED_JOIN_Q1_TO_Q2, "B", n1, n2); 
    	n0.setNodeID("n0");
    	
    	return n0;
    }
    
    public static QueryPlanNode AB_BC_BD_BE_DB_DC_ED_Cycle(Hashtable<String, Query> ht) {
    	QueryPlanNode n11 = new QueryPlanNode(getQuery(ht, "B 0 D\n"), "B 0 D\n"); 
    	n11.setNodeID("n11");
    	QueryPlanNode n12 = new QueryPlanNode(getQuery(ht, "D 0 B\n"), "D 0 B\n"); 
    	n12.setNodeID("n12");    	
    	QueryPlanNode n9 = new QueryPlanNode(getQuery(ht, "B 0 D\nD 0 B\n"), QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1, "B", n11, n12); 
    	n9.setNodeID("n9");
    	
    	QueryPlanNode n10 = new QueryPlanNode(getQuery(ht, "B 0 E\n"), "B 0 E\n"); 
    	n10.setNodeID("n10");    	
    	QueryPlanNode n7 = new QueryPlanNode(getQuery(ht, "B 0 D\nB 0 E\nD 0 B\n"), QueryPlanNodeType.COLLOCATED_JOIN, "B", n9, n10); 
    	n7.setNodeID("n7");
    	
    	QueryPlanNode n8 = new QueryPlanNode(getQuery(ht, "E 0 D\n"), "E 0 D\n"); 
    	n8.setNodeID("n8");
    	QueryPlanNode n5 = new QueryPlanNode(getQuery(ht, "B 0 D\nB 0 E\nD 0 B\nE 0 D\n"), QueryPlanNodeType.HASH_JOIN, "D", n7, n8); 
    	n5.setNodeID("n5");
    	
    	QueryPlanNode n6 = new QueryPlanNode(getQuery(ht, "D 0 C\n"), "D 0 C\n"); 
    	n6.setNodeID("n6");
    	QueryPlanNode n3 = new QueryPlanNode(getQuery(ht, "B 0 D\nB 0 E\nD 0 B\nD 0 C\nE 0 D\n"), QueryPlanNodeType.COLLOCATED_JOIN, "D", n5, n6); 
    	n3.setNodeID("n3");
    	
    	QueryPlanNode n4 = new QueryPlanNode(getQuery(ht, "B 0 C\n"), "B 0 C\n"); 
    	n4.setNodeID("n4");
    	QueryPlanNode n1 = new QueryPlanNode(getQuery(ht, "B 0 C\nB 0 D\nB 0 E\nD 0 B\nD 0 C\nE 0 D\n"), QueryPlanNodeType.DIRECTED_JOIN_Q1_TO_Q2, "B", n3, n4); 
    	n1.setNodeID("n1");
    	
    	QueryPlanNode n2 = new QueryPlanNode(getQuery(ht, "A 0 B\n"), "A 0 B\n"); 
    	n2.setNodeID("n2");
    	QueryPlanNode n0 = new QueryPlanNode(getQuery(ht, "A 0 B\nB 0 C\nB 0 D\nB 0 E\nD 0 B\nD 0 C\nE 0 D\n"), QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1, "B", n1, n2); 
    	n0.setNodeID("n0");    
    	
    	return n0;
    }
    
    public static QueryPlanNode AB_BA_BD_BE_CB_CD_CE_Cycle(Hashtable<String, Query> ht) {
    	QueryPlanNode n11 = new QueryPlanNode(getQuery(ht, "A 0 B\n"), "A 0 B\n"); 
    	n11.setNodeID("n11");
    	QueryPlanNode n12 = new QueryPlanNode(getQuery(ht, "B 0 A\n"), "B 0 A\n"); 
    	n12.setNodeID("n12");    	
    	QueryPlanNode n9 = new QueryPlanNode(getQuery(ht, "A 0 B\nB 0 A\n"), QueryPlanNodeType.DIRECTED_JOIN_Q1_TO_Q2, "B", n11, n12); 
    	n9.setNodeID("n9");
    	
    	QueryPlanNode n10 = new QueryPlanNode(getQuery(ht, "B 0 D\n"), "B 0 D\n"); 
    	n10.setNodeID("n10");    	
    	QueryPlanNode n7 = new QueryPlanNode(getQuery(ht, "A 0 B\nB 0 A\nB 0 D\n"), QueryPlanNodeType.COLLOCATED_JOIN, "B", n9, n10); 
    	n7.setNodeID("n7");
    	
    	QueryPlanNode n8 = new QueryPlanNode(getQuery(ht, "C 0 B\n"), "C 0 B\n"); 
    	n8.setNodeID("n8");
    	QueryPlanNode n5 = new QueryPlanNode(getQuery(ht, "A 0 B\nB 0 A\nB 0 D\nC 0 B\n"), QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1, "B", n7, n8); 
    	n5.setNodeID("n5");
    	
    	QueryPlanNode n6 = new QueryPlanNode(getQuery(ht, "C 0 D\n"), "C 0 D\n"); 
    	n6.setNodeID("n6");
    	QueryPlanNode n3 = new QueryPlanNode(getQuery(ht, "A 0 B\nB 0 A\nB 0 D\nC 0 B\nC 0 D\n"), QueryPlanNodeType.HASH_JOIN, "C", n5, n6); 
    	n3.setNodeID("n3");
    	
    	QueryPlanNode n4 = new QueryPlanNode(getQuery(ht, "C 0 E\n"), "C 0 E\n"); 
    	n4.setNodeID("n4");
    	QueryPlanNode n1 = new QueryPlanNode(getQuery(ht, "A 0 B\nB 0 A\nB 0 D\nC 0 B\nC 0 D\nC 0 E\n"), QueryPlanNodeType.DIRECTED_JOIN_Q2_TO_Q1, "C", n3, n4); 
    	n1.setNodeID("n1");
    	
    	QueryPlanNode n2 = new QueryPlanNode(getQuery(ht, "B 0 E\n"), "B 0 E\n"); 
    	n2.setNodeID("n2");
    	QueryPlanNode n0 = new QueryPlanNode(getQuery(ht, "A 0 B\nB 0 A\nB 0 D\nB 0 E\nC 0 B\nC 0 D\nC 0 E\n"), QueryPlanNodeType.HASH_JOIN, "B", n1, n2); 
    	n0.setNodeID("n0");    
    	
    	return n0;
    }
    
    public static QueryPlanNode AB_CA_CB_Linear(Hashtable<String, Query> ht) {
    	QueryPlanNode n1 = new QueryPlanNode(getQuery(ht, "C 0 A\nC 0 B\n"), "C 0 A\nC 0 B\n"); 
    	n1.setNodeID("n1");
    	QueryPlanNode n2 = new QueryPlanNode(getQuery(ht, "A 0 B\n"), "A 0 B\n"); 
    	n2.setNodeID("n2");    	
    	QueryPlanNode n0 = new QueryPlanNode(getQuery(ht, "A 0 B\nC 0 A\nC 0 B\n"), QueryPlanNodeType.HASH_JOIN, "B", n1, n2); 
    	n0.setNodeID("n0");
    	
    	return n0;
    }
}