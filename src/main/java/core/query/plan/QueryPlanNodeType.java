package core.query.plan;

public enum QueryPlanNodeType {
	// local execution
	LOCAL_EXECUTION,
	// types for linear decomposition
	COLLOCATED_JOIN,   // used by greedy plans
	HASH_JOIN,
	DIRECTED_JOIN_Q1_TO_Q2,
	DIRECTED_JOIN_Q2_TO_Q1,
	BROADCAST_JOIN_Q1_TO_Q2,
	BROADCAST_JOIN_Q2_TO_Q1,
	// types for bushy decomposition
	//BUSHY_PLAN
	BUSHY_HASH_JOIN,
	BUSHY_BROADCAST_JOIN
}
