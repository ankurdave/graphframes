package core.query;

import java.util.Vector;

import core.algo.patternmatching.views.Solution;
import core.data.Edge;
import core.data.Partitioning;

/**
 * The query to run over views
 * 
 * @author alekh
 *
 */
public class ViewQuery extends Query {

	private Vector<Solution> solutions = new Vector<Solution>();
	
	public ViewQuery(String query) {
		super(query);
	}

	public Vector<Solution> getPlanSolutions() {
		return solutions;
	}

	public void setPlanSolutions(Vector<Solution> s) {
		solutions = s;
	}
	
	public boolean hasPlanSolutions() {
		return (solutions.size() > 0);
	}
	
	public boolean hasEdgesCopartitioned(Partitioning p) {
		long pid = -1;
		
		for (Edge e : getGraphQuery().getEdges()){
			if(pid==-1){
				pid = p.getKey(e);
				continue;
			}
			
			if (pid!=p.getKey(e))
				return false;
		};
		
		return true;
	}
	
	public Edge firstEdge(){
		return getGraphQuery().getEdges().get(0);
	}

	public void addPlanSolution(Solution solution) {
		solutions.add(solution);
	}
	
	public int planSolutionCount() {
		return solutions.size();
	}
	
}
