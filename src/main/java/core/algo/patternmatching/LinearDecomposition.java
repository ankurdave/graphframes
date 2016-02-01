package core.algo.patternmatching;

public class LinearDecomposition {
	private String oneHeadSubgraph;
	private String theRest;
	
	public LinearDecomposition(String oneHeadSubgraph, String theRest) {
		this.oneHeadSubgraph = oneHeadSubgraph;
		this.theRest = theRest;
	}
	
	public String getOneHeadSubgraph() {
		return oneHeadSubgraph;
	}

	public String getTheRest() {
		return theRest;
	}
}
