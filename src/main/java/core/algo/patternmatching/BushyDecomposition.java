package core.algo.patternmatching;
import java.util.Vector;

public class BushyDecomposition {
	private Vector<String> decoposition;
	private String joinVertex;
	
	public BushyDecomposition(Vector<String> decoposition, String joinVertex) {
		this.decoposition = decoposition;
		//System.out.println("Construct a bushy decomposition:" + decoposition.size());
		this.joinVertex = joinVertex;
	}
	
	public int size() {
		return decoposition.size();
	}
	
	public String getJoinVertex() {
		return joinVertex;
	}	
	
	public String getDecomposition(int index) {
		return decoposition.get(index);
	}	
}
