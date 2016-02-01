package core.query.plan;

public class Action {
	ActionType type;
	String hashVertex;
	
	public Action(ActionType type, String hashVertex) {
		this.type = type;
		this.hashVertex = hashVertex;
	}
	
	public ActionType getType() {
		return type;
	}
	
	public String getHashVertex() {
		return hashVertex;
	}
}