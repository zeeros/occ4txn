package it.unitn.ds1;

public class DataOperation {
	private final Type type;
	private final Integer key;
	private final Integer value;
	
	public enum Type {
		READ, WRITE
	}
	
	public DataOperation(Type type, Integer key, Integer value) {
		this.type = type;
		this.key = key;
		this.value = value;
	}

	public Type getType() {
		return type;
	}

	public Integer getKey() {
		return key;
	}

	public Integer getValue() {
		return value;
	}
}
