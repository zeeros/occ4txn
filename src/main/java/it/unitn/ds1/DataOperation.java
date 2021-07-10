package it.unitn.ds1;

public class DataOperation {
	private final Type type;
	private final Integer key;
	private DataItem dataItem;

	public enum Type {
		READ, WRITE
	}

	public DataOperation(Type type, Integer key, DataItem dataItem) {
		this.type = type;
		this.key = key;
		this.setDataItem(dataItem);
	}

	public Type getType() {
		return type;
	}

	public Integer getKey() {
		return key;
	}

	public DataItem getDataItem() {
		return dataItem;
	}

	public void setDataItem(DataItem dataItem) {
		this.dataItem = dataItem;
	}

}
