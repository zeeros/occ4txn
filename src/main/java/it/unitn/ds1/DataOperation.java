package it.unitn.ds1;

public class DataOperation {
	private Type type;
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
	public void setType(Type type) {
		this.type = type;
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
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + dataItem.hashCode() + key + type.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataOperation other = (DataOperation) obj;
		if (dataItem != other.dataItem)
			return false;
		if (key != other.key)
			return false;
		if (type != other.type)
			return false;
		return true;
	}

}
