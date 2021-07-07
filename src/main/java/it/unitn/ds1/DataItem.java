package it.unitn.ds1;

public class DataItem {
	private Integer version;
	private Integer value;
	private Integer lock;

	public DataItem(Integer version, Integer value) {
		this.version = version;
		this.value = value;
	}

	public Integer getVersion() {
		return version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

	public Integer getLock() {
		return lock;
	}

	public void setLock(Integer lock) {
		this.lock = lock;
	}

}
