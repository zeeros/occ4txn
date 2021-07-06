package it.unitn.ds1;

public class Txn {
	private final Integer coordinatorId;
	private final Integer clientId;
	//indicates the state of the transaction :
	//validated if the TXN is consistent
	boolean validatedConsistent;
	//overwriten if all the transient dataItems from the private workspace have been successful
	boolean overwritesDone;
	
	
	public Txn(int coordinatorId, int clientId) {
		this.coordinatorId = coordinatorId;
		this.clientId = clientId;
	}

	public Integer getCoordinatorId() {
		return coordinatorId;
	}

	public Integer getClientId() {
		return clientId;
	}
	
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + coordinatorId + clientId;  
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
        Txn other = (Txn) obj;
        if (coordinatorId != other.getCoordinatorId())
            return false;
        if (clientId != other.getClientId())
            return false;
        return true;
    }
}
