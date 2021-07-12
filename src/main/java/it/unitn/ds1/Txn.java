package it.unitn.ds1;

public class Txn {
	private final Integer coordinatorId;
	private final Integer clientId;
	// indicates the state of the transaction :
	// validated if the TXN is consistent
	boolean validatedConsistent;
	// overwriten if all the transient dataItems from the private workspace have
	// been successful
	boolean overwritesDone;
	// number of "commit" votes
	private Integer votes;
	private Boolean resultSentToClient;
	private Integer votesCollected;

	public Txn(int coordinatorId, int clientId) {
		this.coordinatorId = coordinatorId;
		this.clientId = clientId;
		this.votes = 0;
		this.resultSentToClient = false;
		this.votesCollected = 0;
	}

	public Integer getCoordinatorId() {
		return coordinatorId;
	}
	

	public Integer getClientId() {
		return clientId;
	}

	public Integer getVotes() {
		return votes;
	}

	public void setVotes(Integer votes) {
		this.votes = votes;
	}
	
	public Integer getVotesCollected() {
		return votesCollected;
	}

	public void setVotesCollected(Integer numberVotesCollected) {
		this.votesCollected = votesCollected;
		
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + 100 * coordinatorId + clientId;
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
