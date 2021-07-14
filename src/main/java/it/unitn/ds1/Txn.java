package it.unitn.ds1;

public class Txn {
	private final Integer clientId;
	// indicates the state of the transaction :
	// validated if the TXN is consistent
	boolean validatedConsistent;
	// overwriten if all the transient dataItems from the private workspace have
	// been successful
	boolean overwritesDone;
	// number of "commit" votes
	private Integer votes;
	private Integer votesCollected;

	public Txn(int clientId) {
		this.clientId = clientId;
	    // collects the number of "yes "vote answer from the servers involved in the Txn
		this.votes = 0;
	    // collects the number of vote answer from the servers involved in the Txn
		this.votesCollected = 0;
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
	// 
	public Integer getVotesCollected() {
		return votesCollected;
	}

	public void setVotesCollected(Integer votesCollected) {
		this.votesCollected = votesCollected;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + clientId ;
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
		if (clientId != other.getClientId())
			return false;
		return true;
	}
}
