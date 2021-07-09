package it.unitn.ds1;

import akka.actor.*;
import it.unitn.ds1.Server.PrivateWorkspace;
import it.unitn.ds1.TxnClient.TxnResultMsg;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Server extends AbstractActor {
	private final Integer serverId;
//list of every Private Workspaces in the server
	private Map<Integer, PrivateWorkspace> privateWorkspaces = new HashMap<Integer, PrivateWorkspace>();
	private static final Logger log = LogManager.getLogger(Server.class);

	// TXN operation (move some amount from a value to another)
	private Map<Integer, DataItem> datastore;

	/*-- Actor constructor ---------------------------------------------------- */

	public Server(int serverId, Map<Integer, DataItem> datastore) {
		this.serverId = serverId;
		this.datastore = datastore;
	}

	static public Props props(int serverId, Map<Integer, DataItem> datastore) {
		return Props.create(Server.class, () -> new Server(serverId, datastore));
	}

	/*-- Private workspace class ------------------------------------------------------ */
	public class PrivateWorkspace {
		private Txn txn;
		private HashMap<Integer, DataItem> writeCopies;
		private HashMap<Integer, DataItem> readCopies;
		private final Integer serverId;
		//Map a dataId with the number of previous write data operations done by the Txn
		private HashMap<Integer, Integer> previousWriteOperations;

		public PrivateWorkspace(Txn txn, Integer serverId) {
			this.txn = txn;
			this.writeCopies = new HashMap<Integer, DataItem>();
			this.readCopies = new HashMap<Integer, DataItem>();
			this.serverId = serverId;
			this.previousWriteOperations = new HashMap<Integer, Integer>();
			//set all the values to 0
			for (Integer dataId: datastore.keySet())
				previousWriteOperations.put(dataId, 0);
			
		}

		public Integer getServerId() {
			return serverId;
		}

		public Txn getTxn() {
			return txn;
		}
		
		public HashMap<Integer, Integer> getPreviousWriteOperationsByTxn() {
			return (previousWriteOperations);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + txn.hashCode() + serverId;
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
			PrivateWorkspace other = (PrivateWorkspace) obj;
			if (serverId != other.getServerId())
				return false;
			if (txn != other.getTxn())
				return false;
			return true;
		}

	}

	/*-- Actor methods -------------------------------------------------------- */
	public PrivateWorkspace getPrivateWorkspaceByTxn(Txn txn) {
		for (Integer pwId : privateWorkspaces.keySet()) {
			if (privateWorkspaces.get(pwId).txn.hashCode() == txn.hashCode()) {
				return (privateWorkspaces.get(pwId));
			}
		}
		return null;
	}

	/*-- Message classes ------------------------------------------------------ */


	
	public static class WriteMsg implements Serializable {
	}

	public static class TxnVoteMsg implements Serializable {
		public Txn txn;
		public Boolean vote;
		public Integer serverId;

		public TxnVoteMsg(Txn txn, Boolean vote, Integer serverId) {
			this.txn = txn;
			this.vote = vote;
			this.serverId = serverId;
		}
	}
	
	public static class GoodbyeMsg implements Serializable {
		public int serverId;
		public Map<Integer, DataItem> datastore;

		public GoodbyeMsg(int serverId, Map<Integer, DataItem> datastore) {
			this.serverId = serverId;
			this.datastore = datastore;
		}
	}

	/*-- Message handlers ----------------------------------------------------- */

	private void OnReadMsg(Coordinator.ReadMsg msg) {
		Txn txn = msg.txn;
		DataOperation dataoperation = msg.dataoperation;
		// if no private workspace : creation process and write the new data value
		// within

		PrivateWorkspace pw = getPrivateWorkspaceByTxn(txn);
		// Retrieve the current version by first checking if previous writes have been done
		if (pw == null) {
			pw = new PrivateWorkspace(txn, serverId);
			privateWorkspaces.put(pw.hashCode(), pw);
			}
		HashMap<Integer,Integer> previousWriteOperations = pw.getPreviousWriteOperationsByTxn();
		//If no write operations before : retrieve the dataITem in the data store
		if (previousWriteOperations.get(dataoperation.getKey()) == 0) {
			dataoperation.setDataItem(datastore.get(dataoperation.getKey()));
		}else{
		//Otherwise in the PW
			dataoperation.setDataItem(pw.writeCopies.get(dataoperation.getKey()));
		}
		DataItem dataItemCopy = dataoperation.getDataItem();
		log.debug("server" + serverId + "<--[READ(" + dataoperation.getKey() + ")]--coordinator"
				+ txn.getCoordinatorId());
		
		// copy of the dataitem that will be temporary stored in the private workspace
		pw.readCopies.put(dataoperation.getKey(), dataItemCopy);
		// Respond to the coordinator with the serverId, TXN, its data operation and the
		// value in the datastore
		getSender().tell(new Coordinator.ReadResultMsg(serverId, txn, dataoperation), getSelf());
	}

	private void OnWriteMsg(Coordinator.WriteMsg msg) {
		Txn txn = msg.txn;
		DataOperation dataoperation = msg.dataoperation;

		// if no private workspace : creation process and write the new data value
		// within

		PrivateWorkspace pw = getPrivateWorkspaceByTxn(txn);
		if (pw == null) {
			pw = new PrivateWorkspace(txn, serverId);
			privateWorkspaces.put(pw.hashCode(), pw);
		}
		
		// Retrieve the current version
		Integer dataId = dataoperation.getKey();
		DataItem newDataItem = dataoperation.getDataItem();
		

		// we need to retrieve the version of the data item that is wanted to be
		// overwriten on in the private workspace or in the datastore
		
		HashMap<Integer,Integer> previousWriteOperations = pw.getPreviousWriteOperationsByTxn();
		DataItem dataItemOriginal ;
		//If no previous write operations before : retrieve the  original dataItem from the data store
		if (previousWriteOperations.get(dataId) == 0) {
		dataItemOriginal = datastore.get(msg.dataoperation.getKey());
		//Otherwise retrieve from the private workspace
		}else {
		dataItemOriginal = pw.writeCopies.get(dataId);
		}
		// Increase the data version
		Integer version = dataItemOriginal.getVersion();
		newDataItem.setVersion(version + 1);
		//Increase the number of previous write operation for the Txn
		previousWriteOperations.put(dataId, previousWriteOperations.get(dataId) + 1);
		log.debug("server" + serverId + "<--[WRITE(" + dataoperation.getKey() + ")=" + newDataItem.getValue() + ", version=" + version
				+ "]--coordinator" + txn.getCoordinatorId());
		

		// copy of the dataitem that will be temporary stored in the private workspace

		pw.writeCopies.put(dataoperation.getKey(), newDataItem);
		
		
	}

	private void OnTxnAskVoteMsg(Coordinator.TxnAskVoteMsg msg) {
		Txn txn = msg.txn;
		Boolean vote = true;
		// Local validation: in the private workspace
		PrivateWorkspace pw = getPrivateWorkspaceByTxn(txn);
		

		if (!(pw == null)) {
			DataItem dataItemReadCheck, dataItemWriteCheck;
			HashMap<Integer,Integer> previousWriteOperations = pw.getPreviousWriteOperationsByTxn();
			// We check if the version of the data read is the same as the one in the
			// datastore or the last in the private workspace and set lock if the data Items
			//are not already locked by another TXN
		
			for (Integer dataId : pw.readCopies.keySet()) {
				dataItemReadCheck = pw.readCopies.get(dataId);
				if (dataItemReadCheck != null) {
					// Get the lock for the current data item
					Integer lock = datastore.get(dataId).getLock();
					// Check if item is locked by another transaction
					// if so, cast an ABORT vote
					if(lock != null && lock != txn.hashCode()) {
						vote = false;
					} else {
						//Set the lock for the current item
						datastore.get(dataId).setLock(txn.hashCode());
					}
					
					if (dataItemReadCheck.getVersion() != datastore.get(dataId).getVersion() + previousWriteOperations.get(dataId)) {
						vote = false;
					}
				}
			}

			// We check if the version of the data writen is the same as the one in the
			// datastore or the last in the private workspace and set lock if the data Items
			//are not already locked by another TXN
			for (Integer dataId : pw.writeCopies.keySet()) {
				dataItemWriteCheck = pw.writeCopies.get(dataId);
				if (dataItemWriteCheck != null) {
					// Check if item is locked by another transaction
					// if so, cast an ABORT vote
					Integer lock = datastore.get(dataId).getLock();
					if(lock != null && lock != txn.hashCode()) {
						vote = false;
					} else {
						//Set the lock for the current item
						datastore.get(dataId).setLock(txn.hashCode());
					}
					if (dataItemWriteCheck.getVersion() != (datastore.get(dataId).getVersion() + previousWriteOperations.get(dataId))) {
						vote = false;
					}
				}
			}
		}
		
		getSender().tell(new TxnVoteMsg(txn, vote, serverId), getSelf());
		log.info("ServerId : " + serverId + " -> coordinator : " + getSender() + "(local vote result = " + vote);

	}

	private void OnTxnVoteResultMsg(Coordinator.TxnVoteResultMsg msg) {
		Txn txn = msg.txn;
		boolean commit = msg.commit;
		PrivateWorkspace pw = getPrivateWorkspaceByTxn(txn);

		// if the message is commit, we replace the dataItem from the private workspace
		// to the datastore
		// the msg will be sent to every server so we need to check again if there is a
		// private workspace
		if (!(pw == null)) {
			if (commit) {
				for (Integer dataId : pw.writeCopies.keySet()) {
					log.info("DataItem(" + dataId + ") =  (value = " + pw.writeCopies.get(dataId).getValue()
							+ ",version = " + pw.writeCopies.get(dataId).getVersion() + " -> replace : (value = "
							+ datastore.get(dataId).getValue() + ",version = " + datastore.get(dataId).getVersion()
							+ ")");
					datastore.replace(dataId, pw.writeCopies.get(dataId));
				}
			}

			// We remove the the private workspace from the server either the decision is
			// commit or not
			privateWorkspaces.remove(pw.hashCode());
			pw = null;
		}
	
		

		
		// Release the locks set by the current transaction over all the data items
		for (Map.Entry<Integer,DataItem> entry : datastore.entrySet()) {
			Integer lock = entry.getValue().getLock();
			if(lock != null && lock == txn.hashCode()) {
				entry.getValue().setLock(null);
			}
		}
	}
	
	
	private void OnGoodbyeMsg(ConsistencyTester.GoodbyeMsg msg) {
		getSender().tell(new Server.GoodbyeMsg(serverId, datastore), getSelf());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(Coordinator.ReadMsg.class, this::OnReadMsg)
				.match(Coordinator.WriteMsg.class, this::OnWriteMsg)
				.match(Coordinator.TxnAskVoteMsg.class, this::OnTxnAskVoteMsg)
				.match(Coordinator.TxnVoteResultMsg.class, this::OnTxnVoteResultMsg)
				.match(ConsistencyTester.GoodbyeMsg.class, this::OnGoodbyeMsg).build();
	}

	// Depends only on account number
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + serverId;
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
		Server other = (Server) obj;
		if (serverId != other.serverId)
			return false;
		return true;
	}

	private <P extends Object> void OnLocalSumCheckMsg(P p1) {
	}

}
