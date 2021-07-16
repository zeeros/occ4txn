package it.unitn.ds1;

import akka.actor.*;
import it.unitn.ds1.DataOperation.Type;
import it.unitn.ds1.Server.PrivateWorkspace;
import it.unitn.ds1.TxnClient.TxnResultMsg;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Server extends AbstractActor {
	private final Integer serverId;
//list of every Private Workspaces in the server
	private Map<Txn, PrivateWorkspace> privateWorkspaces = new HashMap<Txn, PrivateWorkspace>();
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
		//WriteCopies will contain the history of write operations for a Txn
		private List<DataOperation> writeCopies;
		//WriteCopies will contain the history of write operations for a Txn
		private List<DataOperation> readCopies;
		private final Integer serverId;
		//Map a dataId with the number of write operations done in the server for a Txn
		private HashMap<Integer, Integer> previousWriteOperations;

		public PrivateWorkspace(Txn txn, Integer serverId) {
			this.txn = txn;
			this.writeCopies = new ArrayList<DataOperation>();
			this.readCopies = new ArrayList<DataOperation>();
			this.serverId = serverId;
			this.previousWriteOperations = new HashMap<Integer, Integer>();
			// set all the values to 0
			for (Integer dataId : datastore.keySet())
				previousWriteOperations.put(dataId, 0);

		}

		public Integer getServerId() {
			return serverId;
		}

		public Txn getTxn() {
			return txn;
		}
		/*
		 * Given a dataId and list of copies of operations (write or reads), return the last version of the dataItem with id dataId
		 * 
		 * 
		 * 
		 */
		public DataItem getLastDataItemOfCopies(Integer dataId, List<DataOperation> copies) {
			List<DataItem> itemsWithSameId = new ArrayList<DataItem>();
			//retrieve all the dataItem with id DataId
			for (DataOperation dataoperation : copies) {
				if (dataoperation.getKey() == dataId)
					itemsWithSameId.add(dataoperation.getDataItem());

			}
			if (itemsWithSameId.isEmpty())
				return null;
			else if (itemsWithSameId.size() == 1) {
				return itemsWithSameId.get(0);
				//retrieve the dataItem with the max version of the previous collection
			} else {
				Integer version = 0;
				DataItem lastDataItem = null;
				for (DataItem dataItem : itemsWithSameId) {
					if (dataItem.getVersion() >= version)
						version = dataItem.getVersion();
					lastDataItem = dataItem;
				}
				return lastDataItem;
			}
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + 100 * txn.getClientId() + txn.getCoordinatorId() + 1000 * serverId;
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
		for (Txn pwId : privateWorkspaces.keySet()) {
			if (pwId.equals(txn)) {
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
// Goodbye message sent when the system is frozen, the server is sending the content of its datastore to the Consistency Tester
	public static class GoodbyeMsg implements Serializable {
		public int serverId;
		public Map<Integer, DataItem> datastore;

		public GoodbyeMsg(int serverId, Map<Integer, DataItem> datastore) {
			this.serverId = serverId;
			this.datastore = datastore;
		}
	}

	/*-- Message handlers ----------------------------------------------------- */

	//Handling the read requests from a coordinator for a given Txn
	
	private void OnReadMsg(Coordinator.ReadMsg msg) {
		Txn txn = msg.txn;
		DataOperation dataoperation = msg.dataoperation;
		Integer dataId = dataoperation.getKey();
		DataItem readDataItem;
		
		// First, get the pw of the Txn
		// if no private workspace of the Txn txn : creation process
		PrivateWorkspace pw = getPrivateWorkspaceByTxn(txn);
		if (pw == null) {
			pw = new PrivateWorkspace(txn, serverId);
			privateWorkspaces.put(txn, pw);
		}
		// Retrieve the current version by first checking if previous writes have been done
		HashMap<Integer, Integer> previousWriteOperations = pw.previousWriteOperations;
		
		// If no write operations before : retrieve the dataItem in the data store and set the value of the data operation
		if (previousWriteOperations.get(dataId) == 0) {
			dataoperation.setDataItem(datastore.get(dataId));
			
		// Otherwise : retrieve the last version of the dataItem in the data store in the PW and set the value of the data operation
		} else {
			dataoperation.setDataItem(pw.getLastDataItemOfCopies(dataId, pw.writeCopies));
		}
		readDataItem = new DataItem(dataoperation.getDataItem().getVersion(),
				dataoperation.getDataItem().getValue());
		log.debug("server" + serverId + "<--[READ(" + dataoperation.getKey() + ")]--coordinator"
				+ txn.getCoordinatorId());
		
		// copy & store the dataitem in the private workspace
		pw.readCopies.add(new DataOperation(Type.READ, dataId, dataoperation.getDataItem()));
		// Answer to the coordinator with the serverId, the txn and the updated data operation
		getSender().tell(new Coordinator.ReadResultMsg(serverId, txn, dataoperation), getSelf());
	}

	//Handling the write requests from a coordinator for a given Txn
	private void OnWriteMsg(Coordinator.WriteMsg msg) {
		Txn txn = msg.txn;
		DataOperation dataoperation = msg.dataoperation;
		Integer dataId = dataoperation.getKey();
		DataItem dataItemOverwriten;
		// First, get the pw of the Txn
		// if no private workspace of the Txn txn : creation process
		PrivateWorkspace pw = getPrivateWorkspaceByTxn(txn);
		if (pw == null) {
			pw = new PrivateWorkspace(txn, serverId);
			privateWorkspaces.put(txn, pw);
		}
		HashMap<Integer, Integer> previousWriteOperations = pw.previousWriteOperations;
		
		// Creating the new dataItem
		DataItem newDataItem = new DataItem(dataoperation.getDataItem().getVersion(),
				dataoperation.getDataItem().getValue());
		
		// If no previous write operations before on the data Item of id Dataid :
		// retrieve the original dataItem from the data store
		
		if (previousWriteOperations.get(dataId) == 0) {
			dataItemOverwriten = datastore.get(dataId);
			// Otherwise : retrieve the last version of the dataItem in the data store in the PW 
		} else {
			dataItemOverwriten = pw.getLastDataItemOfCopies(dataId, pw.writeCopies);
		}
		// Retrieve the data version of the dataItem and Increase the data version of the future new dataItem<
		Integer version = dataItemOverwriten.getVersion();
		newDataItem.setVersion(version + 1);

		// Increase the number of previous write operation for the Txn
		Integer newDataOperationCounter = previousWriteOperations.get(dataId) + 1;
		previousWriteOperations.put(dataId, newDataOperationCounter);
		
		log.debug("server" + serverId + "<--[WRITE(" + dataoperation.getKey() + ")=" + newDataItem.getValue()
				+ ", previousversion=" + version + "]--coordinator" + txn.getCoordinatorId());

		// copy the dataitem that will be temporary stored in the private workspace
		pw.writeCopies.add(new DataOperation(DataOperation.Type.WRITE, dataId, newDataItem));

	}
	
	/*
	 * Given a transaction, checking if the dataItems involved are locked
	 * If locking
	 * 
	 * 
	 * @param pw, datastore
	 * @return
	 */
	private Boolean CheckAndSetLocks (Txn txn, PrivateWorkspace pw) {
		Boolean possibleToLock = true;
		DataItem dataItemReadCheck, dataItemWriteCheck;
		//first check in the read copies of the private workspace
		for (DataOperation dataoperation : pw.readCopies) {
			dataItemReadCheck = dataoperation.getDataItem();
			Integer dataId = dataoperation.getKey();
			if (dataItemReadCheck != null) {
				// Get the lock for the current data item
				Integer lock = datastore.get(dataId).getLock();
				// Check if item is locked by another transaction
				// if so, cast an ABORT vote
				if (lock != null && lock != txn.hashCode()) {
					possibleToLock = false;
					return possibleToLock;
				
				}else{
					datastore.get(dataId).setLock(txn.hashCode());
				}
			}
		}
		if (pw.writeCopies != null) {
			
		//do the same process with the write copies of the private workspace
			
			for (DataOperation dataoperation : pw.writeCopies) {
				dataItemWriteCheck = dataoperation.getDataItem();
				Integer dataId = dataoperation.getKey();
				if (dataItemWriteCheck != null) {
					// Get the lock for the current data item
					Integer lock = datastore.get(dataId).getLock();
					// Check if item is locked by another transaction
					// if so, cast an ABORT vote
					if (lock != null && lock != txn.hashCode()) {
						possibleToLock = false;
						return possibleToLock;
						
					}else{
						datastore.get(dataId).setLock(txn.hashCode());
					} 
				}
			}
		}
		return possibleToLock;
	}
	
	// Release the locks set by the current transaction over all the data items
	
	private void ReleaseLocks(Txn txn) {
		for (Map.Entry<Integer, DataItem> entry : datastore.entrySet()) {
			Integer lock = entry.getValue().getLock();
			if (lock != null && lock == txn.hashCode()) {
				entry.getValue().setLock(null);
			}
		}
	}

	// Local validation of the Txn validated by the server
	
	private void OnTxnAskVoteMsg(Coordinator.TxnAskVoteMsg msg) {
		Txn txn = msg.txn;
		Boolean vote = true;
		PrivateWorkspace pw = getPrivateWorkspaceByTxn(txn);
		DataItem dataItemReadCheck, dataItemWriteCheck;
		
		//Check if items involved in the txn are already locked
		//If so : vote false
		//If not : set locks to the dataitems
		if (CheckAndSetLocks(txn, pw) == false) {
			vote = false;
		}
		
		//local validation of the read operations
		
		for (DataOperation dataoperation : pw.readCopies) {
			Integer dataId = dataoperation.getKey();
			dataItemReadCheck = dataoperation.getDataItem();
			dataoperation.setType(DataOperation.Type.WRITE);
			
			if (datastore.get(dataId) != dataItemReadCheck) {
				if (pw.writeCopies != null) {
					if (!pw.writeCopies.contains(dataoperation)) {
						vote = false;
					}
				}
			}

		}

		//local validation of the write operations

		for (DataOperation dataoperation : pw.writeCopies) {
			Integer dataId = dataoperation.getKey();
			dataItemWriteCheck = dataoperation.getDataItem();
			DataItem dataItemOriginal = datastore.get(dataId);
			if ((dataItemWriteCheck.getVersion() - dataItemOriginal.getVersion()) <= 0) {
				vote = false;
			}
		}
		//Notify the coordinator of the decision of server
		getSender().tell(new TxnVoteMsg(txn, vote, serverId), getSelf());
		log.info("ServerId : " + serverId + " -> coordinator : " + txn.getCoordinatorId() + "(local vote result = " + vote);
	}

	private void OnTxnVoteResultMsg(Coordinator.TxnVoteResultMsg msg) {
		Txn txn = msg.txn;
		boolean commit = msg.commit;
		PrivateWorkspace pw = getPrivateWorkspaceByTxn(txn);

		log.debug("Server " + serverId + " gets the final vote result: " + commit);

		// if the message is commit, we replace the dataItem from the private workspace
		// in the datastore

		if (!(pw == null)) {
			if (commit) {

				for (DataOperation dataoperation : pw.writeCopies) {
					Integer dataId = dataoperation.getKey();

					//We make overwrites in the datastore
					Integer originalVersion = datastore.get(dataId).getVersion();
					Integer version = dataoperation.getDataItem().getVersion();
					 
					log.info("DataItem(" + dataId + ") =  (value = " + dataoperation.getDataItem().getValue()
							+ ",version = " + dataoperation.getDataItem().getVersion() + " -> replace : (value = "
							+ datastore.get(dataId).getValue() + ",version = " + datastore.get(dataId).getVersion()
							+ ")");
					datastore.put(dataId, new DataItem(dataoperation.getDataItem().getVersion(),
							dataoperation.getDataItem().getValue()));

				}
				
			}
			// We remove the the private workspace from the server either the decision is
			// commit or not
			pw.previousWriteOperations = null;
			pw.readCopies = null;
			pw.writeCopies = null;
			pw = null;
			
		}
		privateWorkspaces.remove(txn);
		// Release the locks set by the current transaction over all the data items
		ReleaseLocks(txn);

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

}
