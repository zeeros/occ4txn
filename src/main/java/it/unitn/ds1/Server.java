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
		private List<DataOperation> writeCopies;
		private List<DataOperation> readCopies;
		private final Integer serverId;
		//Map a dataId with the number of previous write data operations done by the Txn
		private HashMap<Integer, Integer> previousWriteOperations;

		public PrivateWorkspace(Txn txn, Integer serverId) {
			this.txn = txn;
			this.writeCopies = new ArrayList<DataOperation>();
			this.readCopies = new ArrayList<DataOperation>();
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
		public DataItem getLastDataItemByCopies(Integer dataId, List<DataOperation> copies) {
			List<DataItem> itemsWithSameId = new ArrayList<DataItem>();
			for (DataOperation dataoperation: copies) {
				if (dataoperation.getKey() == dataId)
					itemsWithSameId.add(dataoperation.getDataItem());
					
			}
			if (itemsWithSameId.isEmpty())
			return null;
			else if (itemsWithSameId.size()==1){
				return itemsWithSameId.get(0);
		}else{
				Integer version = 0;
				DataItem lastDataItem = null;
			for (DataItem dataItem: itemsWithSameId) {
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
		Integer dataId = dataoperation.getKey();
		// if no private workspace : creation process and write the new data value
		// within

		PrivateWorkspace pw = getPrivateWorkspaceByTxn(txn);
		// Retrieve the current version by first checking if previous writes have been done
		if (pw == null) {
			pw = new PrivateWorkspace(txn, serverId);
			privateWorkspaces.put(pw.hashCode(), pw);
			}
		HashMap<Integer,Integer> previousWriteOperations = pw.getPreviousWriteOperationsByTxn();
		//If no write operations before : retrieve the dataItem in the data store
		if (previousWriteOperations.get(dataId) == 0) {
			dataoperation.setDataItem(datastore.get(dataId));
		}else{
		//Otherwise in the PW

			dataoperation.setDataItem(pw.getLastDataItemByCopies(dataId, pw.writeCopies));
		}
		//DataItem dataItemCopy = dataoperation.getDataItem();
		log.debug("server" + serverId + "<--[READ(" + dataoperation.getKey() + ")]--coordinator"
				+ txn.getCoordinatorId());
		
		// copy of the dataitem that will be temporary stored in the private workspace
		pw.readCopies.add(dataoperation);
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
		DataItem dataItemOverwriten ;
		//If no previous write operations before : retrieve the  original dataItem from the data store
		if (previousWriteOperations.get(dataId) == 0) {
		dataItemOverwriten = datastore.get(msg.dataoperation.getKey());
		//Otherwise retrieve from the private workspace
		}else {
		dataItemOverwriten = pw.getLastDataItemByCopies(dataId, pw.writeCopies);
		}
		// Increase the data version
		
		Integer version = dataItemOverwriten.getVersion();
		newDataItem.setVersion(version + 1);
		
		//Increase the number of previous write operation for the Txn
		previousWriteOperations.put(dataId, previousWriteOperations.get(dataId) + 1);
		log.debug("server" + serverId + "<--[WRITE(" + dataoperation.getKey() + ")=" + newDataItem.getValue() + ", oldversion=" + version
				+ "]--coordinator" + txn.getCoordinatorId());
		

		// copy of the dataitem that will be temporary stored in the private workspace
		pw.writeCopies.add(new DataOperation(DataOperation.Type.WRITE, dataId, newDataItem));
		
		
	}

	private void OnTxnAskVoteMsg(Coordinator.TxnAskVoteMsg msg) {
		Txn txn = msg.txn;
		Boolean vote = true;
		// Local validation: in the private workspace
		PrivateWorkspace pw = getPrivateWorkspaceByTxn(txn);
		

		
			
			DataItem dataItemReadCheck, dataItemWriteCheck;
			HashMap<Integer,Integer> previousWriteOperations = pw.getPreviousWriteOperationsByTxn();
			// We check if the version of the data read is the same as the one in the
			// datastore or the last in the private workspace and set lock if the data Items
			//are not already locked by another TXN
			
			for (DataOperation dataoperation : pw.readCopies) {
				Integer dataId = dataoperation.getKey();
				dataItemReadCheck = dataoperation.getDataItem();
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
					dataoperation.setType(DataOperation.Type.WRITE);
					if (datastore.get(dataId) != dataItemReadCheck) {
						if (pw.writeCopies !=null) {
							if (! pw.writeCopies.contains(dataoperation)) {
								vote = false;
							}
						}
					}
					
				}
			
			}

			// We check if the version of the data writen is the same as the one in the
			// datastore or the last in the private workspace and set lock if the data Items
			//are not already locked by another TXN
			
			for (DataOperation dataoperation : pw.writeCopies) {
				Integer dataId = dataoperation.getKey();
				dataItemWriteCheck = dataoperation.getDataItem();
				if (dataItemWriteCheck != null) {
					// Check if item is locked by another transaction
					// if so, cast an ABORT vote
					DataItem dataItemOriginal = datastore.get(dataId);
					Integer lock = dataItemOriginal.getLock();
					if(lock != null && lock != txn.hashCode()) {
						log.debug("hello version dataoperation : "+dataoperation.getDataItem().getVersion()+"version Origanal : "+dataItemOriginal.getVersion() + "lock : " + lock +"txnhashcode : " + txn.hashCode());
						vote = false;
					} else{
						//Set the lock for the current item
						dataItemOriginal.setLock(txn.hashCode());
					}
					if ((dataoperation.getDataItem().getVersion() - dataItemOriginal.getVersion())<=0) {
						vote = false;
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
				
				for (DataOperation dataoperation : pw.writeCopies) {
					Integer dataId = dataoperation.getKey();
					log.info("DataItem(" + dataId + ") =  (value = " + dataoperation.getDataItem().getValue()
							+ ",version = " + dataoperation.getDataItem().getVersion() + " -> replace : (value = "
							+ datastore.get(dataId).getValue() + ",version = " + datastore.get(dataId).getVersion()
							+ ")");
					datastore.put(dataId, dataoperation.getDataItem());
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
