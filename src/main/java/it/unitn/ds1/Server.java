package it.unitn.ds1;

import akka.actor.*;
import it.unitn.ds1.Server.PrivateWorkspace;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Server extends AbstractActor {
	private final Integer serverId;
//list of every Private Workspaces in the server
    static Map<Integer,PrivateWorkspace> privateWorkspaces = new HashMap<Integer,PrivateWorkspace>();
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
	public static class PrivateWorkspace {
		private Txn txn;
		private HashMap<Integer, DataItem> writeCopies;
		private HashMap<Integer, DataItem> readCopies;
		public PrivateWorkspace(Txn txn) {
			this.txn = txn;
			this.writeCopies = new HashMap<Integer, DataItem>();
			this.readCopies = new HashMap<Integer, DataItem>();
		}
	}

	/*-- Message classes ------------------------------------------------------ */

	// WRITE request from the coordinator to the server
	public static class WriteMsg implements Serializable {
	}
	
	public static class OverwritingConfirmationMsg implements Serializable{
		public final Txn txn;
		public OverwritingConfirmationMsg(Txn txn) {
			this.txn = txn;
		}
	}
	/*-- Message handlers ----------------------------------------------------- */

	private void OnReadMsg(Coordinator.ReadMsg msg) {
		Txn txn = msg.txn;
		DataOperation dataoperation = msg.dataoperation;
		//if no private workspace : creation process and write the new data value within
				PrivateWorkspace pw = privateWorkspaces.get(txn.hashCode());
				// Retrieve the current version
				DataItem dataItemCopy = dataoperation.getDataItem();
				log.debug("server" + serverId + "<--[READ(" + dataoperation.getKey() + ")]--coordinator" + txn.getCoordinatorId());
				if (pw == null) {
			    	pw = new PrivateWorkspace(txn);
			    	privateWorkspaces.put(txn.hashCode(), pw);
			    	//copy of the dataitem that will be temporary stored in the private workspace
			    	
					pw.readCopies.put(dataoperation.getKey(), dataItemCopy);
			    }
			    else  {
			    	pw.readCopies.put(dataoperation.getKey(), dataItemCopy);
			    }
		dataoperation.setDataItem(datastore.get(dataoperation.getKey()));
		// Respond to the coordinator with the serverId, TXN, its data operation and the value in the datastore
		getSender().tell(new Coordinator.ReadResultMsg(serverId, txn, dataoperation), getSelf());
	}
	
	private void OnWriteMsg(Coordinator.WriteMsg msg) {
		Txn txn = msg.txn;
		DataOperation dataoperation = msg.dataoperation;
		
		
		//if no private workspace : creation process and write the new data value within
		
		PrivateWorkspace pw = privateWorkspaces.get(txn.hashCode());
		// Retrieve the current version
		Integer value = dataoperation.getDataItem().getValue();
		DataItem dataItemCopy = dataoperation.getDataItem();
		
		// we need to retrieve the version of the data item that is wanted to be overwriten
		DataItem dataItemOriginal = datastore.get(msg.dataoperation.getKey());
		// And increase it
		dataItemCopy.setVersion(dataItemOriginal.getVersion()+1);
		Integer version = dataItemCopy.getVersion();
		log.debug("server" + serverId + "<--[WRITE(" + dataoperation.getKey() + ")="+value+", version="+version+"]--coordinator" + txn.getCoordinatorId());
	    if (pw == null) {
	    	pw = new PrivateWorkspace(txn);
	    	privateWorkspaces.put(txn.hashCode(), pw);
	    	//copy of the dataitem that will be temporary stored in the private workspace
	    	
			pw.writeCopies.put(dataoperation.getKey(), dataItemCopy);
	    }
	    else  {
	    	pw.writeCopies.put(dataoperation.getKey(), dataItemCopy);
	    }
	}
	private void OnTxnValidationMsg(Coordinator.TxnValidationMsg msg) {
		Txn txn = msg.txn;
		boolean commit = msg.commit;
		PrivateWorkspace pw = privateWorkspaces.get(txn.hashCode());
		//if the message is commit, we replace the dataItem from the private workspace to the datastore
		//the msg will be sent to every server so we need to check again if there is a private workspace
		if (!(pw==null)) {
			if (commit) {
				for(Integer dataid: pw.writeCopies.keySet()) {
					datastore.replace(dataid, pw.writeCopies.get(dataid));
				}	
			}
		
			// We remove the the private workspace from the server either the decision is commit or not
			privateWorkspaces.remove(txn.hashCode());
			pw = null;
			//we confirm to the coordinator that overwrites have been done
			getSender().tell(new OverwritingConfirmationMsg(txn), getSender());
		}
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Coordinator.ReadMsg.class, this::OnReadMsg)
				.match(Coordinator.WriteMsg.class, this::OnWriteMsg)
				.match(Coordinator.TxnValidationMsg.class, this::OnTxnValidationMsg).build();
	}

}
