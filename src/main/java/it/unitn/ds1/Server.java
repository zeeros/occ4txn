package it.unitn.ds1;

import akka.actor.*;
import it.unitn.ds1.TxnClient.TxnEndMsg;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Server extends AbstractActor {
	private final Integer serverId;
	//list of every Private Workspaces
	

    static Map<Integer,PrivateWorkspace> PrivateWorkspaces = new HashMap<Integer,PrivateWorkspace>();
    
	private static final Logger log = LogManager.getLogger(Server.class);

	// TXN operation (move some amount from a value to another)
	private static Map<Integer, Integer> datastore;

	/*-- Actor constructor ---------------------------------------------------- */

	public Server(int serverId, Map<Integer, Integer> datastore) {
		this.serverId = serverId;
		this.datastore = datastore;
	}

	static public Props props(int serverId, Map<Integer, Integer> datastore) {
		return Props.create(Server.class, () -> new Server(serverId, datastore));
	}
	/*-- Private workspace classes ------------------------------------------------------ */
	public static class PrivateWorkspace {
		private Txn txn;
		private HashMap<Integer, Integer> copies;

		public PrivateWorkspace(Txn txn) {
			this.txn = txn;
		}
	}

	/*-- Message classes ------------------------------------------------------ */

	// WRITE request from the coordinator to the server
	public static class WriteMsg implements Serializable {
	}
	
	/*-- Message handlers ----------------------------------------------------- */

	private void OnReadMsg(Coordinator.ReadMsg msg) {
		Txn txn = msg.txn;
		DataOperation dataoperation = msg.dataoperation;
		log.debug("server" + serverId + "<--[READ(" + dataoperation.getKey() + ")]--coordinator" + txn.getCoordinatorId());
		Integer value = datastore.get(dataoperation.getKey());
		// Respond to the coordinator with the serverId, TXN, its data operation and the value in the datastore
		getSender().tell(new Coordinator.ReadResultMsg(serverId, txn, dataoperation, value), getSelf());
	}
	
	private void OnWriteMsg(Coordinator.WriteMsg msg) {
		Txn txn = msg.txn;
		DataOperation dataoperation = msg.dataoperation;
		log.debug("server" + serverId + "<--[WRITE(" + dataoperation.getKey() + ")="+dataoperation.getValue()+"]--coordinator" + txn.getCoordinatorId());
		// Overwrite the data item value
		//datastore.put(dataoperation.getKey(), dataoperation.getValue());
		
		// No answer in case of write
		
		//if no private workspace : creation process and write the new data value within
	
		PrivateWorkspace pw = PrivateWorkspaces.get(txn.hashCode());
	    if (pw == null) {
	    	pw = new PrivateWorkspace(txn);
	    	PrivateWorkspaces.put(txn.hashCode(), pw);
	    	//copy of the dataitem that will be temporary stored in the private workspace
			pw.copies.put(dataoperation.getKey(), dataoperation.getValue());
	    }
	    else  {
	    	pw.copies.put(dataoperation.getKey(), dataoperation.getValue());
	    }
	}
	private void OnTxnValidationMsg(Coordinator.TxnValidationMsg msg) {
		Txn txn = msg.txn;
		boolean commit = msg.commit;
		PrivateWorkspace pw = PrivateWorkspaces.get(txn.hashCode());
		
		if (commit) {
			for(Integer dataid: pw.copies.keySet()) {
				datastore.replace(dataid, pw.copies.get(dataid));
			}	
	
		
		}
		// We remove the the private workspace from the server
		PrivateWorkspaces.remove(txn.hashCode());
		pw = null;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Coordinator.ReadMsg.class, this::OnReadMsg)
				.match(Coordinator.WriteMsg.class, this::OnWriteMsg)
	            .match(Coordinator.TxnValidationMsg.class, this::OnTxnValidationMsg).build();
	}

}
