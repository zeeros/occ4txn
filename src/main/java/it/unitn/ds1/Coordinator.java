package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import akka.actor.*;
import it.unitn.ds1.TxnClient.*;

public class Coordinator extends AbstractActor {
	private final Integer coordinatorId;
	private Map<Integer, ActorRef> clients, servers;
	private Map<Txn, List<DataOperation>> transactions;
	private Integer N_KEY_SERVER;

	private static final Logger log = LogManager.getLogger(Coordinator.class);

	/*-- Actor constructor ---------------------------------------------------- */

	public Coordinator(int coordinatorId) {
		this.coordinatorId = coordinatorId;
	}

	static public Props props(int coordinatorId) {
		return Props.create(Coordinator.class, () -> new Coordinator(coordinatorId));
	}

	/*-- Message classes ------------------------------------------------------ */

	// send this message to the coordinator at startup to inform it about the
	// clients and the servers
	public static class WelcomeMsg implements Serializable {
		public final Map<Integer, ActorRef> clients, servers;
		public final Integer N_KEY_SERVER;

		public WelcomeMsg(Map<Integer, ActorRef> clients, Map<Integer, ActorRef> servers, Integer N_KEY_SERVER) {
			this.clients = Collections.unmodifiableMap(new HashMap<Integer, ActorRef> (clients));
			this.servers = Collections.unmodifiableMap(new HashMap<Integer, ActorRef> (servers));
			this.N_KEY_SERVER = N_KEY_SERVER;
		}
	}

	// READ request from the coordinator to the server
	public static class ReadMsg implements Serializable {
		public final Txn txn;
		public final DataOperation dataoperation;

		public ReadMsg(Txn txn, DataOperation dataoperation) {
			this.txn = txn;
			this.dataoperation = dataoperation;
		}
	}
	
	// WRITE request from the coordinator to the server
	public static class WriteMsg implements Serializable {
		public final Txn txn;
		public final DataOperation dataoperation;

		public WriteMsg(Txn txn, DataOperation dataoperation) {
			this.txn = txn;
			this.dataoperation = dataoperation;
		}
	}
	
	// msg from the coordinator to the server to start overwriting the data item accessed by the TXN from the private workspace to the data store
	public static class TxnValidationMsg implements Serializable {
		public final Txn txn;
		public final boolean commit;
		
		public TxnValidationMsg(Txn txn, boolean commit) {
			this.txn = txn;
			this.commit = commit;
		}
		
	}
	
	// reply from the server when requested a READ on a given key
	public static class ReadResultMsg implements Serializable {
		public final Integer serverId;
		public final Txn txn;
		public final DataOperation dataoperation;

		public ReadResultMsg(Integer serverId, Txn txn, DataOperation dataoperation) {
			this.serverId = serverId;
			this.txn = txn;
			this.dataoperation = dataoperation;
		}
	}

	/*-- Actor methods -------------------------------------------------------- */

	// Retrieve the server by inferring its id from the key value
	// The coordinator is aware of the convention used by the distributed data store
	private ActorRef getServerByKey(Integer key) {
		return servers.get(key/N_KEY_SERVER);
	}

	/*-- Message handlers ---------------------------------------------------- - */

	private void onWelcomeMsg(WelcomeMsg msg) {
		this.clients = msg.clients;
		this.servers = msg.servers;
		this.N_KEY_SERVER = msg.N_KEY_SERVER;
		this.transactions = new HashMap<Txn, List<DataOperation>>();
	}

	private void OnTxnBeginMsg(TxnBeginMsg msg) {
		Integer clientId = msg.clientId;
		log.debug("coordinator" + coordinatorId + "<--[TXN_BEGIN]--client" + clientId);
		
		transactions.put(
				new Txn(coordinatorId, clientId),
				new ArrayList<DataOperation>());
		getSender().tell(new TxnAcceptMsg(), getSelf());
	}

	private void OnReadMsg(TxnClient.ReadMsg msg) {
		Integer clientId = msg.clientId;
		Integer key = msg.key;
		log.debug("coordinator" + coordinatorId + "<--[READ(" + key + ")]--client" + clientId);

		// Set the transaction
		Txn txn = new Txn(coordinatorId, clientId);
		// Set the operation to be add to the transaction
		DataOperation dataOperation = new DataOperation(DataOperation.Type.READ, key, null);
		// Retrieve the transaction for clientId and add append to it the READ operation
		List<DataOperation> dataoperations = transactions.get(txn);
		dataoperations.add(dataOperation);
		getServerByKey(key).tell(new Coordinator.ReadMsg(txn, dataOperation), getSelf());
	}

	private void OnReadResultMsg(Coordinator.ReadResultMsg msg) {
		Integer serverId = msg.serverId;
		Txn txn = msg.txn;
		DataOperation dataoperation = msg.dataoperation;
		
		log.debug("coordinator" + coordinatorId + "<--[READ("+ dataoperation.getKey() +")="+dataoperation.getDataItem().getValue()+"]--server" + serverId);
		
		Integer clientId = txn.getClientId();
		ActorRef client = clients.get(clientId);
		client.tell(new TxnClient.ReadResultMsg(dataoperation.getKey(), dataoperation.getDataItem().getValue()), getSelf());
	}
	
	private void OnWriteMsg(TxnClient.WriteMsg msg) {
		Integer clientId = msg.clientId;
		Integer key = msg.key;
		Integer value = msg.value;
		log.debug("coordinator" + coordinatorId + "<--[WRITE(" + key + ")="+value+"]--client" + clientId);

		// Set the transaction
		Txn txn = new Txn(coordinatorId, clientId);
		// Retrieve the transaction for clientId
		List<DataOperation> dataoperations = transactions.get(txn);
		// Set the operation to be add to the transaction
		DataItem dataItem = new DataItem(null, value);
		DataOperation dataOperation = new DataOperation(DataOperation.Type.WRITE, key, dataItem);
		// Append the WRITE operation to the transaction
		dataoperations.add(dataOperation);
		getServerByKey(key).tell(new Coordinator.WriteMsg(txn, dataOperation), getSelf());
	}

	private void OnOverwritingConfirmationMsg(OverwritingConfirmationMsg msg) {
		
		
	}
	
	private void OnTxnEndMsg(TxnEndMsg msg) {
		Integer clientId = msg.clientId;
		log.debug("coordinator" + coordinatorId + "<--[TXN_END]--client" + clientId);
		getSender().tell(new TxnResultMsg(true), getSelf());
	}
	
	

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(Coordinator.WelcomeMsg.class, this::onWelcomeMsg)
				.match(TxnClient.TxnBeginMsg.class, this::OnTxnBeginMsg)
				.match(TxnClient.ReadMsg.class, this::OnReadMsg)
				.match(Coordinator.ReadResultMsg.class, this::OnReadResultMsg)
				.match(TxnClient.WriteMsg.class, this::OnWriteMsg)
				.match(OverwritingConfirmationMsg.class, this::OnOverwritingConfirmationMsg)
				.match(TxnClient.TxnEndMsg.class, this::OnTxnEndMsg).build();
	}

}
