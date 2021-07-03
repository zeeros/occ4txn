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

		public WelcomeMsg(Map<Integer, ActorRef> clients, Map<Integer, ActorRef> servers) {
			this.clients = Collections.unmodifiableMap(new HashMap<Integer, ActorRef> (clients));
			this.servers = Collections.unmodifiableMap(new HashMap<Integer, ActorRef> (servers));
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
	
	// reply from the server when requested a READ on a given key
	public static class ReadResultMsg implements Serializable {
		public final Integer serverId;
		public final Txn txn;
		public final DataOperation dataoperation;
		public final Integer result;

		public ReadResultMsg(Integer serverId, Txn txn, DataOperation dataoperation, Integer result) {
			this.serverId = serverId;
			this.txn = txn;
			this.dataoperation = dataoperation;
			this.result = result;
		}
	}

	/*-- Actor methods -------------------------------------------------------- */

	// Retrieve the server by inferring its id from the key value
	// The coordinator is aware of the convention used by the distributed data store
	private ActorRef getServerByKey(Integer key) {
		ActorRef server = servers.get(0);
		return server;
	}

	/*-- Message handlers ---------------------------------------------------- - */

	private void onWelcomeMsg(WelcomeMsg msg) {
		this.clients = msg.clients;
		this.servers = msg.servers;
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
		// Retrieve the server holding the item by its key
		getServerByKey(key).tell(new Coordinator.ReadMsg(txn, dataOperation), getSelf());
	}

	private void OnReadResultMsg(Coordinator.ReadResultMsg msg) {
		Integer serverId = msg.serverId;
		Txn txn = msg.txn;
		DataOperation dataoperation = msg.dataoperation;
		Integer result = msg.result;
		
		log.debug("coordinator" + coordinatorId + "<--[READ("+ dataoperation.getKey() +")="+result+"]--server" + serverId);
		
		Integer clientId = txn.getClientId();
		ActorRef client = clients.get(clientId);
		client.tell(new TxnClient.ReadResultMsg(dataoperation.getKey(), result), getSelf());
	}
	
	private void OnWriteMsg(TxnClient.WriteMsg msg) {}

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
				.match(TxnClient.TxnEndMsg.class, this::OnTxnEndMsg).build();
	}

}
