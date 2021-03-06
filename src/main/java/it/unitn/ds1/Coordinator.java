package it.unitn.ds1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
		this.transactions = new HashMap<Txn, List<DataOperation>>();
	}

	static public Props props(int coordinatorId) {
		return Props.create(Coordinator.class, () -> new Coordinator(coordinatorId));
	}

	/*-- Message classes ------------------------------------------------------ */

	/*
	 * Welcome message informs about client, servers, and number of items per server
	 */
	public static class WelcomeMsg implements Serializable {
		public final Map<Integer, ActorRef> clients, servers;
		public final Integer N_KEY_SERVER;

		public WelcomeMsg(Map<Integer, ActorRef> clients, Map<Integer, ActorRef> servers, Integer N_KEY_SERVER) {
			this.clients = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(clients));
			this.servers = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(servers));
			this.N_KEY_SERVER = N_KEY_SERVER;
		}
	}

	/*
	 * READ request from the coordinator to the server
	 */
	public static class ReadMsg implements Serializable {
		public final Txn txn;
		public final DataOperation dataoperation;

		public ReadMsg(Txn txn, DataOperation dataoperation) {
			this.txn = txn;
			this.dataoperation = dataoperation;
		}
	}

	/*
	 * WRITE request from the coordinator to the server
	 */
	public static class WriteMsg implements Serializable {
		public final Txn txn;
		public final DataOperation dataoperation;

		public WriteMsg(Txn txn, DataOperation dataoperation) {
			this.txn = txn;
			this.dataoperation = dataoperation;
		}
	}

	/*
	 * Message sent to ask for a vote to a server
	 */
	public static class TxnAskVoteMsg implements Serializable {
		public final Txn txn;

		public TxnAskVoteMsg(Txn txn) {
			this.txn = txn;
		}

	}

	/*
	 * Message sent to share the result of the vote to a server
	 */
	public static class TxnVoteResultMsg implements Serializable {
		public final Txn txn;
		public final boolean commit;

		public TxnVoteResultMsg(Txn txn, boolean commit) {
			this.txn = txn;
			this.commit = commit;
		}

	}

	/*
	 * Message sent to share the result of a READ request to a client
	 */
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

	/*
	 * Retrieve the server ID using the key value
	 * The coordinator is aware of the convention used by the distributed data store
	 * 
	 * @param key
	 * @return
	 */
	private Integer getServerIdByKey(Integer key) {
		return (key / N_KEY_SERVER);
	}

	/*
	 * Retrieve the server actor using the key value
	 * 
	 * @param key
	 * @return
	 */
	private ActorRef getServerByKey(Integer key) {
		return servers.get(getServerIdByKey(key));
	}
	
	/*
	 * Retrieve the client ID using the key value
	 * 
	 * @param clientId
	 * @return
	 */
	private Txn getTxnByClientId(Integer clientId) {
		for (Txn txn : transactions.keySet()) {
			Integer clientIdCheck = txn.getClientId();
			if (clientIdCheck == clientId) {
				return (txn);
			}
		}
		return null;
	}

	/*-- Message handlers ---------------------------------------------------- - */

	private void onWelcomeMsg(WelcomeMsg msg) {
		this.clients = msg.clients;
		this.servers = msg.servers;
		this.N_KEY_SERVER = msg.N_KEY_SERVER;
	}

	private void OnTxnBeginMsg(TxnBeginMsg msg) {
		Integer clientId = msg.clientId;
		Txn txn = new Txn(coordinatorId, clientId);

		log.debug("coordinator" + coordinatorId + "<--[TXN_BEGIN]--client" + clientId);

		List<DataOperation> dataOperations = transactions.get(new Txn(coordinatorId, clientId));
		if (dataOperations == null) {
			// The client has no ongoing transaction, initialize the data operations
			transactions.put(txn, new ArrayList<DataOperation>());
			// Send a an accept message to the client
			getSender().tell(new TxnAcceptMsg(), getSelf());
		} else {
			// The client has an ongoing transaction, don't respond
			log.debug("coordinator" + coordinatorId + ": client" + clientId + " has an ongoing transaction, ignore");
		}
	}

	private void OnReadMsg(TxnClient.ReadMsg msg) {
		Integer clientId = msg.clientId;
		Integer key = msg.key;

		log.debug("coordinator" + coordinatorId + "<--[READ(" + key + ")]--client" + clientId);

		// Retrieve the transaction
		Txn txn = getTxnByClientId(clientId);
		// Set the operation to be add to the transaction
		DataOperation dataOperation = new DataOperation(DataOperation.Type.READ, key, null);
		// Append the READ operation to the list of data operations of the transaction
		List<DataOperation> dataoperations = transactions.get(txn);
		dataoperations.add(dataOperation);
		// Send the READ request to the server
		getServerByKey(key).tell(new Coordinator.ReadMsg(txn, dataOperation), getSelf());
	}

	private void OnReadResultMsg(Coordinator.ReadResultMsg msg) {
		Integer serverId = msg.serverId;
		Txn txn = msg.txn;
		DataOperation dataoperation = msg.dataoperation;

		log.debug("coordinator" + coordinatorId + "<--[READ(" + dataoperation.getKey() + ")="
				+ dataoperation.getDataItem().getValue() + "]--server" + serverId);

		ActorRef client = clients.get(txn.getClientId());
		// Send the READ result to the client
		client.tell(new TxnClient.ReadResultMsg(dataoperation.getKey(), dataoperation.getDataItem().getValue()),
				getSelf());
	}

	private void OnWriteMsg(TxnClient.WriteMsg msg) {
		Integer clientId = msg.clientId;
		Integer key = msg.key;
		Integer value = msg.value;
		log.debug("coordinator" + coordinatorId + "<--[WRITE(" + key + ")=" + value + "]--client" + clientId);

		// Retrieve the transaction
		Txn txn = getTxnByClientId(clientId);
		// Retrieve the data operations list
		List<DataOperation> dataoperations = transactions.get(txn);
		// Set the operation to be add to the transaction
		DataItem dataItem = new DataItem(null, value);
		DataOperation dataOperation = new DataOperation(DataOperation.Type.WRITE, key, dataItem);
		// Append the WRITE operation to the list of data operations of the transaction
		dataoperations.add(dataOperation);
		// Send the WRITE request to the server
		getServerByKey(key).tell(new Coordinator.WriteMsg(txn, dataOperation), getSelf());
	}

	/*
	 * Given a transaction, return the set of servers involved in its operations
	 * 
	 * @param txn
	 * @return
	 */
	private Set<Integer> getServersId(Txn txn) {
		List<DataOperation> dataoperations = transactions.get(txn);
		if (dataoperations == null) {
			// No data operation in the transaction, return an empty set
			return new HashSet<Integer>();
		}
		Set<Integer> keys = new HashSet<Integer>();
		// Retrieve the IDs of the items involved in the data operations
		for (DataOperation dataOperation : dataoperations) {
			keys.add(dataOperation.getKey());
		}
		Set<Integer> serverIds = new HashSet<Integer>();
		// From the keys, retrieve the IDs of the servers involved in the transaction
		for (Integer key : keys) {
			serverIds.add(getServerIdByKey(key));
			
		}
		return serverIds;
	}

	private void OnTxnVoteMsg(Server.TxnVoteMsg msg) throws InterruptedException {
		Txn txn = msg.txn;
		Boolean vote = msg.vote;
		Integer clientId = txn.getClientId();
		
		txn.setVotesCollected(txn.getVotesCollected() + 1);

		Set<Integer> serverIds = getServersId(txn);
		if (vote) {
			// Increase the number of votes for "COMMIT"
			txn.setVotes(txn.getVotes() + 1);
			if (txn.getVotes() == serverIds.size()) {
				// Everybody voted COMMIT
				for (Integer serverId : serverIds) {
					// Tell all servers to COMMIT
					servers.get(serverId).tell(new Coordinator.TxnVoteResultMsg(txn, true), getSelf());
				}
				// Inform the client of the successful transaction
				clients.get(clientId).tell(new TxnResultMsg(true), getSelf());
				// Remove the transaction
				transactions.remove(txn);
			}
		} else {
			// ABORT vote, send ABORT result to all the servers
			for (Integer serverId : serverIds) {
				servers.get(serverId).tell(new Coordinator.TxnVoteResultMsg(txn, false), getSelf());
			}
			if (txn.getVotesCollected() == serverIds.size()) {
				// Inform the client of the unsuccessful transaction
				clients.get(clientId).tell(new TxnResultMsg(false), getSelf());
			}
		}
		if (txn.getVotesCollected() == serverIds.size()) {
			// When all votes are collected, remove the transaction
			transactions.remove(txn);
		}
	}

	private void OnTxnEndMsg(TxnEndMsg msg) throws InterruptedException {
		Integer clientId = msg.clientId;
		Boolean commit = msg.commit;

		log.debug("coordinator" + coordinatorId + "<--[TXN_END=" + commit + "]--client" + clientId);

		// Retrieve the transaction
		Txn txn = new Txn(coordinatorId, clientId);
		Set<Integer> serverIds = getServersId(txn);

		if (commit == true) {
			// The client wants to commit, ask a vote to each server involved in the
			// transaction
			for (Integer serverId : serverIds) {
				servers.get(serverId).tell(new Coordinator.TxnAskVoteMsg(txn), getSelf());
			}
		} else {
			// The client wants to abort, tell to each server involved in the transaction to
			// abort
			for (Integer serverId : serverIds) {
				servers.get(serverId).tell(new Coordinator.TxnVoteResultMsg(txn, false), getSelf());
			}
			
			Thread.sleep(300);
			//Remove the transaction
			transactions.remove(txn);
			getSender().tell(new TxnResultMsg(commit), getSelf());
		}
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(Coordinator.WelcomeMsg.class, this::onWelcomeMsg)
				.match(TxnClient.TxnBeginMsg.class, this::OnTxnBeginMsg).match(TxnClient.ReadMsg.class, this::OnReadMsg)
				.match(Coordinator.ReadResultMsg.class, this::OnReadResultMsg)
				.match(Server.TxnVoteMsg.class, this::OnTxnVoteMsg).match(TxnClient.WriteMsg.class, this::OnWriteMsg)
				.match(TxnClient.TxnEndMsg.class, this::OnTxnEndMsg).build();
	}

}
