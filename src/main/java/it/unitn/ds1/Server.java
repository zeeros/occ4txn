package it.unitn.ds1;

import akka.actor.*;
import akka.actor.AbstractActor.Receive;
import it.unitn.ds1.Coordinator.WelcomeMsg;
import it.unitn.ds1.TxnClient.TxnAcceptMsg;
import it.unitn.ds1.TxnClient.TxnBeginMsg;
import it.unitn.ds1.TxnClient.TxnEndMsg;
import it.unitn.ds1.TxnClient.TxnResultMsg;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Server extends AbstractActor {
	private final Integer serverId;

	private static final Logger log = LogManager.getLogger(CtrlSystem.class);

	// TXN operation (move some amount from a value to another)
	private Map<Integer, Integer> datastore;

	/*-- Actor constructor ---------------------------------------------------- */

	public Server(int serverId, Map<Integer, Integer> datastore) {
		this.serverId = serverId;
		this.datastore = datastore;
	}

	static public Props props(int serverId, Map<Integer, Integer> datastore) {
		return Props.create(Server.class, () -> new Server(serverId, datastore));
	}

	/*-- Message classes ------------------------------------------------------ */

	// reply from the server when requested a READ on a given key
	public static class ReadResultMsg implements Serializable {
		public final Txn txn;
		public final DataOperation dataoperation;
		public final Integer result;

		public ReadResultMsg(Txn txn, DataOperation dataoperation, Integer result) {
			this.txn = txn;
			this.dataoperation = dataoperation;
			this.result = result;
		}
	}

	// WRITE request from the coordinator to the server
	public static class WriteMsg implements Serializable {
	}
	
	/*-- Message handlers ----------------------------------------------------- */

	private void OnReadMsg(Coordinator.ReadMsg msg) {
		Txn txn = msg.txn;
		DataOperation dataoperation = msg.dataoperation;
		log.debug("Server " + serverId + " receives READ(" + dataoperation.getKey() + ") from coordinator " + txn.getCoordinatorId());
		Integer value = datastore.get(dataoperation.getKey());
		// Respons to the coordinator with the TXN, its data operation and the value in the datastore
		getSender().tell(new Server.ReadResultMsg(txn, dataoperation, value), getSelf());
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Coordinator.ReadMsg.class, this::OnReadMsg).build();
	}

}
