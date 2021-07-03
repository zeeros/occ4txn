package it.unitn.ds1;

import akka.actor.*;

import java.io.Serializable;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Server extends AbstractActor {
	private final Integer serverId;

	private static final Logger log = LogManager.getLogger(Server.class);

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
		datastore.put(dataoperation.getKey(), dataoperation.getValue());
		// No answer in case of write
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Coordinator.ReadMsg.class, this::OnReadMsg)
				.match(Coordinator.WriteMsg.class, this::OnWriteMsg).build();
	}

}
