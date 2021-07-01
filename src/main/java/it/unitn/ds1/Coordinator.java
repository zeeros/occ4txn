package it.unitn.ds1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import akka.actor.*;
import it.unitn.ds1.TxnClient.TxnAcceptMsg;
import it.unitn.ds1.TxnClient.TxnBeginMsg;
import it.unitn.ds1.TxnClient.TxnEndMsg;

public class Coordinator extends AbstractActor {
	private final Integer coordinatorId;
	
	private static final Logger log= LogManager.getLogger(CtrlSystem.class);

	// True if the coordinator is handling a TXN for a client.
	private boolean coordinatorOccupied = false;

	/*-- Actor constructor ---------------------------------------------------- */

	public Coordinator(int coordinatorId) {
		this.coordinatorId = coordinatorId;
	}

	static public Props props(int coordinatorId) {
		return Props.create(Coordinator.class, () -> new Coordinator(coordinatorId));
	}

	/*-- Message handlers ---------------------------------------------------- - */

	private void OnTxnBeginMsg(TxnBeginMsg msg) {
		Integer clientId = msg.clientId;
		log.debug("Coordinator "+ coordinatorId + " receives BEGIN from client " + clientId);
		coordinatorOccupied = false;
		if (coordinatorOccupied == false) {
			log.debug("Coordinator "+ coordinatorId + " is occupied");
			coordinatorOccupied = true;
			getSender().tell(new TxnAcceptMsg(), getSelf());
		}
	}

	private void OnTxnEndMsg(TxnEndMsg msg) {
		Integer clientId = msg.clientId;
		log.debug("Coordinator "+ coordinatorId + " receives END from client " + clientId);
		if (coordinatorOccupied) {
			coordinatorOccupied = false;
			log.debug("Coordinator "+ coordinatorId + " is free");
		}

	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(TxnBeginMsg.class, this::OnTxnBeginMsg).match(TxnEndMsg.class, this::OnTxnEndMsg)
				.build();
	}

}
