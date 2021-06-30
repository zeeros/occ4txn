package it.unitn.ds1;

import akka.actor.*;
import it.unitn.ds1.TxnClient.TxnAcceptMsg;
import it.unitn.ds1.TxnClient.TxnBeginMsg;
import it.unitn.ds1.TxnClient.TxnEndMsg;


public class Coordinator extends AbstractActor {
  private final Integer coordinatorId;
  
//boolean that is true if the coordinator is handling a TXN for a client. False if not.
private boolean coordinatorOccupied = false ;

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
	  coordinatorOccupied = false;
	    if (coordinatorOccupied == false) {
	    	System.out.println("the client" + clientId + "is attempting a Txn");
	    	coordinatorOccupied = true;
	    	System.out.println("The value of the coordinatorOccupied indicator is  "+ coordinatorOccupied);
            getSender().tell(new TxnAcceptMsg() ,getSelf());
	    } 
  }  
  private void OnTxnEndMsg(TxnEndMsg msg) {
	  Integer clientId = msg.clientId;
	  if (coordinatorOccupied) {
          System.out.println("We will remove a client with the clientId" + coordinatorOccupied);
		  coordinatorOccupied = false;
		  System.out.println("The value of the coordinatorOccupied indicator is  "+ coordinatorOccupied);
	  }
	  
  }
  
  @Override
  public Receive createReceive() {
    return receiveBuilder()
    		.match(TxnBeginMsg.class, this::OnTxnBeginMsg)
    		.match(TxnEndMsg.class, this::OnTxnEndMsg)
            .build();
  }

}
