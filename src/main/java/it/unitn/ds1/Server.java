package it.unitn.ds1;

import akka.actor.*;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Server extends AbstractActor {
  private final Integer serverId;
  
  private static final Logger log= LogManager.getLogger(CtrlSystem.class);

  // TXN operation (move some amount from a value to another)
  private Map<Integer,Integer> datastore;

  /*-- Actor constructor ---------------------------------------------------- */

  public Server(int serverId, Map<Integer,Integer> datastore) {
    this.serverId = serverId;
    this.datastore = datastore;
  }

  static public Props props(int serverId, Map datastore) {
    return Props.create(Server.class, () -> new Server(serverId, datastore));
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .build();
  }

}
