package it.unitn.ds1;

import akka.actor.*;
import java.util.Map;

public class Coordinator extends AbstractActor {
  private final Integer coordinatorId;

  /*-- Actor constructor ---------------------------------------------------- */

  public Coordinator(int coordinatorId) {
    this.coordinatorId = coordinatorId;
  }

  static public Props props(int coordinatorId) {
    return Props.create(Coordinator.class, () -> new Coordinator(coordinatorId));
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .build();
  }

}
