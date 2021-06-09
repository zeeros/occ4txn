package it.unitn.ds1;
import java.io.IOException;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class CtrlSystem {
  final static int N_CLIENTS = 6;

  public static void main(String[] args) {
    // Create an actor system named "ctrlakka"
    final ActorSystem system = ActorSystem.create("ctrlakka");

    // Create multiple Client actors
    for (int i=0; i<N_CLIENTS; i++) {
      System.out.println("client"+i);
      system.actorOf(TxnClient.props(i), "client" + i);
    }

    System.out.println("Press ENTER to exit");
    try {
      System.in.read();
    }
    catch (IOException ioe) {}
    finally {
      system.terminate();
    }
  }
}
