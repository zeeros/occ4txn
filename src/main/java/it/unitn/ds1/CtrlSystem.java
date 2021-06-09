package it.unitn.ds1;
import java.io.IOException;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import java.util.HashMap;

public class CtrlSystem {
  final static int N_CLIENTS = 6;
  final static int N_SERVERS = 3;
  final static int MAX_KEY = 10;

  public static void main(String[] args) {
    // Create an actor system named "ctrlakka"
    final ActorSystem system = ActorSystem.create("ctrlakka");

    // Create multiple Client actors
    for (int i=0; i<N_CLIENTS; i++) {
      System.out.println("client"+i);
      system.actorOf(TxnClient.props(i), "client" + i);
    }

    // Create multiple Server actors
    for (int i=0; i<N_SERVERS; i++) {
      System.out.println("server"+i);
      HashMap<Integer, Integer> datastore = new HashMap<Integer,Integer>();
      for(int j=0; j<MAX_KEY; j++){
	  Integer k = (i*MAX_KEY) + j;
	  datastore.put(k, 10);
      }
      system.actorOf(Server.props(i, datastore), "server" + i);
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
