package it.unitn.ds1;

import java.io.IOException;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.TxnClient.WelcomeMsg;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CtrlSystem {
	final static int N_CLIENTS = 6;
	final static int N_COORDINATORS = 3;
	final static int N_SERVERS = 3;
	final static int MAX_KEY = 10;
	
	private static final Logger log= LogManager.getLogger(CtrlSystem.class);

	public static void main(String[] args) {
		// Create an actor system named "ctrlakka"
		final ActorSystem system = ActorSystem.create("ctrlakka");

		// Create multiple Client actors
		List<ActorRef> group_clients = new ArrayList<>();
		for (int i = 0; i < N_CLIENTS; i++) {
			log.debug("Client "+i+" created");
			group_clients.add(system.actorOf(TxnClient.props(i), "client" + i));
		}

		// Create multiple Coordinator actors
		List<ActorRef> group_coordinators = new ArrayList<>();
		for (int i = 0; i < N_COORDINATORS; i++) {
			log.debug("Coordinator "+i+" created");
			group_coordinators.add(system.actorOf(Coordinator.props(i), "coordinator" + i));
		}

		// Create multiple Server actors
		for (int i = 0; i < N_SERVERS; i++) {
			log.debug("Server "+i+" created");
			HashMap<Integer, Integer> datastore = new HashMap<Integer, Integer>();
			for (int j = 0; j < MAX_KEY; j++) {
				Integer k = (i * MAX_KEY) + j;
				datastore.put(k, 10);
			}
			system.actorOf(Server.props(i, datastore), "server" + i);
		}

		// We send the welcome message to the first client
		WelcomeMsg start = new WelcomeMsg(MAX_KEY, group_coordinators);

		for (ActorRef peer : group_clients) {
			peer.tell(start, null);
		}

		log.info("Press ENTER to exit");
		try {
			System.in.read();
		} catch (IOException ioe) {
		} finally {
			system.terminate();
		}
	}
}
