package it.unitn.ds1;

import java.io.IOException;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CtrlSystem {
	final static int N_CLIENTS = 10;
	final static int N_COORDINATORS = 10;
	final static int N_SERVERS = 10;
	final static Integer N_KEY_SERVER = 10;
	final static int MAX_KEY = N_KEY_SERVER * N_SERVERS - 1;

	private static final Logger log = LogManager.getLogger(CtrlSystem.class);

	public static void main(String[] args) throws InterruptedException {
		// Create an actor system named "ctrlakka"
		final ActorSystem system = ActorSystem.create("ctrlakka");

		// Create client actors
		Map<Integer, ActorRef> clients = new HashMap<Integer, ActorRef>();
		for (int i = 0; i < N_CLIENTS; i++) {
			log.debug("Client " + i + " created");
			clients.put(i, system.actorOf(TxnClient.props(i), "client" + i));
		}

		// Create coordinator actors
		List<ActorRef> coordinators = new ArrayList<ActorRef>();
		for (int i = 0; i < N_COORDINATORS; i++) {
			log.debug("Coordinator " + i + " created");
			coordinators.add(system.actorOf(Coordinator.props(i), "coordinator" + i));
		}

		// Create multiple Server actors
		Integer k = 0;
		Map<Integer, ActorRef> servers = new HashMap<Integer, ActorRef>();
		for (int i = 0; i < N_SERVERS; i++) {
			log.debug("Server " + i + " created");
			HashMap<Integer, DataItem> datastore = new HashMap<Integer, DataItem>();
			for (int j = 0; j < N_KEY_SERVER; j++) {
				datastore.put(k++, new DataItem(0, 100));
			}
			servers.put(i, system.actorOf(Server.props(i, datastore), "server" + i));
		}

		// Send welcome messages to clients, coordinators and servers

		TxnClient.WelcomeMsg wClient = new TxnClient.WelcomeMsg(MAX_KEY, coordinators);
		for (Map.Entry<Integer, ActorRef> entry : clients.entrySet()) {
			entry.getValue().tell(wClient, null);
		}

		Coordinator.WelcomeMsg wCoordinator = new Coordinator.WelcomeMsg(clients, servers, N_KEY_SERVER);
		for (ActorRef peer : coordinators) {
			peer.tell(wCoordinator, null);
		}
	
		
		
		log.info("Press ENTER to exit");
		try {
			System.in.read();
		} catch (IOException ioe) {
		} finally {
			system.terminate();
		}
		
		
		//the server to check the sum
		Integer iterator = 25;
		while(iterator > 0) {
			for (Map.Entry<Integer, ActorRef> entry : servers.entrySet()) {
				entry.getValue().tell(new Server.LocalSumCheckMsg(), null);
			}
			Thread.sleep(2000);
			iterator-=1;
			
		}
		
	}

}
