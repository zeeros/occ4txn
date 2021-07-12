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
	final static int N_CLIENTS = 2;
	final static int N_COORDINATORS = 2;
	final static int N_SERVERS = 2;
	final static int N_KEY_SERVER = 10;
	final static int MAX_KEY = N_KEY_SERVER * N_SERVERS - 1;
	final static int INIT_ITEM_VALUE = 100;

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
				DataItem dataItem = new DataItem(0,INIT_ITEM_VALUE);
				dataItem.setLock(null);
				datastore.put(k++, dataItem);
			}
			servers.put(i, system.actorOf(Server.props(i, datastore), "server" + i));
		}
		
		// The consistency tester is used to check if the distributed data store has a consistent state
		
		ActorRef consistencyTester = system.actorOf(ConsistencyTester.props(0), "consistencyTester");

		// Send welcome messages to clients, coordinators and servers

		TxnClient.WelcomeMsg wClient = new TxnClient.WelcomeMsg(MAX_KEY, coordinators);
		for (Map.Entry<Integer, ActorRef> entry : clients.entrySet()) {
			entry.getValue().tell(wClient, null);
		}

		Coordinator.WelcomeMsg wCoordinator = new Coordinator.WelcomeMsg(clients, servers, N_KEY_SERVER);
		for (ActorRef peer : coordinators) {
			peer.tell(wCoordinator, null);
		}
		
		consistencyTester.tell(new ConsistencyTester.WelcomeMsg(servers, N_KEY_SERVER, INIT_ITEM_VALUE), null);	
		
		log.info("Press ENTER to exit");
		try {
			System.in.read();
		} catch (IOException ioe) {
		} finally {
			// Stop all the clients
			for (Map.Entry<Integer, ActorRef> entry : clients.entrySet()) {
				entry.getValue().tell(new TxnClient.StopMsg(), null);
			}
			Thread.sleep(3000);
			consistencyTester.tell(new ConsistencyTester.GoodbyeMsg(), null);
			// Wait for the consistency tester to check the data stores
			Thread.sleep(3000);
			system.terminate();
		}
		
	}

}
