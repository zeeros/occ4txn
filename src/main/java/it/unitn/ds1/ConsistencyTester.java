package it.unitn.ds1;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.Coordinator.TxnVoteResultMsg;

public class ConsistencyTester extends AbstractActor{
	private final Integer ctesterId;
	private Map<Integer, ActorRef> servers;
	private static final Logger log = LogManager.getLogger(ConsistencyTester.class);
	private int N_KEY_SERVER, INIT_ITEM_VALUE;
	private int server_replies;
	private Integer counter_values = 0;
	
	public ConsistencyTester(int ctesterId) {
		this.ctesterId = ctesterId;
	}

	static public Props props(int ctesterId) {
		return Props.create(ConsistencyTester.class, () -> new ConsistencyTester(ctesterId));
	}
	
	/*-- Message classes ------------------------------------------------------ */

	public static class WelcomeMsg implements Serializable {
		public final Map<Integer, ActorRef> servers;
		public final int N_KEY_SERVER, INIT_ITEM_VALUE;

		public WelcomeMsg(Map<Integer, ActorRef> servers, int N_KEY_SERVER, int INIT_ITEM_VALUE) {
			this.servers = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(servers));
			this.N_KEY_SERVER = N_KEY_SERVER;
			this.INIT_ITEM_VALUE = INIT_ITEM_VALUE;
		}
	}
	
	public static class GoodbyeMsg implements Serializable {}
	
	/*-- Message handlers ---------------------------------------------------- - */

	private void onWelcomeMsg(ConsistencyTester.WelcomeMsg msg) {
		this.servers = msg.servers;
		this.N_KEY_SERVER = msg.N_KEY_SERVER;
		this.INIT_ITEM_VALUE = msg.INIT_ITEM_VALUE;
		this.server_replies = 0;
	}

	private void OnGoodbyeMsg(ConsistencyTester.GoodbyeMsg msg) {
		log.debug("OnGoodbyeMsg from CtrlSystem");
		for (Map.Entry<Integer, ActorRef> entry : servers.entrySet()) {
			// we tell below to the server to do overwrites
			entry.getValue().tell(new ConsistencyTester.GoodbyeMsg(), getSelf());
		}
	}
	
	private void OnGoodbyeMsg(Server.GoodbyeMsg msg) {
	log.debug("OnGoodbyeMsg from Server "+msg.serverId);
		server_replies++;
		// TODO get the data store of the server
		Map<Integer, DataItem> datastore = msg.datastore;
		for (Integer dataId: datastore.keySet()) {
			counter_values += datastore.get(dataId).getValue();
		}
		if(server_replies == servers.size()) {
			log.debug("Total values : "+ counter_values + "; server_replies = " + server_replies );
			// TODO if we have all the data stores, then check the consistency
		}	
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(ConsistencyTester.WelcomeMsg.class, this::onWelcomeMsg)
				.match(ConsistencyTester.GoodbyeMsg.class, this::OnGoodbyeMsg)
				.match(Server.GoodbyeMsg.class, this::OnGoodbyeMsg).build();
	}

}
