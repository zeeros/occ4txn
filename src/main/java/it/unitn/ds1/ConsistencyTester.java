package it.unitn.ds1;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class ConsistencyTester extends AbstractActor{
	private Map<Integer, ActorRef> servers;
	private static final Logger log = LogManager.getLogger(ConsistencyTester.class);
	private int server_replies;
	private Integer counter_values = 0;
	
	static public Props props(int ctesterId) {
		return Props.create(ConsistencyTester.class, () -> new ConsistencyTester());
	}
	
	/*-- Message classes ------------------------------------------------------ */

	public static class WelcomeMsg implements Serializable {
		public final Map<Integer, ActorRef> servers;

		public WelcomeMsg(Map<Integer, ActorRef> servers) {
			this.servers = Collections.unmodifiableMap(new HashMap<Integer, ActorRef>(servers));
		}
	}
	
	public static class GoodbyeMsg implements Serializable {}
	
	/*-- Message handlers ---------------------------------------------------- - */

	private void onWelcomeMsg(ConsistencyTester.WelcomeMsg msg) {
		this.servers = msg.servers;
		this.server_replies = 0;
	}

	private void OnGoodbyeMsg(ConsistencyTester.GoodbyeMsg msg) {
		log.debug("OnGoodbyeMsg from CtrlSystem");
		for (Map.Entry<Integer, ActorRef> entry : servers.entrySet()) {
			entry.getValue().tell(new ConsistencyTester.GoodbyeMsg(), getSelf());
		}
	}
	
	private void OnGoodbyeMsg(Server.GoodbyeMsg msg) {
		log.debug("OnGoodbyeMsg from Server "+msg.serverId);
		Integer serverId = msg.serverId;
		Map<Integer, DataItem> datastore = msg.datastore;
		log.debug("server" + serverId +" has the following datastore");
		for (Integer dataId: datastore.keySet()) {
			counter_values += datastore.get(dataId).getValue();
			log.debug(dataId + ": " + datastore.get(dataId).getValue() + " (v: " + datastore.get(dataId).getVersion()+")");
		}

		server_replies++;
		if(server_replies == servers.size()) {
			// If all the servers answered, print the total sum
			log.debug("Total sum of visible values: "+ counter_values);
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
