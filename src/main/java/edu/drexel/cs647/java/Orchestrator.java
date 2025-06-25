package edu.drexel.cs647.java;

import java.util.ArrayList;
import java.util.Random;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import java.util.Random;


public class Orchestrator extends AbstractBehavior<String> {
	private ArrayList<ActorRef<RaftServerMessage>> currentServers;
	private int clientRequestNumber = 0;

	private Orchestrator(ActorContext ctxt, ArrayList<ActorRef<RaftServerMessage>> currentServers) {
		super(ctxt);
		this.currentServers = currentServers;
	}

	public static Behavior<String> create(String ns) {
		return Behaviors.setup(context -> {
			int numServers = Integer.parseInt(ns);
			ArrayList<ActorRef<RaftServerMessage>> currentServers = new ArrayList<ActorRef<RaftServerMessage>>();

			for (int i = 0; i < numServers; i++) {
				String sId = "s" + String.valueOf(i);
				var server = context.spawn(RaftFollower.create(sId), sId);
				currentServers.add(server);
			}

			return new Orchestrator(context, currentServers);
		});
	}

	@Override
	public Receive<String> createReceive() {
		return newReceiveBuilder().onMessage(String.class, this::dispatch).build();
	}

	public Behavior<String> dispatch(String txt) {
		Random rand = new Random();
		getContext().getLog().info("[Orchestrator] received " + txt);
		switch (txt) {
		case "start-algo":
			for (ActorRef<RaftServerMessage> server : currentServers) {
				server.tell(new RaftServerMessage.Start(currentServers));
			}
			break;

		case String cmd when cmd.startsWith("submit "):
			String clientCommand = txt.split(" ", 2)[1];

			clientRequestNumber += 1;
			String requestId = "c" + String.valueOf(clientRequestNumber);

			// Random rand = new Random();
			int randomServer = rand.nextInt(5); 

			ClientRequest clientRequest = new ClientRequest(clientCommand, requestId);

			// Contact a random server
			currentServers.get(randomServer).tell(new RaftServerMessage.clientRequest(clientRequest, getContext().getSelf()));
			break;

		case String cmd when cmd.startsWith("send "):

			// This is the case where we recive replicated commands back from the servers
			String clientResult = txt.split(" ", 2)[1];

			// Log behavior
			getContext().getLog().info("[Orchestrator] received replicated command " + clientResult);
			
			break;

		case String cmd when cmd.startsWith("retry "):

			// This is the case where we recive replicated commands back from the servers
			String[] clientCommandSplit = txt.split(" ");

			String clientCommandRetry = clientCommandSplit[1] + " " + clientCommandSplit[2];
			String requestIdRetry = clientCommandSplit[3];

			// Log behavior
			getContext().getLog().info("[Orchestrator] will retry " + clientCommandRetry);

			// Random rand = new Random();
			int randomServerRetry = rand.nextInt(5); 

			ClientRequest clientRequestRetry = new ClientRequest(clientCommandRetry, requestIdRetry);

			// Contact a random server
			currentServers.get(randomServerRetry).tell(new RaftServerMessage.clientRequest(clientRequestRetry, getContext().getSelf()));

			break;


		default:
			break;

		}
		return this;
	}
}