package edu.drexel.cs647.java;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import edu.drexel.cs647.java.RaftServerMessage.clientRequest;
import java.util.Comparator;

import akka.actor.typed.javadsl.*;
import akka.actor.typed.SupervisorStrategy;




public class RaftLeader extends AbstractBehavior<RaftServerMessage> {

	// Server id
	private String serverId;
	// Timer
	private TimerScheduler<RaftServerMessage> timers;

	// Volatile states
	// Current Servers
	private ArrayList<ActorRef<RaftServerMessage>> currentServers;
	// Persistant States
	private PersistentServerState persistentServerState;
	// Index of the highest log entry that was commited
	private int commitIndex;
	// Index of the highest log entry applied to the state machine
	private int lastApplied;

	// Leaader states
	private Map<ActorRef<RaftServerMessage>, Integer> nextIndex = new HashMap<>();
	private Map<ActorRef<RaftServerMessage>, Integer> matchIndex = new HashMap<>();

	// Local state machine
	private StateMachine stateMachine;

	// Current leader
	private ActorRef<RaftServerMessage> currentLeader;

	// Client requests to process 
	private PriorityQueue<ClientRequest> clientRequestQueue;

	// Client requests processes
	private Set<String> requestsProcessed;

	// Current client
	private ActorRef<String> currentClient;

	public RaftLeader(ActorContext ctxt, String serverId, TimerScheduler<RaftServerMessage> timers, int commitIndex,
			int lastApplied, ArrayList<ActorRef<RaftServerMessage>> currentServers,
			PersistentServerState persistentServerState, StateMachine stateMachine) {
		super(ctxt);
		this.serverId = serverId;

		this.commitIndex = commitIndex;
		this.lastApplied = lastApplied;

		this.currentServers = currentServers;
		this.persistentServerState = persistentServerState;

		// Set up the timer
		this.timers = timers;

		this.stateMachine = stateMachine;

		currentLeader = getContext().getSelf();

		clientRequestQueue =  clientRequestQueue = new PriorityQueue<>(
			Comparator.comparing(ClientRequest::requestId));

		requestsProcessed = new HashSet<>();

		// Initialize leader stated
		this.nextIndex = new HashMap<>();
		this.matchIndex = new HashMap<>();

		for (ActorRef<RaftServerMessage> server : currentServers) {
			nextIndex.put(server, persistentServerState.getLog().size() + 1);
			matchIndex.put(server, 0);
		}

		// Schedule a recurring heartbeat message
		Random random = new Random();
		int delay = 150 + random.nextInt(151);
		Duration timeout = Duration.ofMillis(delay);

		// Start heartbeat timeout
		startHeartBeatTimer(timers);

	}


	// Helper function to save persistant state to the disk
	public void writeToDisk() throws IOException{
		persistentServerState.saveState(serverId + ".ser");
	}

	// Helper function to start heatbeat timer
	public void startHeartBeatTimer(TimerScheduler<RaftServerMessage> timers) {
		Random random = new Random();
		int delay = 150 + random.nextInt(151); // [150, 300]
		Duration timeout = Duration.ofMillis(delay);

		timers.startSingleTimer("heartbeat-timeout", new RaftServerMessage.sendHeartBeats(), timeout);

	}

	@Override
	public Receive<RaftServerMessage> createReceive() {
		return newReceiveBuilder().onMessage(RaftServerMessage.class, this::dispatch).build();
	}

	public Behavior<RaftServerMessage> dispatch(RaftServerMessage msg) throws IOException {
		// This style of switch statement is technically a preview feature in many
		// versions of Java, so you'll need to compile with --enable-preview
		switch (msg) {

		case RaftServerMessage.convertToFollower f:

			// Log the behavior
			getContext().getLog().info(serverId + " is converting from LEADER to FOLLOWER.");

			// Before I change my state, I am going write all changes to the persistant
			// state to disk
			writeToDisk();

			// Set the state to appropriate value
			// return RaftFollower.create(serverId);
			// return new RaftFollower(getContext(), serverId, timers, persistentServerState, commitIndex, lastApplied, stateMachine);
			return RaftFollower.create(serverId, persistentServerState, commitIndex, lastApplied, stateMachine, currentServers);
//			return new RaftFollower(serverId, timers, persistentServerState, commitIndex, lastApplied);

		case RaftServerMessage.commitLogEntries c:
			// This is the case where the leader commits the log entries that have been
			// acknowleged by a majority of nodes

			while (commitIndex < persistentServerState.getLog().size()) {
				int acks = 0;

				for (ActorRef<RaftServerMessage> server : currentServers) {
					if (matchIndex.get(server) > commitIndex) {
						acks += 1;
					}
				}

				if (acks >= Math.ceil((currentServers.size() + 1) / 2)) {
					
					// Update the state machine
					stateMachine.addEntry(persistentServerState.getEntry(commitIndex + 1));

					// Log Behavior
					getContext().getLog().info("{}(leader) has committed all log entries", serverId);

					// Send the command to the client
					currentClient.tell("send " + persistentServerState.getEntry(commitIndex + 1).command());

					// Other servers will be nottified of this change in future appendEntries
					// requests
					commitIndex += 1;
				} else {
					break;
				}

			}
			break;

		case RaftServerMessage.appendEntriesReply c:
			// This is the case where the leader receives an append entries reply from a
			// follower

			if (c.term() == persistentServerState.getCurrentTerm()) {

				if (c.success() && (c.ack() >= matchIndex.get(c.sender()))) {
					// Update nextIndex and matchIndex
					nextIndex.put(c.sender(), c.ack());
					matchIndex.put(c.sender(), c.ack());

					// Commit log entries
					getContext().getSelf().tell(new RaftServerMessage.commitLogEntries());
					break;
				} else if (nextIndex.get(c.sender()) > 0) {
					// Decrement nextIndex and retry ( we keep doing this till te follower's log catches up)
					nextIndex.put(c.sender(), nextIndex.get(c.sender()) - 1);

					getContext().getSelf().tell(new RaftServerMessage.replicateLog(persistentServerState.getLog()));
					break;
				}
			} else if (c.term() > persistentServerState.getCurrentTerm()) {
				persistentServerState.setVotedForAtTerm(persistentServerState.getCurrentTerm(), null);
				persistentServerState.setCurrentTerm(c.term());

				// Cancel the heartbeat timer
				timers.cancel("heartbeat-timeout");

				// Convert to follower
				getContext().getSelf().tell(new RaftServerMessage.convertToFollower());
				break;
			}

			break;

		case RaftServerMessage.replicateLog r:
			// This is the case where the leader sends appendEntries requestes to all
			// followers to replicate the log

			// Append the new message to the local log
			persistentServerState.getLog().addAll(r.log());

			// Log Behavior
			getContext().getLog().info(serverId + " is sending log messages to all servers.");

			// Send a message to all other servers except me
			for (ActorRef<RaftServerMessage> server : currentServers) {
				if (!server.equals(getContext().getSelf())) {

					int prefixLength = nextIndex.get(server);
					ArrayList<LogEntry> suffix = r.log();

					int prefixTerm = 0;

					if (prefixLength > 0) {
						prefixTerm = persistentServerState.getLog().get(prefixLength - 1).term();
					}

					// server.tell(new RaftServerMessage.appendEntriesRPC(getContext().getSelf(),
					// 		persistentServerState.getCurrentTerm(), serverId, persistentServerState.getLastLogIndex(),
					// 		persistentServerState.getLastLogTerm(), r.log(), commitIndex));

					server.tell(new RaftServerMessage.appendEntriesRPC(getContext().getSelf(),
							persistentServerState.getCurrentTerm(), serverId, prefixLength,
							prefixTerm, r.log(), commitIndex));

				}
			}

			break;

		case RaftServerMessage.clientRequest c:
			// This is the case where the leader receives or is redircted to a client request 

			// Add the request to our queue
			clientRequestQueue.add(c.clientRequest());

			// Set the current client
			currentClient = c.client();

			// Try to process the requests in the queue
			ArrayList<LogEntry> currentLog = new ArrayList<>();

			while (!clientRequestQueue.isEmpty()) {
				ClientRequest currentRequest = clientRequestQueue.poll();

				if (!requestsProcessed.contains(currentRequest.requestId())) {
					// Add the requestId to the processed set
					requestsProcessed.add(currentRequest.requestId());

					// Create a log entry
					LogEntry entry = new LogEntry(persistentServerState.getCurrentTerm(), currentRequest.command());
					currentLog.add(entry);

				} else {
					// I reply to the client immeadiately if I have already processed the request
					currentClient.tell("send " + currentRequest.command());
				}
				
			}

			// Update the match index
			matchIndex.put(getContext().getSelf(), currentLog.size());

			// Send message to replicate the log
			getContext().getSelf().tell(new RaftServerMessage.replicateLog(currentLog));

			break;

		case RaftServerMessage.sendHeartBeats s:
			// This is the case were the current leader sends heart beats to every other
			// server

			// Cancel timer
			timers.cancel("heartbeat-timeout");

			// if this is the first heartbeat sent
			if (nextIndex.isEmpty()) {
				for (ActorRef<RaftServerMessage> server : currentServers) {
					if (!server.equals(getContext().getSelf())) {
						nextIndex.put(server, persistentServerState.getLastLogIndex() + 1);

					}
				}
			}

			if (matchIndex.isEmpty()) {
				for (ActorRef<RaftServerMessage> server : currentServers) {
					if (!server.equals(getContext().getSelf())) {
						matchIndex.put(server, 0);
					}
				}
			}

			// Log Behavior
			getContext().getLog().info(serverId + " is sending heartbeats to all servers.");

			// Send a message to all other servers except me
			for (ActorRef<RaftServerMessage> server : currentServers) {
				if (!server.equals(getContext().getSelf())) {

					// Update leader states (no need to update for every heartbeat)
					// nextIndex.put(server, persistentServerState.getLog().size());
					// matchIndex.put(server, 0);

					server.tell(new RaftServerMessage.appendEntriesRPC(getContext().getSelf(),
							persistentServerState.getCurrentTerm(), serverId, persistentServerState.getLastLogIndex(),
							persistentServerState.getLastLogTerm(), new ArrayList<LogEntry>(), commitIndex));
				}
			}

			// Restart timer
			startHeartBeatTimer(timers);

			break;

		default:
			break;

		}

		// Keep the same message handling behavior
		return this;
	}
}