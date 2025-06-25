package edu.drexel.cs647.java;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import edu.drexel.cs647.java.RaftServerMessage.clientRequest;
import akka.actor.typed.SupervisorStrategy;


public class RaftFollower extends AbstractBehavior<RaftServerMessage> {

	// Server id
	private String serverId;

	// Timer
	private TimerScheduler<RaftServerMessage> timers;

	// Current Servers
	private ArrayList<ActorRef<RaftServerMessage>> currentServers;

	// Persistant States
	private PersistentServerState persistentServerState;

	// Volatile states

	// Index of the highest log entry that was commited
	private int commitIndex;

	// Index of the highest log entry applied to the state machine
	private int lastApplied;

	// Local state machine
	private StateMachine stateMachine;

	// Current leader
	private ActorRef<RaftServerMessage> currentLeader;

	// Constructor to use while starting up the actor
	private RaftFollower(ActorContext ctxt, String serverId, TimerScheduler<RaftServerMessage> timers) {
		super(ctxt);
		this.serverId = serverId;

		commitIndex = 0;
		lastApplied = 0;

		// Set up the timer
		this.timers = timers;

		// Intialize state machine
		stateMachine = new StateMachine(serverId);

		// Create a new state if file with current sId does not already exist on the
		// disk
		try {
			persistentServerState = loadState(serverId + ".ser");
		} catch (IOException e) {
			persistentServerState = new PersistentServerState();
		} catch (ClassNotFoundException e) {
			return;
		}
	

	}

	// Constructor to use while switching states
	public RaftFollower(ActorContext ctxt, String serverId, TimerScheduler<RaftServerMessage> timers,
			PersistentServerState persistentServerState, int commitIndex, int lastApplied, StateMachine stateMachine, ArrayList<ActorRef<RaftServerMessage>> currentServers) {
		super(ctxt);
		this.serverId = serverId;

		this.commitIndex = commitIndex;
		this.lastApplied = lastApplied;

		// Set up the timer
		this.timers = timers;

		this.persistentServerState = persistentServerState;

		this.stateMachine = stateMachine;

		this.currentServers = currentServers;

		// Start election timer
		startElectionTimer(timers);

	}

	// Factory method to create the actor after switching states
	public static Behavior<RaftServerMessage> create(String serverId,
			PersistentServerState persistentServerState, int commitIndex, int lastApplied, StateMachine stateMachine
			, ArrayList<ActorRef<RaftServerMessage>> currentServers) {
        return Behaviors.<RaftServerMessage>supervise(
            Behaviors.setup(context ->
                Behaviors.withTimers(timers ->
                    new RaftFollower(context, serverId, timers, persistentServerState, commitIndex, lastApplied, stateMachine, currentServers)
                )
            )
        ).onFailure(Exception.class, SupervisorStrategy.restart());
    }

	// Factory method to create the actor on startup
	public static Behavior<RaftServerMessage> create(String serverId) {
        return Behaviors.<RaftServerMessage>supervise(
            Behaviors.setup(context ->
                Behaviors.withTimers(timers ->
                    new RaftFollower(context, serverId, timers)
                )
            )
        ).onFailure(Exception.class, SupervisorStrategy.restart());
    }

	// Helper function to start elecetion timer
	public void startElectionTimer(TimerScheduler<RaftServerMessage> timers) {
		Random random = new Random();
		int delay = 1500 + random.nextInt(1501); // [150, 300]
		Duration timeout = Duration.ofMillis(delay);

		timers.startSingleTimer("election-timeout", new RaftServerMessage.Timeout(), timeout);

	}

	// Helper function load persistant state from the disk
	public PersistentServerState loadState(String filename) throws IOException, ClassNotFoundException {
		try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filename))) {
			return (PersistentServerState) ois.readObject();
		}
	}

	// Helper function to save persistant state to the disk
	public void writeToDisk() throws IOException{
		persistentServerState.saveState(serverId + ".ser");
	}

	@Override
	public Receive<RaftServerMessage> createReceive() {
		return newReceiveBuilder().onMessage(RaftServerMessage.class, this::dispatch).build();
	}


	public Behavior<RaftServerMessage> dispatch(RaftServerMessage msg) throws IOException {

		switch (msg) {

		case RaftServerMessage.Start s:

			// This is the case where the current server recives the list of all other
			// servers

			// Log behavior
			getContext().getLog().info(serverId + " recived the list of other servers");

			// Save the list of processes
			currentServers = s.currentServers();

			// Start election timer
			startElectionTimer(timers);

			break;

		case RaftServerMessage.convertToCandidate c:

			// This is the case where the server has timedot or hasnt recived messages in a
			// while and needs to convert to candidate

			// Before I change my state, I am going write all changes to the persistant
			// state to disk
			writeToDisk();

			// Log the behavior
			getContext().getLog().info(serverId + " is converting from FOLLOWER to CANDIDATE.");

			// Set the state to appropriate value
//			return RaftCandidate.create(serverId, currentServers, persistentServerState, commitIndex, lastApplied);
			// return new RaftCandidate(getContext(), serverId, timers, currentServers, persistentServerState, commitIndex,
			// 		lastApplied, stateMachine);

			return RaftCandidate.create(serverId, currentServers, persistentServerState, commitIndex, lastApplied,
					stateMachine);

//			return new RaftCandidate(timers, serverId, currentServers, persistentServerState, commitIndex, lastApplied);

		case RaftServerMessage.appendEntriesRPC c:

			// This is the case where a follower recives an append entries message from
			// anaother server (leader)

			if (c.term() > persistentServerState.getCurrentTerm()) {
				persistentServerState.setVotedForAtTerm(c.term(), null);
				persistentServerState.setCurrentTerm(c.term());
				
				// Cancel election timer
				timers.cancel("election-timeout");

				// Convert to Follower - skip because we are already a follower
			}

			currentLeader = c.sender();

			boolean logOk = ((persistentServerState.getLastLogIndex() >= c.prevLogIndex())
					&& ((c.prevLogIndex() == 0) || (c.prevLogTerm() == persistentServerState.getLastLogTerm())));


			if (c.term() == persistentServerState.getCurrentTerm() && logOk) {

				// Log Behaviour
				getContext().getLog().info("{} accpeted log entries sent by the leader", serverId);

				// Update our local log
				getContext().getSelf()
						.tell(new RaftServerMessage.updateLocalLog(c.prevLogIndex(), c.leaderCommit(), c.entries()));

				int ack = c.prevLogIndex() + c.entries().size();

				// Send a reply back to the leader
				c.sender().tell(new RaftServerMessage.appendEntriesReply(getContext().getSelf(),
						persistentServerState.getCurrentTerm(), true, ack));
			} else {
				// Log Behaviour
				getContext().getLog().info("{} rejected log entries sent by the leader", serverId);

				// Send a reply back to the leader
				c.sender().tell(new RaftServerMessage.appendEntriesReply(getContext().getSelf(),
						persistentServerState.getCurrentTerm(), false, 0));
			}

			// I have now confirmerd the receipt of some heartbeat/message from the leader
			// so I can restart my election timer
			// Cancel election timer
			// timers.cancel("election-timeout");
			startElectionTimer(timers);
			break;

		case RaftServerMessage.updateLocalLog c:
			// This is the case where a follower has decided to accept the leaders log
			// entries

			// If an exsisting entry conflicts with a new one, delete conflicting entries
			if ((c.entries().size() > 0) && (persistentServerState.getLastLogIndex() > c.prevLogIndex())) {
				int index = Math.min(persistentServerState.getLastLogIndex(), (c.prevLogIndex() + c.entries().size())) - 1;

				if (persistentServerState.getLog().get(index).term() != c.entries().get((index - c.prevLogIndex()))
						.term()) {
					persistentServerState.deleteConflictingEntriesFromIndex(index);
				}
			}

			// Append new entries to the log
			if ((c.prevLogIndex() + c.entries().size()) > persistentServerState.getLog().size()) {
				persistentServerState.getLog().addAll(c.entries());
			}

			if (c.leaderCommit() > commitIndex) {

				// System.out.println("Leader commit index: " + c.leaderCommit());
				// System.out.println("Current commit index: " + commitIndex);

				// Update the state machine
				for (int i = commitIndex; i < c.leaderCommit() - 1; i++) {
					// System.out.println("Adding entry to state machine: " + persistentServerState.getEntry(i));
					stateMachine.addEntry(persistentServerState.getEntry(i));
				}

				commitIndex = Math.min(c.leaderCommit(), persistentServerState.getLastLogIndex());
			}

			break;

		case RaftServerMessage.requestVoteRPC c:
			// This is the case where the follower recives a request vote for another
			// candidate during elections

			if (c.term() > persistentServerState.getCurrentTerm()) {
				persistentServerState.setCurrentTerm(c.term());
				persistentServerState.setVotedForAtTerm(c.term(), null);

				// Convert to Follower - skip because we are already a follower
			}

			// Get the last term of ther log
			int lastTerm = persistentServerState.getLastLogTerm();

			// Check if the log was updated
			boolean logOK = (c.lastLogTerm() > lastTerm)
					|| (c.lastLogTerm() == lastTerm) && (c.lastLogIndex() >= persistentServerState.getLastLogIndex());

			if ((c.term() == persistentServerState.getCurrentTerm()) && logOK
					&& (persistentServerState.getVotedForAtTerm(c.term()) == null 
					|| persistentServerState.getVotedForAtTerm(c.term()).equals(c.candidateId()))) {

				// Update the voted for value
				persistentServerState.setVotedForAtTerm(c.term(), c.candidateId());

				// Log behavior
				getContext().getLog().info("{} granted a vote to {}", serverId, c.candidateId());

				// Send a vote granted response
				c.sender().tell(
						new RaftServerMessage.requestVoteReply(persistentServerState.getCurrentTerm(), true, serverId));

			} else {

				// Log behavior
				getContext().getLog().info("{} did NOT grant a vote to {}", serverId, c.candidateId());

				// Send a vote NOT granted response
				c.sender().tell(new RaftServerMessage.requestVoteReply(persistentServerState.getCurrentTerm(), false,
						serverId));
			}

//			writeToDisk();

			break;

		case RaftServerMessage.clientRequest c:
			// This is the case where the server recives a client request

			if (currentLeader == null) {
				// There is no leader, so I will ask the client to retry
				String clientMessage = "retry " + c.clientRequest().command() + " " + c.clientRequest().requestId();
				c.client().tell(clientMessage);
				
			} else {
			// I am not the leader, so I redierct it to the current leader
			currentLeader.tell(new RaftServerMessage.clientRequest(c.clientRequest(), c.client()));
			}

			break;

		case RaftServerMessage.Timeout t:
			// This is the case where we check if we have not reiced heartbeat messages in a
			// a while

			// Log behavior
			getContext().getLog().info("{} did NOT receive heartbeats from the leader before timeout", serverId);

			// Convert to candidate
			getContext().getSelf().tell(new RaftServerMessage.convertToCandidate());

			break;

		default:
			break;

		}

		// Keep the same message handling behavior
		return this;
	}
}