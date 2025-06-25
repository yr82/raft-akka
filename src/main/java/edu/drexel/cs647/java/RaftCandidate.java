package edu.drexel.cs647.java;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;


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


public class RaftCandidate extends AbstractBehavior<RaftServerMessage> {

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

	// Set to track the votes recived
	private Set<String> votesRecived;

	// Local state machine
	private StateMachine stateMachine;

	// Current leader
	private ActorRef<RaftServerMessage> currentLeader;

	public RaftCandidate(ActorContext ctxt, String serverId, TimerScheduler<RaftServerMessage> timers,
			ArrayList<ActorRef<RaftServerMessage>> currentServers, PersistentServerState persistentServerState,
			int commitIndex, int lastApplied, StateMachine stateMachine) {
		super(ctxt);
		this.serverId = serverId;

		this.commitIndex = commitIndex;
		this.lastApplied = lastApplied;
		this.persistentServerState = persistentServerState;
		this.currentServers = currentServers;

		// Set up the timer
		this.timers = timers;

		this.stateMachine = stateMachine;

		// Set for votes
		votesRecived = new HashSet<>();

		// Send a message to myself to start an election
		getContext().getSelf().tell(new RaftServerMessage.startElection());

	}

	// Helper function to save persistant state to the disk
	public void writeToDisk() throws IOException{
		persistentServerState.saveState(serverId + ".ser");
	}

	// Helper function to start elecetion timer
	public void startElectionTimer(TimerScheduler<RaftServerMessage> timers) {
		Random random = new Random();
		int delay = 1500 + random.nextInt(1501); // [150, 300]
		Duration timeout = Duration.ofMillis(delay);

		timers.startSingleTimer("election-timeout", new RaftServerMessage.Timeout(), timeout);
		getContext().getLog().info("{} started election with timeout of {}ms", serverId, delay);

	}

	public static Behavior<RaftServerMessage> create(String serverId, ArrayList<ActorRef<RaftServerMessage>> currentServers, PersistentServerState persistentServerState,
			int commitIndex, int lastApplied, StateMachine stateMachine) {
        return Behaviors.<RaftServerMessage>supervise(
            Behaviors.setup(context ->
                Behaviors.withTimers(timers ->
					new RaftCandidate(context, serverId, timers, currentServers, persistentServerState, commitIndex, lastApplied, stateMachine) // Pass the state machine to the constructor
				)
				
		)
            ).onFailure(Exception.class, SupervisorStrategy.restart());
    }


	@Override
	public Receive<RaftServerMessage> createReceive() {
		return newReceiveBuilder().onMessage(RaftServerMessage.class, this::dispatch).build();
	}

	public Behavior<RaftServerMessage> dispatch(RaftServerMessage msg) throws IOException {

		switch (msg) {

		case RaftServerMessage.convertToFollower f:

			// Log the behavior
			getContext().getLog().info(serverId + " is converting from CANDIDATE to FOLLOWER.");

			// Before I change my state, I am going write all changes to the persistant
			// state to disk
			writeToDisk();

			// Set the state to appropriate value
			// return RaftFollower.create(serverId);
			// return new RaftFollower(getContext(), serverId, timers, persistentServerState, commitIndex, lastApplied, stateMachine);
			return RaftFollower.create(serverId, persistentServerState, commitIndex, lastApplied, stateMachine, currentServers);
//			return new RaftFollower(serverId, timers, persistentServerState, commitIndex, lastApplied);

		case RaftServerMessage.convertToLeader l:

			// Log the behavior
			getContext().getLog().info(serverId + " is converting from CANDIDATE to LEADER.");

			// Before I change my state, I am going write all changes to the persistant
			// state to disk
			writeToDisk();

			// Set the state to appropriate value
			return new RaftLeader(getContext(), serverId, timers, commitIndex, lastApplied, currentServers,
					persistentServerState, stateMachine);
//			return new RaftLeader(serverId, timers, commitIndex, lastApplied, currentServers, persistentServerState);

		case RaftServerMessage.startElection e:
			// This is the case where the candidate is trying to get votes for an election

			// Log behavior 
			getContext().getLog().info("{} is starting an election", serverId);

			// Increment the currrent term
			persistentServerState.incrementCurrentTerm();

			// Vote for myself
			persistentServerState.setVotedForAtTerm(persistentServerState.getCurrentTerm(), serverId);
			votesRecived.add(serverId);
			getContext().getLog().info("{} voted for itself", serverId);


			// Send request for votes to all other servers except me
			for (ActorRef<RaftServerMessage> server : currentServers) {
				if (!server.equals(getContext().getSelf())) {
					server.tell(new RaftServerMessage.requestVoteRPC(getContext().getSelf(),
							persistentServerState.getCurrentTerm(), serverId, persistentServerState.getLastLogIndex(),
							persistentServerState.getLastLogTerm()));
				}
			}

			// Start the election timer
			startElectionTimer(timers);

			break;

		case RaftServerMessage.requestVoteRPC c:
			// This is the case where the candidate recives a request vote for another
			// candidate during elections

			// convert to the follower at the end after voting at the election
			if (c.term() > persistentServerState.getCurrentTerm()) {
				persistentServerState.setCurrentTerm(c.term());
				persistentServerState.setVotedForAtTerm(c.term(), null);

				// Convert to Follower
				getContext().getSelf().tell(new RaftServerMessage.convertToFollower());
				break; // Do not break here
			}

			// Get the last term of ther log
			int lastTerm = persistentServerState.getLastLogTerm();

			// Check if the log was updated
			boolean logOK = ((c.lastLogTerm() > lastTerm)
					|| ((c.lastLogTerm() == lastTerm) 
					&& (c.lastLogIndex() >= persistentServerState.getLastLogIndex())));

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

			break;

		case RaftServerMessage.requestVoteReply r:
			// This is the case where a candidate recives a reply for a vote request from
			// another server

			if ((r.term() == persistentServerState.getCurrentTerm()) && r.voteGranted()) {

				// Log Behavior
				getContext().getLog().info("{} recived a vote from {}", serverId, r.voterId());

				// Update local variables
				votesRecived.add(r.voterId());

				if (votesRecived.size() >= Math.ceil((currentServers.size() + 1) / 2)) {

					// Cancel election timer
					timers.cancel("election-timeout");

					// Log Behavior
					getContext().getLog().info("{} won the election", serverId);

					// Convert to leader
					getContext().getSelf().tell(new RaftServerMessage.convertToLeader());
					break;

				}

			} else if (r.term() > persistentServerState.getCurrentTerm()) {

				// Update the current term
				persistentServerState.setVotedForAtTerm(persistentServerState.getCurrentTerm(), null);
				persistentServerState.setCurrentTerm(r.term());
				
				// Cancel election timer
				timers.cancel("election-timeout");

				// Convert to Follower
				getContext().getSelf().tell(new RaftServerMessage.convertToFollower());
				break;
			}

			break;

		case RaftServerMessage.appendEntriesRPC c:

			// This is the case where a follower recives an append entries message from
			// anaother server (leader)

			if (c.term() >= persistentServerState.getCurrentTerm()) {
				persistentServerState.setVotedForAtTerm(c.term(), null);
				persistentServerState.setCurrentTerm(c.term());
				
				// Cancel election timer
				timers.cancel("election-timeout");

				// Convert to Follower
				getContext().getSelf().tell(new RaftServerMessage.convertToFollower());

				// break;
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
			timers.cancel("election-timeout");
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
				// Update the state machine
				for (int i = commitIndex; i < c.leaderCommit() - 1; i++) {
					stateMachine.addEntry(persistentServerState.getEntry(i));
				}
				
				commitIndex = Math.min(c.leaderCommit(), persistentServerState.getLastLogIndex());
			}

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
			// This is the case where the candidate failed to get enough votes during the
			// election or has not
			// recived heartbeats in a while

			// Log behavior
			getContext().getLog().info("{} did NOT receive heartbeats from the leader before timeout", serverId);

			// Convert to candidate
			getContext().getSelf().tell(new RaftServerMessage.convertToFollower());

			break;

		default:
			break;

		}

		// Keep the same message handling behavior
		return this;
	}
}