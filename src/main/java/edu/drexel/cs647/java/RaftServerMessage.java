package edu.drexel.cs647.java;

import java.util.ArrayList;

import akka.actor.typed.ActorRef;


public sealed interface RaftServerMessage {
	
	public final record Start(ArrayList<ActorRef<RaftServerMessage>> currentServers) implements RaftServerMessage {
	}

	public final record convertToLeader() implements RaftServerMessage {
	}

	public final record convertToFollower() implements RaftServerMessage {
	}

	public final record convertToCandidate() implements RaftServerMessage {
	}

	public final record appendEntriesRPC(ActorRef<RaftServerMessage> sender, int term, String leaderId,
			int prevLogIndex, int prevLogTerm, ArrayList<LogEntry> entries, int leaderCommit)
			implements RaftServerMessage {

	}

	public final record requestVoteRPC(ActorRef<RaftServerMessage> sender, int term, String candidateId,
			int lastLogIndex, int lastLogTerm) implements RaftServerMessage {
	}

	public final record appendEntriesReply(ActorRef<RaftServerMessage> sender, int term, boolean success, int ack)
			implements RaftServerMessage {
	}

	public final record requestVoteReply(int term, boolean voteGranted, String voterId) implements RaftServerMessage {
	}

	public final record startElection() implements RaftServerMessage {
	}

	public final record sendHeartBeats() implements RaftServerMessage {
	}

	public final record replicateLog(ArrayList<LogEntry> log) implements RaftServerMessage {
	}

	public final record updateLocalLog(int prevLogIndex, int leaderCommit, ArrayList<LogEntry> entries)
			implements RaftServerMessage {
	}

	public final record commitLogEntries() implements RaftServerMessage {
	}

	public final record clientRequest(ClientRequest clientRequest, ActorRef<String> client) implements RaftServerMessage {
	}

	public final record Timeout() implements RaftServerMessage {
	}

}