package edu.drexel.cs647.java;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class PersistentServerState implements Serializable {

	// Serialization helpers
	private static final long serialVersionUID = 1L;
	// Latest term that the server has seen
	private int currentTerm;

	// Candidate Id that revived vote in the current term
	private Map<Integer, String> votedFor;

	// Log entries
	private ArrayList<LogEntry> log;

	public PersistentServerState() {
		currentTerm = 0;
		votedFor = new HashMap<>();
		log = new ArrayList<>();
	}

	public int getCurrentTerm() {
		return currentTerm;
	}

	public void setCurrentTerm(int currentTerm) {
		this.currentTerm = currentTerm;
	}

	public void incrementCurrentTerm() {
		this.currentTerm += 1;
	}

	public String getVotedForAtTerm(int term) {
		return votedFor.get(term);
	}

	public void setVotedForAtTerm(int term, String candidateId) {
		votedFor.put(term, candidateId);
	}

	public ArrayList<LogEntry> getLog() {
		return log;
	}

//	public void appendLog(LogEntry logEntry) {
//		log.add(logEntry);
//	}

	public void deleteConflictingEntriesFromIndex(int index) {

		index -= 1;

		if (index < log.size()) {
			log.subList(index, log.size()).clear();
		}
	}

	public LogEntry getEntry(int index) {
		index -= 1;

	 	if (index < log.size()) {
			return log.get(index);
		} else {
			return null;
		}
	}

	public int getLastLogIndex() {
		return log.size();
	}

	public int getLastLogTerm() {
		if (log.isEmpty()) {
			return 0;
		} else {
			return log.get(log.size() - 1).term();
		}
	}

	public void saveState(String filename) throws IOException {
		try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename))) {
			oos.writeObject(this);
		}
	}

}
