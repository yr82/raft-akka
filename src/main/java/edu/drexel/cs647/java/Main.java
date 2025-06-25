package edu.drexel.cs647.java;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import akka.actor.typed.ActorSystem;

public class Main {
	// The only IO we're doing here is console IO, if that fails we can't really
	// recover
	public static void main(String[] args) throws IOException, InterruptedException {
		System.out.println("Raft Consensus Algorithm Implementation");
		var done = false;

		// Read the number of processes and create the orchestrator
		String numProcesses = "5";

		var orc = ActorSystem.create(Orchestrator.create(numProcesses), "raft-orchestrator");

		// Start the algorithm
		orc.tell("start-algo");

		// Add a delay for the elections to take place
		Thread.sleep(5000);


		// Replace console input with a list of commands
		ArrayList<String> commands = new ArrayList<>();
		commands.add("submit x 1");
		commands.add("submit x 2");
		commands.add("submit x 3");

		// Send each command to the orchestrator
		for (String command : commands) {
			orc.tell(command);

			// Add a delay between the excecution of each command
			Thread.sleep(1000);
			
		}

	}
}