package edu.drexel.cs647.java;

import java.util.ArrayList;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class StateMachine {

    // private ArrayList<LogEntry> stateMachine;

    private String fileName;

    public StateMachine(String fileName) {
        this.fileName = fileName + "stateMachine.txt";
        // this.stateMachine = new ArrayList<LogEntry>();

    }

    // public ArrayList<LogEntry> getStateMachine() {
    //     return stateMachine;
    // }

    // public void addEntry(LogEntry entry) {
    //     stateMachine.add(entry);
    // }

    public void addEntry(LogEntry entry) {
        String command = entry.command();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
            writer.write(command);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
