package com.bounteous.FlowTide.server.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InMemoryLog {

    private final List<String> messages = new ArrayList<>();
    private long nextOffset = 0;

    public synchronized long append(String message) {
        messages.add(message != null ? message : "");
        return nextOffset++;
    }

    public synchronized List<String> readFromOffset(long startOffset, int maxMessages) {
        if (startOffset < 0 || maxMessages <= 0 || startOffset >= nextOffset) {
            return Collections.emptyList();
        }
        int from = (int) startOffset;
        int to = (int) Math.min(from + maxMessages, messages.size());
        return new ArrayList<>(messages.subList(from, to));
    }

    public synchronized List<String> getAllMessages() {
        return new ArrayList<>(messages);
    }

    public synchronized long getCurrentOffset() {
        return nextOffset;
    }

    public synchronized int size() {
        return messages.size();
    }

    public synchronized void clear() {
        messages.clear();
        nextOffset = 0;
    }
}
