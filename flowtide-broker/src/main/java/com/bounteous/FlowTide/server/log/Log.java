package com.bounteous.FlowTide.server.log;


import java.io.*;
import java.util.concurrent.atomic.AtomicLong;

public class Log {

    private final File file;
    private final AtomicLong offset;

    public Log(String filePath) throws IOException {
        this.file = new File(filePath);

        // Create file if not exists
        if (!file.exists()) {
            file.createNewFile();
        }

        // Initialize offset based on file size (simple version)
        this.offset = new AtomicLong(file.length());
    }

    // Append message and return offset
    public synchronized long append(String message) throws IOException {

        try (FileWriter writer = new FileWriter(file, true)) {
            writer.write(message + "\n");
        }

        long currentOffset = offset.get();
        offset.addAndGet(message.length() + 1); // +1 for newline

        return currentOffset;
    }

    public File getFile() {
        return file;
    }
}
