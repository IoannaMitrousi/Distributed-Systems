package com.example.opalakia;

import org.jetbrains.annotations.NotNull;
import java.io.Serializable;
import java.lang.Comparable;

public class Value implements Serializable, Comparable <Value> {
    private static final long serialVersionUID = -6643274596837043061L;
    private String filename;
    private byte [] data;
    private int counter;
    private String artistName;

    public Value(String filename, @NotNull byte[] data, int size, String artistName, int counter) {
        this.filename = filename;
        this.data = new byte[size];
        System.arraycopy(data, 0, this.data, 0, size);
        this.artistName = artistName;
        this.counter = counter;
    }

    public String getFilename() {
        return filename;
    }

    public byte[] getData() {
        return data;
    }

    public int getCounter() {
        return counter;
    }

    public String getArtistName() {
        return artistName;
    }

    public int compareTo(@NotNull Value b) {
        return counter>b.getCounter() ? 1:-1;
    }

}
