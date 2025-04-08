package org.opencb.opencga.storage.hadoop.variant.index.core;

public interface IndexCodec<T> {

    int encode(T value);

    T decode(int code);

    default boolean ambiguous(T value) {
        return ambiguous(encode(value));
    }

    boolean ambiguous(int code);

}
