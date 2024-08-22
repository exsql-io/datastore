package io.exsql.bytegraph;

import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphSchema;

public abstract class ByteGraphReader {

    protected byte[] bytes;

    public ByteGraphSchema schema;

    public int _partition_id;

    public Object[] read(final byte[] bytes) throws ClassNotFoundException {
        this.bytes = bytes;
        return read();
    }

    abstract protected Object[] read() throws ClassNotFoundException;

    public byte[] encodedAssertedGraph() {
        return this.bytes;
    }

}
