package io.exsql.bytegraph;

import io.exsql.bytegraph.bytes.ByteGraphBytes;
import io.exsql.bytegraph.metadata.ByteGraphSchema.ByteGraphSchema;

public abstract class ByteGraphWriter {
    public ByteGraphSchema schema;
    public abstract ByteGraphBytes write(final Object[] row,
                                         final boolean writeTypeFlag);
}
