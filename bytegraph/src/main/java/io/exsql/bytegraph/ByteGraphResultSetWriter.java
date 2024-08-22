package io.exsql.bytegraph;

import io.exsql.bytegraph.bytes.ByteGraphBytes;
import io.exsql.bytegraph.metadata.ByteGraphSchema;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class ByteGraphResultSetWriter {
    public ByteGraphSchema.ByteGraphSchema schema;
    public abstract ByteGraphBytes write(final ResultSet resultSet,
                                         final boolean writeTypeFlag) throws ClassNotFoundException, SQLException;
}
