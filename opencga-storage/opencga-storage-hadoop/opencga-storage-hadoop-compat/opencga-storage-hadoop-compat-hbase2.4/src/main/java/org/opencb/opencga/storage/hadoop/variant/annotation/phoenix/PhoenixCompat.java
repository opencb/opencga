package org.opencb.opencga.storage.hadoop.variant.annotation.phoenix;

import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;

import java.sql.SQLException;
import java.util.List;

public class PhoenixCompat {

    public static PTable makePTable(List<PColumn> columns) throws SQLException {
        return new PTableImpl.Builder().setColumns(columns).build();
    }

}
