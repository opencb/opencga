package org.opencb.opencga.storage.hadoop.variant.annotation.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.mapreduce.PhoenixRecordReader;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;

import java.sql.SQLException;
import java.util.List;

public class PhoenixCompat implements PhoenixCompatApi {

    @Override
    public PTable makePTable(List<PColumn> columns) throws SQLException {
        return PTableImpl.makePTable(new PTableImpl(), columns);
    }

    @Override
    public <T extends DBWritable> PhoenixRecordReader<T> newPhoenixRecordReader(Class<T> inputClass, Configuration configuration,
                                                                                QueryPlan queryPlan) {
        return new PhoenixRecordReader<>(inputClass, configuration, queryPlan);
    }

    @Override
    public boolean isDropColumnFromViewSupported() {
        return true;
    }
}
