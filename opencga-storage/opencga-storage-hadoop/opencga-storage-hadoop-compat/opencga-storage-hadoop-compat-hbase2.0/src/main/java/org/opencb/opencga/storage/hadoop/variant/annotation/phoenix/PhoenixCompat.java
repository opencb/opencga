package org.opencb.opencga.storage.hadoop.variant.annotation.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.mapreduce.PhoenixRecordReader;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;

import java.lang.reflect.Constructor;
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
        try {
            Constructor<PhoenixRecordReader> constructor = PhoenixRecordReader.class
                    .getConstructor(Class.class, Configuration.class, QueryPlan.class, MapReduceParallelScanGrouper.class);
            constructor.setAccessible(true);
            return constructor.newInstance(inputClass, configuration, queryPlan, MapReduceParallelScanGrouper.getInstance());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }
}
