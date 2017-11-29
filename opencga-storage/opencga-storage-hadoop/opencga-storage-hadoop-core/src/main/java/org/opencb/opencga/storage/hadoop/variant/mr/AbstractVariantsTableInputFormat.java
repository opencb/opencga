package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.opencb.biodata.models.variant.Variant;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * Created on 23/10/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractVariantsTableInputFormat<KEYIN, VALUEIN> extends InputFormat<KEYIN, Variant> {

    protected InputFormat<KEYIN, VALUEIN> inputFormat;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        if (inputFormat == null) {
            init(context.getConfiguration());
        }
        return inputFormat.getSplits(context);
    }

    protected abstract void init(Configuration configuration) throws IOException;

    protected static class RecordReaderTransform<KEYIN, VALUEIN, VALUEOUT> extends RecordReader<KEYIN, VALUEOUT> {

        private final RecordReader<KEYIN, VALUEIN> recordReader;
        private final Function<VALUEIN, VALUEOUT> transform;

        public RecordReaderTransform(RecordReader<KEYIN, VALUEIN> recordReader, Function<VALUEIN, VALUEOUT> transform) {
            this.recordReader = recordReader;
            this.transform = transform;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            recordReader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return recordReader.nextKeyValue();
        }

        @Override
        public KEYIN getCurrentKey() throws IOException, InterruptedException {
            return recordReader.getCurrentKey();
        }

        @Override
        public VALUEOUT getCurrentValue() throws IOException, InterruptedException {
            return transform.apply(recordReader.getCurrentValue());
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return recordReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            recordReader.close();
        }
    }

}
