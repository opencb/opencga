package org.opencb.opencga.storage.hadoop.utils;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class ValueOnlyTextOutputFormat<K, V> extends TextOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new ValueOnlyRecordWriter(super.getRecordWriter(job));
    }

    private class ValueOnlyRecordWriter extends RecordWriter<K, V> {
        private final RecordWriter<K, V> recordWriter;

        ValueOnlyRecordWriter(RecordWriter<K, V> recordWriter) {
            this.recordWriter = recordWriter;
        }

        @Override
        public void write(K key, V value) throws IOException, InterruptedException {
            recordWriter.write(null, value);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            recordWriter.close(context);
        }
    }
}
