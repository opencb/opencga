package org.opencb.opencga.storage.hadoop.variant.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.commons.io.DataWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class VariantParquetWriter implements DataWriter<Variant> {
    private final URI fileUri;
    private ParquetWriter<VariantAvro> writer;
    private CompressionCodecName codecName;
    private final Configuration conf;

    public VariantParquetWriter(URI fileUri, CompressionCodecName codecName, Configuration conf) {
        this.fileUri = fileUri;
        this.codecName = codecName;
        this.conf = conf;
    }

    @Override
    public boolean open() {
        try {
            // Disable internal java logger
            Logger.getLogger("org.apache.parquet.hadoop.ColumnChunkPageWriteStore").setLevel(Level.WARNING);
            writer = new AvroParquetWriter<>(new Path(fileUri), VariantAvro.getClassSchema(),
                    codecName, AvroParquetWriter.DEFAULT_BLOCK_SIZE, AvroParquetWriter.DEFAULT_PAGE_SIZE,
                    AvroParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED, conf);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public boolean write(List<Variant> list) {
        for (Variant variant : list) {
            try {
                writer.write(variant.getImpl());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return true;
    }

}
