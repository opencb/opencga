package org.opencb.opencga.storage.core.variant.io.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.VariantFileUtils;
import org.xerial.snappy.SnappyInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created on 06/10/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAvroReader implements VariantReader {

    private final File metadataFile;
    private VariantSource source;
    private LinkedHashMap<String, Integer> samplesPosition;
    private final AvroDataReader<VariantAvro> avroDataReader;

    public VariantAvroReader(File variantsFile, File metadataFile, VariantSource source) {
        this.metadataFile = metadataFile;
        this.source = source;
        avroDataReader = new AvroDataReader<>(variantsFile, VariantAvro.class);
    }

    @Override
    public boolean open() {
        return avroDataReader.open();
    }

    @Override
    public boolean close() {
        return avroDataReader.close();
    }

    @Override
    public boolean pre() {
        try (InputStream inputStream = metadataFile.toString().endsWith("gz")
                ? new GZIPInputStream(new FileInputStream(metadataFile))
                : metadataFile.toString().endsWith("snappy")
                        ? new SnappyInputStream(new FileInputStream(metadataFile))
                        : new FileInputStream(metadataFile)) {
            ObjectMapper jsonObjectMapper = new ObjectMapper();

            // Read global JSON file and copy its info into the already available VariantSource object
            VariantSource readSource = jsonObjectMapper.readValue(inputStream, VariantSource.class);
            source.setFileName(readSource.getFileName());
            source.setFileId(readSource.getFileId());
            source.setStudyName(readSource.getStudyName());
            source.setStudyId(readSource.getStudyId());
            source.setAggregation(readSource.getAggregation());
            source.setMetadata(readSource.getMetadata());
            source.setPedigree(readSource.getPedigree());
            source.setSamplesPosition(readSource.getSamplesPosition());
            source.setStats(readSource.getStats());
            source.setType(readSource.getType());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        Map<String, Integer> samplesPosition = source.getSamplesPosition();
        this.samplesPosition = new LinkedHashMap<>(samplesPosition.size());
        String[] samples = new String[samplesPosition.size()];
        for (Map.Entry<String, Integer> entry : samplesPosition.entrySet()) {
            samples[entry.getValue()] = entry.getKey();
        }

        for (int i = 0; i < samples.length; i++) {
            this.samplesPosition.put(samples[i], i);
        }
        return avroDataReader.pre();
    }

    @Override
    public boolean post() {
        return avroDataReader.post();
    }

    @Override
    public List<Variant> read(int batchSize) {
        List<Variant> batch = new ArrayList<>(batchSize);
        List<VariantAvro> read = avroDataReader.read(batchSize);
        for (VariantAvro variantAvro : read) {
            Variant variant = new Variant(variantAvro);
            if (!variant.getStudies().isEmpty()) {
                variant.getStudies().get(0).setSamplesPosition(samplesPosition);
            }
            batch.add(variant);
        }
        return batch;
    }

    @Override
    public List<String> getSampleNames() {
        return source.getSamples();
    }

    @Override
    public String getHeader() {
        return source.getMetadata().get(VariantFileUtils.VARIANT_FILE_HEADER).toString();
    }
}
