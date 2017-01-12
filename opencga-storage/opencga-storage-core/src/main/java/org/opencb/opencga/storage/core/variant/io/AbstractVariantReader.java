package org.opencb.opencga.storage.core.variant.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.tools.variant.VariantFileUtils;
import org.opencb.commons.utils.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Created on 08/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractVariantReader implements VariantReader {

    private Path metadataPath;
    private VariantSource source;
    private LinkedHashMap<String, Integer> samplesPosition;
    private Map<String, LinkedHashMap<String, Integer>> samplesPositions;

    public AbstractVariantReader(Path metadataPath, VariantSource source) {
        this.metadataPath = metadataPath;
        this.source = source;
        this.samplesPositions = Collections.emptyMap();
    }

    public AbstractVariantReader(Map<String, LinkedHashMap<String, Integer>> samplesPositions) {
        this.metadataPath = null;
        this.source = null;
        this.samplesPositions = samplesPositions;
    }

    @Override
    public boolean pre() {
        if (metadataPath != null) {
            Files.exists(metadataPath);
            try (InputStream inputStream = FileUtils.newInputStream(metadataPath)) {
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
        }

        if (source != null) {
            Map<String, Integer> samplesPosition = source.getSamplesPosition();
            this.samplesPosition = new LinkedHashMap<>(samplesPosition.size());
            String[] samples = new String[samplesPosition.size()];
            for (Map.Entry<String, Integer> entry : samplesPosition.entrySet()) {
                samples[entry.getValue()] = entry.getKey();
            }

            for (int i = 0; i < samples.length; i++) {
                this.samplesPosition.put(samples[i], i);
            }
        }
        return true;
    }

    protected List<Variant> addSamplesPosition(List<Variant> variants) {
        if (source == null) {
            for (Variant variant : variants) {
                for (StudyEntry studyEntry : variant.getStudies()) {
                    LinkedHashMap samplesPosition = samplesPositions.get(studyEntry.getStudyId());
                    if (samplesPosition != null) {
                        studyEntry.setSamplesPosition(samplesPosition);
                    }
                }
            }
        } else {
            for (Variant variant : variants) {
                variant.getStudy(source.getStudyId()).setSamplesPosition(samplesPosition);
            }
        }

        return variants;

    }


    @Override
    public List<String> getSampleNames() {
        return new ArrayList<>(source.getSamplesPosition().keySet());
    }

    @Override
    public String getHeader() {
        return source.getMetadata().get(VariantFileUtils.VARIANT_FILE_HEADER).toString();
    }

}
