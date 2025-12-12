package org.opencb.opencga.storage.core.variant.annotation.annotators.extensions.hgmd;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.VariantVcfHtsjdkReader;

import java.nio.file.Path;
import java.util.*;

public class HgmdIterator implements Iterator<Variant> {

    private final String version;
    private final String assembly;

    private final VariantVcfHtsjdkReader reader;

    private List<Variant> buffer = new ArrayList<>();
    private int bufferIndex = 0;

    public HgmdIterator(Path hgmdFile, String version, String assembly) {
        this.version = version;
        this.assembly = assembly;

        VariantStudyMetadata metadata = new VariantFileMetadata(null, hgmdFile.toString()).toVariantStudyMetadata("study");
        this.reader = new VariantVcfHtsjdkReader(hgmdFile.toAbsolutePath(), metadata);
        this.reader.open();
        this.reader.pre();
    }

    @Override
    public boolean hasNext() {
        if (CollectionUtils.isEmpty(buffer) || bufferIndex >= buffer.size()) {
            buffer = reader.read(100);
            bufferIndex = 0;
        }

        return bufferIndex < buffer.size();
    }

    @Override
    public Variant next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Variant variant = buffer.get(bufferIndex++);
        parseHgmdInfo(variant);
        return variant;
    }

    public void close() {
        if (reader != null) {
            reader.post();
            reader.close();
        }
    }

    private void parseHgmdInfo(Variant variant) {
        if (CollectionUtils.isNotEmpty(variant.getStudies())
                && CollectionUtils.isNotEmpty(variant.getStudies().get(0).getFiles())
                && MapUtils.isNotEmpty(variant.getStudies().get(0).getFiles().get(0).getData())) {
            Map<String, String> info = variant.getStudies().get(0).getFiles().get(0).getData();

            EvidenceEntry entry = new EvidenceEntry();

            // ID
            if (CollectionUtils.isNotEmpty(variant.getNames())) {
                entry.setId(variant.getNames().get(0));
            }

            // Source
            entry.setSource(new EvidenceSource("hgmd", version, null));

            // Assembly
            entry.setAssembly(assembly);

            // Genomic features
            setGenomicFeature(info, entry);

            // Heritable traits
            if (info.containsKey("PHEN")) {
                entry.setHeritableTraits(Collections.singletonList(new HeritableTrait(cleanString(info.get("PHEN")), null)));
            }

            // Ethinicity
            entry.setEthnicity(EthnicCategory.Z);

            // Additional properties
            setAdditionalProperties(info, entry);

            if (variant.getAnnotation() == null) {
                variant.setAnnotation(new VariantAnnotation());
            }
            variant.getAnnotation().setTraitAssociation(Collections.singletonList(entry));
        }
    }

    private void setGenomicFeature(Map<String, String> info, EvidenceEntry entry) {
        if (info.containsKey("DNA")) {
            Map<String, String> map = new HashMap<>();
            map.put("RefSeq mRNA", cleanString(info.get("DNA")));

            if (info.containsKey("GENE")) {
                map.put("Gene name", info.get("GENE"));
            }
            if (info.containsKey("PROT")) {
                map.put("RefSeq protein", cleanString(info.get("PROT")));
            }
            if (info.containsKey("DB")) {
                map.put("dbSNP", info.get("DB"));
            }

            entry.setGenomicFeatures(Collections.singletonList(new GenomicFeature(FeatureTypes.transcript, null, map)));
        }
    }

    private void setAdditionalProperties(Map<String, String> info, EvidenceEntry entry) {
        entry.setAdditionalProperties(new ArrayList<>());
        addAdditionalProperty("CLASS", info, entry.getAdditionalProperties());
        addAdditionalProperty("MUT", info, entry.getAdditionalProperties());
        addAdditionalProperty("STRAND", info, entry.getAdditionalProperties());
        addAdditionalProperty("RANKSCORE", info, entry.getAdditionalProperties());
        addAdditionalProperty("SVTYPE", info, entry.getAdditionalProperties());
        addAdditionalProperty("END", info, entry.getAdditionalProperties());
        addAdditionalProperty("SVLEN", info, entry.getAdditionalProperties());
    }

    private void addAdditionalProperty(String key, Map<String, String> info, List<Property> additionalProperties) {
        if (info.containsKey(key)) {
            additionalProperties.add(new Property(null, key, info.get(key)));
        }
    }

    private String cleanString(String input) {
        String output = input.replace("%2C", ",");
        output = output.replace("%3A", ":");
        output = output.replace("%3B", ";");
        output = output.replace("%3D", "=");
        output = output.replace("\"", "");
        return output;
    }

}
