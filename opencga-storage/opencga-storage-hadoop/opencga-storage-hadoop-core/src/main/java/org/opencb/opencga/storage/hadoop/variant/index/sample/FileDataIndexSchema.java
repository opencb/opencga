package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.OriginalCall;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.core.DataField;
import org.opencb.opencga.storage.hadoop.variant.index.core.DataSchema;
import org.opencb.opencga.storage.hadoop.variant.index.core.VarcharDataField;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileDataIndexSchema extends DataSchema {

    private final DataField<OriginalCall> originalCallField;
    private final DataField<List<AlternateCoordinate>> secondaryAlternatesField;
    private boolean includeOriginalCall = true;
    private boolean includeSecondaryAlternates = true;

    public FileDataIndexSchema(SampleIndexConfiguration.FileIndexConfiguration fileIndexConfiguration) {
        if (includeOriginalCall) {
            originalCallField = new VarcharDataField(
                    new IndexFieldConfiguration(IndexFieldConfiguration.Source.FILE, "ORIGINAL_CALL", null))
                    .from(
                            (OriginalCall oc) -> oc == null ? null : oc.getVariantId() + "+" + oc.getAlleleIndex(),
                            (String s) -> {
                                if (s == null || s.isEmpty()) {
                                    return null;
                                } else {
                                    String[] split = s.split("\\+", 2);
                                    return new OriginalCall(split[0], Integer.parseInt(split[1]));
                                }
                            });
            addField(originalCallField);
        } else {
            originalCallField = null;
        }
        if (includeSecondaryAlternates) {
            secondaryAlternatesField = new VarcharDataField(
                    new IndexFieldConfiguration(IndexFieldConfiguration.Source.STUDY, "SECONDARY_ALTERNATES", null))
                    .from((List<AlternateCoordinate> secondaryAlternates) -> {
                        if (secondaryAlternates == null || secondaryAlternates.isEmpty()) {
                            return "";
                        }
                        boolean needsSeparator = false;
                        StringBuilder sb = new StringBuilder();
                        for (AlternateCoordinate alternate : secondaryAlternates) {
                            if (needsSeparator) {
                                sb.append(',');
                            }
                            sb.append(alternate.getChromosome());
                            sb.append("+");
                            sb.append(alternate.getStart());
                            sb.append("+");
                            sb.append(alternate.getEnd());
                            sb.append("+");
                            sb.append(alternate.getReference());
                            sb.append("+");
                            sb.append(alternate.getAlternate());
                            needsSeparator = true;
                        }

                        return sb.toString();
                    }, (String s) -> {
                        if (s == null || s.isEmpty()) {
                            return Collections.emptyList();
                        }
                        String[] split = s.split(",");
                        List<AlternateCoordinate> alternates = new ArrayList<>(split.length);
                        for (String alt : split) {
                            String[] altSplit = alt.split("\\+", 5);
                            String alternate = altSplit.length == 5 ? altSplit[4] : "";
                            alternates.add(new AlternateCoordinate(
                                    altSplit[0],
                                    Integer.parseInt(altSplit[1]),
                                    Integer.parseInt(altSplit[2]),
                                    altSplit[3],
                                    alternate,
                                    VariantBuilder.inferType(altSplit[3], alternate)
                            ));
                        }
                        return alternates;
                    });
            addField(secondaryAlternatesField);
        } else {
            secondaryAlternatesField = null;
        }
    }

    public boolean isIncludeOriginalCall() {
        return includeOriginalCall;
    }

    public DataField<OriginalCall> getOriginalCallField() {
        return originalCallField;
    }

    public boolean isIncludeSecondaryAlternates() {
        return includeSecondaryAlternates;
    }

    public DataField<List<AlternateCoordinate>> getSecondaryAlternatesField() {
        return secondaryAlternatesField;
    }

    public OriginalCall readOriginalCall(ByteBuffer fileDataBitBuffer) {
        return readField(fileDataBitBuffer, originalCallField);
    }

    public List<AlternateCoordinate> readSecondaryAlternates(ByteBuffer fileDataBitBuffer) {
        return readField(fileDataBitBuffer, secondaryAlternatesField);
    }

}
