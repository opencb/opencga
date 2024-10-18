package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantBuilder;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.OriginalCall;
import org.opencb.biodata.tools.commons.BiConverter;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.storage.core.io.bit.ExposedByteArrayOutputStream;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.core.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileDataSchema extends DataSchema {

    private final DataFieldWithContext<Variant, OriginalCall> originalCallField;
    private final DataFieldWithContext<Variant, List<AlternateCoordinate>> secondaryAlternatesField;
    private final SampleIndexConfiguration.FileDataConfiguration fileDataConfiguration;

    public FileDataSchema(SampleIndexConfiguration.FileDataConfiguration fileDataConfiguration) {
        this.fileDataConfiguration = fileDataConfiguration;
        if (fileDataConfiguration.isIncludeOriginalCall()) {
            originalCallField = new VarBinaryDataField(
                    new IndexFieldConfiguration(IndexFieldConfiguration.Source.FILE, "ORIGINAL_CALL", null))
                    .fromWithContext(new VariantOriginalCallToBytesConverter());
            addField(originalCallField);
        } else {
            originalCallField = null;
        }
        if (fileDataConfiguration.isIncludeOriginalCall()) {
            secondaryAlternatesField = new VarBinaryDataField(
                    new IndexFieldConfiguration(IndexFieldConfiguration.Source.STUDY, "SECONDARY_ALTERNATES", null))
                    .fromWithContext(new AlternateCoordinateToBytesConverter());
            addField(secondaryAlternatesField);
        } else {
            secondaryAlternatesField = null;
        }
    }

    public boolean isIncludeOriginalCall() {
        return fileDataConfiguration.isIncludeOriginalCall();
    }

    public DataFieldWithContext<Variant, OriginalCall> getOriginalCallField() {
        return originalCallField;
    }

    public boolean isIncludeSecondaryAlternates() {
        return fileDataConfiguration.isIncludeSecondaryAlternates();
    }

    public DataFieldWithContext<Variant, List<AlternateCoordinate>> getSecondaryAlternatesField() {
        return secondaryAlternatesField;
    }

    public OriginalCall readOriginalCall(ByteBuffer fileDataByteBuffer, Variant variant) {
        return readFieldAndDecode(fileDataByteBuffer, originalCallField, variant);
    }

    public List<AlternateCoordinate> readSecondaryAlternates(ByteBuffer fileDataBitBuffer, Variant variant) {
        return readFieldAndDecode(fileDataBitBuffer, secondaryAlternatesField, variant);
    }

    protected static class VariantOriginalCallToBytesConverter
            implements BiConverter<Pair<Variant, OriginalCall>, Pair<Variant, ByteBuffer>> {

        private final VarIntDataField varint = new VarIntDataField(null);
        private final VarSIntDataField varsint = new VarSIntDataField(null);
        private final VarCharDataField varchar = new VarCharDataField(null);
        private final DataField<String> alleleField = new VarCharDataField(null).from(new AlleleCodec());

        @Override
        public Pair<Variant, ByteBuffer> to(Pair<Variant, OriginalCall> pair) {
            if (pair == null) {
                return Pair.of(null, ByteBuffer.allocate(0));
            } else if (pair.getValue() == null) {
                return Pair.of(pair.getKey(), ByteBuffer.allocate(0));
            } else {
                Variant variant = pair.getKey();
                OriginalCall call = pair.getValue();

                Variant originalVariant = new Variant(call.getVariantId());

                // Read the chromosome from the original variantId
                // Can't read from the "originalVariant", as the chromosome may be normalized in the Variant constructor
                String chromosome = call.getVariantId().substring(0, call.getVariantId().indexOf(":"));
                boolean normalizedChromosome = !variant.getChromosome().equals(chromosome);

                String reference = originalVariant.getReference();

                int alternatesIdx = call.getVariantId().indexOf(",");
                String alternate = originalVariant.getAlternate();
                if (alternatesIdx > 0) {
                    alternate += call.getVariantId().substring(alternatesIdx);
                }
                String alternateAndExtras = VariantPhoenixKeyFactory
                        .buildSymbolicAlternate(reference, alternate, originalVariant.getEnd(), originalVariant.getSv());

                int relativeStart = originalVariant.getStart() - variant.getStart();

                ByteBuffer buffer = ByteBuffer.allocate(
                        varint.getByteLength(call.getAlleleIndex())
                                + varsint.getByteLength(relativeStart)
                                + alleleField.getByteLength(reference)
                                + alleleField.getByteLength(alternateAndExtras)
                                + (normalizedChromosome ? varchar.getByteLength(chromosome) : 0
                        ));

                varint.write(call.getAlleleIndex(), buffer);
                varsint.write(relativeStart, buffer);
                alleleField.write(reference, buffer);
                alleleField.write(alternateAndExtras, buffer);

                if (normalizedChromosome) {
                    varchar.write(chromosome, buffer);
                }

                buffer.limit(buffer.position() - 1);
                buffer.rewind();
                return Pair.of(variant, buffer);
            }
        }

        @Override
        public Pair<Variant, OriginalCall> from(Pair<Variant, ByteBuffer> pair) {
            if (pair == null) {
                return Pair.of(null, null);
            }
            ByteBuffer byteBuffer = pair.getValue();
            Variant variant = pair.getKey();
            if (byteBuffer == null || variant == null || byteBuffer.limit() == 0) {
                return Pair.of(variant, null);
            } else {
                byteBuffer.rewind();
                int alleleIndex = varint.readAndDecode(byteBuffer);
                int start = varsint.readAndDecode(byteBuffer) + variant.getStart();
                String reference = alleleField.readAndDecode(byteBuffer);
                String alternate = alleleField.readAndDecode(byteBuffer);

                String variantId = VariantPhoenixKeyFactory.buildVariant(variant.getChromosome(), start, reference, alternate, null, null)
                        .toString();

                if (byteBuffer.hasRemaining()) {
                    // Replace chromosome with the original chromosome
                    // Can't set the chromosome directly, as the chromosome may be normalized in the Variant constructor
                    String originalChromosome = varchar.readAndDecode(byteBuffer);
                    variantId = variantId.substring(variantId.indexOf(":") + 1);
                    variantId = originalChromosome + ":" + variantId;
                }
                return Pair.of(variant, new OriginalCall(variantId, alleleIndex));
            }
        }
    }


    private static class AlternateCoordinateToBytesConverter
            implements BiConverter<Pair<Variant, List<AlternateCoordinate>>, Pair<Variant, ByteBuffer>> {

        private final VarSIntDataField varsint = new VarSIntDataField(null);
        private final VarCharDataField varchar = new VarCharDataField(null);
        private final DataField<String> alleleField = new VarCharDataField(null).from(new AlleleCodec());


        @Override
        public Pair<Variant, ByteBuffer> to(Pair<Variant, List<AlternateCoordinate>> pair) {
            if (pair == null) {
                return Pair.of(null, ByteBuffer.allocate(0));
            }
            if (pair.getValue() == null || pair.getValue().isEmpty() || pair.getKey() == null) {
                return Pair.of(pair.getKey(), ByteBuffer.allocate(0));
            }
            Variant variant = pair.getKey();

            ExposedByteArrayOutputStream stream = new ExposedByteArrayOutputStream();
            for (AlternateCoordinate alternate : pair.getValue()) {
                if (!alternate.getChromosome().equals(variant.getChromosome())) {
                    varchar.write(alternate.getChromosome(), stream);
                } else {
                    varchar.write("", stream);
                }

                varsint.write(alternate.getStart() - variant.getStart(), stream);
                varsint.write(alternate.getEnd() - variant.getStart(), stream);
                alleleField.write(alternate.getAlternate(), stream);
                alleleField.write(alternate.getReference(), stream);
            }

            return Pair.of(variant, stream.toByteByffer());
        }

        @Override
        public Pair<Variant, List<AlternateCoordinate>> from(Pair<Variant, ByteBuffer> pair) {
            if (pair == null) {
                return Pair.of(null, Collections.emptyList());
            }
            if (pair.getValue() == null || pair.getKey() == null) {
                return Pair.of(pair.getKey(), Collections.emptyList());
            }
            List<AlternateCoordinate> alternates = new ArrayList<>(2);
            Variant variant = pair.getKey();
            ByteBuffer byteBuffer = pair.getValue();
            while (byteBuffer.hasRemaining()) {
                String chr = varchar.readAndDecode(byteBuffer);
                if (chr == null || chr.isEmpty()) {
                    chr = variant.getChromosome();
                }
                int start = varsint.readAndDecode(byteBuffer) + variant.getStart();
                int end = varsint.readAndDecode(byteBuffer) + variant.getStart();
                String alternate = alleleField.readAndDecode(byteBuffer);
                String reference = alleleField.readAndDecode(byteBuffer);
                alternates.add(new AlternateCoordinate(
                        chr,
                        start,
                        end,
                        reference,
                        alternate,
                        VariantBuilder.inferType(reference, alternate)));

            }
            return Pair.of(variant, alternates);
        }
    }

}
