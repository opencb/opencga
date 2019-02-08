package org.opencb.opencga.storage.core.variant.io;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.vcf.VCFHeader;
import org.opencb.biodata.formats.variant.vcf4.VcfUtils;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.tools.variant.converters.VariantContextConverter;
import org.opencb.biodata.tools.variant.converters.avro.VariantAvroToVariantContextConverter;
import org.opencb.biodata.tools.variant.converters.avro.VariantStudyMetadataToVCFHeaderConverter;
import org.opencb.biodata.tools.variant.converters.proto.VariantProtoToVariantContextConverter;
import org.opencb.commons.io.DataWriter;

import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 10/11/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VcfDataWriter<T> implements DataWriter<T> {

    private final VariantMetadata metadata;
    private final List<String> annotations;
    private final OutputStream outputStream;
    private VariantContextWriter variantContextWriter;
    private VariantContextConverter<T> converter;

    protected VcfDataWriter(VariantMetadata metadata, List<String> annotations, OutputStream outputStream) {
        this.metadata = metadata;
        this.annotations = annotations;
        this.outputStream = outputStream;
    }

    public static VcfDataWriter<Variant> newWriterForAvro(VariantMetadata metadata, List<String> annotations, OutputStream outputStream) {
        return new VariantVcfDataWriter(metadata, annotations, outputStream);
    }

    public static VcfDataWriter<VariantProto.Variant> newWriterForProto(VariantMetadata metadata, List<String> annotations,
                                                                        OutputStream outputStream) {
        return new VariantProtoVcfDataWriter(metadata, annotations, outputStream);
    }

    private static class VariantVcfDataWriter extends VcfDataWriter<Variant> {

        VariantVcfDataWriter(VariantMetadata metadata, List<String> annotations, OutputStream outputStream) {
            super(metadata, annotations, outputStream);
        }

        @Override
        public VariantContextConverter<Variant> newConverter(String study, List<String> samples, List<String> annotations) {
            return new VariantAvroToVariantContextConverter(study, samples, annotations);
        }
    }

    private static class VariantProtoVcfDataWriter extends VcfDataWriter<VariantProto.Variant> {

        VariantProtoVcfDataWriter(VariantMetadata metadata, List<String> annotations, OutputStream outputStream) {
            super(metadata, annotations, outputStream);
        }

        @Override
        public VariantContextConverter<VariantProto.Variant> newConverter(String study, List<String> samples, List<String> annotations) {
            return new VariantProtoToVariantContextConverter(study, samples, annotations);
        }
    }

    @Override
    public boolean pre() {
        String study = metadata.getStudies().get(0).getId();
        List<String> samples = metadata.getStudies().get(0).getIndividuals().stream()
                .flatMap(individual -> individual.getSamples().stream()).map(Sample::getId).collect(Collectors.toList());

        VCFHeader vcfHeader = new VariantStudyMetadataToVCFHeaderConverter().convert(metadata.getStudies().get(0), annotations);
        // Warning: Calling to vcfHeader.getSequenceDictionary() may fail if any contig has null length
        variantContextWriter = VcfUtils.createVariantContextWriter(outputStream, null, Options.ALLOW_MISSING_FIELDS_IN_HEADER);
        variantContextWriter.writeHeader(vcfHeader);
        converter = newConverter(study, samples, annotations);

        return true;
    }

    public abstract VariantContextConverter<T> newConverter(String study, List<String> samples, List<String> annotations);

    @Override
    public boolean write(List<T> list) {
        List<VariantContext> contexts = converter.apply(list);
        for (VariantContext variantContext : contexts) {
            variantContextWriter.add(variantContext);
        }
        return true;
    }

    @Override
    public boolean close() {
        variantContextWriter.close();
        return true;
    }
}
