package org.opencb.opencga.app.cli.main.io;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.vcf.VCFHeader;
import org.opencb.biodata.formats.variant.vcf4.VcfUtils;
import org.opencb.biodata.models.metadata.Sample;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.biodata.tools.variant.converters.avro.VariantAvroToVariantContextConverter;
import org.opencb.biodata.tools.variant.converters.avro.VariantStudyMetadataToVCFHeaderConverter;
import org.opencb.biodata.tools.variant.converters.proto.VariantProtoToVariantContextConverter;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.core.results.VariantQueryResult;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 03/11/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VcfOutputWriter extends AbstractOutputWriter {

    private final List<String> annotations;
    private final PrintStream outputStream;
    private VariantMetadata metadata;

    public VcfOutputWriter(VariantMetadata metadata, List<String> annotations, PrintStream outputStream) {
        this.metadata = metadata;
        this.annotations = annotations;
        this.outputStream = outputStream;
    }

    @Override
    public void print(QueryResponse queryResponse) {
        if (checkErrors(queryResponse)) {
            return;
        }
        print((VariantQueryResult<Variant>) queryResponse.first(), null);
    }

    public void print(VariantQueryResult<Variant> variantQueryResult) {
        print(variantQueryResult, null);
    }

    public void print(Iterator<VariantProto.Variant> variantIterator) {
        print(null, variantIterator);
    }

    private void print(VariantQueryResult<Variant> variantQueryResult, Iterator<VariantProto.Variant> variantIterator) {

        String study = metadata.getStudies().get(0).getId();
        List<String> samples = metadata.getStudies().get(0).getIndividuals().stream()
                .flatMap(individual -> individual.getSamples().stream()).map(Sample::getId).collect(Collectors.toList());

//            List<String> samples = getSamplesFromVariantQueryResult(variantQueryResult, study);

        // Prepare other VCF fields
//            List<String> cohorts = new ArrayList<>(); // Arrays.asList("ALL", "MXL");
//            List<String> formats = getFormats(study);

        VCFHeader vcfHeader = new VariantStudyMetadataToVCFHeaderConverter().convert(metadata.getStudies().get(0), annotations);
        VariantContextWriter variantContextWriter = VcfUtils.createVariantContextWriter(outputStream, vcfHeader.getSequenceDictionary());
        variantContextWriter.writeHeader(vcfHeader);

        if (variantQueryResult != null) {
            VariantAvroToVariantContextConverter converter = new VariantAvroToVariantContextConverter(study, samples, annotations);
            for (Variant variant : variantQueryResult.getResult()) {
                // FIXME: This should not be needed! VariantAvroToVariantContextConverter must be fixed
                if (variant.getStudies().isEmpty()) {
                    StudyEntry studyEntry = new StudyEntry(study);
                    studyEntry.getFiles().add(new FileEntry("", null, Collections.emptyMap()));
                    variant.addStudyEntry(studyEntry);
                }

                VariantContext variantContext = converter.convert(variant);
                variantContextWriter.add(variantContext);
            }
        } else {
            VariantProtoToVariantContextConverter converter = new VariantProtoToVariantContextConverter(study, samples, annotations);
            while (variantIterator.hasNext()) {
                VariantProto.Variant next = variantIterator.next();
                variantContextWriter.add(converter.convert(next));
            }
        }
        variantContextWriter.close();
        outputStream.close();

    }

}
