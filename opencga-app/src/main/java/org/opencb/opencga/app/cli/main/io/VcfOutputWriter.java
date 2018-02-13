package org.opencb.opencga.app.cli.main.io;

import org.opencb.biodata.models.metadata.SampleSetType;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.metadata.VariantFileHeader;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.models.variant.protobuf.VariantProto;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.io.VcfDataWriter;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

        // Prepare other VCF fields
//            List<String> cohorts = new ArrayList<>(); // Arrays.asList("ALL", "MXL");
//            List<String> formats = getFormats(study);

        if (variantQueryResult != null) {
            if (metadata.getStudies().isEmpty()) {
                // If excluding studies, we need to create a dummy study.
                metadata.getStudies().add(VariantStudyMetadata
                        .newBuilder()
                        .setId("any")
                        .setSampleSetType(SampleSetType.UNKNOWN)
                        .setAggregatedHeader(VariantFileHeader
                                .newBuilder()
                                .setVersion("")
                                .build())
                        .build());
            }
            String study = metadata.getStudies().get(0).getId();
            VcfDataWriter<Variant> writer = VcfDataWriter.newWriterForAvro(metadata, annotations, outputStream);
            writer.open();
            writer.pre();
            for (Variant variant : variantQueryResult.getResult()) {
                // FIXME: The server may be returning the StudyEntry with a different name
                String shortStudy = study.substring(study.lastIndexOf(':') + 1, study.length());
                if (variant.getStudy(study) == null && variant.getStudy(shortStudy) != null) {
                    variant.addStudyEntry(variant.getStudy(shortStudy).setStudyId(study));
                }

                // FIXME: This should not be needed! VariantAvroToVariantContextConverter must be fixed
                if (variant.getStudy(study) == null) {
                    StudyEntry studyEntry = new StudyEntry(study);
                    studyEntry.getFiles().add(new FileEntry("", null, Collections.emptyMap()));
                    variant.addStudyEntry(studyEntry);
                }
                writer.write(variant);
            }
            writer.post();
            writer.close();
        } else {
            VcfDataWriter<VariantProto.Variant> writer = VcfDataWriter.newWriterForProto(metadata, annotations, outputStream);
            writer.open();
            writer.pre();
            while (variantIterator.hasNext()) {
                VariantProto.Variant next = variantIterator.next();
                writer.write(next);
            }
            writer.post();
            writer.close();
        }
        outputStream.close();

    }

}
