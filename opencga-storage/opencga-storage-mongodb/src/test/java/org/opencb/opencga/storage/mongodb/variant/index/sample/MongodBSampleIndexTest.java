package org.opencb.opencga.storage.mongodb.variant.index.sample;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexTest;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.query.SampleIndexQuery;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;

import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Category(LongTests.class)
public class MongodBSampleIndexTest extends SampleIndexTest implements MongoDBVariantStorageTest {

    @Override
    public void load() throws Exception {
        super.load();

        URI outdir = newOutputUri();
        for (String study : studies) {
            System.out.println("=== Study: " + study + " ===");
            printSampleIndexContents(study, outdir);
        }
    }


    public void printSampleIndexContents(String study, URI outdir) throws Exception {
        VariantMongoDBAdaptor dbAdaptor = (VariantMongoDBAdaptor) super.dbAdaptor;
        int studyId = dbAdaptor.getMetadataManager().getStudyId(study);

        for (Integer sampleId : metadataManager.getIndexedSamples(studyId)) {
            String sampleName = metadataManager.getSampleName(studyId, sampleId);

            SampleIndexQuery query = sampleIndexDBAdaptor.parseSampleIndexQuery(new VariantQuery().study(study).sample(sampleName));
            String collectionName = ((MongoDBSampleIndexDBAdaptor) sampleIndexDBAdaptor)
                    .getSampleIndexCollectionName(studyId, query.getSchema().getVersion());

            Path output = Paths.get(outdir.resolve(collectionName + "." + sampleName + ".detailed.txt"));
            System.out.println("output = " + output);
            try (OutputStream os = Files.newOutputStream(output);
                 PrintStream out = new PrintStream(os)) {

                out.println();
                out.println();
                out.println();
                out.println("SAMPLE: " + sampleName + " (id=" + sampleId + ")");

                int variantIds = 0;
                try (CloseableIterator<SampleIndexVariant> it = sampleIndexDBAdaptor.indexVariantIterator(query)) {
                    while (it.hasNext()) {
                        SampleIndexVariant entry = it.next();
                        variantIds++;
                        out.println("_______________________");
                        out.println("#" + variantIds);
                        out.println(entry.toString(query.getSchema()));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }


}
