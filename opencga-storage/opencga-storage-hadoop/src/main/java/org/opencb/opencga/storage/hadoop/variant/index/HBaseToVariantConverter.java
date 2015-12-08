package org.opencb.opencga.storage.hadoop.variant.index;

import com.google.common.collect.BiMap;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.tools.variant.converter.Converter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.HBaseStudyConfigurationManager;
import org.opencb.opencga.storage.hadoop.variant.index.annotation.HBaseToVariantAnnotationConverter;

import java.io.IOException;
import java.util.*;

/**
 * Created on 20/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToVariantConverter implements Converter<Result, Variant> {

    private final StudyConfigurationManager scm;
    private final HBaseToVariantAnnotationConverter annotationConverter;
    private final GenomeHelper genomeHelper;
    private final QueryOptions scmOptions = new QueryOptions(StudyConfigurationManager.READ_ONLY, true)
            .append(StudyConfigurationManager.CACHED, true);
    private final Map<Integer, LinkedHashMap<String, Integer>> returnedSamplesPosition = new HashMap<>();

    private List<String> returnedSamples = Collections.emptyList();

    public HBaseToVariantConverter(VariantTableHelper variantTableHelper) throws IOException {
        this(variantTableHelper, new HBaseStudyConfigurationManager(variantTableHelper.getOutputTableAsString(),
                variantTableHelper.getConf(), new ObjectMap()));
    }

    public HBaseToVariantConverter(GenomeHelper genomeHelper, StudyConfigurationManager scm) {
        this.genomeHelper = genomeHelper;
        this.scm = scm;
        this.annotationConverter = new HBaseToVariantAnnotationConverter(genomeHelper);
    }



    @Override
    public Variant convert(Result result) {

        Variant variant = genomeHelper.extractVariantFromVariantRowKey(result.getRow());

        List<VariantTableStudyRow> rows = VariantTableStudyRow.parse(result, genomeHelper);

        for (VariantTableStudyRow row : rows) {
            QueryResult<StudyConfiguration> queryResult = scm.getStudyConfiguration(row.getStudyId(), scmOptions);
            if (queryResult.getResult().isEmpty()) {
                continue;
            }
            StudyConfiguration studyConfiguration = queryResult.first();
            StudyEntry studyEntry = new StudyEntry(studyConfiguration.getStudyName());


            LinkedHashMap<String, Integer> returnedSamplesPosition = getReturnedSamplesPosition(studyConfiguration);
            studyEntry.setSamplesPosition(returnedSamplesPosition);

            @SuppressWarnings("unchecked")
            List<String>[] samplesDataArray = new List[returnedSamplesPosition.size()];
            List<List<String>> samplesData = Arrays.asList(samplesDataArray);

            for (String genotype : row.getGenotypes()) {
                String returnedGenotype = genotype.equals(VariantTableStudyRow.OTHER) ? "." : genotype;
                for (Integer sampleId : row.getSampleIds(genotype)) {
                    ArrayList<String> data = new ArrayList<>(1);
                    data.add(returnedGenotype);
                    String sampleName = studyConfiguration.getSampleIds().inverse().get(sampleId);
                    samplesDataArray[returnedSamplesPosition.get(sampleName)] = data;
                }
            }

            int homRef = 0;
            for (int i = 0; i < samplesDataArray.length; i++) {
                if (samplesDataArray[i] == null) {
                    ArrayList<String> data = new ArrayList<>(1);
                    data.add(VariantTableStudyRow.HOM_REF);
                    samplesDataArray[i] = data;
                    homRef++;
                }
            }
            if (homRef != row.getHomRefCount()) {
                throw new IllegalArgumentException("Wrong number of HomRef samples");
            }

            studyEntry.setSamplesData(samplesData);
            studyEntry.setFormat(Collections.singletonList("GT"));
            studyEntry.setFiles(Collections.singletonList(new FileEntry("", "", Collections.emptyMap())));

            variant.addStudyEntry(studyEntry);
        }
        variant.setAnnotation(annotationConverter.convert(result));

        return variant;
    }



    private LinkedHashMap<String, Integer> getReturnedSamplesPosition(StudyConfiguration studyConfiguration) {
        if (!returnedSamplesPosition.containsKey(studyConfiguration.getStudyId())) {
            LinkedHashMap<String, Integer> samplesPosition;
            if (returnedSamples.isEmpty()) {
                BiMap<Integer, String> unorderedSamplesPosition = StudyConfiguration.getSamplesPosition(studyConfiguration).inverse();
                samplesPosition = new LinkedHashMap<>(unorderedSamplesPosition.size());
                for (int i = 0; i < unorderedSamplesPosition.size(); i++) {
                    samplesPosition.put(unorderedSamplesPosition.get(i), i);
                }
            } else {
                samplesPosition = new LinkedHashMap<>(returnedSamples.size());
                int index = 0;
                BiMap<String, Integer> indexedSamplesId = StudyConfiguration.getIndexedSamples(studyConfiguration);
                for (String returnedSample : returnedSamples) {
                    if (indexedSamplesId.containsKey(returnedSample)) {
                        samplesPosition.put(returnedSample, index++);
                    }
                }
            }
            returnedSamplesPosition.put(studyConfiguration.getStudyId(), samplesPosition);
        }
        return returnedSamplesPosition.get(studyConfiguration.getStudyId());
    }
}
