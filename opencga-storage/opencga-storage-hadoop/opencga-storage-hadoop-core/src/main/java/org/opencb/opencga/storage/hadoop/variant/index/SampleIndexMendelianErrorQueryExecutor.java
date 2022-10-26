package org.opencb.opencga.storage.hadoop.variant.index;

import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.IssueEntry;
import org.opencb.biodata.models.variant.avro.IssueType;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.Trio;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SampleIndexMendelianErrorQueryExecutor extends SampleIndexVariantQueryExecutor {

    public SampleIndexMendelianErrorQueryExecutor(VariantHadoopDBAdaptor dbAdaptor, SampleIndexDBAdaptor sampleIndexDBAdaptor,
                                                  String storageEngineId, ObjectMap options) {
        super(dbAdaptor, sampleIndexDBAdaptor, storageEngineId, options);
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) {
        if (VariantQueryUtils.isValidParam(query, VariantQueryUtils.SAMPLE_MENDELIAN_ERROR)
                || VariantQueryUtils.isValidParam(query, VariantQueryUtils.SAMPLE_DE_NOVO)
                || VariantQueryUtils.isValidParam(query, VariantQueryUtils.SAMPLE_DE_NOVO_STRICT)) {
            return super.canUseThisExecutor(query, options);
        } else {
            return false;
        }
    }

    @Override
    protected Object getOrIterator(Query query, QueryOptions options, boolean iterator, SampleIndexQuery sampleIndexQuery) {

        List<Trio> trios = new ArrayList<>(sampleIndexQuery.getMendelianErrorSet().size());
        int studyId = metadataManager.getStudyId(sampleIndexQuery.getStudy());
        for (String sample : sampleIndexQuery.getMendelianErrorSet()) {
            SampleMetadata sampleMetadata = metadataManager.getSampleMetadata(studyId, metadataManager.getSampleId(studyId, sample));
            String father;
            String mother;
            if (sampleMetadata.getFather() != null) {
                father = metadataManager.getSampleName(studyId, sampleMetadata.getFather());
            } else {
                father = null;
            }
            if (sampleMetadata.getMother() != null) {
                mother = metadataManager.getSampleName(studyId, sampleMetadata.getMother());
            } else {
                mother = null;
            }

            trios.add(new Trio(null, father, mother, sampleMetadata.getName()));
        }

        Object object = super.getOrIterator(query, options, iterator, sampleIndexQuery);

        if (iterator) {
            VariantDBIterator variantIterator = (VariantDBIterator) object;
            return variantIterator.map(v -> addIssueEntries(v, sampleIndexQuery.getStudy(), trios));
        } else {
            DataResult<Variant> result = (DataResult<Variant>) object;
            result.getResults().replaceAll(v -> addIssueEntries(v, sampleIndexQuery.getStudy(), trios));
            return result;
        }
    }

    private Variant addIssueEntries(Variant variant, String studyName, List<Trio> trios) {
        for (StudyEntry study : variant.getStudies()) {
            if (study.getStudyId().equals(studyName)) {
                for (Trio trio : trios) {
                    Genotype fatherGt = null;
                    Genotype motherGt = null;
                    Genotype childGt;
                    if (trio.getFather() != null) {
                        fatherGt = parseGenotype(study.getSampleData(trio.getFather(), "GT"));
                    }
                    if (trio.getMother() != null) {
                        motherGt = parseGenotype(study.getSampleData(trio.getMother(), "GT"));
                    }
                    childGt = parseGenotype(study.getSampleData(trio.getChild(), "GT"));
                    int code = MendelianError.compute(fatherGt, motherGt, childGt, variant.getChromosome());
                    if (code != 0) {
                        if (study.getIssues() == null) {
                            study.setIssues(new ArrayList<>());
                        }
                        study.getIssues()
                                .add(new IssueEntry(
                                        MendelianError.isDeNovo(code) ? IssueType.DE_NOVO : IssueType.MENDELIAN_ERROR,
                                        new SampleEntry(
                                                trio.getChild(),
                                                null,
                                                Collections.singletonList(String.valueOf(code))),
                                        Collections.singletonMap("meCode", String.valueOf(code))));
                    }
                }
            }
        }
        return variant;
    }

    private Genotype parseGenotype(String gt) {
        if (gt.equals(GenotypeClass.UNKNOWN_GENOTYPE)) {
            gt = "0/0";
        }
        return new Genotype(gt);
    }
}
