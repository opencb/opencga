/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.benchmark.variant.generators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.benchmark.variant.queries.RandomQueries;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by jtarraga on 07/04/17.
 */
public abstract class TermQueryGenerator extends ConfiguredQueryGenerator {
    protected List<String> terms = new ArrayList<>();
    private String queryKey;
    private Logger logger = LogManager.getLogger(getClass());

    public TermQueryGenerator(String queryKey) {
        super();
        this.queryKey = queryKey;
    }

    @Override
    public void setUp(Map<String, String> params, RandomQueries randomQueries) {
        super.setUp(params);
        loadTerms(params, randomQueries);
    }

    protected abstract void loadTerms(Map<String, String> params, RandomQueries randomQueries);

    @Override
    public Query generateQuery(Query query) {
        String value = IntStream.range(0, getArity())
                .mapToObj(i -> terms.get(random.nextInt(terms.size())))
                .collect(Collectors.joining(","));
        query.append(queryKey, value);
        return query;
    }

    public static class FileQueryGenerator extends TermQueryGenerator {

        public FileQueryGenerator() {
            super(VariantQueryParam.FILE.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getFile() != null) {
                this.terms = randomQueries.getFile();
            }
        }
    }

    public static class SampleQueryGenerator extends TermQueryGenerator {

        public SampleQueryGenerator() {
            super(VariantQueryParam.SAMPLE.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getSample() != null) {
                this.terms = randomQueries.getSample();
            }
        }
    }

    public static class IncludeSampleQueryGenerator extends TermQueryGenerator {

        public IncludeSampleQueryGenerator() {
            super(VariantQueryParam.INCLUDE_SAMPLE.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getIncludeSample() != null) {
                this.terms = randomQueries.getIncludeSample();
            }
        }
    }

    public static class IncludeFileQueryGenerator extends TermQueryGenerator {

        public IncludeFileQueryGenerator() {
            super(VariantQueryParam.INCLUDE_FILE.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getIncludeFile() != null) {
                this.terms = randomQueries.getIncludeFile();
            }
        }
    }

    public static class IncludeStudyQueryGenerator extends TermQueryGenerator {

        public IncludeStudyQueryGenerator() {
            super(VariantQueryParam.INCLUDE_STUDY.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getIncludeStudy() != null) {
                this.terms = randomQueries.getIncludeStudy();
            }
        }
    }

    public static class FilterQueryGenerator extends TermQueryGenerator {

        public FilterQueryGenerator() {
            super(VariantQueryParam.FILTER.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getFilter() != null) {
                this.terms = randomQueries.getFilter();
            }
        }
    }

    public static class TranscriptionFlagsQueryGenerator extends TermQueryGenerator {

        public TranscriptionFlagsQueryGenerator() {
            super(VariantQueryParam.ANNOT_TRANSCRIPT_FLAG.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getTranscriptionFlags() != null) {
                this.terms = randomQueries.getTranscriptionFlags();
            }
        }
    }

    public static class DrugQueryGenerator extends TermQueryGenerator {

        public DrugQueryGenerator() {
            super(VariantQueryParam.ANNOT_DRUG.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getDrug() != null) {
                this.terms = randomQueries.getDrug();
            }
        }
    }

    public static class ClinicalSignificanceQueryGenerator extends TermQueryGenerator {

        public ClinicalSignificanceQueryGenerator() {
            super(VariantQueryParam.ANNOT_CLINICAL_SIGNIFICANCE.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getClinicalSignificance() != null) {
                this.terms = randomQueries.getClinicalSignificance();
            }
        }
    }


    public static class XrefQueryGenerator extends TermQueryGenerator {

        public XrefQueryGenerator() {
            super(VariantQueryParam.ANNOT_XREF.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getXref() != null) {
                this.terms = randomQueries.getXref();
            }
        }
    }

    public static class BiotypeQueryGenerator extends TermQueryGenerator {
        private Logger logger = LogManager.getLogger(getClass());

        public BiotypeQueryGenerator() {
            super(VariantQueryParam.ANNOT_BIOTYPE.key());
        }


        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getBiotype() != null) {
                this.terms = randomQueries.getBiotype();
            }
        }
    }

    public static class GeneQueryGenerator extends TermQueryGenerator {

        public GeneQueryGenerator() {
            super(VariantQueryParam.GENE.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getGene() != null) {
                this.terms = randomQueries.getGene();
            }
        }
    }

    public static class StudyQueryGenerator extends TermQueryGenerator {

        public StudyQueryGenerator() {
            super(VariantQueryParam.STUDY.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getStudy() != null) {
                this.terms = randomQueries.getStudy();
            }
        }
    }

    public static class TypeQueryGenerator extends TermQueryGenerator {

        public TypeQueryGenerator() {
            super(VariantQueryParam.TYPE.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getType() != null && !randomQueries.getType().isEmpty()) {
                this.terms = randomQueries.getType();
            } else {
                for (VariantType variantType : VariantType.values()) {
                    terms.add(variantType.toString());
                }
            }
        }
    }

    public static class ConsequenceTypeQueryGenerator extends TermQueryGenerator {
        private Logger logger = LogManager.getLogger(getClass());

        public ConsequenceTypeQueryGenerator() {
            super(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, RandomQueries randomQueries) {
            if (randomQueries.getCt() != null && !randomQueries.getCt().isEmpty()) {
                this.terms = randomQueries.getCt();
            } else {
                for (String term : ConsequenceTypeMappings.termToAccession.keySet()) {
                    terms.add(term);
                    terms.add(ConsequenceTypeMappings.getSoAccessionString(term));
                }
            }
        }

    }
}
