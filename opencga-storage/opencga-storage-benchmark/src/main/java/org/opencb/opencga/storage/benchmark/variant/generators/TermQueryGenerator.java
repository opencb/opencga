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

import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by jtarraga on 07/04/17.
 */
public abstract class TermQueryGenerator extends QueryGenerator {
    protected ArrayList<String> terms = new ArrayList<>();
    private String termFilename;
    private String queryKey;
    private Logger logger = LoggerFactory.getLogger(getClass());

    public TermQueryGenerator(String termFilename, String queryKey) {
        super();
        this.termFilename = termFilename;
        this.queryKey = queryKey;
    }

    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);
        loadTerms(params, Paths.get(params.get(DATA_DIR), termFilename));
        terms.trimToSize();
    }

    protected void loadTerms(Map<String, String> params, Path path) {
        readCsvFile(path, strings -> terms.add(strings.get(0)));
    }

    @Override
    public Query generateQuery(Query query) {
        String value = IntStream.range(0, getArity())
                .mapToObj(i -> terms.get(random.nextInt(terms.size())))
                .collect(Collectors.joining(","));
        query.append(queryKey, value);
        return query;
    }

    public static class XrefQueryGenerator extends TermQueryGenerator {

        public XrefQueryGenerator() {
            super("xrefs.csv", VariantQueryParam.ANNOT_XREF.key());
        }
    }

    public static class BiotypeQueryGenerator extends TermQueryGenerator {
        private Logger logger = LoggerFactory.getLogger(getClass());

        public BiotypeQueryGenerator() {
            super("biotypes.csv", VariantQueryParam.ANNOT_BIOTYPE.key());
        }
    }

    public static class GeneQueryGenerator extends TermQueryGenerator {

        public GeneQueryGenerator() {
            super("genes.csv", VariantQueryParam.GENE.key());
        }

    }

    public static class StudyQueryGenerator extends TermQueryGenerator {

        public StudyQueryGenerator() {
            super("studies.csv", VariantQueryParam.STUDIES.key());
        }
    }

    public static class TypeQueryGenerator extends TermQueryGenerator {

        public TypeQueryGenerator() {
            super("types.csv", VariantQueryParam.TYPE.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, Path path) {
            if (path.toFile().exists()) {
                super.loadTerms(params, path);
            } else {
                for (VariantType variantType : VariantType.values()) {
                    terms.add(variantType.toString());
                }
            }
        }
    }

    public static class ConsequenceTypeQueryGenerator extends TermQueryGenerator {
        private Logger logger = LoggerFactory.getLogger(getClass());

        public ConsequenceTypeQueryGenerator() {
            super("consequence_types.csv", VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key());
        }

        @Override
        protected void loadTerms(Map<String, String> params, Path path) {
            if (path.toFile().exists()) {
                super.loadTerms(params, path);
            } else {
                for (String term : ConsequenceTypeMappings.termToAccession.keySet()) {
                    terms.add(term);
                    terms.add(ConsequenceTypeMappings.getSoAccessionString(term));
                }
            }
        }

    }
}
