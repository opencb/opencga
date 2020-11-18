/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.utils;

import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.models.pedigree.Individual;
import org.opencb.biodata.models.pedigree.Pedigree;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AnnotationSetManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileUtils;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;

/**
 * Created by jacobo on 29/01/15.
 */
public class CatalogSampleAnnotationsLoader {

    private static Logger logger = LoggerFactory.getLogger(FileUtils.class);
    private final CatalogManager catalogManager;

    public CatalogSampleAnnotationsLoader(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    protected CatalogSampleAnnotationsLoader() {
        this.catalogManager = null;
    }

    public DataResult<Sample> loadSampleAnnotations(File pedFile, String variableSetId, String sessionId) throws CatalogException {
        if (!pedFile.getFormat().equals(File.Format.PED)) {
            throw new CatalogException(pedFile.getUid() + " is not a pedigree file");
        }

        URI fileUri = catalogManager.getFileManager().getUri(pedFile);
        Study study = catalogManager.getFileManager().getStudy(pedFile, sessionId);
        long auxTime;
        long startTime = System.currentTimeMillis();

        //Read Pedigree file
        Pedigree ped = readPedigree(fileUri.getPath());
        Map<String, Sample> sampleMap = new HashMap<>();

        //Take or infer the VariableSet
        VariableSet variableSet;
        if (variableSetId != null) {
            variableSet = catalogManager.getStudyManager().getVariableSet(study.getFqn(), variableSetId, null, sessionId).first();
        } else {
            variableSet = getVariableSetFromPedFile(ped);
            AnnotationUtils.checkVariableSet(variableSet);
        }

        //Check VariableSet for all samples
        for (Individual individual : ped.getIndividuals().values()) {
            Map<String, Object> annotation = getAnnotation(individual, sampleMap, variableSet, ped.getFields());
            try {
                AnnotationUtils.checkAnnotationSet(variableSet, new AnnotationSet("", variableSet.getId(), annotation, "", 1,
                        null), null, true);
            } catch (CatalogException e) {
                String message = "Validation with the variableSet {id: " + variableSetId + "} over ped File = {path: " + pedFile.getPath()
                        + ", name: \"" + pedFile.getName() + "\"} failed";
                logger.info(message);
                throw new CatalogException(message, e);
            }
        }

        /** Pedigree file validated. Add samples and VariableSet **/


        //Add VariableSet (if needed)
        if (variableSetId == null) {
            auxTime = System.currentTimeMillis();
            List<Variable> variableList = new ArrayList<>();
            variableList.addAll(variableSet.getVariables());
            String name = pedFile.getName();
            variableSet = catalogManager.getStudyManager().createVariableSet(study.getFqn(), name, name, true, false, "Auto-generated  "
                            + "VariableSet from File = {path: " + pedFile.getPath() + ", name: \"" + pedFile.getName() + "\"}", null,
                    variableList, Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionId).getResults().get(0);
            variableSetId = variableSet.getId();
            logger.debug("Added VariableSet = {id: {}} in {}ms", variableSetId, System.currentTimeMillis() - auxTime);
        }

        //Add Samples
        Query samplesQuery = new Query(SampleDBAdaptor.QueryParams.ID.key(), new LinkedList<>(ped.getIndividuals().keySet()));
        Map<String, Sample> loadedSamples = new HashMap<>();
        for (Sample sample : catalogManager.getSampleManager().search(study.getFqn(), samplesQuery, null, sessionId).getResults()) {
            loadedSamples.put(sample.getId(), sample);
        }

        QueryOptions options = new QueryOptions()
                .append(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.BasicUpdateAction.ADD));
        auxTime = System.currentTimeMillis();
        for (Individual individual : ped.getIndividuals().values()) {
            Map<String, Object> annotations = getAnnotation(individual, sampleMap, variableSet, ped.getFields());
            AnnotationSet annotationSet = new AnnotationSet("pedigreeAnnotation", variableSet.getId(), annotations);
            Sample sample;
            if (loadedSamples.containsKey(individual.getId())) {
                sample = loadedSamples.get(individual.getId());
                logger.info("Sample " + individual.getId() + " already loaded with id : " + sample.getId());
                logger.info("Annotating sample {}", individual.getId());
                catalogManager.getSampleManager().update(study.getFqn(), individual.getId(), new SampleUpdateParams()
                        .setAnnotationSets(Collections.singletonList(annotationSet)), options, sessionId);
            } else {
                DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().create(study.getFqn(),
                        new Sample()
                                .setId(individual.getId())
                                .setFileIds(Collections.singletonList(pedFile.getPath()))
                                .setDescription("Sample loaded from the pedigree File = {path: " + pedFile.getPath() + ", name: \""
                                        + pedFile.getName() + "\" }")
                                .setAnnotationSets(Collections.singletonList(annotationSet)),
                        QueryOptions.empty(), sessionId);
                sample = sampleDataResult.getResults().get(0);
            }
            sampleMap.put(individual.getId(), sample);
        }
        logger.debug("Added {} samples in {}ms", ped.getIndividuals().size(), System.currentTimeMillis() - auxTime);

//        //Annotate Samples
//        auxTime = System.currentTimeMillis();
//        QueryOptions options = new QueryOptions()
//                .append(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.UpdateAction.ADD));
//        for (Map.Entry<String, Sample> entry : sampleMap.entrySet()) {
//            Map<String, Object> annotations = getAnnotation(ped.getIndividuals().get(entry.getKey()), sampleMap, variableSet, ped
//                    .getFields());
//            catalogManager.getSampleManager().update(study.getFqn(), entry.getValue().getId(), new SampleUpdateParams()
//                    .setAnnotationSets(Collections.singletonList(
//                            new AnnotationSet("pedigreeAnnotation", variableSet.getId(), annotations)
//                    )), options, sessionId);
//        }
//        logger.debug("Annotated {} samples in {}ms", ped.getIndividuals().size(), System.currentTimeMillis() - auxTime);

        //TODO: Create Cohort

        DataResult<Sample> sampleDataResult = catalogManager.getSampleManager().search(study.getFqn(),
                new Query(SampleDBAdaptor.QueryParams.ANNOTATION.key(), Constants.VARIABLE_SET + "=" + variableSetId), null, sessionId);
        return new DataResult<>((int) (System.currentTimeMillis() - startTime), Collections.emptyList(),
                sampleMap.size(), sampleDataResult.getResults(), sampleMap.size());
    }

    /**
     * @param individual  Individual from Pedigree file
     * @param sampleMap   Map<String, Sample>, to relate "sampleName" with "sampleId"
     * @param variableSet VariableSet to annotate
     * @param fields      fields
     * @return Map<String, Object> Map
     */
    protected Map<String, Object> getAnnotation(Individual individual, Map<String, Sample> sampleMap, VariableSet variableSet,
                                                Map<String, Integer> fields) {
        if (sampleMap == null) {
            sampleMap = new HashMap<>();
        }
        Map<String, Object> annotations = new HashMap<>();
        for (Variable variable : variableSet.getVariables()) {
            switch (variable.getId()) {
                case "family":
                    annotations.put("family", individual.getFamily());
                    break;
                case "name":
                    annotations.put("name", individual.getId());
                    break;
                case "fatherName":
                    annotations.put("fatherName", individual.getFatherId());
                    break;
                case "motherName":
                    annotations.put("motherName", individual.getMotherId());
                    break;
                case "sex":
                    annotations.put("sex", individual.getSex());
                    break;
                case "phenotype":
                    annotations.put("phenotype", individual.getPhenotype());
                    break;
                case "id":
                    Sample sample = sampleMap.get(individual.getId());
                    if (sample != null) {
                        annotations.put("id", sample.getUid());
                    } else {
                        annotations.put("id", -1);
                    }
                    break;
                case "fatherId":
                    Sample father = sampleMap.get(individual.getFatherId());
                    if (father != null) {
                        annotations.put("fatherId", father.getUid());
                    }
                    break;
                case "motherId":
                    Sample mother = sampleMap.get(individual.getMotherId());
                    if (mother != null) {
                        annotations.put("motherId", mother.getUid());
                    }
                    break;
                default:
                    Integer idx = fields.get(variable.getId());
                    if (idx != null) {
                        annotations.put(variable.getId(), individual.getFields()[idx]);
                    }
                    break;
            }
        }


        return annotations;
    }

    protected VariableSet getVariableSetFromPedFile(Pedigree ped) throws CatalogException {
        List<Variable> variableList = new LinkedList<>();

        String category = "PEDIGREE";
        variableList.add(new Variable("family", category, Variable.VariableType.STRING, null, true,
                false, Collections.<String>emptyList(), null, variableList.size(), null, "", null, null));
        variableList.add(new Variable("id", category, Variable.VariableType.DOUBLE, null, true,
                false, Collections.<String>emptyList(), null, variableList.size(), null, "", null, null));
        variableList.add(new Variable("name", category, Variable.VariableType.STRING, null, true,
                false, Collections.<String>emptyList(), null, variableList.size(), null, "", null, null));
        variableList.add(new Variable("fatherId", category, Variable.VariableType.DOUBLE, null, false,
                false, Collections.<String>emptyList(), null, variableList.size(), null, "", null, null));
        variableList.add(new Variable("fatherName", category, Variable.VariableType.STRING, null, false,
                false, Collections.<String>emptyList(), null, variableList.size(), null, "", null, null));
        variableList.add(new Variable("motherId", category, Variable.VariableType.DOUBLE, null, false,
                false, Collections.<String>emptyList(), null, variableList.size(), null, "", null, null));
        variableList.add(new Variable("motherName", category, Variable.VariableType.STRING, null, false,
                false, Collections.<String>emptyList(), null, variableList.size(), null, "", null, null));

        Set<String> allowedSexValues = new HashSet<>();
        HashSet<String> allowedPhenotypeValues = new HashSet<>();
        for (Individual individual : ped.getIndividuals().values()) {
            allowedPhenotypeValues.add(individual.getPhenotype());
            allowedSexValues.add(individual.getSex());
        }
        variableList.add(new Variable("sex", category, Variable.VariableType.CATEGORICAL, null, true,
                false, new LinkedList<>(allowedSexValues), null, variableList.size(), null, "", null, null));
        variableList.add(new Variable("phenotype", category, Variable.VariableType.CATEGORICAL, null, true,
                false, new LinkedList<>(allowedPhenotypeValues), null, variableList.size(), null, "", null, null));


        int categoricalThreshold = (int) (ped.getIndividuals().size() * 0.1);
        for (Map.Entry<String, Integer> entry : ped.getFields().entrySet()) {
            boolean isNumerical = true;
            Set<String> allowedValues = new HashSet<>();
            for (Individual individual : ped.getIndividuals().values()) {
                String s = individual.getFields()[entry.getValue()];
                if (isNumerical) {
                    try {
                        Double.parseDouble(s);
                    } catch (Exception e) {
                        isNumerical = false;
                    }
                }
                allowedValues.add(s);
            }
            Variable.VariableType type;
            if (allowedValues.size() < categoricalThreshold) {
                float meanSize = 0;
                for (String value : allowedValues) {
                    meanSize += value.length();
                }
                meanSize /= allowedValues.size();
                float deviation = 0;
                for (String value : allowedValues) {
                    deviation += (value.length() - meanSize) * (value.length() - meanSize);
                }
                deviation /= allowedValues.size();
                if (deviation < 10) {
                    type = Variable.VariableType.CATEGORICAL;
                } else {
                    if (isNumerical) {
                        type = Variable.VariableType.DOUBLE;
                    } else {
                        type = Variable.VariableType.STRING;
                    }
                }
            } else {
                if (isNumerical) {
                    type = Variable.VariableType.DOUBLE;
                } else {
                    type = Variable.VariableType.STRING;
                }
            }

            if (!type.equals(Variable.VariableType.CATEGORICAL)) {
                allowedValues.clear();
            }

            variableList.add(new Variable(entry.getKey(), category, type, null, false, false, new ArrayList<>(allowedValues),
                    null, variableList.size(), null, "", null, null));
        }

        VariableSet variableSet = new VariableSet("", "", false, false, false, "",
                new HashSet(variableList), Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), 1, null);
        return variableSet;
    }

    protected Pedigree readPedigree(String fileName) {
        PedigreeReader reader = new PedigreePedReader(fileName);

        reader.open();
        reader.pre();


        List<Pedigree> read = reader.read();


        reader.post();
        reader.close();
        return read.get(0);
    }


}
