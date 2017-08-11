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

package org.opencb.opencga.catalog.models;

import org.opencb.opencga.core.common.TimeUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 02/05/17.
 */
public class Family extends Annotable {

    private long id;
    private String name;

    private List<Disease> diseases;
    private List<Relatives> members;

    @Deprecated
    private Individual father;
    @Deprecated
    private Individual mother;
    @Deprecated
    private List<Individual> children;

    @Deprecated
    private boolean parentalConsanguinity;

    private String creationDate;
    private Status status;
    private String description;

    private List<OntologyTerm> ontologyTerms;

    private int release;
    private Map<String, Object> attributes;

    public Family() {
    }

    public Family(String name, List<Disease> diseases, List<Relatives> members, String description, int release,
                  Map<String, Object> attributes) {
        this(name, diseases, members, TimeUtils.getTime(), new Status(Status.READY), description, release, attributes);
    }

    public Family(String name, List<Disease> diseases, List<Relatives> members, String creationDate, Status status, String description,
                  int release, Map<String, Object> attributes) {
        this.name = name;
        this.diseases = diseases;
        this.members = members;
        this.creationDate = creationDate;
        this.status = status;
        this.description = description;
        this.release = release;
        this.attributes = attributes;
    }

    @Deprecated
    public Family(String name, Individual father, Individual mother, List<Individual> children, boolean parentalConsanguinity,
                  String description, int release) {
        this(name, father, mother, children, parentalConsanguinity, description, Collections.emptyList(), Collections.emptyList(),
                release, Collections.emptyMap());
    }

    @Deprecated
    public Family(String name, Individual father, Individual mother, List<Individual> children, boolean parentalConsanguinity,
                  String description, List<OntologyTerm> ontologyTerms, List<AnnotationSet> annotationSets, int release,
                  Map<String, Object> attributes) {
        this(-1, name, father, mother, children, parentalConsanguinity, TimeUtils.getTime(), new Status(), description, ontologyTerms,
                annotationSets, release, attributes);
    }

    @Deprecated
    public Family(long id, String name, Individual father, Individual mother, List<Individual> children, boolean parentalConsanguinity,
                  String creationDate, Status status, String description, List<OntologyTerm> ontologyTerms,
                  List<AnnotationSet> annotationSets, int release, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.father = father;
        this.mother = mother;
        this.children = children;
        this.parentalConsanguinity = parentalConsanguinity;
        this.creationDate = creationDate;
        this.status = status;
        this.description = description;
        this.ontologyTerms = ontologyTerms;
        this.release = release;
        this.attributes = attributes;
        this.annotationSets = annotationSets;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Family{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", diseases=").append(diseases);
        sb.append(", members=").append(members);
        sb.append(", father=").append(father);
        sb.append(", mother=").append(mother);
        sb.append(", children=").append(children);
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", status=").append(status);
        sb.append(", description='").append(description).append('\'');
        sb.append(", ontologyTerms=").append(ontologyTerms);
        sb.append(", release=").append(release);
        sb.append(", attributes=").append(attributes);
        sb.append(", annotationSets=").append(annotationSets);
        sb.append('}');
        return sb.toString();
    }


    public long getId() {
        return id;
    }

    public Family setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Family setName(String name) {
        this.name = name;
        return this;
    }

    public List<Disease> getDiseases() {
        return diseases;
    }

    public Family setDiseases(List<Disease> diseases) {
        this.diseases = diseases;
        return this;
    }

    public List<Relatives> getMembers() {
        return members;
    }

    public Family setMembers(List<Relatives> members) {
        this.members = members;
        return this;
    }

    public Individual getFather() {
        return father;
    }

    public Family setFather(Individual father) {
        this.father = father;
        return this;
    }

    public Individual getMother() {
        return mother;
    }

    public Family setMother(Individual mother) {
        this.mother = mother;
        return this;
    }

    public List<Individual> getChildren() {
        return children;
    }

    public Family setChildren(List<Individual> children) {
        this.children = children;
        return this;
    }

    public boolean isParentalConsanguinity() {
        return parentalConsanguinity;
    }

    public Family setParentalConsanguinity(boolean parentalConsanguinity) {
        this.parentalConsanguinity = parentalConsanguinity;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Family setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Family setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Family setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<OntologyTerm> getOntologyTerms() {
        return ontologyTerms;
    }

    public Family setOntologyTerms(List<OntologyTerm> ontologyTerms) {
        this.ontologyTerms = ontologyTerms;
        return this;
    }

    public int getRelease() {
        return release;
    }

    public Family setRelease(int release) {
        this.release = release;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Family setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
