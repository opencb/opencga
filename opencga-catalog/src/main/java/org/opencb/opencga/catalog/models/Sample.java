/*
 * Copyright 2015 OpenCB
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

import java.util.*;

/**
 * Created by jacobo on 11/09/14.
 */
public class Sample {

    private int id;
    private String name;
    private String source;
    private int individualId;
    private String description;

    private List<Acl> acl;
    private List<AnnotationSet> annotationSets;

    private Map<String, Object> attributes;

    public Sample() {
    }

    public Sample(int id, String name, String source, int individualId, String description) {
        this(id, name, source, individualId, description, Collections.<Acl>emptyList(), new LinkedList<AnnotationSet>(), new HashMap<String, Object>());
    }

    public Sample(int id, String name, String source, int individualId, String description,
                  List<Acl> acl, List<AnnotationSet> annotationSets, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.source = source;
        this.individualId = individualId;
        this.description = description;
        this.acl = acl;
        this.annotationSets = annotationSets;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Sample{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", source='" + source + '\'' +
                ", individualId=" + individualId +
                ", description='" + description + '\'' +
                ", acl=" + acl +
                ", annotationSets=" + annotationSets +
                ", attributes=" + attributes +
                '}';
    }

    public int getId() {
        return id;
    }

    public Sample setId(int id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Sample setName(String name) {
        this.name = name;
        return this;
    }

    public String getSource() {
        return source;
    }

    public Sample setSource(String source) {
        this.source = source;
        return this;
    }

    public int getIndividualId() {
        return individualId;
    }

    public Sample setIndividualId(int individualId) {
        this.individualId = individualId;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Sample setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<Acl> getAcl() {
        return acl;
    }

    public Sample setAcl(List<Acl> acl) {
        this.acl = acl;
        return this;
    }

    public List<AnnotationSet> getAnnotationSets() {
        return annotationSets;
    }

    public Sample setAnnotationSets(List<AnnotationSet> annotationSets) {
        this.annotationSets = annotationSets;
        return this;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public Sample setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
        return this;
    }
}
