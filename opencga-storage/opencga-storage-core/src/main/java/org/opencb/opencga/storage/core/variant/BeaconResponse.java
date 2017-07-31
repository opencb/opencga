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

package org.opencb.opencga.storage.core.variant;

import org.opencb.commons.datastore.core.ObjectMap;

/**
 * Created by pfurio on 30/03/17.
 */
public class BeaconResponse {

    private Beacon beacon;
    private Query query;
    private boolean response;
    private ObjectMap info;

    public BeaconResponse() {
    }

    public BeaconResponse(Beacon beacon, Query query, boolean response, ObjectMap info) {
        this.beacon = beacon;
        this.query = query;
        this.response = response;
        this.info = info;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BeaconResponse{");
        sb.append("beacon=").append(beacon);
        sb.append(", query=").append(query);
        sb.append(", response=").append(response);
        sb.append(", info=").append(info);
        sb.append('}');
        return sb.toString();
    }

    public Beacon getBeacon() {
        return beacon;
    }

    public BeaconResponse setBeacon(Beacon beacon) {
        this.beacon = beacon;
        return this;
    }

    public Query getQuery() {
        return query;
    }

    public BeaconResponse setQuery(Query query) {
        this.query = query;
        return this;
    }

    public boolean isResponse() {
        return response;
    }

    public BeaconResponse setResponse(boolean response) {
        this.response = response;
        return this;
    }

    public ObjectMap getInfo() {
        return info;
    }

    public BeaconResponse setInfo(ObjectMap info) {
        this.info = info;
        return this;
    }

    public static class Beacon {
        private String id;
        private String name;
        private String organization;
        private String description;

        public Beacon() {
        }

        public Beacon(String id, String name, String organization, String description) {
            this.id = id;
            this.name = name;
            this.organization = organization;
            this.description = description;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Beacon{");
            sb.append("id='").append(id).append('\'');
            sb.append(", name='").append(name).append('\'');
            sb.append(", organization='").append(organization).append('\'');
            sb.append(", description='").append(description).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getId() {
            return id;
        }

        public Beacon setId(String id) {
            this.id = id;
            return this;
        }

        public String getName() {
            return name;
        }

        public Beacon setName(String name) {
            this.name = name;
            return this;
        }

        public String getOrganization() {
            return organization;
        }

        public Beacon setOrganization(String organization) {
            this.organization = organization;
            return this;
        }

        public String getDescription() {
            return description;
        }

        public Beacon setDescription(String description) {
            this.description = description;
            return this;
        }
    }

    public static class Query {
        private String chromosome;
        private int position;
        private String allele;
        private String reference;

        public Query() {
        }

        public Query(String chromosome, int position, String allele, String reference) {
            this.chromosome = chromosome;
            this.position = position;
            this.allele = allele;
            this.reference = reference;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Query{");
            sb.append("chromosome='").append(chromosome).append('\'');
            sb.append(", position=").append(position);
            sb.append(", allele='").append(allele).append('\'');
            sb.append(", reference='").append(reference).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public String getChromosome() {
            return chromosome;
        }

        public Query setChromosome(String chromosome) {
            this.chromosome = chromosome;
            return this;
        }

        public int getPosition() {
            return position;
        }

        public Query setPosition(int position) {
            this.position = position;
            return this;
        }

        public String getAllele() {
            return allele;
        }

        public Query setAllele(String allele) {
            this.allele = allele;
            return this;
        }

        public String getReference() {
            return reference;
        }

        public Query setReference(String reference) {
            this.reference = reference;
            return this;
        }
    }

}
