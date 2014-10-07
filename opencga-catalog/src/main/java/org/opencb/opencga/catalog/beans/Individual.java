package org.opencb.opencga.catalog.beans;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class Individual {

    private String name;
    private String gender;
    private String type;

    private String taxonomyCode;
    private String scientificName;
    private String commonName;

    private Map<String, Object> attributes;

    public Individual() {
    }

    public Individual(String name, String gender, String type, String taxonomyCode, String scientificName, String commonName) {
        this(name, gender, type, taxonomyCode, scientificName, commonName, new HashMap<String, Object>());
    }

    public Individual(String name, String gender, String type, String taxonomyCode, String scientificName,
                      String commonName, Map<String, Object> attributes) {
        this.name = name;
        this.gender = gender;
        this.type = type;
        this.taxonomyCode = taxonomyCode;
        this.scientificName = scientificName;
        this.commonName = commonName;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Individual{" +
                "name='" + name + '\'' +
                ", gender='" + gender + '\'' +
                ", type='" + type + '\'' +
                ", taxonomyCode='" + taxonomyCode + '\'' +
                ", scientificName='" + scientificName + '\'' +
                ", commonName='" + commonName + '\'' +
                ", annotations=" + attributes +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTaxonomyCode() {
        return taxonomyCode;
    }

    public void setTaxonomyCode(String taxonomyCode) {
        this.taxonomyCode = taxonomyCode;
    }

    public String getScientificName() {
        return scientificName;
    }

    public void setScientificName(String scientificName) {
        this.scientificName = scientificName;
    }

    public String getCommonName() {
        return commonName;
    }

    public void setCommonName(String commonName) {
        this.commonName = commonName;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }
}
