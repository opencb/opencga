package org.opencb.opencga.catalog.core.beans;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 11/09/14.
 */
public class Experiment {

    private int id;
    private String name;
    private String type;
    private String platform;
    private String manufacturer;
    private String date;
    private String lab;
    private String center;
    private String responsible;
    private String description;

    private Map<String, Object> attributes;


    public Experiment() {
    }

    public Experiment(int id, String name, String type, String platform, String manufacturer, String date, String lab, String center, String responsible, String description) {
        this(id, name, type, platform, manufacturer, date, lab, center, responsible, description, new HashMap<String, Object>());
    }

    public Experiment(int id, String name, String type, String platform, String manufacturer, String date, String lab, String center,
                      String responsible, String description, Map<String, Object> attributes) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.platform = platform;
        this.manufacturer = manufacturer;
        this.date = date;
        this.lab = lab;
        this.center = center;
        this.responsible = responsible;
        this.description = description;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Experiment{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", platform='" + platform + '\'' +
                ", manufacturer='" + manufacturer + '\'' +
                ", date='" + date + '\'' +
                ", lab='" + lab + '\'' +
                ", center='" + center + '\'' +
                ", responsible='" + responsible + '\'' +
                ", description='" + description + '\'' +
                ", attributes=" + attributes +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getManufacturer() {
        return manufacturer;
    }

    public void setManufacturer(String manufacturer) {
        this.manufacturer = manufacturer;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getLab() {
        return lab;
    }

    public void setLab(String lab) {
        this.lab = lab;
    }

    public String getCenter() {
        return center;
    }

    public void setCenter(String center) {
        this.center = center;
    }

    public String getResponsible() {
        return responsible;
    }

    public void setResponsible(String responsible) {
        this.responsible = responsible;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

}
