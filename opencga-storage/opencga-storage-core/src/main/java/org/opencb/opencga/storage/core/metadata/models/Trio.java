package org.opencb.opencga.storage.core.metadata.models;

public class Trio {
    private final String id;
    private final String father;
    private final String mother;
    private final String child;

    public Trio(String id, String father, String mother, String child) {
        this.id = id;
        this.father = father;
        this.mother = mother;
        this.child = child;
    }

    public String getId() {
        return id;
    }

    public String getFather() {
        return father;
    }

    public String getMother() {
        return mother;
    }

    public String getChild() {
        return child;
    }
}
