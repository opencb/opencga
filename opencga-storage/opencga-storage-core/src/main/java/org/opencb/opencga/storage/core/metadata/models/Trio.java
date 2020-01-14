package org.opencb.opencga.storage.core.metadata.models;

import java.util.ArrayList;
import java.util.List;

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

    public List<String> toList() {
        ArrayList<String> list = new ArrayList<>(3);
        list.add(getChild());
        if (getFather() != null) {
            list.add(getFather());
        }
        if (getMother() != null) {
            list.add(getMother());
        }
        return list;
    }
}
