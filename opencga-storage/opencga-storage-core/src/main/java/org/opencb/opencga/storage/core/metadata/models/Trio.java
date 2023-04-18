package org.opencb.opencga.storage.core.metadata.models;

import org.apache.logging.log4j.util.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Trio {
    private final String id;
    private final String father;
    private final String mother;
    private final String child;

    public Trio(List<String> trio) {
        this(null, trio);
    }

    public Trio(String id, List<String> trio) {
        this.id = id;
        this.father = trio.get(1);
        this.mother = trio.get(2);
        this.child = trio.get(0);
    }

    public Trio(String father, String mother, String child) {
        this(null, father, mother, child);
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Trio trio = (Trio) o;
        return Objects.equals(id, trio.id)
                && Objects.equals(father, trio.father)
                && Objects.equals(mother, trio.mother)
                && Objects.equals(child, trio.child);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, father, mother, child);
    }

    @Override
    public String toString() {
        return Strings.join(toList(), ',');
    }
}
