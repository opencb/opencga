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

package org.opencb.opencga.core.models;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class Relatives {

    private Individual member;
    private Individual father;
    private Individual mother;
    private List<String> diseases;
    private List<String> carrier;
    private boolean parentalConsanguinity;
    private Multiples multiples;

    public Relatives() {
    }

    public Relatives(Individual member, Individual father, Individual mother) {
        this(member, father, mother, null, null, null, false);
    }

    public Relatives(Individual member, Individual father, Individual mother, List<String> diseases, List<String> carrier,
                     Multiples multiples, boolean parentalConsanguinity) {
        this.member = member;
        this.father = father;
        this.mother = mother;
        this.diseases = defaultObject(diseases, Collections::emptyList);
        this.carrier = defaultObject(carrier, Collections::emptyList);
        this.multiples = multiples;
        this.parentalConsanguinity = parentalConsanguinity;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Relatives{");
        sb.append("member=").append(member);
        sb.append(", father=").append(father);
        sb.append(", mother=").append(mother);
        sb.append(", diseases=").append(diseases);
        sb.append(", carrier=").append(carrier);
        sb.append(", parentalConsanguinity=").append(parentalConsanguinity);
        sb.append(", multiples=").append(multiples);
        sb.append('}');
        return sb.toString();
    }


    public Individual getMember() {
        return member;
    }

    public Relatives setMember(Individual member) {
        this.member = member;
        return this;
    }

    public Individual getFather() {
        return father;
    }

    public Relatives setFather(Individual father) {
        this.father = father;
        return this;
    }

    public Individual getMother() {
        return mother;
    }

    public Relatives setMother(Individual mother) {
        this.mother = mother;
        return this;
    }

    public List<String> getDiseases() {
        return diseases;
    }

    public Relatives setDiseases(List<String> diseases) {
        this.diseases = diseases;
        return this;
    }

    public List<String> getCarrier() {
        return carrier;
    }

    public Relatives setCarrier(List<String> carrier) {
        this.carrier = carrier;
        return this;
    }

    public Multiples getMultiples() {
        return multiples;
    }

    public Relatives setMultiples(Multiples multiples) {
        this.multiples = multiples;
        return this;
    }

    public boolean isParentalConsanguinity() {
        return parentalConsanguinity;
    }

    public Relatives setParentalConsanguinity(boolean parentalConsanguinity) {
        this.parentalConsanguinity = parentalConsanguinity;
        return this;
    }

    public static <O> O defaultObject(O object, O defaultObject) {
        if (object == null) {
            object = defaultObject;
        }
        return object;
    }

    public static <O> O defaultObject(O object, Supplier<O> supplier) {
        if (object == null) {
            object = supplier.get();
        }
        return object;
    }

}
