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

package org.opencb.opencga.core.models.individual;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import org.opencb.opencga.core.api.ParamConstants;

public class Location {


    @DataField(id = "address", indexed = true, description = FieldConstants.LOCATION_ADDRESS)
    private String address;
    @DataField(id = "postalCode", indexed = true, description = FieldConstants.LOCATION_POSTAL_CODE)
    private String postalCode;
    @DataField(id = "city", indexed = true, description = FieldConstants.LOCATION_CITY)
    private String city;
    @DataField(id = "state", indexed = true, description = FieldConstants.LOCATION_STATE)
    private String state;
    @DataField(id = "country", indexed = true, description = FieldConstants.LOCATION_COUNTRY)
    private String country;


    public Location() {
    }

    public Location(String address, String postalCode, String city, String state, String country) {
        this.address = address;
        this.postalCode = postalCode;
        this.city = city;
        this.state = state;
        this.country = country;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Location{");
        sb.append("address='").append(address).append('\'');
        sb.append(", postalCode='").append(postalCode).append('\'');
        sb.append(", city='").append(city).append('\'');
        sb.append(", state='").append(state).append('\'');
        sb.append(", country='").append(country).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getAddress() {
        return address;
    }

    public Location setAddress(String address) {
        this.address = address;
        return this;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public Location setPostalCode(String postalCode) {
        this.postalCode = postalCode;
        return this;
    }

    public String getCity() {
        return city;
    }

    public Location setCity(String city) {
        this.city = city;
        return this;
    }

    public String getState() {
        return state;
    }

    public Location setState(String state) {
        this.state = state;
        return this;
    }

    public String getCountry() {
        return country;
    }

    public Location setCountry(String country) {
        this.country = country;
        return this;
    }
}
