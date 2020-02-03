package org.opencb.opencga.core.models.individual;

public class Location {
    private String address;
    private String postalCode;
    private String city;
    private String state;
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
