package org.opencb.opencga.core.models.clinical;

public class ClinicalResponsible {

    private String id;
    private String name;
    private String email;
    private String organization;
    private String department;
    private String address;
    private String city;
    private String postcode;

    public ClinicalResponsible() {
    }

    public ClinicalResponsible(String id, String name, String email, String organization, String department, String address, String city,
                               String postcode) {
        this.id = id;
        this.name = name;
        this.email = email;
        this.organization = organization;
        this.department = department;
        this.address = address;
        this.city = city;
        this.postcode = postcode;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalResponsible{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", organization='").append(organization).append('\'');
        sb.append(", department='").append(department).append('\'');
        sb.append(", address='").append(address).append('\'');
        sb.append(", city='").append(city).append('\'');
        sb.append(", postcode='").append(postcode).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalResponsible setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ClinicalResponsible setName(String name) {
        this.name = name;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public ClinicalResponsible setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getOrganization() {
        return organization;
    }

    public ClinicalResponsible setOrganization(String organization) {
        this.organization = organization;
        return this;
    }

    public String getDepartment() {
        return department;
    }

    public ClinicalResponsible setDepartment(String department) {
        this.department = department;
        return this;
    }

    public String getAddress() {
        return address;
    }

    public ClinicalResponsible setAddress(String address) {
        this.address = address;
        return this;
    }

    public String getCity() {
        return city;
    }

    public ClinicalResponsible setCity(String city) {
        this.city = city;
        return this;
    }

    public String getPostcode() {
        return postcode;
    }

    public ClinicalResponsible setPostcode(String postcode) {
        this.postcode = postcode;
        return this;
    }
}
