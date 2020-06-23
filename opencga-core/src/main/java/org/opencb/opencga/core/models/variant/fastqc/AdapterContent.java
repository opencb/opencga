package org.opencb.opencga.core.models.variant.fastqc;

public class AdapterContent {
// #Position	Illumina Universal Adapter	Illumina Small RNA 3' Adapter	Illumina Small RNA 5' Adapter	Nextera Transposase Sequence	SOLID Small RNA Adapter

    private String position;
    private double illumina;
    private double illumina3;
    private double illumina5;
    private double nextera;
    private double solid;

    public AdapterContent() {
    }

    public AdapterContent(String position, double illumina, double illumina3, double illumina5, double nextera, double solid) {
        this.position = position;
        this.illumina = illumina;
        this.illumina3 = illumina3;
        this.illumina5 = illumina5;
        this.nextera = nextera;
        this.solid = solid;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AdapterContent{");
        sb.append("position='").append(position).append('\'');
        sb.append(", illumina=").append(illumina);
        sb.append(", illumina3=").append(illumina3);
        sb.append(", illumina5=").append(illumina5);
        sb.append(", nextera=").append(nextera);
        sb.append(", solid=").append(solid);
        sb.append('}');
        return sb.toString();
    }

    public String getPosition() {
        return position;
    }

    public AdapterContent setPosition(String position) {
        this.position = position;
        return this;
    }

    public double getIllumina() {
        return illumina;
    }

    public AdapterContent setIllumina(double illumina) {
        this.illumina = illumina;
        return this;
    }

    public double getIllumina3() {
        return illumina3;
    }

    public AdapterContent setIllumina3(double illumina3) {
        this.illumina3 = illumina3;
        return this;
    }

    public double getIllumina5() {
        return illumina5;
    }

    public AdapterContent setIllumina5(double illumina5) {
        this.illumina5 = illumina5;
        return this;
    }

    public double getNextera() {
        return nextera;
    }

    public AdapterContent setNextera(double nextera) {
        this.nextera = nextera;
        return this;
    }

    public double getSolid() {
        return solid;
    }

    public AdapterContent setSolid(double solid) {
        this.solid = solid;
        return this;
    }
}
