package org.opencb.opencga.core.models.variant.fastqc;

public class PerBaseSeqContent {
    // #Base	G	A	T	C

    private String base;
    private double g;
    private double a;
    private double t;
    private double c;

    public PerBaseSeqContent() {
    }

    public PerBaseSeqContent(String base, double g, double a, double t, double c) {
        this.base = base;
        this.g = g;
        this.a = a;
        this.t = t;
        this.c = c;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PerBaseSeqContent{");
        sb.append("base='").append(base).append('\'');
        sb.append(", g=").append(g);
        sb.append(", a=").append(a);
        sb.append(", t=").append(t);
        sb.append(", c=").append(c);
        sb.append('}');
        return sb.toString();
    }

    public String getBase() {
        return base;
    }

    public PerBaseSeqContent setBase(String base) {
        this.base = base;
        return this;
    }

    public double getG() {
        return g;
    }

    public PerBaseSeqContent setG(double g) {
        this.g = g;
        return this;
    }

    public double getA() {
        return a;
    }

    public PerBaseSeqContent setA(double a) {
        this.a = a;
        return this;
    }

    public double getT() {
        return t;
    }

    public PerBaseSeqContent setT(double t) {
        this.t = t;
        return this;
    }

    public double getC() {
        return c;
    }

    public PerBaseSeqContent setC(double c) {
        this.c = c;
        return this;
    }
}
