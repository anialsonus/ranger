package io.arenadata.ranger.service.client.model;

import java.util.ArrayList;
import java.util.List;

public class AdsccServicesList {
    private List<String> services;

    public AdsccServicesList() {
        this.services = new ArrayList<>();
    }

    public AdsccServicesList(List<String> services) {
        this.services = services;
    }

    public List<String> getServices() {
        return services;
    }

    public void setServices(List<String> services) {
        this.services = services;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AdsccServicesList that = (AdsccServicesList) o;

        return services.equals(that.services);
    }

    @Override
    public int hashCode() {
        return services.hashCode();
    }

    @Override
    public String toString() {
        return "AdsccServicesList{" +
                "services=" + services +
                '}';
    }
}
