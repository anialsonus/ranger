package org.apache.ranger.services.adscc;

import org.apache.http.impl.client.HttpClientBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class TestMain {
    public static void main(String[] args) {
        RangerAdsccLookupService service = new RangerAdsccLookupService(HttpClientBuilder.create().build());
        HashMap<String, List<String>> resources = new HashMap<>();
        resources.put(AdsccEntityEnum.CLUSTER.name().toLowerCase(), Collections.singletonList("local_cluster"));
        List<String> result = service.lookup(AdsccEntityEnum.CONNECT, resources, "asa", "http://localhost:8888", "admin", "Orion123");
    }
}
