package org.apache.ranger.services.adscc;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RangerAdsccLookupService {
    private final HttpClient httpClient;
    private final JsonParser jsonParser;

    public RangerAdsccLookupService(final HttpClient httpClient) {
        this.httpClient = httpClient;
        this.jsonParser = new JsonParser();
    }

    public List<String> lookup(final AdsccEntityEnum resource,
                               final Map<String, List<String>> resources,
                               final String input,
                               final String host,
                               final String username,
                               final String password) {
        return Optional.ofNullable(host)
                .flatMap(url -> resource.getUrl(resources).map(resUrl -> url + resUrl))
                .map(url -> getResponse(url, username, password, resource.getResponseFunc()))
                .map(result -> result.stream()
                        .filter(value -> value.toLowerCase().contains(input.toLowerCase()))
                        .distinct()
                        .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
    }

    private Set<String> getResponse(final String url,
                                    final String username,
                                    final String password,
                                    final Function<JsonElement, Set<String>> func) {
        try {
            HttpUriRequest request = new HttpGet(url);
            request.addHeader(HttpHeaders.ACCEPT, "application/json");
            Optional.ofNullable(username)
                    .map(value -> "Basic " + Base64.getEncoder().encodeToString((username + ":" + Optional.ofNullable(password).orElse("")).getBytes()))
                    .ifPresent(auth -> request.addHeader(HttpHeaders.AUTHORIZATION, auth));
            return func.apply(jsonParser.parse(IOUtils.toString(httpClient.execute(request).getEntity().getContent(), StandardCharsets.UTF_8)));
        } catch (Exception e) {
            return Collections.emptySet();
        }
    }
}
