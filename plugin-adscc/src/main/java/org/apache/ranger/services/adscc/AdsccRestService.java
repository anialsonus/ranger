package org.apache.ranger.services.adscc;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

public class AdsccRestService {
    private static final Logger logger = Logger.getLogger(AdsccRestService.class);
    private final HttpClient httpClient;
    private final JsonParser jsonParser;

    public AdsccRestService(final HttpClient httpClient, final JsonParser jsonParser) {
        this.httpClient = httpClient;
        this.jsonParser = jsonParser;
    }

    public Optional<JsonElement> execute(final String url,
                                         final String username,
                                         final String password) {
        try {
            return Optional.ofNullable(jsonParser.parse(IOUtils.toString(getResponse(url, username, password).getEntity().getContent(), StandardCharsets.UTF_8)));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return Optional.empty();
        }
    }

    public int getStatusCode(final String url,
                         final String username,
                         final String password) {
        try {
            return getResponse(url, username, password).getStatusLine().getStatusCode();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return 404;
        }
    }


    private HttpResponse getResponse(final String url,
                                     final String username,
                                     final String password) throws IOException {
        HttpUriRequest request = new HttpGet(url);
        request.addHeader(HttpHeaders.ACCEPT, "application/json");
        Optional.ofNullable(username)
                .map(value -> "Basic " + Base64.getEncoder().encodeToString((username + ":" + Optional.ofNullable(password).orElse("")).getBytes()))
                .ifPresent(auth -> request.addHeader(HttpHeaders.AUTHORIZATION, auth));
        return httpClient.execute(request);
    }
}
