package org.apache.ranger.services.adscc;

import com.google.gson.JsonParser;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerServiceAdscc extends RangerBaseService {
    private static final String HOST_CONFIG = "hostname";
    private static final String USERNAME_CONFIG = "username";
    private static final String PASSWORD_CONFIG = "password";

    private final AdsccRestService adsccRestService;
    private final RangerAdsccLookupService rangerAdsccLookupService;

    public RangerServiceAdscc() {
        super();
        adsccRestService = new AdsccRestService(HttpClientBuilder.create().build(), new JsonParser());
        rangerAdsccLookupService = new RangerAdsccLookupService(adsccRestService);
    }

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> result = new HashMap<>();
        try {
            String hostname = configs.get(HOST_CONFIG);
            if (hostname.lastIndexOf("/") == hostname.length() - 1) {
                result.put(HOST_CONFIG, "Incorrect format, remove last character /");
            } else {
                int statusCode = adsccRestService.getStatusCode(configs.get(HOST_CONFIG) + AdsccEntityEnum.CLUSTER.getUrl(new HashMap<>()),
                        configs.get(USERNAME_CONFIG),
                        configs.get(PASSWORD_CONFIG));
                if (statusCode == 400) {
                    result.put(HOST_CONFIG, "Wrong hostname");
                } else if (statusCode == 401) {
                    result.put(HOST_CONFIG, "Wrong username or password");
                }
            }
        } catch (Exception e) {
            result.put(HOST_CONFIG, "Wrong hostname");
        }
        return result;
    }

    @Override
    public List<String> lookupResource(final ResourceLookupContext context) throws Exception {
        try {
            return rangerAdsccLookupService.lookup(AdsccEntityEnum.valueOf(context.getResourceName().toUpperCase()),
                    context.getResources(),
                    context.getUserInput(),
                    configs.get(HOST_CONFIG),
                    configs.get(USERNAME_CONFIG),
                    configs.get(PASSWORD_CONFIG));
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }
}
