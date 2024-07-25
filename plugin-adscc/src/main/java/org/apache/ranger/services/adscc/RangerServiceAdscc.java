package org.apache.ranger.services.adscc;

import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;

import java.util.*;

public class RangerServiceAdscc extends RangerBaseService {
        private static final String HOST_CONFIG = "hostname";
    private static final String USERNAME_CONFIG = "username";
    private static final String PASSWORD_CONFIG = "password";

    private final RangerAdsccLookupService rangerAdsccLookupService;

    public RangerServiceAdscc() {
        super();
        rangerAdsccLookupService = new RangerAdsccLookupService(HttpClientBuilder.create().build());
    }

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        return new HashMap<>();
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
