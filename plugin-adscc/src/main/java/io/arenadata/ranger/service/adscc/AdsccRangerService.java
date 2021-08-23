package io.arenadata.ranger.service.adscc;

import io.arenadata.ranger.service.client.service.AdsccResourceManager;
import io.arenadata.ranger.service.client.service.AdsccResourceManagerDispatcher;
import io.arenadata.ranger.service.client.service.impl.AdsccResourceManagerDispatcherImpl;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AdsccRangerService extends RangerBaseService {

    private static final Logger LOG = Logger.getLogger(AdsccRangerService.class);
    private final AdsccResourceManagerDispatcher dispatcher;

    public AdsccRangerService() {
        super();
        dispatcher = new AdsccResourceManagerDispatcherImpl();
    }

    @Override
    public Map<String, Object> validateConfig() {
        Map<String, Object> ret = new HashMap<String, Object>();

        String serviceName = getServiceName();
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceAdscc.validateConfig() service: " + serviceName);
        }
        if (configs != null) {
            try {
                AdsccResourceManager manager = dispatcher.getResourceManager(serviceName, configs);
                ret = manager.validateConfig();
            } catch (Exception e) {
                LOG.error("<== RangerServiceAdscc.validateConfig() error: " + e);
                throw e;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceAdscc.validateConfig() result: " + ret);
        }

        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) {
        List<String> ret;
        String serviceName = getServiceName();
        String userInput = context.getUserInput();
        List<String> existsResources = context.getResources().get("path");
        try {
            AdsccResourceManager manager = dispatcher.getResourceManager(serviceName, configs);
            ret = manager.getServicePaths().stream()
                    .filter(resource -> resource.startsWith(userInput))
                    .filter(resource -> !existsResources.contains(resource))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error("<== RangerServiceAdscc.lookupResource() error: " + e);
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceAdscc.lookupResource() result: " + ret);
        }

        return ret;
    }
}