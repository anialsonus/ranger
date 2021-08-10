package io.arenadata.ranger.service.client.service.impl;

import io.arenadata.ranger.service.client.service.AdsccResourceManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

public class AdsccResourceManagerImpl implements AdsccResourceManager {

    private static final Logger LOG = Logger.getLogger(AdsccResourceManagerImpl.class);
    private final String serviceName;
    private final Map<String, String> configs;
    private final AdsccClientImpl adsccClientImpl;

    public AdsccResourceManagerImpl(String serviceName, Map<String, String> configs) {
        this.serviceName = serviceName;
        this.configs = configs;
        this.adsccClientImpl = new AdsccClientImpl(serviceName, configs);
    }

    @Override
    public Map<String, Object> validateConfig() {
        Map<String, Object> ret;
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdsccResourceMgr.validateConfig() serviceName: " + serviceName + ", configs: "
                    + configs);
        }
        try {
            ret = adsccClientImpl.connectionTest();
        } catch (Exception e) {
            LOG.error("<== AdsccResourceMgr.validateConfig() error: " + e);
            throw e;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdsccResourceMgr.validateConfig() result: " + ret);
        }
        return ret;
    }

    @Override
    public List<String> getServicePaths() {
        List<String> ret;
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AdsccResourceMgr.getServicePaths() serviceName: " + serviceName + ", configs: "
                    + configs);
        }
        try {
            ret = adsccClientImpl.getAdsccEndpoints().getServices();
        } catch (Exception e) {
            LOG.error("<== AdsccResourceMgr.getServicePaths() error: " + e);
            throw e;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AdsccResourceMgr.getServicePaths() result: " + ret);
        }
        return ret;
    }
}
