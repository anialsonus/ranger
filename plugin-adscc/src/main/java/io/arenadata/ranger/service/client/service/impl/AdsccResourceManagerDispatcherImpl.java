package io.arenadata.ranger.service.client.service.impl;

import io.arenadata.ranger.service.client.service.AdsccResourceManager;
import io.arenadata.ranger.service.client.service.AdsccResourceManagerDispatcher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AdsccResourceManagerDispatcherImpl implements AdsccResourceManagerDispatcher {

    private final ConcurrentHashMap<String, AdsccResourceManager> resourceManagerMap;

    public AdsccResourceManagerDispatcherImpl() {
        resourceManagerMap = new ConcurrentHashMap<>();
    }

    public AdsccResourceManagerDispatcherImpl(ConcurrentHashMap<String, AdsccResourceManager> resourceMgrMap) {
        this.resourceManagerMap = resourceMgrMap;
    }

    @Override
    public AdsccResourceManager getResourceManager(String serviceName, Map<String, String> configs) {
        if (resourceManagerMap.get(serviceName) == null) {
            AdsccResourceManager manager = new AdsccResourceManagerImpl(serviceName, configs);
            resourceManagerMap.putIfAbsent(serviceName, manager);
            return manager;
        } else {
            return resourceManagerMap.get(serviceName);
        }
    }

}
