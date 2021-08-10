package io.arenadata.ranger.service.client.service;

import java.util.Map;

public interface AdsccResourceManagerDispatcher {

    AdsccResourceManager getResourceManager(String serviceName, Map<String, String> configs);
}
