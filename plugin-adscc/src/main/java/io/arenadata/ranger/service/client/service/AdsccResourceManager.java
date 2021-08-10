package io.arenadata.ranger.service.client.service;

import java.util.List;
import java.util.Map;

public interface AdsccResourceManager {

    Map<String, Object> validateConfig();

    List<String> getServicePaths();

}
