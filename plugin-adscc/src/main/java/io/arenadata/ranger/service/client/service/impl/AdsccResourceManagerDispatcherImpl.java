package io.arenadata.ranger.service.client.service.impl;

import io.arenadata.ranger.service.client.model.AdsccClientConfiguration;
import io.arenadata.ranger.service.client.service.AdsccResourceManager;
import io.arenadata.ranger.service.client.service.AdsccResourceManagerDispatcher;
import org.apache.log4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AdsccResourceManagerDispatcherImpl implements AdsccResourceManagerDispatcher {

    public static final String ADSCC_URL_PATH = "adscc.url";
    public static final String ADSCC_SSL_TRUSTSTORE = "adscc.ssl.truststore";
    public static final String ADSCC_SSL_TRUSTSTORE_TYPE = "adscc.ssl.truststoreType";
    public static final String ADSCC_SSL_TRUSTSTORE_PASSWORD = "adscc.ssl.truststorePassword";
    public static final String INVALID_URL_MSG = "ADSCC URL must be a valid URL of the form " +
            "http(s)://<hostname>(:<port>)/api/v1/endpoints";
    private static final Logger LOG = Logger.getLogger(AdsccResourceManagerDispatcherImpl.class);
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
            String url = configs.get(ADSCC_URL_PATH);
            validateNotBlank(url, "ADSCC URL is required for " + serviceName);
            URI validatedUrl = validateUrl(url);
            SSLContext sslContext = null;
            if (validatedUrl.getScheme().equals("https")) {
                String trustStore = configs.get(ADSCC_SSL_TRUSTSTORE);
                String trustStoreType = configs.get(ADSCC_SSL_TRUSTSTORE_TYPE);
                String trustStorePassword = configs.get(ADSCC_SSL_TRUSTSTORE_PASSWORD);

                validateNotBlank(trustStore, "Keystore is required for " + serviceName + " with Authentication Type of SSL");
                validateNotBlank(trustStoreType, "Keystore Type is required for " + serviceName + " with Authentication Type of SSL");
                validateNotBlank(trustStorePassword, "Keystore Password is required for " + serviceName + " with Authentication Type of SSL");

                LOG.debug("Creating SSLContext for ADSCC connection");
                try {
                    sslContext = createSslContext(trustStore, trustStorePassword, trustStoreType);
                } catch (Exception e) {
                    LOG.error(e);
                }
            }
            AdsccClientConfiguration configuration = new AdsccClientConfiguration(validatedUrl, sslContext);
            AdsccResourceManager manager = new AdsccResourceManagerImpl(serviceName, configs, configuration);
            resourceManagerMap.putIfAbsent(serviceName, manager);
            return manager;
        } else {
            return resourceManagerMap.get(serviceName);
        }
    }

    private URI validateUrl(String url) {
        URI adsccUri;
        try {
            adsccUri = new URI(url);
            if (!adsccUri.getPath().endsWith("/api/v1/endpoints")) {
                LOG.error(INVALID_URL_MSG);
                throw new IllegalArgumentException(INVALID_URL_MSG);
            }
        } catch (URISyntaxException e) {
            LOG.error(INVALID_URL_MSG);
            throw new IllegalArgumentException(INVALID_URL_MSG);
        }
        return adsccUri;
    }

    private void validateNotBlank(String input, String message) {
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }

    private SSLContext createSslContext(String trustStore, String trustStorePassword, String trustStoreType)
            throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, KeyManagementException {
        KeyStore trustStoreInstance = KeyStore.getInstance(trustStoreType);
        try (InputStream trustStoreStream = new FileInputStream(trustStore)) {
            trustStoreInstance.load(trustStoreStream, trustStorePassword.toCharArray());
        }
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStoreInstance);
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), new SecureRandom());
        return sslContext;
    }
}
