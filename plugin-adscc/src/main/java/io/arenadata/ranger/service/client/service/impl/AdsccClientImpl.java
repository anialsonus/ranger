package io.arenadata.ranger.service.client.service.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import io.arenadata.ranger.service.client.model.AdsccClientConfiguration;
import io.arenadata.ranger.service.client.model.AdsccServicesList;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.util.PasswordUtils;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.security.auth.Subject;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.lang.reflect.Type;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdsccClientImpl extends BaseClient {

    private static final Logger LOG = Logger.getLogger(AdsccClientImpl.class);
    private final AdsccClientConfiguration configuration;

    public AdsccClientImpl(String serviceName, Map<String, String> configs, AdsccClientConfiguration configuration) {
        super(serviceName, configs, "adscc-client");
        this.configuration = configuration;
    }

    public Map<String, Object> connectionTest() {
        boolean isAdsccUp = getAdsccHealthStatus();
        Map<String, Object> responseData = new HashMap<String, Object>();
        if (isAdsccUp) {
            String successMsg = "ConnectionTest Successful.";
            BaseClient.generateResponseDataMap(true, successMsg, successMsg, null, null, responseData);
        } else {
            String failureMsg = "Unable to retrieve any adscc status using given parameters.";
            BaseClient.generateResponseDataMap(false, failureMsg, failureMsg + DEFAULT_ERROR_MESSAGE, null,
                    null, responseData);
        }
        return responseData;
    }


    public AdsccServicesList getAdsccEndpoints(boolean decryptPassword) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get adscc resources list");
        }
        Subject subject = getLoginSubject();
        if (subject == null) {
            return new AdsccServicesList();
        }
        return Subject.doAs(subject, (PrivilegedAction<AdsccServicesList>) () -> {
            ClientResponse response = getClientResponse(configuration, decryptPassword);
            return getAdsccResourceResponse(response, new TypeToken<AdsccServicesList>() {
            }.getType());
        });
    }

    private boolean getAdsccHealthStatus() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get adscc health status");
        }
        try {
            List<String> endPoints = getAdsccEndpoints(false).getServices();
            return !endPoints.isEmpty();
        } catch (Exception e) {
            LOG.error("Cannot get adscc health status", e);
            return false;
        }
    }

    private ClientResponse getClientResponse(AdsccClientConfiguration configuration, boolean decryptPassword) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getClientResponse():calling " + configuration.getAdsccUri());
        }
        ClientConfig config = new DefaultClientConfig();
        if (configuration.getSslContext() != null) {
            HostnameVerifier hostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();
            config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, new HTTPSProperties(hostnameVerifier, configuration.getSslContext()));
        }
        Client client = Client.create(config);
        try {
            client.addFilter(
                    new HTTPBasicAuthFilter(connectionProperties.get("username"),
                            decryptPassword ? PasswordUtils.decryptPassword(connectionProperties.get("password")) : connectionProperties.get("password")));
        } catch (IOException e) {
            LOG.error("Cannot decode password", e);
        }
        WebResource webResource = client.resource(configuration.getAdsccUri());
        ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

        if (response != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getClientResponse():response.getStatus()= " + response.getStatus());
            }
            if (response.getStatus() != HttpStatus.SC_OK) {
                LOG.warn("getClientResponse():response.getStatus()= " + response.getStatus() + " for URL " + configuration.getAdsccUri()
                        + ", failed to get adscc status, response= " + response.getEntity(String.class));
            }
        }

        return response;
    }

    private <T> T getAdsccResourceResponse(ClientResponse response, Type type) {
        T resource = null;
        try {
            if (response != null && response.getStatus() == HttpStatus.SC_OK) {
                String jsonString = response.getEntity(String.class);
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                resource = gson.fromJson(jsonString, type);
            } else {
                String msgDesc = "Unable to get a valid response for " + "expected mime type : ["
                        + MediaType.APPLICATION_JSON + "], adscc url: " + configuration.getAdsccUri()
                        + " - got null response.";
                LOG.error(msgDesc);
                HadoopException hdpException = new HadoopException(msgDesc);
                hdpException.generateResponseDataMap(false, msgDesc, msgDesc + DEFAULT_ERROR_MESSAGE, null, null);
                throw hdpException;
            }
        } catch (HadoopException he) {
            throw he;
        } catch (Exception e) {
            String msgDesc = "Exception while getting adscc resource response, adscc url: "
                    + configuration.getAdsccUri();
            HadoopException hdpException = new HadoopException(msgDesc, e);

            LOG.error(msgDesc, e);

            hdpException.generateResponseDataMap(false, BaseClient.getMessage(e), msgDesc + DEFAULT_ERROR_MESSAGE, null,
                    null);
            throw hdpException;
        } finally {
            if (response != null) {
                response.close();
            }
        }
        return resource;
    }
}
