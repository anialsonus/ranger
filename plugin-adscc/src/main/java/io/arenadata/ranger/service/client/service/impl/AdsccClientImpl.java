package io.arenadata.ranger.service.client.service.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import io.arenadata.ranger.service.client.model.AdsccServicesList;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;

import javax.security.auth.Subject;
import javax.ws.rs.core.MediaType;
import java.lang.reflect.Type;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

public class AdsccClientImpl extends BaseClient {

    private static final String ADSCC_API_ENDPOINT = "/api/v1/endpoints";
    private static final Logger LOG = Logger.getLogger(AdsccClientImpl.class);
    private final String adsccUrl;

    public AdsccClientImpl(String serviceName, Map<String, String> configs) {
        super(serviceName, configs, "adscc-client");
        this.adsccUrl = configs.get("adscc.url");
        if (StringUtils.isEmpty(this.adsccUrl)) {
            LOG.error("No value found for configuration 'adscc.url'. Adscc resource lookup will fail.");
        }
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


    public AdsccServicesList getAdsccEndpoints() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get adscc resources list");
        }
        Subject subject = getLoginSubject();
        if (subject == null) {
            return new AdsccServicesList();
        }
        return Subject.doAs(subject, (PrivilegedAction<AdsccServicesList>) () -> {
            String url = adsccUrl + ADSCC_API_ENDPOINT;
            ClientResponse response = getClientResponse(url);
            return getAdsccResourceResponse(response, new TypeToken<AdsccServicesList>() {
            }.getType());
        });
    }

    private boolean getAdsccHealthStatus() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get adscc health status");
        }
        return !getAdsccEndpoints().getServices().isEmpty();
    }

    private ClientResponse getClientResponse(String url) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getClientResponse():calling " + url);
        }

        Client client = Client.create();
        WebResource webResource = client.resource(url);
        ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

        if (response != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getClientResponse():response.getStatus()= " + response.getStatus());
            }
            if (response.getStatus() != HttpStatus.SC_OK) {
                LOG.warn("getClientResponse():response.getStatus()= " + response.getStatus() + " for URL " + url
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
                        + MediaType.APPLICATION_JSON + "], adscc url: " + adsccUrl
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
                    + adsccUrl;
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
