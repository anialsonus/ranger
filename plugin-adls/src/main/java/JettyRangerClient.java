/*
 *
 * JettyRangerClient
 *
 */

import model.RangerRoles;
import model.ServicePolicies;
import util.ADLSRangerServiceNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

public class JettyRangerClient {
    private static final Logger LOG = LoggerFactory.getLogger(JettyRangerClient.class);
    private static Client client;
    // private static final String URLService = "http://10.92.3.29:6080/service/plugins/policies/download/Logsearch";
    private static final String URLService = "http://10.92.3.119:6080/service/plugins/policies/download/Logsearch";
    // private static final String URLRoles = "http://10.92.3.29:6080/service/roles/download/Logsearch";
    private static final String URLRoles = "http://10.92.3.119:6080/service/roles/download/Logsearch";
    private static final String serviceName = "Logsearch";

    public static void main(String[] args) {
        LOG.info("==> [start] JettyRangerClient");

        try {
            ServicePolicies servicePolicies = getServicePoliciesIfUpdated(0, 0);
            LOG.info("servicePolicies.Users : " + servicePolicies.getPolicies().get(0).getPolicyItems().get(0).getUsers());

            // for PolicyRefresher::loadRoles() from Ranger 2.2.0
            //
            RangerRoles rangerRoles = getRolesIfUpdated(0, 0);
            LOG.info("rangerRoles : " + rangerRoles);

        } catch (Exception e) {
            LOG.error("Main: ", e);
        }

        LOG.info("<== [stop] JettyRangerClient");
    }

    static Response get(String relativeUrl, Map<String, String> parametrs) throws Exception {
        Response response = null;

        if (client == null) client = ClientBuilder.newClient();

        WebTarget target = client.target(relativeUrl);

        for (String paramKey : parametrs.keySet()) {
            LOG.debug("GET : " + paramKey + " -> " + parametrs.get(paramKey));
            target = target.queryParam(paramKey, parametrs.get(paramKey));
        }

        response = target.request(MediaType.APPLICATION_JSON_TYPE).get();

        LOG.debug("GET : Response : " + response);

        return response;
    }

    public static ServicePolicies getServicePoliciesIfUpdated(final long lastKnownVersion, final long lastActivationTimeInMillis) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerAdminRESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + ", " + lastActivationTimeInMillis + ")");
        }

        ServicePolicies servicePolicies = null;

        // final UserGroupInformation user = new UserGroupInformation(); // MiscUtil.getUGILoginUser();
        final boolean isSecureMode = false; // user != null && UserGroupInformation.isSecurityEnabled();
        final Response response;

        Map<String, String> queryParams = new HashMap<String, String>();
        // queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION, Long.toString(lastKnownVersion));
        queryParams.put("lastKnownRoleVersion", "-1");
        // queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
        queryParams.put("lastActivationTime", "0");
        // queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
        queryParams.put("pluginId", "elasticsearch@elk-ipro-01.ru-central1.internal-Logsearch");
        // queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, clusterName);
        //
        // queryParams.put(RangerRESTUtils.REST_PARAM_SUPPORTS_POLICY_DELTAS, Boolean.toString(supportsPolicyDeltas));
        //
        // queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);
        queryParams.put("pluginCapabilities", "1ffff");

        if (isSecureMode) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[SecureMode] Checking Service policy if updated as user : " + "user"); // user);
            }
            /*PrivilegedAction<ClientResponse> action = new PrivilegedAction<ClientResponse>() {
                public ClientResponse run() {
                    ClientResponse clientRes = null;
                    String relativeURL = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + serviceNameUrlParam;
                    try {
                        clientRes =  restClient.get(relativeURL, queryParams);
                    } catch (Exception e) {
                        LOG.error("Failed to get response, Error is : "+e.getMessage());
                    }
                    return clientRes;
            }; }*/
            // response = user.doAs(action);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[NOT SecureMode] Checking Service policy if updated with old api call");
            }
            // String relativeURL = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED + serviceNameUrlParam;
            String relativeURL = URLService;

            response = get(relativeURL, queryParams);
        }

        if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED) {
            if (response == null) {
                LOG.error("Error getting policies; Received NULL response!!. secureMode=" + isSecureMode + ", user=" + "user" + ", serviceName=" + serviceName);
            } else {
                // RESTResponse resp = RESTResponse.fromClientResponse(response);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No change in policies. secureMode=" + isSecureMode + ", user=" + "user" + ", response=" + "resp" + ", serviceName=" + serviceName);
                }
            }
            servicePolicies = null;
        } else if (response.getStatus() == HttpServletResponse.SC_OK) {
            //
            servicePolicies = response.readEntity(ServicePolicies.class);
            // LOG.info("servicePolicies: " + response.readEntity(String.class));
            //
        } else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
            LOG.error("Error getting policies; service not found. secureMode=" + isSecureMode + ", user=" + "user"
                    + ", response=" + response.getStatus() + ", serviceName=" + serviceName
                    + ", " + "lastKnownVersion=" + lastKnownVersion
                    + ", " + "lastActivationTimeInMillis=" + lastActivationTimeInMillis);
            servicePolicies = null;
            String exceptionMsg = response.hasEntity() ? response.readEntity(String.class) : null;

            ADLSRangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);

            LOG.warn("Received 404 error code with body:[" + exceptionMsg + "], Ignoring");
        } else {
            // RESTResponse resp = RESTResponse.fromClientResponse(response);
            LOG.warn("Error getting policies. secureMode=" + isSecureMode + ", user=" + "user" + ", response=" + "resp" + ", serviceName=" + serviceName);
            servicePolicies = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerAdminRESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + ", " + lastActivationTimeInMillis + "): " + servicePolicies);
        }

        return servicePolicies;
    }

    public static RangerRoles getRolesIfUpdated(final long lastKnownRoleVersion, final long lastActivationTimeInMillis) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerAdminRESTClient.getRolesIfUpdated(" + lastKnownRoleVersion + ", " + lastActivationTimeInMillis + ")");
        }

        RangerRoles rangerRoles = null;

        Response response;

        String relativeURL = URLRoles;

        Map<String, String> queryParams = new HashMap<String, String>();
        // queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION, Long.toString(lastKnownVersion));
        queryParams.put("lastKnownRoleVersion", "-1");
        // queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME, Long.toString(lastActivationTimeInMillis));
        queryParams.put("lastActivationTime", "0");
        // queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
        queryParams.put("pluginId", "elasticsearch@elk-ipro-01.ru-central1.internal-Logsearch");
        // queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, clusterName);
        //
        // queryParams.put(RangerRESTUtils.REST_PARAM_SUPPORTS_POLICY_DELTAS, Boolean.toString(supportsPolicyDeltas));
        //
        // queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);
        queryParams.put("pluginCapabilities", "1ffff");

        response = get(relativeURL, queryParams);

        rangerRoles = response.readEntity(RangerRoles.class);
        // LOG.debug("GET : rangerRoles : " + response.readEntity(String.class));

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerAdminRESTClient.getRolesIfUpdated(" + lastKnownRoleVersion + ", " + lastActivationTimeInMillis + "): " + rangerRoles);
        }

        return rangerRoles;
    }
}

