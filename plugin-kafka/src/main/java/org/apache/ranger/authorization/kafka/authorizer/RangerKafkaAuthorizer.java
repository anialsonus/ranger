/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.authorization.kafka.authorizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.kafka.authorizer.utils.RangerKafkaCheckAccess;
import org.apache.ranger.authorization.kafka.authorizer.utils.RangerKafkaGrantAccess;
import org.apache.ranger.authorization.kafka.authorizer.utils.RangerKafkaListAccess;
import org.apache.ranger.authorization.kafka.authorizer.utils.RangerKafkaRevokeAccess;
import org.apache.ranger.authorization.kafka.authorizer.utils.RangerKafkaUtils;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerKafkaAuthorizer implements Authorizer {
  private static final Logger logger = LoggerFactory.getLogger(RangerKafkaAuthorizer.class);
  private static final Logger PERF_KAFKAAUTH_REQUEST_LOG = RangerPerfTracer.getPerfLogger("kafkaauth.request");
  private static volatile RangerBasePlugin rangerPlugin = null;

  RangerKafkaAuditHandler auditHandler = null;
  RangerKafkaUtils rangerKafkaUtils = new RangerKafkaUtils();

  public RangerKafkaAuthorizer() {
  }

  @Override
  public void close() {
    logger.info("close() called on authorizer.");
    try {
      if (rangerPlugin != null) {
        rangerPlugin.cleanup();
      }
    } catch (Throwable t) {
      logger.error("Error closing RangerPlugin.", t);
    }
  }

  @Override
  public void configure(Map<String, ?> configs) {
    RangerBasePlugin me = rangerPlugin;
    if (me == null) {
      synchronized (RangerKafkaAuthorizer.class) {
        me = rangerPlugin;
        if (me == null) {
          try {
            // Possible to override JAAS configuration which is used by Ranger, otherwise
            // SASL_PLAINTEXT is used, which force Kafka to use 'sasl_plaintext.KafkaServer',
            // if it's not defined, then it reverts to 'KafkaServer' configuration.
            final Object jaasContext = configs.get("ranger.jaas.context");
            final String listenerName = (jaasContext instanceof String
                && StringUtils.isNotEmpty((String) jaasContext)) ? (String) jaasContext
                : SecurityProtocol.SASL_PLAINTEXT.name();
            final String saslMechanism = SaslConfigs.GSSAPI_MECHANISM;
            JaasContext context = JaasContext.loadServerContext(new ListenerName(listenerName), saslMechanism, configs);
            MiscUtil.setUGIFromJAASConfig(context.name());
            UserGroupInformation loginUser = MiscUtil.getUGILoginUser();
            logger.info("LoginUser={}", loginUser);
          } catch (Throwable t) {
            logger.error("Error getting principal.", t);
          }
          rangerPlugin = new RangerBasePlugin("kafka", "kafka");
          logger.info("Calling plugin.init()");
          rangerPlugin.init();
          auditHandler = new RangerKafkaAuditHandler();
          rangerPlugin.setResultProcessor(auditHandler);
        }
      }
    }
  }

  @Override
  public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
    return serverInfo.endpoints().stream()
        .collect(Collectors.toMap(endpoint -> endpoint, endpoint -> CompletableFuture.completedFuture(null), (a, b) -> b));
  }

  @Override
  public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
    if (rangerPlugin == null) {
      MiscUtil.logErrorMessageByInterval(logger, "Authorizer is still not initialized");
      return RangerKafkaUtils.denyAll(actions);
    }

    RangerPerfTracer perf = null;
    if (RangerPerfTracer.isPerfTraceEnabled(PERF_KAFKAAUTH_REQUEST_LOG)) {
      perf = RangerPerfTracer.getPerfTracer(PERF_KAFKAAUTH_REQUEST_LOG, "RangerKafkaAuthorizer.authorize(actions=" + actions + ")");
    }
    try {
      return rangerKafkaUtils.wrappedAuthorization(requestContext, actions, rangerPlugin, auditHandler);
    } finally {
      RangerPerfTracer.log(perf);
    }
  }

  /**
   *
   * @param requestContext Request context including request resourceType, security protocol and listener name
   * @param aclBindings    AclBindings
   * @return               Return {@link AclDeleteResult}
   *
   */
  @Override
  public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
    List<? extends CompletionStage<AclCreateResult>> ret;

    if (logger.isDebugEnabled()) {
      logger.debug("==> RangerKafkaAuthorizer.createAcls(): AuthorizableRequestContext: " + requestContext.toString() + " aclBindings: " + aclBindings);
    }
    RangerKafkaGrantAccess rangerKafkaGrantAccess = new RangerKafkaGrantAccess();

    ret = aclBindings.stream()
            .map(aclBinding -> {
              CompletableFuture<AclCreateResult> completableFuture = new CompletableFuture<>();
              completableFuture.complete(rangerKafkaGrantAccess.grant(requestContext, aclBinding, rangerPlugin, auditHandler));
              return completableFuture;
            })
            .collect(Collectors.toList());

    if (logger.isDebugEnabled()) {
      logger.debug("<== RangerKafkaAuthorizer.createAcls() result: " + ret );
    }
    return ret;
  }

  /**
   *
   * @param requestContext    Request context including request resourceType, security protocol and listener name
   * @param aclBindingFilters AclBindingFilters
   * @return                  Return {@link AclDeleteResult}
   *
   */
  @Override
  public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
    if (logger.isDebugEnabled()) {
      logger.debug("==> RangerKafkaAuthorizer.deleteAcls(): AuthorizableRequestContext: " + requestContext.toString() + " aclBindingFilters :  " + aclBindingFilters);
    }

    List<? extends CompletionStage<AclDeleteResult>> ret;
    List<CompletableFuture<AclDeleteResult>> completableFutures = new ArrayList<>();

    for (AclBindingFilter filter : aclBindingFilters) {
      RangerKafkaRevokeAccess rangerKafkaRevokeAccess = new RangerKafkaRevokeAccess();
      CompletableFuture<AclDeleteResult> completableFuture = new CompletableFuture<>();
      try {
        AclDeleteResult aclDeleteResult = rangerKafkaRevokeAccess.revoke(filter, rangerPlugin, auditHandler, requestContext);
        completableFuture.complete(aclDeleteResult);
      } catch (Exception e) {
        completableFuture.completeExceptionally(new ApiException(e.getMessage()));
      }
      completableFutures.add(completableFuture);
    }

    ret = completableFutures.stream().collect(Collectors.toList());

    if (logger.isDebugEnabled()) {
      logger.debug("<== RangerKafkaAuthorizer.deleteAcls() result: " + ret );
    }

    return ret;
  }

  /**
   * Check if the caller is authorized to perform the given ACL operation on at least one
   * resource of the given type.
   *
   * Custom authorizer implementations should consider overriding this default implementation because:
   * 1. The default implementation iterates all AclBindings multiple times, without any caching
   *    by principal, host, operation, permission types, and resource types. More efficient
   *    implementations may be added in custom authorizers that directly access cached entries.
   * 2. The default implementation cannot integrate with any audit logging included in the
   *    authorizer implementation.
   * 3. The default implementation does not support any custom authorizer configs or other access
   *    rules apart from ACLs.
   *
   * @param requestContext Request context including request resourceType, security protocol and listener name
   * @param op             The ACL operation to check
   * @param resourceType   The resource type to check
   * @return               Return {@link AuthorizationResult#ALLOWED} if the caller is authorized
   *                       to perform the given ACL operation on at least one resource of the
   *                       given type. Return {@link AuthorizationResult#DENIED} otherwise.
   */

  @Override
  public AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation op, ResourceType resourceType){

    AuthorizationResult ret = AuthorizationResult.DENIED;

    SecurityUtils.authorizeByResourceTypeCheckArgs(op, resourceType);

    if (logger.isDebugEnabled()) {
      logger.debug("==> RangerKafkaAuthorizer.authorizeByResourceType() requestContext: " + requestContext.toString() + " AclOperation : " + op + " ResourceType : " + resourceType);
    }

    RangerKafkaCheckAccess rangerKafkaCheckAccess = new RangerKafkaCheckAccess();

    ret =  rangerKafkaCheckAccess.authorizeByResourceType(requestContext, op, resourceType, rangerPlugin, auditHandler);

    if (logger.isDebugEnabled()) {
      logger.debug("<== RangerKafkaAuthorizer.authorizeByResourceType() requestContext: " + requestContext.toString() + " AclOperation : " + op + " ResourceType : " + resourceType + "AuthorizationResult : " + ret);
    }

    return ret;
  }

  /**
   * Returns the List of Acls from Ranger for the give AclBinding Filters
   * @param filter AclBindingFilters
   * @return       Return {@link Iterable<AclBinding>}
   *
   */

  @Override
  public Iterable<AclBinding> acls(AclBindingFilter filter){
    if (logger.isDebugEnabled()) {
      logger.debug("==> RangerKafkaAuthorizer.acls(): AclBindingFilter: " + filter.toString());
    }
    RangerKafkaListAccess rangerKafkaListAccess = new RangerKafkaListAccess();
    Iterable<AclBinding> ret = null;
    ret = rangerKafkaListAccess.getAclBindings(filter, rangerPlugin);
    if (logger.isDebugEnabled()) {
      logger.debug("<== RangerKafkaAuthorizer.acls(): AclBindingFilter: " + ret);
    }
    return ret;
  }
}
