/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package io.camunda.operate.webapp.security;

public final class OperateURIs {

  // Used as constants class
   private OperateURIs(){}

   public static final String
      RESPONSE_CHARACTER_ENCODING = "UTF-8";
  public static final String ROOT = "/";
  public static final String API = "/api/**";

  public static final String LOGIN_RESOURCE = "/api/login";
  public static final String LOGOUT_RESOURCE = "/api/logout";
  public static final String COOKIE_JSESSIONID = "OPERATE-SESSION";

  public static final String SSO_CALLBACK_URI = "/sso-callback";
  public static final String NO_PERMISSION = "/noPermission";

  public static final String IAM_CALLBACK_URI = "/iam-callback";
  public static final String IAM_LOGOUT_CALLBACK_URI = "/iam-logout-callback";

  public static final String// For redirects after login
      REQUESTED_URL = "requestedUrl"
  ;

  public static final String[] AUTH_WHITELIST = {
       "/swagger-resources",
       "/swagger-resources/**",
       "/swagger-ui.html",
       "/documentation",
       LOGIN_RESOURCE,
       LOGOUT_RESOURCE
   };

}
