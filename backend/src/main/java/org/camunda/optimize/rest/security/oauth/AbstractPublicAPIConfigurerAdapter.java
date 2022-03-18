/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package org.camunda.optimize.rest.security.oauth;

import lombok.Getter;
import org.camunda.optimize.service.util.configuration.ConfigurationService;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static org.camunda.optimize.jetty.OptimizeResourceConstants.REST_API_PATH;
import static org.camunda.optimize.rest.IngestionRestService.INGESTION_PATH;
import static org.camunda.optimize.rest.IngestionRestService.VARIABLE_SUB_PATH;


@Component
@Order(1)
@EnableWebSecurity
public abstract class AbstractPublicAPIConfigurerAdapter extends WebSecurityConfigurerAdapter {
  protected static final String PUBLIC_API_PATH = createApiPath("/public/**");
  protected final ConfigurationService configurationService;
  @Getter
  protected final String jwtSetUri;

  protected AbstractPublicAPIConfigurerAdapter(final ConfigurationService configurationService) {
    this.configurationService = configurationService;
    this.jwtSetUri = readJwtSetUriFromConfig();
  }

  @Bean
  public abstract JwtDecoder jwtDecoder();

  @Override
  public void configure(HttpSecurity http) throws Exception {
    http
      .requestMatchers()
      // Public APIs allowed in all modes (SaaS, CCSM and Platform)
      .antMatchers(PUBLIC_API_PATH,
                   createApiPath(INGESTION_PATH, VARIABLE_SUB_PATH))
      .and()
      // since these calls will not be used in a browser, we can disable csrf
      .csrf().disable()
      .httpBasic().disable()
      // spring session management is not needed as we have stateless session handling using a JWT token
      .sessionManagement()
      .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
      .and()
      .authorizeRequests()
      // everything requires authentication
      .anyRequest().authenticated()
      .and()
      .oauth2ResourceServer()
      .jwt();
  }

  private String readJwtSetUriFromConfig() {
    return Optional.ofNullable(configurationService.getOptimizeApiConfiguration().getJwtSetUri()).orElse("");
  }

  protected static String createApiPath(final String... subPath) {
    return REST_API_PATH + String.join("", subPath);
  }
}
