/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under one or more contributor license agreements.
 * Licensed under a proprietary license. See the License.txt file for more information.
 * You may not use this file except in compliance with the proprietary license.
 */
package org.camunda.optimize.service.os;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.util.Timeout;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.camunda.optimize.plugin.OpensearchCustomHeaderProvider;
import org.camunda.optimize.service.es.schema.OptimizeIndexNameService;
import org.camunda.optimize.service.es.schema.RequestOptionsProvider;
import org.camunda.optimize.service.exceptions.OptimizeRuntimeException;
import org.camunda.optimize.service.util.BackoffCalculator;
import org.camunda.optimize.service.util.configuration.ConfigurationService;
import org.camunda.optimize.service.util.configuration.condition.OpenSearchCondition;
import org.camunda.optimize.service.util.configuration.elasticsearch.DatabaseConnectionNodeConfiguration;
import org.elasticsearch.client.RequestOptions;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.springframework.context.annotation.Conditional;
import org.springframework.util.StringUtils;

import javax.net.ssl.SSLContext;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.time.Duration;

import static org.camunda.optimize.service.util.DatabaseVersionChecker.checkOSVersionSupport;


@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Conditional(OpenSearchCondition.class)
@Slf4j
public class OptimizeOpensearchClientFactory {

  // TODO add the Schema manager when implementing OPT-7229
  public static OptimizeOpensearchClient create(final ConfigurationService configurationService,
                                                   final OptimizeIndexNameService optimizeIndexNameService,
                                                   final OpensearchCustomHeaderProvider opensearchCustomHeaderProvider,
                                                   final BackoffCalculator backoffCalculator) throws IOException {

    log.info("Creating OpenSearch connection...");
    final RequestOptionsProvider requestOptionsProvider =
      new RequestOptionsProvider(opensearchCustomHeaderProvider.getPlugins(), configurationService);
    final HttpHost host = getHttpHost(configurationService.getFirstOpensearchConnectionNode());
    final ApacheHttpClient5TransportBuilder builder =
        ApacheHttpClient5TransportBuilder.builder(host);

    builder.setHttpClientConfigCallback(
        httpClientBuilder -> {
          configureHttpClient(httpClientBuilder, configurationService);
          return httpClientBuilder;
        });

    builder.setRequestConfigCallback(
        requestConfigBuilder -> {
          setTimeouts(requestConfigBuilder, configurationService);
          return requestConfigBuilder;
        });

    final JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper(new ObjectMapper());
    builder.setMapper(jsonpMapper);

    final OpenSearchTransport transport = builder.build();
    final OpenSearchClient openSearchClient = new OpenSearchClient(transport);
    waitForOpensearch(openSearchClient, backoffCalculator, requestOptionsProvider.getRequestOptions());
    log.info("OpenSearch cluster successfully started");

    return new OptimizeOpensearchClient(openSearchClient, optimizeIndexNameService, requestOptionsProvider);
  }

  private static void waitForOpensearch(final OpenSearchClient osClient,
                                       final BackoffCalculator backoffCalculator,
                                       final RequestOptions requestOptions) throws IOException {
    boolean isConnected = false;
    while (!isConnected) {
      try {
        isConnected = getNumberOfClusterNodes(osClient) > 0;
      } catch (final Exception e) {
        log.error(
          "Can't connect to any Opensearch node {}. Please check the connection!",
          osClient.nodes(), e
        );
      } finally {
        if (!isConnected) {
          long sleepTime = backoffCalculator.calculateSleepTime();
          log.info("No Opensearch nodes available, waiting [{}] ms to retry connecting", sleepTime);
          try {
            Thread.sleep(sleepTime);
          } catch (final InterruptedException e) {
            log.warn("Got interrupted while waiting to retry connecting to Opensearch.", e);
            Thread.currentThread().interrupt();
          }
        }
      }
    }
    checkOSVersionSupport(osClient, requestOptions);
  }

  public static String getCurrentOSVersion(final OpenSearchClient osClient) throws IOException {
    String version = osClient.info().version().number();
    return version;
  }

  private static int getNumberOfClusterNodes(final OpenSearchClient openSearchClient) throws IOException {
    return openSearchClient.cluster().health().numberOfNodes();
  }


  private static HttpHost getHttpHost(DatabaseConnectionNodeConfiguration configuration) {
    String uriConfig = String.format("http://%s:%d", configuration.getHost(), configuration.getHttpPort());
    try {
      final URI uri = new URI(uriConfig);
      return new HttpHost(uri.getScheme(), uri.getHost(), uri.getPort());
    } catch (URISyntaxException e) {
      throw new OptimizeRuntimeException("Error in url: " + uriConfig, e);
    }
  }

  private static HttpAsyncClientBuilder setupAuthentication(
      final HttpAsyncClientBuilder builder, ConfigurationService configurationService) {
    // TODO change to opensearch?
    String username = configurationService.getElasticsearchSecurityUsername();
    String password = configurationService.getElasticsearchSecurityPassword();
    if (!StringUtils.hasText(username)
        || !StringUtils.hasText(password)) {
      log.warn(
          "Username and/or password for are empty. Basic authentication for OpenSearch is not used.");
      return builder;
    }

    final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        new AuthScope(getHttpHost(configurationService.getFirstOpensearchConnectionNode())),
        new UsernamePasswordCredentials(
            username, password.toCharArray()));

    builder.setDefaultCredentialsProvider(credentialsProvider);
    return builder;
  }

  private static void setupSSLContext(
    HttpAsyncClientBuilder httpAsyncClientBuilder, ConfigurationService configurationService) {
    try {
      final ClientTlsStrategyBuilder tlsStrategyBuilder = ClientTlsStrategyBuilder.create();
      tlsStrategyBuilder.setSslContext(getSSLContext(configurationService));
      if (configurationService.getElasticsearchSkipHostnameVerification()) {
        tlsStrategyBuilder.setHostnameVerifier(NoopHostnameVerifier.INSTANCE);
      }

      final TlsStrategy tlsStrategy = tlsStrategyBuilder.build();
      final PoolingAsyncClientConnectionManager connectionManager =
          PoolingAsyncClientConnectionManagerBuilder.create().setTlsStrategy(tlsStrategy).build();

      httpAsyncClientBuilder.setConnectionManager(connectionManager);

    } catch (Exception e) {
      log.error("Error in setting up SSLContext", e);
    }
  }

  private static SSLContext getSSLContext(ConfigurationService configurationService)

      throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
    final KeyStore truststore = loadCustomTrustStore(configurationService);
    final TrustStrategy trustStrategy =
        configurationService.getElasticsearchSecuritySslSelfSigned() ? new TrustSelfSignedStrategy() : null; // default;
    if (truststore.size() > 0) {
      return SSLContexts.custom().loadTrustMaterial(truststore, trustStrategy).build();
    } else {
      // default if custom truststore is empty
      return SSLContext.getDefault();
    }
  }

  private static KeyStore loadCustomTrustStore(ConfigurationService configurationService) {
    try {
      final KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(null);
      // load custom es server certificate if configured
      final String serverCertificate = configurationService.getElasticsearchSecuritySSLCertificate();
      if (serverCertificate != null) {
        setCertificateInTrustStore(trustStore, serverCertificate);
      }
      return trustStore;
    } catch (Exception e) {
      final String message =
          "Could not create certificate trustStore for the secured OpenSearch Connection!";
      throw new OptimizeRuntimeException(message, e);
    }
  }

  private static void setCertificateInTrustStore(
    final KeyStore trustStore, final String serverCertificate) {
    try {
      final Certificate cert = loadCertificateFromPath(serverCertificate);
      trustStore.setCertificateEntry("opensearch-host", cert);
    } catch (Exception e) {
      final String message =
          "Could not load configured server certificate for the secured OpenSearch Connection!";
      throw new OptimizeRuntimeException(message, e);
    }
  }

  private static Certificate loadCertificateFromPath(final String certificatePath)
      throws IOException, CertificateException {
    final Certificate cert;
    try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(certificatePath))) {
      final CertificateFactory cf = CertificateFactory.getInstance("X.509");

      if (bis.available() > 0) {
        cert = cf.generateCertificate(bis);
        log.debug("Found certificate: {}", cert);
      } else {
        throw new OptimizeRuntimeException(
            "Could not load certificate from file, file is empty. File: " + certificatePath);
      }
    }
    return cert;
  }

  private static HttpAsyncClientBuilder configureHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder,
                                                     ConfigurationService configurationService) {
    setupAuthentication(httpAsyncClientBuilder, configurationService);
    if (configurationService.getElasticsearchSecuritySSLEnabled()) {
      setupSSLContext(httpAsyncClientBuilder, configurationService);
    }
    return httpAsyncClientBuilder;
  }

  private static RequestConfig.Builder setTimeouts(
      final RequestConfig.Builder builder, final ConfigurationService configurationService) {

    builder.setResponseTimeout(Timeout.ofMilliseconds(configurationService.getElasticsearchConnectionTimeout()));
    //TODO check

    builder.setConnectTimeout(Timeout.ofMilliseconds(configurationService.getElasticsearchConnectionTimeout()));
    //TODO check
    return builder;
  }

  private RetryPolicy<Boolean> getConnectionRetryPolicy(ConfigurationService configurationService) {
    final String logMessage = String.format("connect to OpenSearch at %s",
                                            configurationService.getFirstOpensearchConnectionNode().getHost());
    return new RetryPolicy<Boolean>()
        .handle(IOException.class, OpenSearchException.class)
        .withDelay(Duration.ofSeconds(3))
        .withMaxAttempts(50)
        .onRetry(
            e ->
                log.info(
                    "Retrying #{} {} due to {}",
                    e.getAttemptCount(),
                    logMessage,
                    e.getLastFailure()))
        .onAbort(e -> log.error("Abort {} by {}", logMessage, e.getFailure()))
        .onRetriesExceeded(
            e -> log.error("Retries {} exceeded for {}", e.getAttemptCount(), logMessage));
  }
}
