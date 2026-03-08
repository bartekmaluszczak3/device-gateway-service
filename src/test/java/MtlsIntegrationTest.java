
import com.device.service.Application;
import lombok.SneakyThrows;
import org.apache.coyote.http11.Http11NioProtocol;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.SSLHostConfigCertificate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.boot.test.web.server.LocalServerPort;
import utils.CertificateGenerator;
import utils.CertificateGenerator.CertAndKey;

import javax.net.ssl.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = Application.class)
@Import({MtlsIntegrationTest.InMemorySslConfig.class, TestController.class})
class MtlsIntegrationTest {

    @LocalServerPort
    private int port;

    static final CertAndKey ROOT_CA;
    static final CertAndKey SERVER_CERT;
    static final CertAndKey TRUSTED_CLIENT_CERT;
    static final CertAndKey ROGUE_CA;
    static final CertAndKey UNTRUSTED_CLIENT_CERT;

    static {
        try {
            ROOT_CA               = CertificateGenerator.generateCA("Test Root CA", 1);
            SERVER_CERT           = CertificateGenerator.generateSignedCert("localhost",       true,  ROOT_CA, 1);
            TRUSTED_CLIENT_CERT   = CertificateGenerator.generateSignedCert("trusted-client",  false, ROOT_CA, 1);
            ROGUE_CA              = CertificateGenerator.generateCA("Rogue CA", 1);
            UNTRUSTED_CLIENT_CERT = CertificateGenerator.generateSignedCert("rogue-client",    false, ROGUE_CA, 1);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final String ENDPOINT_PATH = "/api/secure";
    @TestConfiguration
    static class InMemorySslConfig {

        @Bean
        WebServerFactoryCustomizer<TomcatServletWebServerFactory> inMemorySslCustomizer() {
            return factory -> {
                try {
                    char[] pass = "test".toCharArray();

                    KeyStore serverKeyStore = CertificateGenerator.buildKeyStore(
                            "server", pass, SERVER_CERT, ROOT_CA.cert());
                    KeyStore serverTrustStore = CertificateGenerator.buildTrustStore(
                            pass, ROOT_CA.cert());
                    factory.setSsl(null);

                    factory.addConnectorCustomizers(connector -> {
                        connector.setScheme("https");
                        connector.setSecure(true);
                        SSLHostConfig sslHostConfig = new SSLHostConfig();
                        sslHostConfig.setCertificateVerification("required");
                        SSLHostConfigCertificate certConfig = new SSLHostConfigCertificate(
                                sslHostConfig, SSLHostConfigCertificate.Type.RSA);
                        certConfig.setCertificateKeystore(serverKeyStore);
                        certConfig.setCertificateKeystorePassword(new String(pass));
                        certConfig.setCertificateKeyAlias("server");
                        sslHostConfig.addCertificate(certConfig);
                        sslHostConfig.setTrustStore(serverTrustStore);
                        connector.addSslHostConfig(sslHostConfig);
                        Http11NioProtocol protocol = (Http11NioProtocol) connector.getProtocolHandler();
                        protocol.setSSLEnabled(true);
                    });

                } catch (Exception e) {
                    throw new RuntimeException("Error in configuration", e);
                }
            };
        }
    }

    @Test
    void shouldReturn200WhenClientPresentsValidCertificate() throws Exception {
        // given
        HttpClient client = buildMtlsClient(TRUSTED_CLIENT_CERT);

        // when
        HttpResponse<String> response = client.send(
                buildGetRequest(),
                HttpResponse.BodyHandlers.ofString()
        );

        // then
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("test");
    }

    @SneakyThrows
    @Test
    void shouldThrowSSLExceptionWhenClientHasNoCertificate() {
        // given
        HttpClient httpClient = buildTlsOnlyClient();

        // when and then
        assertThrows(Exception.class, () ->
                httpClient.send(
                        buildGetRequest(),
                        HttpResponse.BodyHandlers.ofString()
                )
        );
    }

    @SneakyThrows
    @Test
    void shouldThrowSSLExceptionWhenClientCertIsSignedByUntrustedCA() {
        // given
        HttpClient httpClient = buildMtlsClient(UNTRUSTED_CLIENT_CERT);

        // when and then
        assertThrows(Exception.class, () ->
                httpClient.send(
                        buildGetRequest(),
                        HttpResponse.BodyHandlers.ofString()
                )
        );
    }

    private HttpClient buildMtlsClient(CertAndKey clientCert) throws Exception {
        char[] pass = "test".toCharArray();

        KeyStore clientKeyStore = CertificateGenerator.buildKeyStore(
                "client", pass, clientCert, ROOT_CA.cert());

        KeyStore clientTrustStore = CertificateGenerator.buildTrustStore(
                pass, ROOT_CA.cert());

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(clientKeyStore, pass);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(clientTrustStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        return HttpClient.newBuilder().sslContext(sslContext).build();
    }

    private HttpClient buildTlsOnlyClient() throws Exception {
        char[] pass = "test".toCharArray();

        KeyStore clientTrustStore = CertificateGenerator.buildTrustStore(pass, ROOT_CA.cert());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(clientTrustStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmf.getTrustManagers(), null);

        return HttpClient.newBuilder().sslContext(sslContext).build();
    }

    private HttpRequest buildGetRequest() {
        return HttpRequest.newBuilder()
                .uri(URI.create("https://localhost:" + port + ENDPOINT_PATH))
                .GET()
                .build();
    }
}
