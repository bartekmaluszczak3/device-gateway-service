package com.device.service.web;

import com.device.service.kafka.event.DataReceivedEvent;
import com.device.service.service.CaService;
import com.device.service.utils.CertificateGenerator;
import com.device.service.utils.KafkaTestConsumer;
import lombok.SneakyThrows;
import org.apache.coyote.http11.Http11NioProtocol;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.SSLHostConfigCertificate;
import org.assertj.core.api.Assertions;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.security.Security;
import java.time.Duration;
import java.util.List;

import static com.device.service.utils.CertificateGenerator.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(DeviceControllerTest.InMemorySslConfig.class)
@Testcontainers
public class DeviceControllerTest {

    @LocalServerPort
    private int port;

    @MockBean
    private CaService caService;

    static CertAndKey ROOT_CA;
    static CertAndKey ENROLLED_DEVICE_CERT;
    private static final String TOPIC = "device-data-events";

    static {
        try {
            if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
                Security.addProvider(new BouncyCastleProvider());
            }
            ROOT_CA                  = generateCA("Test CA", 1);
            ENROLLED_DEVICE_CERT     = generateSignedCert("device-001",   false, ROOT_CA, 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private KafkaTestConsumer kafkaConsumer;

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }


    @SneakyThrows
    @BeforeEach
    void beforeEach(){
        kafkaConsumer = new KafkaTestConsumer(kafka.getBootstrapServers());

    }

    @SneakyThrows
    @Test
    void shouldAcceptDataWhenCertIsPresent(){
        // given
        String payload = """
                {
                    "temperature": 23.5,
                    "pressure": 1.03
                }
                """;

        // when
        HttpResponse<String> resp = buildMtlsClient(ENROLLED_DEVICE_CERT).send(
                postJson("/api/devices/data", payload),
                HttpResponse.BodyHandlers.ofString()
        );

        // then
        Assertions.assertThat(resp.statusCode()).isEqualTo(204);

        // and
        List<DataReceivedEvent> events = kafkaConsumer.consumeEvents(TOPIC, Duration.ofSeconds(2), DataReceivedEvent.class);
        Assertions.assertThat(events.size()).isEqualTo(1);
        var event = events.get(0);
        Assertions.assertThat(event.getDeviceId()).isEqualTo("device-001");
        Assertions.assertThat(event.getPayload()).containsKey("temperature");
        Assertions.assertThat(event.getPayload()).containsKey("pressure");
    }

    @SneakyThrows
    @Test
    void everyRequestGenerateNewEvent(){
        // given
        String payload = """
                {
                    "temperature": 23.5,
                    "pressure": 1.03
                }
                """;
        var client = buildMtlsClient(ENROLLED_DEVICE_CERT);

        // when
        client.send(postJson("/api/devices/data", payload), HttpResponse.BodyHandlers.ofString());
        client.send(postJson("/api/devices/data", payload), HttpResponse.BodyHandlers.ofString());

        // then
        List<DataReceivedEvent> events = kafkaConsumer.consumeEvents(TOPIC, Duration.ofSeconds(2), DataReceivedEvent.class);
        Assertions.assertThat(events.size()).isEqualTo(2);
        var firstEvent = events.get(0);
        Assertions.assertThat(firstEvent.getDeviceId()).isEqualTo("device-001");
        Assertions.assertThat(firstEvent.getPayload()).containsKey("temperature");
        Assertions.assertThat(firstEvent.getPayload()).containsKey("pressure");
    }

    @Test
    void shouldRejectDataWhenCertificateIsNotPresent(){
        assertThrows(Exception.class, () ->
                buildTlsOnlyClient().send(
                        postJson("/api/devices/data", "{\"data\":\"test\"}"),
                        HttpResponse.BodyHandlers.ofString()
                )
        );
    }

    private HttpClient buildTlsOnlyClient() throws Exception {
        char[] pass = "test".toCharArray();
        KeyStore ts = CertificateGenerator.buildTrustStore(pass, ROOT_CA.cert());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return HttpClient.newBuilder().sslContext(ctx).build();
    }

    private HttpClient buildMtlsClient(CertAndKey clientCert) throws Exception {
        char[] pass = "test".toCharArray();
        KeyStore ks = CertificateGenerator.buildKeyStore("client", pass, clientCert, ROOT_CA.cert());
        KeyStore ts = CertificateGenerator.buildTrustStore(pass, ROOT_CA.cert());
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, pass);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return HttpClient.newBuilder().sslContext(ctx).build();
    }

    private HttpRequest postJson(String path, String body) {
        return HttpRequest.newBuilder()
                .uri(URI.create("https://localhost:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    @TestConfiguration
    static class InMemorySslConfig {

        @Bean
        WebServerFactoryCustomizer<TomcatServletWebServerFactory> deviceSslCustomizer() {
            return factory -> {
                try {
                    char[] pass = "test".toCharArray();

                    CertAndKey serverCert = CertificateGenerator.generateSignedCert(
                            "localhost", true, ROOT_CA, 1);
                    KeyStore serverKS = CertificateGenerator.buildKeyStore(
                            "server", pass, serverCert, ROOT_CA.cert());
                    KeyStore serverTS = CertificateGenerator.buildTrustStore(
                            pass, ROOT_CA.cert());

                    factory.setSsl(null);

                    factory.addConnectorCustomizers(connector -> {
                        connector.setScheme("https");
                        connector.setSecure(true);

                        SSLHostConfig ssl = new SSLHostConfig();
                        ssl.setCertificateVerification("required");

                        SSLHostConfigCertificate certCfg = new SSLHostConfigCertificate(
                                ssl, SSLHostConfigCertificate.Type.RSA);
                        certCfg.setCertificateKeystore(serverKS);
                        certCfg.setCertificateKeystorePassword(new String(pass));
                        certCfg.setCertificateKeyAlias("server");
                        ssl.addCertificate(certCfg);
                        ssl.setTrustStore(serverTS);
                        connector.addSslHostConfig(ssl);

                        ((Http11NioProtocol) connector.getProtocolHandler()).setSSLEnabled(true);
                    });

                } catch (Exception e) {
                    throw new RuntimeException("Error", e);
                }
            };
        }
    }
}
