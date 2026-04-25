package com.device.service.web;

import com.device.service.Application;
import com.device.service.kafka.event.DeviceEnrolledEvent;
import com.device.service.service.CaService;
import com.device.service.utils.CertificateGenerator;
import com.device.service.utils.KafkaTestConsumer;
import lombok.SneakyThrows;
import org.apache.coyote.http11.Http11NioProtocol;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.SSLHostConfigCertificate;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.junit.jupiter.api.Assertions;
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
import javax.security.auth.x500.X500Principal;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Import({WellKnownTest.InMemorySslConfig.class})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = Application.class)
@Testcontainers
public class WellKnownTest {

    static CertificateGenerator.CertAndKey ROOT_CA;
    static CertificateGenerator.CertAndKey BOOTSTRAP_CERT;
    static CertificateGenerator.CertAndKey OPERATIONAL_CERT;
    static final AtomicLong SERIAL = new AtomicLong(System.currentTimeMillis());
    private static final String TOPIC = "device-enrolled";

    static {
        try {
            ROOT_CA          = CertificateGenerator.generateCA("Test CA", 1);
            BOOTSTRAP_CERT   = CertificateGenerator.generateSignedCert("device-001-bootstrap", false, ROOT_CA, 1);
            OPERATIONAL_CERT = CertificateGenerator.generateSignedCert("device-001", false, ROOT_CA, 1);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @LocalServerPort
    private int port;

    @MockBean
    private CaService caService;

    private KafkaTestConsumer kafkaConsumer;

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeEach
    @SneakyThrows
    void beforeEach(){
        when(caService.getCaCert()).thenReturn(ROOT_CA.cert());
        when(caService.signCsr(any())).thenAnswer(invocation -> {
            PKCS10CertificationRequest csr = invocation.getArgument(0);
            return signWithInMemoryCa(csr);
        });
        kafkaConsumer = new KafkaTestConsumer(kafka.getBootstrapServers());

    }

    private X509Certificate signWithInMemoryCa(PKCS10CertificationRequest csr) throws Exception {
        X500Name issuer = X500Name.getInstance(ROOT_CA.cert().getSubjectX500Principal().getEncoded());
        Instant now = Instant.now();

        X509v3CertificateBuilder builder = new X509v3CertificateBuilder(
                issuer,
                BigInteger.valueOf(SERIAL.incrementAndGet()),
                Date.from(now),
                Date.from(now.plus(1, ChronoUnit.DAYS)),
                csr.getSubject(),
                csr.getSubjectPublicKeyInfo()
        );

        JcaX509ExtensionUtils ext = new JcaX509ExtensionUtils();
        PublicKey subjectKey = BouncyCastleProvider.getPublicKey(csr.getSubjectPublicKeyInfo());

        builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(false));
        builder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
        builder.addExtension(Extension.extendedKeyUsage, false,
                new ExtendedKeyUsage(KeyPurposeId.id_kp_clientAuth));
        builder.addExtension(Extension.subjectKeyIdentifier, false,
                ext.createSubjectKeyIdentifier(subjectKey));
        builder.addExtension(Extension.authorityKeyIdentifier, false,
                ext.createAuthorityKeyIdentifier(ROOT_CA.cert().getPublicKey()));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA")
                .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                .build(ROOT_CA.privateKey());

        return new JcaX509CertificateConverter()
                .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                .getCertificate(builder.build(signer));
    }

    @TestConfiguration
    static class InMemorySslConfig {

        @Bean
        WebServerFactoryCustomizer<TomcatServletWebServerFactory> estSslCustomizer() {
            return factory -> {
                try {
                    char[] pass = "test".toCharArray();

                    KeyStore serverKS = CertificateGenerator.buildKeyStore(
                            "server",
                            CertificateGenerator.generateSignedCert("localhost", true, ROOT_CA, 1),
                            ROOT_CA.cert());
                    KeyStore serverTS = CertificateGenerator.buildTrustStore(pass, ROOT_CA.cert());
                    factory.setSsl(null);
                    factory.addConnectorCustomizers(connector -> {
                        connector.setScheme("https");
                        connector.setSecure(true);
                        SSLHostConfig ssl = new SSLHostConfig();
                        ssl.setCertificateVerification("optionalNoCA");
                        SSLHostConfigCertificate cert = new SSLHostConfigCertificate(
                                ssl, SSLHostConfigCertificate.Type.RSA);
                        cert.setCertificateKeystore(serverKS);
                        cert.setCertificateKeystorePassword(new String(pass));
                        cert.setCertificateKeyAlias("server");
                        ssl.addCertificate(cert);
                        ssl.setTrustStore(serverTS);
                        connector.addSslHostConfig(ssl);
                        Http11NioProtocol protocol =
                                (Http11NioProtocol) connector.getProtocolHandler();
                        protocol.setSSLEnabled(true);
                    });

                } catch (Exception e) {
                    throw new RuntimeException("Configuration error", e);
                }
            };
        }
    }

    @SneakyThrows
    @Test
    void shouldReturnCaCert(){
        // given
        HttpClient httpClient = buildTlsOnlyClient();

        // when
        HttpResponse<String> resp = httpClient.send(
                buildRequest("GET", "/.well-known/cacerts", null, null),
                HttpResponse.BodyHandlers.ofString()
        );

        // then
        Assertions.assertEquals(200, resp.statusCode());
    }

    @SneakyThrows
    @Test
    void shouldEnrollDeviceWithBootstrapCert(){
        // given
        String csr = generateCsrBase64("CN=device-000, O=Test, C=PL", CertificateGenerator.generateKeyPair());
        var client = buildMtlsClient(BOOTSTRAP_CERT);

        // when
        HttpResponse<String> resp = client.send(
                buildRequest("POST", "/.well-known/enroll", "application/pkcs10", csr),
                HttpResponse.BodyHandlers.ofString());

        // then
        Assertions.assertEquals(200, resp.statusCode());

        // and
        List<DeviceEnrolledEvent> events = kafkaConsumer.consumeEvents(TOPIC, Duration.ofSeconds(2), DeviceEnrolledEvent.class);
        Assertions.assertEquals(1, events.size());
        Assertions.assertEquals("device-001-bootstrap", events.get(0).getDeviceId());
    }

    @Test
    @SneakyThrows
    void shouldRejectEnrollWithoutCertificate(){
        // given
        String csr = generateCsrBase64("CN=rogue, O=Test, C=PL", CertificateGenerator.generateKeyPair());
        var client = buildTlsOnlyClient();

        // when
        var response = client.send(buildRequest("POST", "/.well-known/enroll", "application/pkcs10", csr),
                HttpResponse.BodyHandlers.ofString());

        // then
        Assertions.assertEquals(401, response.statusCode());
    }

    @SneakyThrows
    @Test
    void shouldRejectRenewWhenCNNotMatch(){
        // given
        String csr = generateCsrBase64("CN=different-device, O=Test, C=PL", CertificateGenerator.generateKeyPair());
        var client = buildMtlsClient(OPERATIONAL_CERT);

        // when
        var response = client.send(buildRequest("POST", "/.well-known/renew", "application/pkcs10", csr),
                HttpResponse.BodyHandlers.ofString());

        // then
        Assertions.assertEquals(400, response.statusCode());
    }

    @SneakyThrows
    private HttpClient buildTlsOnlyClient() {
        String password = "test";
        KeyStore ts = CertificateGenerator.buildTrustStore(password.toCharArray(), ROOT_CA.cert());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);

        return HttpClient.newBuilder().sslContext(ctx).build();
    }

    private HttpRequest buildRequest(String method, String path,
                                     String contentType, String body) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("https://localhost:" + port + path));
        if ("GET".equals(method)) {
            builder.GET();
        } else {
            builder.method(method,
                    body != null
                            ? HttpRequest.BodyPublishers.ofString(body)
                            : HttpRequest.BodyPublishers.noBody());
            if (contentType != null) builder.header("Content-Type", contentType);
        }

        return builder.build();
    }

    private String generateCsrBase64(String dn, KeyPair keyPair) throws Exception {
        X500Name subject = X500Name.getInstance(new X500Principal(dn).getEncoded());
        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA")
                .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                .build(keyPair.getPrivate());
        PKCS10CertificationRequest csr = new JcaPKCS10CertificationRequestBuilder(
                subject, keyPair.getPublic()).build(signer);
        return Base64.getEncoder().encodeToString(csr.getEncoded());
    }

    private HttpClient buildMtlsClient(CertificateGenerator.CertAndKey clientCert) throws Exception {
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


}
