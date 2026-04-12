package com.device.service.web;

import com.device.service.service.CaService;
import com.device.service.utils.CertificateGenerator;
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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.security.PublicKey;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static com.device.service.utils.CertificateGenerator.*;
import static com.device.service.web.WellKnownTest.SERIAL;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(DeviceControllerTest.InMemorySslConfig.class)
public class DeviceControllerTest {

    @LocalServerPort
    private int port;

    @MockBean
    private CaService caService;

    static CertAndKey ROOT_CA;
    static CertAndKey ENROLLED_DEVICE_CERT;

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

    @SneakyThrows
    @BeforeEach
    void beforeEach(){
        when(caService.getCaCert()).thenReturn(ROOT_CA.cert());
        when(caService.signCsr(any())).thenAnswer(inv ->
                signWithInMemoryCa(inv.getArgument(0)));
    }

    @SneakyThrows
    @Test
    void shouldAcceptDataWhenCertIsPresent(){
        String payload = """
                {
                    "temperature": 23.5,
                    "pressure": 1.03
                }
                """;
        HttpResponse<String> resp = buildMtlsClient(ENROLLED_DEVICE_CERT).send(
                postJson("/api/devices/data", payload),
                HttpResponse.BodyHandlers.ofString()
        );
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
        PublicKey subjKey  = BouncyCastleProvider.getPublicKey(csr.getSubjectPublicKeyInfo());

        builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(false));
        builder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
        builder.addExtension(Extension.extendedKeyUsage, false,
                new ExtendedKeyUsage(KeyPurposeId.id_kp_clientAuth));
        builder.addExtension(Extension.subjectKeyIdentifier, false,
                ext.createSubjectKeyIdentifier(subjKey));
        builder.addExtension(Extension.authorityKeyIdentifier, false,
                ext.createAuthorityKeyIdentifier(ROOT_CA.cert().getPublicKey()));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA")
                .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                .build(ROOT_CA.privateKey());

        return new JcaX509CertificateConverter()
                .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                .getCertificate(builder.build(signer));
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
