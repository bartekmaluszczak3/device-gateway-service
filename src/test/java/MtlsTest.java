import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;

import com.device.service.Application;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = Application.class)
public class MtlsTest {
    private static final String CLIENT_KEYSTORE = "/keystore/client.p12";
    private static final String CLIENT_PASS = "clientpass";

    private static final String CLIENT_TRUSTSTORE  = "/keystore/truststore.p12";
    private static final String TRUST_PASS         = "trustpass";

    @LocalServerPort
    private int port;

    @SneakyThrows
    @Test
    void shouldReturn200WhenValidClientCertificateIsPresent(){
        // given
        var client = buildMtlsClient();

        // when
        HttpResponse<String> response = client.send(
                buildGetRequest("/api/secure"),
                HttpResponse.BodyHandlers.ofString()
        );

        // then
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("test", response.body());
    }

    @Test
    void shouldFailWhenClientSendsNoCertificate(){
        // given
        HttpClient clientWithoutCert = buildTlsOnlyClient();

        // when and then
        assertThrows(Exception.class, () -> {
            clientWithoutCert.send(
                    buildGetRequest("/api/hello"),
                    HttpResponse.BodyHandlers.ofString());
        });
    }

    @SneakyThrows
    private HttpClient buildMtlsClient(){
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(loadKeyStore(CLIENT_KEYSTORE, CLIENT_PASS), CLIENT_PASS.toCharArray());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(loadKeyStore(CLIENT_TRUSTSTORE, TRUST_PASS));

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        return HttpClient.newBuilder()
                .sslContext(sslContext)
                .build();
    }

    @SneakyThrows
    private HttpClient buildTlsOnlyClient(){
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(loadKeyStore(CLIENT_TRUSTSTORE, TRUST_PASS));
        SSLContext sslContext = SSLContext.getInstance("TLS");

        sslContext.init(null, tmf.getTrustManagers(), null);
        return HttpClient.newBuilder()
                .sslContext(sslContext)
                .build();
    }

    @SneakyThrows
    private KeyStore loadKeyStore(String classpathPath, String password){
        KeyStore ks = KeyStore.getInstance("PKCS12");
        InputStream is = getClass().getResourceAsStream(classpathPath);
        ks.load(is, password.toCharArray());
        return ks;
    }

    private HttpRequest buildGetRequest(String path) {
        return HttpRequest.newBuilder()
                .uri(URI.create("https://localhost:" + port + path))
                .GET()
                .build();
    }
}
