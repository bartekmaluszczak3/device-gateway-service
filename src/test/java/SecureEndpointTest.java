import com.device.service.Application;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        classes = Application.class
)
@Import(RestConfig.class)
class SecureEndpointTest {

    @Autowired
    RestTemplate restTemplate;

    @LocalServerPort
    int port;


    @Test
    void test(){
        HttpEntity<String> entity = restTemplate.getForEntity("https://localhost:" + port + "/api/secure", String.class);
        Assertions.assertEquals("test", entity.getBody());
    }
}

