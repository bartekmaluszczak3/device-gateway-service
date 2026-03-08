import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/secure")
public class TestController {

    @GetMapping
    public ResponseEntity<String> secureEndpoint(){
        return ResponseEntity.ok("test");
    }
}
