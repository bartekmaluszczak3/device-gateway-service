package com.device.service.web;

import com.device.service.WellKnownException;
import com.device.service.service.WellKnownService;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.security.cert.X509Certificate;

@RestController
@RequestMapping("/.well-known")
@Slf4j
public class WellKnownController {
    private static final String MIME_PKCS7  = "application/pkcs7-mime; smime-type=certs-only";
    private static final String MIME_PKCS10 = "application/pkcs10";

    private final WellKnownService wellKnownService;

    public WellKnownController(WellKnownService wellKnownService) {
        this.wellKnownService = wellKnownService;
    }

    @GetMapping(value = "/cacerts", produces = MIME_PKCS7)
    public ResponseEntity<String> getCaCerts(){
        try {
            String pkcs7 = wellKnownService.getCaCertsPkcs7Base64();
            return ResponseEntity.ok()
                    .header("Content-Transfer-Encoding", "base64")
                    .contentType(MediaType.parseMediaType(MIME_PKCS7))
                    .body(pkcs7);
        } catch (Exception e) {
            log.error("Error during fetching ca certs {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }
    @PostMapping(value = "/enroll", consumes = MIME_PKCS10, produces = MIME_PKCS7)
    public ResponseEntity<String> enroll(@RequestBody String csrBase64, HttpServletRequest request){
        X509Certificate bootstrapCert = extractClientCert(request);
        if (bootstrapCert == null) {
            log.warn("simpleenroll: brak certyfikatu klienta");
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        String deviceId = extractCN(bootstrapCert);
        log.info("Requested enroll for  `CN={}", deviceId);

        try {
            String signed = wellKnownService.enroll(csrBase64.trim(), bootstrapCert);
            log.info("Certificate issued for CN = {}", deviceId);
            return ResponseEntity.ok()
                    .header("Content-Transfer-Encoding", "base64")
                    .contentType(MediaType.parseMediaType(MIME_PKCS7))
                    .body(signed);
        } catch (WellKnownException e) {
            log.warn("Rejected CN={}: {}", deviceId, e.getMessage());
            return ResponseEntity.status(e.getHttpStatus()).build();
        }catch (Exception e){
            log.error("Cannot issue certificate {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }
    @PostMapping(value = "/renew", consumes = MIME_PKCS10, produces = MIME_PKCS7)
    public ResponseEntity<String> renew(@RequestBody String csr, HttpServletRequest request){
        X509Certificate activeCert = extractClientCert(request);
        if (activeCert == null) {
            log.warn("Client certificate not present");
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        String deviceId = extractCN(activeCert);
        log.info("Requested renew CN={}", deviceId);
        try {
            String renewed = wellKnownService.renew(csr.trim(), activeCert);
            log.info("Certificate renewed dla CN={}", deviceId);
            return ResponseEntity.ok()
                    .header("Content-Transfer-Encoding", "base64")
                    .contentType(MediaType.parseMediaType(MIME_PKCS7))
                    .body(renewed);
        } catch (WellKnownException e) {
            log.warn("Rejected CN={}: {}", deviceId, e.getMessage());
            return ResponseEntity.status(e.getHttpStatus()).build();
        } catch (Exception e) {
            log.error("Internal error CN={}", deviceId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    private X509Certificate extractClientCert(HttpServletRequest request) {
        X509Certificate[] certs = (X509Certificate[])
                request.getAttribute("jakarta.servlet.request.X509Certificate");
        return (certs != null && certs.length > 0) ? certs[0] : null;
    }

    private String extractCN(X509Certificate cert) {
        for (String part : cert.getSubjectX500Principal().getName().split(",")) {
            part = part.trim();
            if (part.startsWith("CN=")) return part.substring(3);
        }
        return cert.getSubjectX500Principal().getName();
    }
}
