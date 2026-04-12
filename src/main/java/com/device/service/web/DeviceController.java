package com.device.service.web;

import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.cert.X509Certificate;
import java.util.Map;

@RestController
@RequestMapping("/api/devices")
@Slf4j
public class DeviceController {

    @PostMapping("/data")
    public ResponseEntity<Void> receiveData(@RequestBody Map<String, Object> payload, HttpServletRequest request){
        X509Certificate certificate = extract(request);

        if(certificate == null){
            log.error("A request without a client certificate reached the controller");
            return ResponseEntity.status(401).build();
        }
        System.out.println(payload);
        return ResponseEntity.status(204).build();
    }

    private X509Certificate extract(HttpServletRequest request) {
        X509Certificate[] certs = (X509Certificate[])
                request.getAttribute("jakarta.servlet.request.X509Certificate");
        return (certs != null && certs.length > 0) ? certs[0] : null;
    }
}
