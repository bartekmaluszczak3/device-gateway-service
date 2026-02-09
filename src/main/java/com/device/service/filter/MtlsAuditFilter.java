package com.device.service.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.security.cert.X509Certificate;

@Slf4j
@Component
public class MtlsAuditFilter extends OncePerRequestFilter {
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        X509Certificate[] certs =
                (X509Certificate[]) request.getAttribute(
                        "jakarta.servlet.request.X509Certificate"
                );

        if (certs != null) {
            X509Certificate cert = certs[0];
            log.debug("mTLS OK | CN={} | Serial={} | IP={}",
                    cert.getSubjectX500Principal().getName(),
                    cert.getSerialNumber(),
                    request.getRemoteAddr());
        }

        filterChain.doFilter(request, response);
    }
}
