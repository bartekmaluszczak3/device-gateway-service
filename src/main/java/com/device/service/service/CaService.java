package com.device.service.service;

import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import java.security.cert.X509Certificate;

public interface CaService {
    X509Certificate signCsr(PKCS10CertificationRequest csr) throws Exception;
    X509Certificate getCaCert();
}
