package com.device.service.service;

import com.device.service.WellKnownException;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.cert.jcajce.JcaCertStore;
import org.bouncycastle.cms.CMSProcessableByteArray;
import org.bouncycastle.cms.CMSSignedData;
import org.bouncycastle.cms.CMSSignedDataGenerator;
import org.bouncycastle.cms.CMSTypedData;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;

@Service
@Slf4j
public class WellKnownService {
    private final CaService caService;
    private static final int RENEWAL_DAYS = 30;

    public WellKnownService(CaService caService) {
        this.caService = caService;
    }

    public String getCaCertsPkcs7Base64() throws Exception {
        return toPkcs7Base64(caService.getCaCert());
    }

    public String enroll(String csrBase64, X509Certificate bootstrapCert) throws Exception {
        PKCS10CertificationRequest csr = parseCsr(csrBase64);
        validateCsrSignature(csr);
        String bootstrapCN = extractCN(bootstrapCert);
        String csrCN = extractCNFromX500Name(csr.getSubject().toString());
        log.debug("enroll: bootstrapCN={} csrCN={}", bootstrapCN, csrCN);

        X509Certificate issued = caService.signCsr(csr);
        return toPkcs7Base64(issued);
    }

    public String renew(String csrBase64, X509Certificate activeCert) throws Exception {
        try{
            activeCert.checkValidity();
        } catch (Exception e) {
            throw new WellKnownException("Certificate expired", HttpStatus.FORBIDDEN, e);
        }
        PKCS10CertificationRequest csr = parseCsr(csrBase64);
        validateCsrSignature(csr);

        String activeCN = extractCN(activeCert);
        String csrCN    = extractCNFromX500Name(csr.getSubject().toString());
        if (!activeCN.equals(csrCN)) {
            throw new WellKnownException(
                    "CN in CSR (" + csrCN + ") does not match with active certificate (" + activeCN + ")",
                    HttpStatus.BAD_REQUEST);
        }

        X509Certificate renewed = caService.signCsr(csr);
        return toPkcs7Base64(renewed);
    }

    private String toPkcs7Base64(X509Certificate cert) throws Exception {
        CMSSignedDataGenerator gen = new CMSSignedDataGenerator();
        gen.addCertificates(new JcaCertStore(List.of(cert)));

        CMSTypedData msg = new CMSProcessableByteArray(new byte[0]);
        CMSSignedData signedData = gen.generate(msg, false);

        return Base64.getEncoder().encodeToString(signedData.getEncoded());
    }

    private String extractCN(X509Certificate cert) {
        for (String part : cert.getSubjectX500Principal().getName().split(",")) {
            part = part.trim();
            if (part.startsWith("CN=")) return part.substring(3);
        }
        return cert.getSubjectX500Principal().getName();
    }

    private String extractCNFromX500Name(String dn) {
        for (String part : dn.split(",")) {
            part = part.trim();
            if (part.startsWith("CN=") || part.startsWith("cn=")) return part.substring(3);
        }
        return dn;
    }

    private PKCS10CertificationRequest parseCsr(String csrBase64) {
        try {
            String cleaned = csrBase64
                    .replace("-----BEGIN CERTIFICATE REQUEST-----", "")
                    .replace("-----END CERTIFICATE REQUEST-----", "")
                    .replaceAll("\\s+", "");

            byte[] derBytes = Base64.getDecoder().decode(cleaned);
            return new PKCS10CertificationRequest(derBytes);
        } catch (Exception e) {
            throw new WellKnownException("Invalid format CSR: " + e.getMessage(),
                    HttpStatus.BAD_REQUEST, e);
        }
    }
    private void validateCsrSignature(PKCS10CertificationRequest csr) {
        try {
            boolean valid = csr.isSignatureValid(
                    new org.bouncycastle.operator.jcajce.JcaContentVerifierProviderBuilder()
                            .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                            .build(csr.getSubjectPublicKeyInfo())
            );
            if (!valid) {
                throw new WellKnownException("Invalid CSR sign", HttpStatus.BAD_REQUEST);
            }
        } catch (WellKnownException e) {
            throw e;
        } catch (Exception e) {
            throw new WellKnownException("Error during CSR verification:" + e.getMessage(),
                    HttpStatus.BAD_REQUEST, e);
        }
    }
}