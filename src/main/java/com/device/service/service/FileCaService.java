package com.device.service.service;

import com.device.service.config.WellKnowConfiguration;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.math.BigInteger;
import java.security.*;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class FileCaService implements CaService{

    static {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }
    private WellKnowConfiguration wellKnowConfiguration;
    private X509Certificate caCert;
    private PrivateKey      caPrivateKey;

    private final AtomicLong serialCounter = new AtomicLong(
            System.currentTimeMillis()
    );

    public FileCaService(WellKnowConfiguration wellKnowConfiguration) throws Exception  {
        this.wellKnowConfiguration = wellKnowConfiguration;
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try(InputStream inputStream = wellKnowConfiguration.getCaKeyResource().getInputStream()) {
            keyStore.load(inputStream, wellKnowConfiguration.getCaKeyStorePassword().toCharArray());
            caCert = (X509Certificate) keyStore.getCertificate(wellKnowConfiguration.getCaKeyAlias());
            caPrivateKey = (PrivateKey) keyStore.getKey(wellKnowConfiguration.getCaKeyAlias(), wellKnowConfiguration.getCaKeyStorePassword().toCharArray());
        } if (caCert == null || caPrivateKey == null) {
            throw new IllegalStateException("CA key/certificate not found under alias: ");
        }
    }

    public X509Certificate signCsr(PKCS10CertificationRequest csr) throws Exception {
        X500Name issuer = X500Name.getInstance(caCert.getSubjectX500Principal().getEncoded());
        Instant now = Instant.now();
        Date notBefore = Date.from(now);
        Date notAfter = Date.from(now.plus(wellKnowConfiguration.getCertValidityDays(), ChronoUnit.DAYS));
        BigInteger serial = BigInteger.valueOf(serialCounter.incrementAndGet());
        X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(
                issuer,
                serial,
                notBefore,
                notAfter,
                csr.getSubject(),
                csr.getSubjectPublicKeyInfo()
        );
        JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();
        PublicKey subjectPublicKey = BouncyCastleProvider.getPublicKey(csr.getSubjectPublicKeyInfo());

        certBuilder.addExtension(Extension.basicConstraints, false, new BasicConstraints(false));
        certBuilder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
        certBuilder.addExtension(Extension.extendedKeyUsage, false,
                new ExtendedKeyUsage(KeyPurposeId.id_kp_clientAuth));
        certBuilder.addExtension(Extension.subjectKeyIdentifier, false,
                extUtils.createSubjectKeyIdentifier(subjectPublicKey));
        certBuilder.addExtension(Extension.authorityKeyIdentifier, false,
                extUtils.createAuthorityKeyIdentifier(caCert.getPublicKey()));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA")
                .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                .build(caPrivateKey);

        X509CertificateHolder holder = certBuilder.build(signer);
        return new JcaX509CertificateConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME).getCertificate(holder);
    }

    public X509Certificate getCaCert() {
        return caCert;
    }
}
