package utils;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.math.BigInteger;
import java.security.*;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

public class CertificateGenerator {

    static {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static final int    KEY_SIZE             = 2048;
    private static final String BC_PROVIDER          = BouncyCastleProvider.PROVIDER_NAME;

    public record CertAndKey(X509Certificate cert, PrivateKey privateKey) {}


    public static KeyPair generateKeyPair() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA", BC_PROVIDER);
        kpg.initialize(KEY_SIZE, new SecureRandom());
        return kpg.generateKeyPair();
    }

    public static CertAndKey generateCA(String cn, int days) throws Exception {
        KeyPair kp = generateKeyPair();
        X500Name subject = new X500Name("CN=" + cn + ",O=Test,C=PL");

        Instant now       = Instant.now();
        Date    notBefore = Date.from(now.minus(1, ChronoUnit.MINUTES));
        Date    notAfter  = Date.from(now.plus(days, ChronoUnit.DAYS));

        ContentSigner signer = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
                .setProvider(BC_PROVIDER)
                .build(kp.getPrivate());

        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                subject,
                BigInteger.probablePrime(64, new SecureRandom()),
                notBefore, notAfter,
                subject,
                kp.getPublic()
        );

        JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();

        builder.addExtension(Extension.basicConstraints, true,
                new BasicConstraints(true));
        builder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
        builder.addExtension(Extension.subjectKeyIdentifier, false,
                extUtils.createSubjectKeyIdentifier(kp.getPublic()));
        builder.addExtension(Extension.authorityKeyIdentifier, false,
                extUtils.createAuthorityKeyIdentifier(kp.getPublic()));

        X509CertificateHolder holder = builder.build(signer);
        X509Certificate cert = new JcaX509CertificateConverter()
                .setProvider(BC_PROVIDER)
                .getCertificate(holder);

        return new CertAndKey(cert, kp.getPrivate());
    }

    public static CertAndKey generateSignedCert(String cn, boolean isServer,
                                                CertAndKey ca, int days) throws Exception {
        KeyPair kp = generateKeyPair();
        X500Name issuer  = X500Name.getInstance(ca.cert().getSubjectX500Principal().getEncoded());
        X500Name subject = new X500Name("CN=" + cn + ",O=Test,C=PL");

        Instant now       = Instant.now();
        Date    notBefore = Date.from(now.minus(1, ChronoUnit.MINUTES));
        Date    notAfter  = Date.from(now.plus(days, ChronoUnit.DAYS));

        ContentSigner signer = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
                .setProvider(BC_PROVIDER)
                .build(ca.privateKey());

        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                issuer,
                BigInteger.probablePrime(64, new SecureRandom()),
                notBefore, notAfter,
                subject,
                kp.getPublic()
        );

        JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();

        builder.addExtension(Extension.basicConstraints, false,
                new BasicConstraints(false));

        int usage = isServer
                ? KeyUsage.digitalSignature | KeyUsage.keyEncipherment
                : KeyUsage.digitalSignature;
        builder.addExtension(Extension.keyUsage, true, new KeyUsage(usage));

        builder.addExtension(Extension.subjectKeyIdentifier, false,
                extUtils.createSubjectKeyIdentifier(kp.getPublic()));
        builder.addExtension(Extension.authorityKeyIdentifier, false,
                extUtils.createAuthorityKeyIdentifier(ca.cert().getPublicKey()));

        X509CertificateHolder holder = builder.build(signer);
        X509Certificate cert = new JcaX509CertificateConverter()
                .setProvider(BC_PROVIDER)
                .getCertificate(holder);

        return new CertAndKey(cert, kp.getPrivate());
    }

    public static KeyStore buildKeyStore(String alias, char[] password,
                                         CertAndKey certAndKey,
                                         X509Certificate... chain) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12", BC_PROVIDER);
        ks.load(null, password);

        X509Certificate[] fullChain = new X509Certificate[1 + chain.length];
        fullChain[0] = certAndKey.cert();
        System.arraycopy(chain, 0, fullChain, 1, chain.length);

        ks.setKeyEntry(alias, certAndKey.privateKey(), password, fullChain);
        return ks;
    }

    public static KeyStore buildTrustStore(char[] password,
                                           X509Certificate... trustedCAs) throws Exception {
        KeyStore ts = KeyStore.getInstance("PKCS12", BC_PROVIDER);
        ts.load(null, password);
        for (int i = 0; i < trustedCAs.length; i++) {
            ts.setCertificateEntry("ca-" + i, trustedCAs[i]);
        }
        return ts;
    }
}