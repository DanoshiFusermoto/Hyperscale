package org.radix.hyperscale.network.discovery;

import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

public final class SSLFix 
{
    private static final Logger log = Logging.getLogger();

    // This, and the code that uses it, is here to placate sonar.
    // It should always be true, unless you do not want to use TLS etc to connect to other sites
    static boolean standardVerifyResult = true;

    public static void trustAllHosts()
    {
        try
        {
            TrustManager[] trustAllCerts = new TrustManager[]
            {
            	new X509ExtendedTrustManager()
                {
                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers()
                    {
                        return null;
                    }

                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) throws CertificateException
                    {
                    	if (standardVerifyResult == false)
                    		throw new CertificateException("Client not trusted by default.");
                    }

                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) throws CertificateException
                    {
                    	if (standardVerifyResult == false)
                    		throw new CertificateException("Server not trusted by default.");
                    }

                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] xcs, String string, Socket socket) throws CertificateException
                    {

                    }

                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] xcs, String string, Socket socket) throws CertificateException
                    {

                    }

                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] xcs, String string, SSLEngine ssle) throws CertificateException
                    {

                    }

                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] xcs, String string, SSLEngine ssle) throws CertificateException
                    {

                    }
                }
            };

            SSLContext sc = SSLContext.getInstance("TLSv1.2");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

            // Install the all-trusting host verifier
            HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> standardVerifyResult);
        }
        catch (GeneralSecurityException e)
        {
            log.error("Error occurred",e);
        }
    }
    
    private SSLFix() {}
}
