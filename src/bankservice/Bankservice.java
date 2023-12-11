package bankservice;

/**
 *
 * @author kadeniran
 */
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;

public class Bankservice {
    
    private DBManager dM = null;
    
    private Bankservice() {
        
        dM = DBManager.getDBManager();
    }

    private void api() {

        try {

            // Bind http to port 5645            
            InetSocketAddress inetAddress = new InetSocketAddress(4843);
            
            HttpServer httpServer = HttpServer.create(inetAddress, 0);

            // Bind https to port 3356            
            InetSocketAddress inetAddress2 = new InetSocketAddress(4844);

            //initialize the HTTPS server
            HttpsServer HTTPS_Server = HttpsServer.create(inetAddress2, 0);
            SSLContext SSL_Context = SSLContext.getInstance("TLS");

            // initialise the keystore
            char[] Password = "57xz4ye0r9ize3se".toCharArray();
            KeyStore Key_Store = KeyStore.getInstance("JKS");
            FileInputStream Input_Stream = new FileInputStream("mservices.jks");
            Key_Store.load(Input_Stream, Password);

            // setup the key manager factory
            KeyManagerFactory Key_Manager = KeyManagerFactory.getInstance("SunX509");
            Key_Manager.init(Key_Store, Password);

            // setup the trust manager factory
            TrustManagerFactory Trust_Manager = TrustManagerFactory.getInstance("SunX509");
            Trust_Manager.init(Key_Store);

            // setup the HTTPS context and parameters
            SSL_Context.init(Key_Manager.getKeyManagers(), Trust_Manager.getTrustManagers(), null);

            HTTPS_Server.setHttpsConfigurator(new HttpsConfigurator(SSL_Context) {
                public void configure(HttpsParameters params) {
                    try {
                        // initialise the SSL context
                        SSLContext SSL_Context = getSSLContext();
                        SSLEngine SSL_Engine = SSL_Context.createSSLEngine();
                        params.setNeedClientAuth(false);
                        params.setCipherSuites(SSL_Engine.getEnabledCipherSuites());
                        params.setProtocols(SSL_Engine.getEnabledProtocols());

                        // Set the SSL parameters
                        SSLParameters SSL_Parameters = SSL_Context.getSupportedSSLParameters();
                        params.setSSLParameters(SSL_Parameters);

                        System.out.println("The HTTPS server is connected");

                    } catch (Exception ex) {

                        System.out.println("Failed to create the HTTPS port");
                    }
                }
            });

            HTTPS_Server.createContext("/comms", new RequestHandler());
            httpServer.createContext("/comms", new RequestHandler());
            
            HTTPS_Server.setExecutor(null); // creates a default executor
            HTTPS_Server.start();
            
            httpServer.start();

        } catch (Exception ex) {

            System.out.println("Formservice:api:" + ex);
        }
    }

    private String extractString(String toExtract, String delim, int tokenCount) {

        String eString = "";
        int dP = 0, pDp = -1, cStart = 0, pStart = 0, dif = 0;

        for (int a = 0; a < tokenCount; a++) {

            dP = toExtract.indexOf(delim, cStart);

            if (dP != -1) {

                dif = dP - pDp;

                if (dif < 2) {

                    a--;

                }

                pStart = cStart;
                cStart = dP + 1;
                pDp = dP;

            } else {

                if (a > 0) {

                    if (a == tokenCount - 1) {

                        pStart = cStart;
                        dP = toExtract.length();
                    }

                } else {

                    dP = -1;
                }

                break;
            }
        }

        if (dP != -1) {

            eString = toExtract.substring(pStart, dP);
        }

        return eString;
    }

    public static void main(String[] args) {

        Bankservice bks = new Bankservice();

        try {

            bks.api();

            System.out.println("!!Bankservice has started!!".toUpperCase());

        } catch (Exception ex) {

            System.out.println("Could not start the Bankservice:" + ex);
        }
    }
    
}
