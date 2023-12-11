package bankservice;

/**
 *
 * @author kadeniran
 */
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RequestHandler implements HttpHandler {

    private Processor pr = null;
    private DBManager dM = null;
    String payload = "";
    String resp = "";
    int respCode = 0;

    private static final String[] IP_HEADER_CANDIDATES = {
        "X-Forwarded-For",
        "Proxy-Client-IP",
        "WL-Proxy-Client-IP",
        "HTTP_X_FORWARDED_FOR",
        "HTTP_X_FORWARDED",
        "HTTP_X_CLUSTER_CLIENT_IP",
        "HTTP_CLIENT_IP",
        "HTTP_FORWARDED_FOR",
        "HTTP_FORWARDED",
        "HTTP_VIA",
        "REMOTE_ADDR"};

    @Override
    public void handle(HttpExchange he) {

//        System.out.println("IN handle: ");
        this.pr = Processor.getProcessor();
        this.dM = DBManager.getDBManager();

        he.getResponseHeaders().add("Access-Control-Allow-Origin", "*");

//        System.out.println("OUT pr: ");
        ObjectMapper mapper = new ObjectMapper();

        System.out.println("Serving the request");

        try {

            // Serve for POST requests only
            if (he.getRequestMethod().equalsIgnoreCase("POST")) {

//                System.out.println("RequestHandler:handle:POST:IN");
                try {

                    // REQUEST Headers
                    Headers requestHeaders = he.getRequestHeaders();
                    Set<Map.Entry<String, List<String>>> entries = requestHeaders.entrySet();

                    int contentLength = Integer.parseInt(requestHeaders.getFirst("Content-length"));
                    String headerAuth = requestHeaders.getFirst("Authorization");

                    // REQUEST Body
                    InputStream is = he.getRequestBody();

                    BufferedReader reader = null;
                    byte[] data = null;
                    reader = new BufferedReader(new InputStreamReader((is)));

                    String tmpStr = null;
                    String resp = "";

                    while ((tmpStr = reader.readLine()) != null) {

                        resp += tmpStr;
                    }

                    System.out.println("resp: " + resp);

                    this.payload = resp;

                    if ((headerAuth != null) && (!headerAuth.isEmpty())) {

//                    System.out.println("RequestHandler:handle:headerAuth:IN");
                        String hAuth = extractString(headerAuth, " ", 2);

//                    System.out.println("RequestHandler:handle:hAuth:" + hAuth);
                        CopyOnWriteArrayList<String> ret = new CopyOnWriteArrayList<String>();
                        String eSalt = "", eIV = "", ePass = "", env = "", dData = "";

                        this.dM.retrieveRecords(ret, "encryption_salt, encryption_iv, encryption_password, environment", "api_clients", " where secret_key ='" + hAuth + "'");

                        System.out.println("RequestHandler:handle:ret.size():" + ret.size());

                        if (ret.size() > 0) {

                            eSalt = extractString(ret.get(0), "Ð", 1);
                            eIV = extractString(ret.get(0), "Ð", 2);
                            ePass = extractString(ret.get(0), "Ð", 3);
                            env = extractString(ret.get(0), "Ð", 4);
                            
                            System.out.println("RequestHandler:handle:authorized:IN");
                            this.resp = this.pr.processRequest(hAuth, this.payload, env);

                            this.respCode = HttpURLConnection.HTTP_OK;
                            
                        } else {

                            HashMap<String, String> payload = new HashMap<String, String>();

                            payload.put("response", "Unauthorised Access");

                            this.resp = mapper.writeValueAsString(payload);

                            this.respCode = HttpURLConnection.HTTP_UNAUTHORIZED;
                        }

                    } else {

                        HashMap<String, String> payload = new HashMap<String, String>();

                        payload.put("response", "Unauthorised Access");

                        this.resp = mapper.writeValueAsString(payload);

                        this.respCode = HttpURLConnection.HTTP_UNAUTHORIZED;
                    }

                    data = this.resp.getBytes();
                    contentLength = data.length;

                    he.getResponseHeaders().set("Content-Type", "application/json");
                    he.sendResponseHeaders(this.respCode, contentLength);

                    // RESPONSE Body
                    OutputStream os = he.getResponseBody();

                    os.write(data);

                    he.close();

                } catch (Exception e) {

                    HashMap<String, String> payload = new HashMap<String, String>();

                    payload.put("response", "Unauthorised Access");

                    this.resp = mapper.writeValueAsString(payload);
                    this.respCode = HttpURLConnection.HTTP_UNAUTHORIZED;

                    byte[] data = resp.getBytes();
                    int contentLength = data.length;

                    he.getResponseHeaders().set("Content-Type", "application/json");
                    he.sendResponseHeaders(this.respCode, contentLength);

                    // RESPONSE Body
                    OutputStream os = he.getResponseBody();

                    os.write(data);

                    he.close();

                    e.printStackTrace();
                }

            } else if (he.getRequestMethod().equalsIgnoreCase("OPTIONS")) {

                System.out.println("RequestHandler OPTIONS");

                he.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS, POST");
                he.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type, Authorization");

                he.sendResponseHeaders(200, -1);
                he.getResponseBody().close();
                return;

            } else if (he.getRequestMethod().equalsIgnoreCase("GET")) {

                System.out.println("RequestHandler GET");

                int contentLength = 0;
                byte[] data = null;
                
                int respCode = HttpURLConnection.HTTP_OK;
                String resp = "Good";

                data = resp.getBytes();
                contentLength = data.length;

                he.getResponseHeaders().set("Content-Type", "application/json");
                he.sendResponseHeaders(respCode, contentLength);

                // RESPONSE Body
                OutputStream os = he.getResponseBody();

                os.write(data);

                he.close();

            } else {

                System.out.println("RequestHandler else");
            }

        } catch (Exception ex) {

            System.out.println("RequestHandler:handle:" + ex);
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

}