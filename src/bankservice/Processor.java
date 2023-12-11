package bankservice;

/**
 *
 * @author kadeniran
 */
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class Processor {

    private static volatile Processor pr = null;
    private DBManager dM = null;

    private Processor() {

        //Prevent form the reflection api.
        if (pr != null) {
            throw new RuntimeException("Use getInstance() method to get the single instance of this class.");
        }

        this.dM = DBManager.getDBManager();
    }

    public static Processor getProcessor() {

        //Double check locking pattern
        if (pr == null) { //Check for the first time

            synchronized (Processor.class) {   //Check for the second time.

                //if there is no instance available... create new one
                if (pr == null) {

                    pr = new Processor();
                }
            }
        }

        return pr;
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

    public String processRequest(String sKey, String input, String env) {

        String res = "", req = "";

        ObjectMapper mapper = new ObjectMapper();

        try {

            JsonNode jInp = mapper.readTree(input);

            req = jInp.at("/request").asText();

            System.out.println("Processor:processRequestreq:" + req);

            if (req.compareToIgnoreCase("GAC") == 0) {

                res = this.getAPIClients(env);

            } else if (req.compareToIgnoreCase("CAC") == 0) {

                res = this.createAccount(input);

            } else if (req.compareToIgnoreCase("RAI") == 0) {

                res = this.retrieveAccountInfo(input);

            } else if (req.compareToIgnoreCase("RCA") == 0) {

                res = this.retrieveCustomerAccounts(input);

            } else if (req.compareToIgnoreCase("RAA") == 0) {

                res = this.retrieveAllAccountInfo();

            } else if (req.compareToIgnoreCase("DEP") == 0) {

                res = this.depositToAccount(input);

            } else if (req.compareToIgnoreCase("WTD") == 0) {

                res = this.withdrawal(input);

            } else if (req.compareToIgnoreCase("TRF") == 0) {

                res = this.transferToAccount(input);
            }

        } catch (Exception ex) {

            System.out.println("Processor:processRequest:Exception: " + ex);
        }

        return res;
    }

    private String getAPIClients(String env) {

        String ret = "DONE";

        ObjectMapper mapper = new ObjectMapper();
        InputStream is = null;
        BufferedReader reader = null;
        String cAt = "", mAt = "", nam = "", sKey = "", aIPs = "", eIV = "",
                ePas = "", eSa = "", user = "", pass = "", hKey = "", aReq = "";

        ArrayNode jSnd = mapper.createArrayNode();

        try {

            CopyOnWriteArrayList<String> res = new CopyOnWriteArrayList<String>();

            this.dM.retrieveRecords(res, "*", "api_clients", " where environment ='" + env + "'");

            System.out.println("res.size(): " + res.size());

            if (res.size() > 0) {

                for (int i = 0; i < res.size(); i++) {

                    cAt = extractString(res.get(i), "Ð", 3);
                    mAt = extractString(res.get(i), "Ð", 4);
                    nam = extractString(res.get(i), "Ð", 5);
                    sKey = extractString(res.get(i), "Ð", 6);
                    aIPs = extractString(res.get(i), "Ð", 7);
                    eIV = extractString(res.get(i), "Ð", 8);
                    ePas = extractString(res.get(i), "Ð", 9);
                    eSa = extractString(res.get(i), "Ð", 10);
                    user = extractString(res.get(i), "Ð", 11);
                    pass = extractString(res.get(i), "Ð", 12);
                    hKey = extractString(res.get(i), "Ð", 13);
                    aReq = extractString(res.get(i), "Ð", 14);

                    System.out.println("nam: " + nam);

                    ObjectNode payload = mapper.createObjectNode();

                    payload.put("create_at", cAt);
                    payload.put("modified_at", mAt);
                    payload.put("name", nam);
                    payload.put("secret_key", sKey);
                    payload.put("allowable_ips", aIPs);
                    payload.put("encryption_iv", eIV);
                    payload.put("encryption_password", ePas);
                    payload.put("encryption_salt", eSa);
                    payload.put("username", user);
                    payload.put("password", pass);
                    payload.put("hash_key", hKey);
                    payload.put("allowable_requests", aReq);

                    jSnd.add(payload);
                }
            }

            ret = mapper.writeValueAsString(jSnd);

            System.out.println("ret: " + ret);

        } catch (Exception ex) {

            System.out.println("Processor:getAPIClients:Exception: " + ex);
        }

        return ret;
    }

    private String createAccount(String request) {

        String res = "";
        String gen = "", fNam = "", mNam = "", sName = "", dob = "", nation = "",
                nationID = "", rAdd = "", rState = "", lGov = "", soo = "", mNum = "",
                ema = "", mStatus = "", bvn = "", accNum = "0696", aType = "";

        String digits = "0123456789";
        int p = 0;

        Random rand = new Random();
        ObjectMapper mapper = new ObjectMapper();

        try {

            JsonNode client = mapper.readTree(request);

            gen = client.at("/gender").asText();
            fNam = client.at("/first_name").asText();
            mNam = client.at("/middle_name").asText();
            sName = client.at("/surname").asText();
            dob = client.at("/date_of_birth").asText();
            nation = client.at("/nationality").asText();
            nationID = client.at("/national_identity_number").asText();
            rAdd = client.at("/residential_address").asText();
            rState = client.at("/residential_state").asText();
            lGov = client.at("/local_government").asText();
            soo = client.at("/state_of_origin").asText();
            mNum = client.at("/mobile_number").asText();
            ema = client.at("/email").asText();
            mStatus = client.at("/marital_status").asText();
            bvn = client.at("/bvn").asText();
            aType = client.at("/account_type").asText();

//            System.out.println("nam:" + nam);
//            System.out.println("aIPs:" + aIPs);
            LocalDateTime cAt = LocalDateTime.now();
            LocalDateTime mAt = cAt;

            for (int t = 0; t < 6; t++) {

                p = rand.nextInt(digits.length());

                accNum += digits.charAt(p);
            }

            System.out.println("accNum:" + accNum);

            String attrib = "ACTIVE" + "Ð" + cAt + "Ð" + mAt + "Ð" + gen + "Ð" + fNam
                    + "Ð" + mNam + "Ð" + sName + "Ð" + dob + "Ð" + nation + "Ð" + nationID
                    + "Ð" + rAdd + "Ð" + rState + "Ð" + lGov + "Ð" + soo + "Ð" + mNum
                    + "Ð" + ema + "Ð" + mStatus + "Ð" + bvn;

            String cID = this.dM.insertRecord("customer_info", attrib);

            attrib = "ACTIVE" + "Ð" + cAt + "Ð" + mAt + "Ð" + cID + "Ð" + aType
                    + "Ð" + accNum + "Ð" + "0.0" + "Ð" + "0.0";

            this.dM.insertRecord("customer_accounts", attrib);

            HashMap<String, String> resPayload = new HashMap<String, String>();

            resPayload.put("first_name", fNam);
            resPayload.put("middle_name", mNam);
            resPayload.put("surname", sName);
            resPayload.put("bvn", bvn);
            resPayload.put("account_type", aType);
            resPayload.put("account_number", accNum);

            res = mapper.writeValueAsString(resPayload);

        } catch (Exception ex) {

            System.out.println("Processor:createAccount:Exception: " + ex);
        }

        return res;
    }

    private String retrieveAllAccountInfo() {

        String ret = "DONE";

        ObjectMapper mapper = new ObjectMapper();
        InputStream is = null;
        BufferedReader reader = null;
        String cAt = "", mAt = "", gen = "", fNam = "", mNam = "", sName = "", dob = "", nation = "",
                nationID = "", rAdd = "", rState = "", lGov = "", soo = "", mNum = "",
                ema = "", mStatus = "", bvn = "", accNum = "0696", aType = "", cID = "";

        ArrayNode jSnd = mapper.createArrayNode();

        try {

            CopyOnWriteArrayList<String> res = new CopyOnWriteArrayList<String>();

            this.dM.retrieveRecords(res, "*", "customer_info", "");

            System.out.println("res.size(): " + res.size());

            if (res.size() > 0) {

                for (int i = 0; i < res.size(); i++) {

                    cID = extractString(res.get(i), "Ð", 1);
                    cAt = extractString(res.get(i), "Ð", 3);
                    mAt = extractString(res.get(i), "Ð", 4);
                    gen = extractString(res.get(i), "Ð", 5);
                    fNam = extractString(res.get(i), "Ð", 6);
                    mNam = extractString(res.get(i), "Ð", 7);
                    sName = extractString(res.get(i), "Ð", 8);
                    dob = extractString(res.get(i), "Ð", 9);
                    nation = extractString(res.get(i), "Ð", 10);
                    nationID = extractString(res.get(i), "Ð", 11);
                    rAdd = extractString(res.get(i), "Ð", 12);
                    rState = extractString(res.get(i), "Ð", 13);
                    lGov = extractString(res.get(i), "Ð", 14);
                    soo = extractString(res.get(i), "Ð", 15);
                    mNum = extractString(res.get(i), "Ð", 16);
                    ema = extractString(res.get(i), "Ð", 17);
                    mStatus = extractString(res.get(i), "Ð", 18);
                    bvn = extractString(res.get(i), "Ð", 19);

                    System.out.println("bvn: " + bvn);

                    ObjectNode payload = mapper.createObjectNode();

                    payload.put("customer_id", cID);
                    payload.put("create_at", cAt);
                    payload.put("modified_at", mAt);
                    payload.put("gender", gen);
                    payload.put("first_name", fNam);
                    payload.put("middle_name", mNam);
                    payload.put("surname", sName);
                    payload.put("date_of_birth", dob);
                    payload.put("nationality", nation);
                    payload.put("national_identity_number", nationID);
                    payload.put("residential_address", rAdd);
                    payload.put("residential_state", rState);
                    payload.put("local_government", lGov);
                    payload.put("state_of_origin", soo);
                    payload.put("mobile_number", mNum);
                    payload.put("email", ema);
                    payload.put("marital_status", mStatus);
                    payload.put("bvn", bvn);

                    jSnd.add(payload);
                }
            }

            ret = mapper.writeValueAsString(jSnd);

            System.out.println("ret: " + ret);

        } catch (Exception ex) {

            System.out.println("Processor:retrieveAccountInfo:Exception: " + ex);
        }

        return ret;
    }

    private String retrieveAccountInfo(String request) {

        String ret = "DONE";

        ObjectMapper mapper = new ObjectMapper();
        InputStream is = null;
        BufferedReader reader = null;
        String cAt = "", mAt = "", gen = "", fNam = "", mNam = "", sName = "", dob = "", nation = "",
                nationID = "", rAdd = "", rState = "", lGov = "", soo = "", mNum = "",
                ema = "", mStatus = "", bvn = "", accNum = "0696", aType = "", cID = "";

        ArrayNode jSnd = mapper.createArrayNode();
        ObjectNode payload = null;

        try {

            JsonNode client = mapper.readTree(request);

            cID = client.at("/customer_id").asText();

            CopyOnWriteArrayList<String> res = new CopyOnWriteArrayList<String>();

            this.dM.retrieveRecords(res, "*", "customer_info", " where id =" + cID + "");

            System.out.println("res.size(): " + res.size());

            if (res.size() > 0) {

                cID = extractString(res.get(0), "Ð", 1);
                cAt = extractString(res.get(0), "Ð", 3);
                mAt = extractString(res.get(0), "Ð", 4);
                gen = extractString(res.get(0), "Ð", 5);
                fNam = extractString(res.get(0), "Ð", 6);
                mNam = extractString(res.get(0), "Ð", 7);
                sName = extractString(res.get(0), "Ð", 8);
                dob = extractString(res.get(0), "Ð", 9);
                nation = extractString(res.get(0), "Ð", 10);
                nationID = extractString(res.get(0), "Ð", 11);
                rAdd = extractString(res.get(0), "Ð", 12);
                rState = extractString(res.get(0), "Ð", 13);
                lGov = extractString(res.get(0), "Ð", 14);
                soo = extractString(res.get(0), "Ð", 15);
                mNum = extractString(res.get(0), "Ð", 16);
                ema = extractString(res.get(0), "Ð", 17);
                mStatus = extractString(res.get(0), "Ð", 18);
                bvn = extractString(res.get(0), "Ð", 19);

                System.out.println("bvn: " + bvn);

                payload = mapper.createObjectNode();

                payload.put("create_at", cAt);
                payload.put("modified_at", mAt);
                payload.put("gender", gen);
                payload.put("first_name", fNam);
                payload.put("middle_name", mNam);
                payload.put("surname", sName);
                payload.put("date_of_birth", dob);
                payload.put("nationality", nation);
                payload.put("national_identity_number", nationID);
                payload.put("residential_address", rAdd);
                payload.put("residential_state", rState);
                payload.put("local_government", lGov);
                payload.put("state_of_origin", soo);
                payload.put("mobile_number", mNum);
                payload.put("email", ema);
                payload.put("marital_status", mStatus);
                payload.put("bvn", bvn);
            }

            ret = mapper.writeValueAsString(payload);

            System.out.println("ret: " + ret);

        } catch (Exception ex) {

            System.out.println("Processor:retrieveAccountInfo:Exception: " + ex);
        }

        return ret;
    }

    private String retrieveCustomerAccounts(String request) {

        String ret = "DONE";

        ObjectMapper mapper = new ObjectMapper();
        InputStream is = null;
        BufferedReader reader = null;
        String cAt = "", mAt = "", gen = "", fNam = "", mNam = "", sName = "", dob = "", nation = "",
                nationID = "", rAdd = "", rState = "", lGov = "", soo = "", aID = "",
                ema = "", aABal = "", aBal = "", aNum = "", aType = "", cID = "";

        ArrayNode jSnd = mapper.createArrayNode();

        try {

            JsonNode client = mapper.readTree(request);

            cID = client.at("/customer_id").asText();

            CopyOnWriteArrayList<String> res = new CopyOnWriteArrayList<String>();

            this.dM.retrieveRecords(res, "*", "customer_accounts", " where customer_info_id ='" + cID + "'");

            System.out.println("res.size(): " + res.size());

            if (res.size() > 0) {

                for (int i = 0; i < res.size(); i++) {

                    aID = extractString(res.get(i), "Ð", 1);
                    cAt = extractString(res.get(i), "Ð", 3);
                    mAt = extractString(res.get(i), "Ð", 4);
                    aType = extractString(res.get(i), "Ð", 6);
                    aNum = extractString(res.get(i), "Ð", 7);
                    aABal = extractString(res.get(i), "Ð", 8);
                    aBal = extractString(res.get(i), "Ð", 9);

                    System.out.println("aBal: " + aBal);

                    ObjectNode payload = mapper.createObjectNode();

                    payload.put("account_id", aID);
                    payload.put("create_at", cAt);
                    payload.put("modified_at", mAt);
                    payload.put("account_type", aType);
                    payload.put("account_number", aNum);
                    payload.put("account_available_balance", aABal);
                    payload.put("account_balance", aBal);

                    jSnd.add(payload);
                }
            }

            ret = mapper.writeValueAsString(jSnd);

            System.out.println("ret: " + ret);

        } catch (Exception ex) {

            System.out.println("Processor:retrieveAccountAccounts:Exception: " + ex);
        }

        return ret;
    }

    private String depositToAccount(String request) {

        String ret = "DONE";

        ObjectMapper mapper = new ObjectMapper();
        InputStream is = null;
        BufferedReader reader = null;
        String cAt = "", mAt = "", gen = "", fNam = "", mNam = "", sName = "", dob = "", nation = "",
                nationID = "", rAdd = "", rState = "", lGov = "", soo = "", amt = "",
                aID = "", aABal = "", aBal = "", aNum = "", aType = "", cID = "", tRef = "DP_";

        ObjectNode payload = null;

        BigDecimal aB = new BigDecimal("0");
        BigDecimal bal = new BigDecimal("0");

        String sChar = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        int p = 0;

        Random rand = new Random();

        try {

            JsonNode client = mapper.readTree(request);

            aNum = client.at("/account_number").asText();
            amt = client.at("/amount").asText();

            CopyOnWriteArrayList<String> res = new CopyOnWriteArrayList<String>();

            this.dM.retrieveRecords(res, "*", "customer_accounts", " where account_number ='" + aNum + "'");

            System.out.println("res.size(): " + res.size());

            if (res.size() > 0) {

                aID = extractString(res.get(0), "Ð", 1);
                cAt = extractString(res.get(0), "Ð", 3);
                mAt = extractString(res.get(0), "Ð", 4);
                cID = extractString(res.get(0), "Ð", 5);
                aType = extractString(res.get(0), "Ð", 6);
                aNum = extractString(res.get(0), "Ð", 7);
                aABal = extractString(res.get(0), "Ð", 8);
                aBal = extractString(res.get(0), "Ð", 9);

                aB = new BigDecimal(aABal);
                bal = new BigDecimal(aBal);

                aB = aB.add(new BigDecimal(amt));
                bal = bal.add(new BigDecimal(amt));

//                System.out.println("bvn: " + bvn);
                dM.updateRecord("customer_accounts", "account_available_balance", aB.toString(), "account_number", aNum, "");
                dM.updateRecord("customer_accounts", "account_balance", bal.toString(), "account_number", aNum, "");

                payload = mapper.createObjectNode();

                payload.put("account_id", aID);
                payload.put("create_at", cAt);
                payload.put("modified_at", mAt);
                payload.put("account_type", aType);
                payload.put("account_number", aNum);
                payload.put("account_available_balance", aB.toString());
                payload.put("account_balance", bal.toString());

                for (int t = 0; t < 12; t++) {

                    p = rand.nextInt(sChar.length());

                    tRef += sChar.charAt(p);
                }

                System.out.println("tRef:" + tRef);

                String attrib = "SUCCESSFUL" + "Ð" + cAt + "Ð" + mAt + "Ð" + tRef + "Ð" + amt
                        + "Ð" + cID + "Ð" + " ";

                this.dM.insertRecord("deposit_transactions", attrib);
            }

            ret = mapper.writeValueAsString(payload);

            System.out.println("ret: " + ret);

        } catch (Exception ex) {

            System.out.println("Processor:depositToAccount:Exception: " + ex);
        }

        return ret;
    }

    private String withdrawal(String request) {

        String ret = "DONE";

        ObjectMapper mapper = new ObjectMapper();
        InputStream is = null;
        BufferedReader reader = null;
        String cAt = "", mAt = "", gen = "", fNam = "", mNam = "", sName = "", dob = "", nation = "",
                nationID = "", rAdd = "", rState = "", lGov = "", soo = "", amt = "", tRef = "WT_",
                aID = "", aABal = "", aBal = "", aNum = "", aType = "", cID = "";

        ObjectNode payload = null;

        BigDecimal aB = new BigDecimal("0");
        BigDecimal bal = new BigDecimal("0");

        String sChar = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        int p = 0;

        Random rand = new Random();

        try {

            JsonNode client = mapper.readTree(request);

            aNum = client.at("/account_number").asText();
            amt = client.at("/amount").asText();

            CopyOnWriteArrayList<String> res = new CopyOnWriteArrayList<String>();

            this.dM.retrieveRecords(res, "*", "customer_accounts", " where account_number ='" + aNum + "'");

            System.out.println("res.size(): " + res.size());

            if (res.size() > 0) {

                aID = extractString(res.get(0), "Ð", 1);
                cAt = extractString(res.get(0), "Ð", 3);
                mAt = extractString(res.get(0), "Ð", 4);
                cID = extractString(res.get(0), "Ð", 5);
                aType = extractString(res.get(0), "Ð", 6);
                aNum = extractString(res.get(0), "Ð", 7);
                aABal = extractString(res.get(0), "Ð", 8);
                aBal = extractString(res.get(0), "Ð", 9);

                aB = new BigDecimal(aABal);
                bal = new BigDecimal(aBal);

                if (aB.compareTo(new BigDecimal(amt)) >= 0) {

                    aB = aB.subtract(new BigDecimal(amt));
                    bal = bal.subtract(new BigDecimal(amt));

//                System.out.println("bvn: " + bvn);
                    dM.updateRecord("customer_accounts", "account_available_balance", aB.toString(), "account_number", aNum, "");
                    dM.updateRecord("customer_accounts", "account_balance", bal.toString(), "account_number", aNum, "");

                    payload = mapper.createObjectNode();

                    payload.put("account_id", aID);
                    payload.put("create_at", cAt);
                    payload.put("modified_at", mAt);
                    payload.put("account_type", aType);
                    payload.put("account_number", aNum);
                    payload.put("account_available_balance", aB.toString());
                    payload.put("account_balance", bal.toString());

                    for (int t = 0; t < 12; t++) {

                        p = rand.nextInt(sChar.length());

                        tRef += sChar.charAt(p);
                    }

                    System.out.println("tRef:" + tRef);

                    String attrib = "SUCCESSFUL" + "Ð" + cAt + "Ð" + mAt + "Ð" + tRef
                            + "Ð" + amt + "Ð" + "0.0" + "Ð" + cID + "Ð" + " ";

                    this.dM.insertRecord("withdraw_transactions", attrib);

                } else {

                    payload = mapper.createObjectNode();

                    payload.put("response_code", "X00");
                    payload.put("response_message", "Balance insufficient to make this withdrawal");
                }
            }

            ret = mapper.writeValueAsString(payload);

            System.out.println("ret: " + ret);

        } catch (Exception ex) {

            System.out.println("Processor:withdrawal:Exception: " + ex);
        }

        return ret;
    }

    private String transferToAccount(String request) {

        String ret = "DONE";

        ObjectMapper mapper = new ObjectMapper();
        InputStream is = null;
        BufferedReader reader = null;
        String cAt = "", mAt = "", gen = "", fNam = "", mNam = "", sName = "", dob = "", nation = "",
                nationID = "", rAdd = "", rState = "", lGov = "", rANum = "", amt = "", tRef = "TF_",
                aID = "", aABal = "", aBal = "", aNum = "", aType = "", cID = "";

        ObjectNode payload = null;

        BigDecimal aB = new BigDecimal("0");
        BigDecimal bal = new BigDecimal("0");

        String sChar = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        int p = 0;

        Random rand = new Random();

        try {

            JsonNode client = mapper.readTree(request);

            aNum = client.at("/account_number").asText();
            amt = client.at("/amount").asText();
            rANum = client.at("/recipient_account_number").asText();

            CopyOnWriteArrayList<String> res = new CopyOnWriteArrayList<String>();

            this.dM.retrieveRecords(res, "*", "customer_accounts", " where account_number ='" + aNum + "'");

            System.out.println("res.size(): " + res.size());

            if (res.size() > 0) {

                aID = extractString(res.get(0), "Ð", 1);
                cAt = extractString(res.get(0), "Ð", 3);
                mAt = extractString(res.get(0), "Ð", 4);
                cID = extractString(res.get(0), "Ð", 5);
                aType = extractString(res.get(0), "Ð", 6);
                aNum = extractString(res.get(0), "Ð", 7);
                aABal = extractString(res.get(0), "Ð", 8);
                aBal = extractString(res.get(0), "Ð", 9);

                aB = new BigDecimal(aABal);
                bal = new BigDecimal(aBal);

                if (aB.compareTo(new BigDecimal(amt)) >= 0) {

                    aB = aB.subtract(new BigDecimal(amt));
                    bal = bal.subtract(new BigDecimal(amt));

//                System.out.println("bvn: " + bvn);
                    dM.updateRecord("customer_accounts", "account_available_balance", aB.toString(), "account_number", aNum, "");
                    dM.updateRecord("customer_accounts", "account_balance", bal.toString(), "account_number", aNum, "");

                    payload = mapper.createObjectNode();

                    payload.put("account_id", aID);
                    payload.put("create_at", cAt);
                    payload.put("modified_at", mAt);
                    payload.put("account_type", aType);
                    payload.put("account_number", aNum);
                    payload.put("account_available_balance", aB.toString());
                    payload.put("account_balance", bal.toString());

                    for (int t = 0; t < 12; t++) {

                        p = rand.nextInt(sChar.length());

                        tRef += sChar.charAt(p);
                    }

                    System.out.println("tRef:" + tRef);

                    String attrib = "SUCCESSFUL" + "Ð" + cAt + "Ð" + mAt + "Ð" + " " + "Ð" + tRef
                            + "Ð" + amt + "Ð" + "0.0" + "Ð" + "NGN" + "Ð" + cID + "Ð" + rANum + "Ð" + " ";

                    this.dM.insertRecord("transfer_transactions", attrib);

                    CopyOnWriteArrayList<String> res2 = new CopyOnWriteArrayList<String>();

                    this.dM.retrieveRecords(res2, "*", "customer_accounts", " where account_number ='" + rANum + "'");

                    System.out.println("res.size(): " + res.size());

                    if (res.size() > 0) {

                        aID = extractString(res2.get(0), "Ð", 1);
                        cAt = extractString(res2.get(0), "Ð", 3);
                        mAt = extractString(res2.get(0), "Ð", 4);
                        aType = extractString(res2.get(0), "Ð", 6);
                        aNum = extractString(res2.get(0), "Ð", 7);
                        aABal = extractString(res2.get(0), "Ð", 8);
                        aBal = extractString(res2.get(0), "Ð", 9);

                        aB = new BigDecimal(aABal);
                        bal = new BigDecimal(aBal);

                        aB = aB.add(new BigDecimal(amt));
                        bal = bal.add(new BigDecimal(amt));

//                System.out.println("bvn: " + bvn);
                        dM.updateRecord("customer_accounts", "account_available_balance", aB.toString(), "account_number", aNum, "");
                        dM.updateRecord("customer_accounts", "account_balance", bal.toString(), "account_number", aNum, "");
                    }

                } else {

                    payload = mapper.createObjectNode();

                    payload.put("response_code", "X00");
                    payload.put("response_message", "Balance insufficient to make this transfer");
                }
            }

            ret = mapper.writeValueAsString(payload);

            System.out.println("ret: " + ret);

        } catch (Exception ex) {

            System.out.println("Processor:transferToAccount:Exception: " + ex);
        }

        return ret;
    }
}
