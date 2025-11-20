package com.example;

import com.telefonica.cose.provenance.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Signer {



    public String sign(String value) throws Exception {

        JSONSignatureInterface sign = new JSONSignature();
        Parameters params = new Parameters();

        return sign.signing(value, params.getProperty("kid"));
    }
}
