package com.weserve.APM.EDI;

import com.navis.external.edi.entity.AbstractEdiLoadInterceptor;

import java.io.Serializable;
import java.util.StringTokenizer;

/**
 * Created by bgopal on 6/14/2017.
 */
public class RTMCoparnLoadInterceptor extends AbstractEdiLoadInterceptor {

    public String beforeEdiLoad(String inFileAsString, Serializable inEdiBatchGkey, String inDelimiter) {
        try {
            return getLibrary("RTMEDILoadUtil").processInboundFile(inFileAsString, inDelimiter,"CA","SH","TO","CU");
        } catch (Exception inException) {
            return inFileAsString;
        }
    }
}

