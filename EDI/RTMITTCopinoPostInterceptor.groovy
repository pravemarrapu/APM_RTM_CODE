package com.weserve.APM.EDI

import java.util.Map;

import org.apache.xmlbeans.XmlObject;

import com.navis.external.edi.entity.AbstractEdiPostInterceptor;

/**
 * This groovy is used to create the pre-announcement for the units that are sent via 
 * @author Praveen Babu M
 *
 */
class RTMITTCopinoPostInterceptor extends AbstractEdiPostInterceptor{

	@Override
	public void beforeEdiPost(XmlObject inXmlTransactionDocument, Map inParams) {
		
	}

}
