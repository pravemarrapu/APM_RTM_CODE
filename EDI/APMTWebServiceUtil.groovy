package com.weserve.APM.EDI

import com.navis.argo.ArgoConfig
import com.navis.argo.ContextHelper
import com.navis.argo.business.api.GroovyApi
import com.navis.argo.business.extract.billing.ConfigurationProperties
import com.navis.argo.webservice.types.v1_0.*
import com.navis.framework.util.BizViolation
import com.navis.www.services.argoservice.ArgoServiceLocator
import com.navis.www.services.argoservice.ArgoServicePort

import javax.xml.rpc.Stub

/*
 Author      : Pradeep Arya
 Date Written: 04/30/2015 
 Requirements: Utility groovy for N4 to call billing webservice
 
 Deployment Steps:
 a) Administration -> System -> Groovy Plug-ins
 b) Click on + (Add).
 c) Add groovy name as APMTWebServiceUtil in short description	
 d) Paste the Groovy Code and click on Save.
 *
 */

class APMTWebServiceUtil extends GroovyApi{
	
	public String sendBillingWSRequest(String xmlString) throws BizViolation {
		ArgoServicePort port = this.getBillingWsStub();
		ScopeCoordinateIdsWsType scopeCoordinates = this.getScopeCoordenatesForWs();
		GenericInvokeResponseWsType invokeResponseWsType = port.genericInvoke(scopeCoordinates, xmlString);
		ResponseType response = invokeResponseWsType.getCommonResponse();
		QueryResultType[] queryResultTypes = response.getQueryResults();

		String responseString = "";
		if (queryResultTypes) {
			responseString = queryResultTypes.getAt(0).getResult();
		}
		else {
			MessageType[] msgType = response.getMessageCollector().getMessages();
			responseString = msgType.size() > 0 ? msgType.getAt(0).getMessage() : "";
		}
		
		return responseString;
	}
	
	 public ArgoServicePort getBillingWsStub() {
		ArgoServiceLocator locator = new ArgoServiceLocator();
		ArgoServicePort port = locator.getArgoServicePort(ConfigurationProperties.getBillingServiceURL());

		port._setProperty(Stub.USERNAME_PROPERTY, ConfigurationProperties.getBillingWebServiceUserId());
		port._setProperty(Stub.PASSWORD_PROPERTY, ConfigurationProperties.getBillingWebServicePassWord());

		return port;
	}

	public ScopeCoordinateIdsWsType getScopeCoordenatesForWs() {
		ScopeCoordinateIdsWsType scopeCoordinates = new ScopeCoordinateIdsWsType();
		scopeCoordinates.setOperatorId(ContextHelper.getThreadOperator().getId());
		scopeCoordinates.setComplexId(ContextHelper.getThreadComplex().getCpxId());
		
		if(ContextHelper.getThreadFacility())
			scopeCoordinates.setFacilityId(ContextHelper.getThreadFacility().getFcyId());
		if(ContextHelper.getThreadYard())
			scopeCoordinates.setYardId(ContextHelper.getThreadYard().getYrdId());

		return scopeCoordinates;
	}
	
	public String sendN4WSRequest(String xmlString) throws BizViolation {
		ArgoServicePort port = this.getN4WsStub();
		ScopeCoordinateIdsWsType scopeCoordinates = this.getScopeCoordenatesForWs();
		GenericInvokeResponseWsType invokeResponseWsType = port.genericInvoke(scopeCoordinates, xmlString);
		ResponseType response = invokeResponseWsType.getCommonResponse();
		QueryResultType[] queryResultTypes = response.getQueryResults();

		String responseString = "";
		if (queryResultTypes) {
			responseString = queryResultTypes[0].result
		}		
		return responseString;
	}
	
	public ArgoServicePort getN4WsStub() {
		ArgoServiceLocator locator = new ArgoServiceLocator();
		ArgoServicePort port = locator.getArgoServicePort(new URL(ArgoConfig.N4_WS_ARGO_URL.getSetting(ContextHelper.getThreadUserContext())));

		port._setProperty(Stub.USERNAME_PROPERTY, "n4api");
		port._setProperty(Stub.PASSWORD_PROPERTY, "lookitup");

		return port;
	}

}

