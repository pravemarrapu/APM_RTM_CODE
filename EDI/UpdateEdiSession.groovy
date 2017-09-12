package com.weserve.APM.EDI

import com.navis.argo.ArgoConfig;
import com.navis.edi.EdiField;
import com.navis.edi.business.entity.EdiSession;
import com.navis.edi.business.entity.EdiSetting;
import com.navis.framework.persistence.HibernateApi;

class UpdateEdiSession {
	
	public String execute(){
		EdiSession session = (EdiSession)HibernateApi.getInstance().load(EdiSession.class, Long.valueOf("20163"));
		EdiSetting setting = new EdiSetting();
		setting.setFieldValue(EdiField.EDISTNG_CONFIG_ID, ArgoConfig.ACK_ACKNOWLEDGE_BY_INTERCHANGE.getConfigId());
		setting.setFieldValue(EdiField.EDISTNG_VALUE, "true");
		setting.setFieldValue(EdiField.EDISTNG_SESSION, session);
		HibernateApi.getInstance().save(setting);
	}

}
