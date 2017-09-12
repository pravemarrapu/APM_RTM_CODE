/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 */

package com.navis.apex;

import java.util.Map;

import com.navis.argo.business.api.IEventType;
import com.navis.argo.business.api.ServicesManager;
import com.navis.argo.business.atoms.EventEnum;
import com.navis.external.framework.entity.AbstractEntityLifecycleInterceptor;
import com.navis.external.framework.entity.EEntityView;
import com.navis.external.framework.util.EFieldChanges;
import com.navis.external.framework.util.EFieldChangesView;
import com.navis.framework.business.Roastery;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.framework.persistence.HibernateApi;
import com.navis.framework.portal.FieldChange;
import com.navis.orders.business.eqorders.Booking;
import com.navis.framework.portal.FieldChanges;

public class BookingInterceptor  extends AbstractEntityLifecycleInterceptor {

    public void onUpdate(EEntityView inEntity, EFieldChangesView inOriginalFieldChanges, EFieldChanges inMoreFieldChanges) {
        FieldChange bookCustomFlexFieldsFc = (FieldChange)inOriginalFieldChanges.findFieldChange(MetafieldIdFactory.valueOf("bookCustomFlexFields"));
        if (bookCustomFlexFieldsFc != null) {

            Map<String,Object> customFields = (Map <String,Object>)bookCustomFlexFieldsFc.getNewValue();
            ServicesManager srvcMgr = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
            if (customFields != null && customFields.get(MetafieldIdFactory.valueOf("bkgCustomDFFoverrideVVbeginRcv")) != null) {
                IEventType customEvent = srvcMgr.getEventType("BOOKING_OVRD_VV_BGN_RCV_FLAG_UPDATE");
                srvcMgr.recordEvent(customEvent, null, null, null, (Booking)inEntity._entity);
                HibernateApi.getInstance().flush();
            }

        }

    }
}