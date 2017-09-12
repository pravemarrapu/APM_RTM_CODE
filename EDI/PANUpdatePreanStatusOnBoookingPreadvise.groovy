package com.weserve.APM.EDI;

import com.navis.external.services.AbstractGeneralNoticeCodeExtension;
import com.navis.orders.business.eqorders.Booking;
import com.navis.services.business.event.GroovyEvent;

/**
 * <Purpose>
 * <p/>
 * Author: <a href="mailto:mpraveen@weservetech.com">
 * M Praveen Babu</a>
 * Date: 06-Sep-17 : 8:26 AM
 * JIRA: <Specify the JIRA tracking number>
 * Called from: <Specify from where this groovy is called>
 */
public class PANUpdatePreanStatusOnBoookingPreadvise extends AbstractGeneralNoticeCodeExtension{
    @Override
    public void execute(GroovyEvent inGroovyEvent) {

        Booking booking = inGroovyEvent.getEntity();



    }
}
