package com.weserve.APM.EDI

import com.navis.argo.ContextHelper
import com.navis.argo.business.atoms.DataSourceEnum
import com.navis.external.road.AbstractGateTaskInterceptor
import com.navis.external.road.EGateTaskInterceptor
import com.navis.inventory.InventoryField
import com.navis.road.business.workflow.TransactionAndVisitHolder
import org.apache.log4j.Level
import org.apache.log4j.Logger;

/**
 * <Purpose>Do not update the flex fields in case of RAIL ITT gate as they are already being set in the post interceptor during COPINO posting
 * <p/>
 * Author: <a href="mailto:mpraveen@weservetech.com">
 * M Praveen Babu</a>
 * Date: 29-Aug-17 : 8:23 PM
 * Called from: <Specify from where this groovy is called>
 */
public class PANRejectFlexStringUpdatesForITT extends AbstractGateTaskInterceptor implements EGateTaskInterceptor {
    @Override
    void execute(TransactionAndVisitHolder inWfCtx) {
        LOGGER.setLevel(Level.DEBUG)
        LOGGER.debug("Inside the PANRejectFlexStringUpdatesForITT  :: Start")
        LOGGER.debug("Thread Data Source :: " + ContextHelper.getThreadDataSource())
        if (DataSourceEnum.EDI_APPOINTMENT.equals(ContextHelper.getThreadDataSource())) {
            if (inWfCtx.getTran() != null && inWfCtx.getTran().getTranGate() != null) {
                LOGGER.debug("Current Gate :: " + inWfCtx.getTran().getTranGate())
                if (!inWfCtx.getTran().getTranGate().getGateId().equalsIgnoreCase("RAIL_ITT")) {
                    executeInternal(inWfCtx)
                }else{
                    if(inWfCtx.getTran().getTranUnit() !=null){
                        inWfCtx.getTran().getTranUnit().setFieldValue(InventoryField.UNIT_FLEX_STRING04, inWfCtx.getTran().getTranUnitFlexString04())
                        inWfCtx.getTran().getTranUnit().setFieldValue(InventoryField.UNIT_FLEX_STRING06, inWfCtx.getTran().getTranUnitFlexString06())
                        inWfCtx.getTran().getTranUnit().setFieldValue(InventoryField.UNIT_FLEX_STRING07, inWfCtx.getTran().getTranUnitFlexString07())
                    }
                }
            }
        } else {
            executeInternal(inWfCtx)
        }
        LOGGER.debug("Inside the PANRejectFlexStringUpdatesForITT  :: END")
    }
    private static Logger LOGGER = Logger.getLogger(this.getClass());
}
