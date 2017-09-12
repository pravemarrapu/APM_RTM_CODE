/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 */





import com.navis.argo.business.reference.LineOperator
import com.navis.external.framework.AbstractExtensionCallback
import com.navis.framework.util.TransactionParms
import com.navis.framework.util.internationalization.PropertyKey
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageCollector
import com.navis.framework.util.message.MessageCollectorFactory
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.business.units.Unit
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.util.RoadBizUtil
import com.navis.road.business.workflow.IUnitBean
import com.navis.road.business.workflow.TransactionAndVisitHolder
import com.navis.external.framework.util.ExtensionUtils
import com.navis.argo.ContextHelper

/*
* Date  28/01/14 CSDV-1630 fixed private vs. public/protected methods issue for 3.0.1 upgrade
*/

public class PANRejectLineNotMatchingUnitLine extends AbstractExtensionCallback {

    public void execute(TransactionAndVisitHolder inWfCtx) {

        MessageCollector origMsgCollector = RoadBizUtil.getMessageCollector();
        MessageCollector thisTaskMsgCollector =  MessageCollectorFactory.createMessageCollector();
        TransactionParms.getBoundParms().setMessageCollector(thisTaskMsgCollector);

        TruckTransaction tran = inWfCtx.getTran();
        if (tran != null) {
            process(tran);
        }


        if (thisTaskMsgCollector.hasError()){
            _panGateTaskHelper.recordPreanError(inWfCtx, getClass().getSimpleName(), RoadBizUtil.getFirstErrorMessage(thisTaskMsgCollector));
        }

        TransactionParms.getBoundParms().setMessageCollector(origMsgCollector);
    }

    private void process(IUnitBean inTran) {
        LineOperator line = inTran.getTranLine();
        if (line != null) {
            Unit unit = inTran.getTranUnit();
            if (unit != null) {
                LineOperator unitLineOperator = LineOperator.resolveLineOprFromScopedBizUnit(unit.getUnitLineOperator());
                if(line != unitLineOperator){
                    getMessageCollector().appendMessage(MessageLevel.SEVERE,
                            CUSTOM_PREAN_LINE_NOT_MATCHING_UNIT_LINE,
                            "Preannounecment line operator not matching unit operator",
                            null);
                }
            }
        }
    }

    private static PropertyKey CUSTOM_PREAN_LINE_NOT_MATCHING_UNIT_LINE = PropertyKeyFactory.valueOf("CUSTOM_PREAN_LINE_NOT_MATCHING_UNIT_LINE");
    private static def _panGateTaskHelper = ExtensionUtils.getLibrary(ContextHelper.getThreadUserContext(), "PANGateTaskHelper");
}