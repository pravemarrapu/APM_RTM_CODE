/*
 * Copyright (c) 2015 Navis LLC. All Rights Reserved.
 * @version $Id$
 */
package com.navis.apex.groovy.mv2_rwg.preannouncements

import com.navis.inventory.business.api.UnitField;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.navis.external.framework.ui.AbstractFormSubmissionCommand
import com.navis.external.framework.util.EFieldChanges
import com.navis.framework.metafields.entity.EntityId
import com.navis.framework.presentation.variforms.FormAction;
import com.navis.framework.util.message.MessageCollector
import com.navis.road.RoadApptsField

/*
   CSDV-3188 This form inteceptor is called from PAN_APT_FORM_GATE_APPOINTMENT

   14/12/2016 CSDV-4222 When Form action is CREATED, call PANResolveDuplicatePreanUI before submit
   12-Sept-17 - Pradeep Arya - WF#888344 - Update new DY flex field unitCustomDFFFormId with appt form ID
   13-Sept-17 - Pradeep Arya - WF#888344 - Added to update Routing Point from ITT-Loc in case of manual prean
*/

class PANGateAppointmentFormInterceptor extends AbstractFormSubmissionCommand {


    MessageCollector mc = null;
    Map<String, Object> parmMap = new HashMap<String, Object>();
    Map<String, Object> results = new HashMap<String, Object>();


    public void doBeforeSubmit(String inVariformId, EntityId inEntityId, List<Serializable> inGkeys, EFieldChanges inOutFieldChanges,
                               EFieldChanges inNonDbFieldChanges, Map<String, Object> inParams) {

        FormAction formAction  = (FormAction)inParams.get("UIParamFormAction");

        if (FormAction.UPDATE.equals(formAction)) {

            parmMap.put("GKEYS",inGkeys);

            if (inOutFieldChanges.hasFieldChange(RoadApptsField.GAPPT_ORDER) ||
                    inOutFieldChanges.hasFieldChange(RoadApptsField.GAPPT_CTR_ID) ||
                    inOutFieldChanges.hasFieldChange(RoadApptsField.GAPPT_LINE_OPERATOR)) {

                mc = executeInTransaction("PANCleanupPreanUnitUI",parmMap,results);
                if (mc.hasError()){
                    registerMessageCollector(mc);
                }
            }
            //WF#888344 - update new DY flex field with appointment form ID
            if (inOutFieldChanges.hasFieldChange(UnitField.UNIT_FLEX_STRING04)) {
                mc = executeInTransaction("PANUpdateFormId", parmMap, results);
                if (mc.hasError()) {
                    registerMessageCollector(mc);
                }
            }

        }
        else if(FormAction.CREATE.equals(formAction)) {

            parmMap.put("EFieldChanges",inOutFieldChanges);

            mc = executeInTransaction("PANResolveDuplicatePreanUI",parmMap,results);
            if (mc.hasError()){
                registerMessageCollector(mc);
            }

        }

    }

    /*WF#888344 - Added to update Routing Point from ITT-Loc in case of manual prean*/
    public void doAfterSubmit(String inVariformId, EntityId inEntityId, List<Serializable> inGkeys, EFieldChanges inOutFieldChanges,
                              EFieldChanges inNonDbFieldChanges, Map<String, Object> inParams) {

        //   log("inParams:$inParams");
        FormAction formAction  = (FormAction)inParams.get("UIParamFormAction");
        if(FormAction.CREATE.equals(formAction)) {
            parmMap.put("GKEYS", inParams.get('UIParamCreatedPrimaryKey'));

            mc = executeInTransaction("PANUpdateRoutingPoint", parmMap, results);
            if (mc.hasError()) {
                registerMessageCollector(mc);
            }
        }

    }

}