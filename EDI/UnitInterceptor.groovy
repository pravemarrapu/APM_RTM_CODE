/*
 * Copyright (c) 2015 Navis LLC. All Rights Reserved.
 *
 */

package com.navis.crane.groovy
import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.api.IEventType
import com.navis.argo.business.api.ServicesManager
import com.navis.argo.business.atoms.*
import com.navis.argo.business.model.CarrierVisit
import com.navis.external.framework.ECallingContext
import com.navis.external.framework.entity.AbstractEntityLifecycleInterceptor
import com.navis.external.framework.entity.EEntityView
import com.navis.external.framework.util.EFieldChanges
import com.navis.external.framework.util.EFieldChangesView
import com.navis.framework.business.Roastery
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.portal.FieldChange
import com.navis.framework.portal.FieldChanges
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.orders.business.eqorders.Booking
import com.navis.services.business.rules.EventType
import com.navis.vessel.business.schedule.VesselVisitDetails
import org.apache.log4j.Level
import org.apache.log4j.Logger

/*
 Modified by: Pradeep Arya
 WF#805275 - For some reason the interceptor didn't invoke on unit create therefore moved the
  code for updating the unitFlexString09 to new groovy RTMUpdateUnitDataSource
 */

public class UnitInterceptor extends AbstractEntityLifecycleInterceptor {
    //Code moved to groovy RTMUpdateUnitDataSource on UNIT_CREATE event
    /*   public void onCreate(EEntityView inEntity, EFieldChangesView inOriginalFieldChanges, EFieldChanges inMoreFieldChanges) {

          setUnitCreatedByProcess(inMoreFieldChanges);

      }

     public void setUnitCreatedByProcess(EFieldChanges inMoreFieldChanges) {

          DataSourceEnum dataSource = ContextHelper.getThreadDataSource();
          log("Datasource:" + dataSource.getKey());
          inMoreFieldChanges.setFieldChange(CREATED_BY_PROCESS, dataSource.getKey());
      }
      private static MetafieldId CREATED_BY_PROCESS = UnitField.UNIT_FLEX_STRING09;*/

    public void onUpdate(EEntityView inEntity, EFieldChangesView inOriginalFieldChanges, EFieldChanges inMoreFieldChanges) {
        Unit unit = (Unit) inEntity._entity;

        processOrphan(inOriginalFieldChanges, inMoreFieldChanges, unit);

        beginReceiveTimeLogic(unit, inOriginalFieldChanges);

        recordUnitExpiryDateChangedEvent(unit,inOriginalFieldChanges);

        recordUnitRerouteByPreanEvent(inOriginalFieldChanges, unit);

        cleanupPriorityAndDueTime(unit,inOriginalFieldChanges, inEntity);

        sendCasUnitUpdateIfNeeded(inEntity, inOriginalFieldChanges, inMoreFieldChanges);

    }

    private void processOrphan(EFieldChangesView inOriginalFieldChanges, EFieldChanges inMoreFieldChanges, Unit inUnit){

        ECallingContext context = getCallingContext();

        if (context != null && context.getAttribute("orphan") != null && UnitCategoryEnum.IMPORT.equals(inUnit.getUnitCategory())) {

            inMoreFieldChanges.setFieldChange(UnitField.UNIT_CATEGORY, UnitCategoryEnum.THROUGH);
            inMoreFieldChanges.setFieldChange(UnitField.UNIT_FREIGHT_KIND, FreightKindEnum.FCL);

            UnitFacilityVisit ufv = inUnit.getUfvForFacilityLiveOnly(ContextHelper.getThreadFacility());

            if (ufv != null) {

                if (ufv != null) {

                    ufv.setUfvRestowType(RestowTypeEnum.RESTOW);
                    ufv.updateObCv(ufv.getUfvActualIbCv());
                    inUnit.setUnitLineOperator(ufv.getUfvActualIbCv().getCarrierOperator());

                }

                inMoreFieldChanges.setFieldChange(UnitField.UNIT_LINE_OPERATOR, ufv.getUfvActualIbCv().getCarrierOperator());

            }

            IEventType customEvnt = _srvcMgr.getEventType("UNIT_CREATE_OVERLANDED");
            if (customEvnt != null) {
                _srvcMgr.recordEvent(customEvnt, context.getAttribute("craneID"), null, null, inUnit);
            }
        }
    }
    private void recordUnitRerouteByPreanEvent(EFieldChangesView inOriginalFieldChanges, Unit unit) {

        log("recordUnitRerouteByPreanEvent", "Start");

        if (isPreanProcessedTimeChanged(inOriginalFieldChanges)) {

            if (inOriginalFieldChanges.hasFieldChange(UnitField.UNIT_ROUTING)) {

                FieldChanges fcs = new FieldChanges();
                fcs.setFieldChange(inOriginalFieldChanges.findFieldChange(UnitField.UNIT_ROUTING));
                unit.recordUnitEvent(EventEnum.UNIT_REROUTE, fcs, REROUTED_BY_PREAN);
            }
        }
        log("recordUnitRerouteByPreanEvent", "End");
    }

    public void beginReceiveTimeLogic(Unit inUnit, EFieldChangesView inOriginalFieldChanges) {
        log("beginReceiveTimeLogic", "Start");

        if (UnitCategoryEnum.EXPORT.equals(inUnit.getUnitCategory())) {
            FieldChange fc = (FieldChange) inOriginalFieldChanges.findFieldChange(UnitField.UNIT_DECLARED_IB_CV);

            if (fc != null) {

                CarrierVisit ibCv = (CarrierVisit) fc.getNewValue();

                if (ibCv != null && !ibCv.isGenericCv() && (_beginRcvHelper.isBarge(ibCv) || ibCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN))) {

                    Date ibEta = ibCv.getCvCvd().getCvdETA();
                    Date beginRcv = null;

                    CarrierVisit obCv = (CarrierVisit) inUnit.getField(UnitField.UNIT_DECLARED_OB_CV);

                    if (obCv != null && !obCv.isGenericCv()) {
                        VesselVisitDetails obVVDtls = VesselVisitDetails.resolveVvdFromCv(obCv);
                        beginRcv = obVVDtls.getVvdTimeBeginReceive();
                    }

                    Boolean needToCompare = false;

                    if (beginRcv != null){
                        Booking bkg = _beginRcvHelper.getOrderItemBkg(inUnit.getUnitPrimaryUe().getUeDepartureOrderItem());

                        if (bkg != null && UnitCategoryEnum.EXPORT.equals(bkg.getEqoCategory())) {
                            if (bkg.getEqoVesselVisit() != obCv) {
                                registerError("Ctr Vessel Visit does not match Booking Vessel Visit");
                            } else {
                                needToCompare = !_beginRcvHelper.isOverrideFlagSet(bkg);
                            }
                        }
                    }

                    if (needToCompare){
                        _beginRcvHelper.compareIbCvEtaAndObVvBeginRcv(ibEta, beginRcv, inUnit);
                    }
                }
            }
        }
        log("beginReceiveTimeLogic", "End");
    }

    public void recordUnitExpiryDateChangedEvent(Unit inUnit, EFieldChangesView inOriginalFieldChanges) {
        log("recordUnitExpiryDateChangedEvent", "Start");

        if (UnitCategoryEnum.IMPORT.equals(inUnit.getUnitCategory())) {

            FieldChange fc = (FieldChange) inOriginalFieldChanges.findFieldChange(UnitField.UNIT_IMPORT_DELIVERY_ORDER_EXPIRY_DATE);

            if (fc != null) {
                inUnit.recordUnitEvent(EventType.findEventType("UNIT_PIN_EXPIRY_DATE_CHANGED"),null, null);
            }

        }
        log("recordUnitExpiryDateChangedEvent", "End");
    }

    public void log(String inMethodName, String inEndString) {
        String str = "MVIIUnitInterceptor." + inMethodName + " " + inEndString + " ";
        super.log(str + getTimeNow());
    }

    private Date getTimeNow() {
        TimeZone timeZone = ContextHelper.getThreadUserTimezone();
        return ArgoUtils.convertDateToLocalDateTime(ArgoUtils.timeNow(), timeZone);
    }

    public void cleanupPriorityAndDueTime(Unit inUnit, EFieldChangesView inOriginalFieldChanges, EEntityView inEntity) {

        if (inOriginalFieldChanges.hasFieldChange(UnitField.UNIT_IS_POWERED) || inOriginalFieldChanges.hasFieldChange(UnitField.UNIT_WANT_POWERED)) {

            Boolean  onPower =  (Boolean)inEntity.getField(UnitField.UNIT_IS_POWERED);
            Boolean  wantPower = (Boolean)inEntity.getField(UnitField.UNIT_WANT_POWERED);

            if ((onPower && wantPower) || (!onPower && !wantPower)) {

                UnitFacilityVisit ufv = inUnit.getUfvForFacilityLiveOnly(ContextHelper.getThreadFacility());

                if (ufv != null) {
                    if (ufv.getFieldString(PRIORITY) != null) {

                        ufv.setFieldValue(PRIORITY, null);
                        ufv.setFieldValue(DUE_TIME, null);
                        ufv.setFieldValue(LAST_UPDATE, ArgoUtils.timeNow());

                    }
                    log("Reset order priority/due time for reefer to null"+ inUnit.getUnitId());
                }

            }

        }

    }

    private boolean isPreanProcessedTimeChanged (EFieldChangesView inOriginalFieldChanges) {
        log("here3");
        Map<String, Object> customFields = new HashMap();
        FieldChange customFlexFieldsFc =  inOriginalFieldChanges.findFieldChange(CUSTOM_FLEX_FIELDS);

        if (customFlexFieldsFc != null) {
            log("here4");
            customFields = (Map<String, Object>) customFlexFieldsFc.getNewValue();
            if (customFields.containsKey(PREAN_PROCESSED_TIME_KEY)){
                log("here5");
            }
            return customFields.containsKey(PREAN_PROCESSED_TIME_KEY);
        }
        return false;
    }

    private void sendCasUnitUpdateIfNeeded(EEntityView inEntity, EFieldChangesView inOriginalFieldChanges, EFieldChanges inMoreFieldChanges) {
        Map mapParam = new HashMap();
        mapParam.put("inTriggerType", "onUpdate");
        mapParam.put("inEntity", inEntity);
        mapParam.put("inOriginalFieldChanges", inOriginalFieldChanges);
        mapParam.put("inMoreFieldChanges", inMoreFieldChanges);

        // Here after call the appropriate Code Extension. Its type should be defined as LIBRARY. The mapParam has all the input values.
        def onBoardUnitUpdateCodeExtInstance = null;
        log("About to execute OnboardUnitUpdate");
        onBoardUnitUpdateCodeExtInstance = getLibrary("OnboardUnitUpdate");
        onBoardUnitUpdateCodeExtInstance.execute(mapParam);
    }

    private static MetafieldId PRIORITY  = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFPriority");
    private static MetafieldId DUE_TIME  = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFDueTime");
    private static MetafieldId LAST_UPDATE  = MetafieldIdFactory.valueOf("customFlexFields.ufvCustomDFFLastUpdate");

    private static MetafieldId CUSTOM_FLEX_FIELDS = MetafieldIdFactory.valueOf("customFlexFields");
    private static String  PREAN_PROCESSED_TIME_KEY = "unitCustomDFFPreanProcessedTime";

    def _beginRcvHelper = getLibrary("MVIIetaVsBeginReceiveValidationHelper");
    private static String REROUTED_BY_PREAN = "Re-routed by prean";

    private static ServicesManager _srvcMgr = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
}