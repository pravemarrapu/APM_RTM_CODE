/*
 
   $Id: RTOCalculateRTOCategory.groovy 227902 2015-09-04 12:25:51Z extneefpa $
 
   Author: Allen Hsieh
   Date: 2013.01.18
   FDR: FDR_APMTMVII+RWG_RTO_20121221.doc
   CSDV-677: Determine and store RTO category code for use in activity messages posted to JMS queue.
			 This is a joint development project with APMT MVII and RWG.
 
   This groovy script will be deployed via Groovy Plug-ins view
   Installation Instructions:
	Load this groovy script to N4:
		   1. Go to Administration --> System --> Groovy Plug-ins
		   2. Click Add (+)
		   3. Copy and paste groovy script to Groovy Code area
			  enter "RTOCalculateRTOCategory" in Short Description field
		   4. Click Save button
  *
  * This groovy script will be triggered by UNIT_CREATE and UNIT_REROUTE events.
  * Hook up groovy script to designated events:
  1. Create general notices for UNIT_CREATE and UNIT_REROUTE events
  2. Set Execute Code  in Action field
  3. Paste following code in text area to hook up groovy script with each event
	 api.getGroovyClassInstance('RTOCalculateRTOCategory').execute(event)
  *
  * *
  RTO Category evaluation Matrix:
	  T	V	B	R	U    (outbound carrier visit)
  T	D	E	D	D	P for Empty, E for full
  V	I	T (R)	I	I	P for Empty, I for Full
  B	D 	E	D (R)	D	P for Empty, E for full
  R	D	E	D	D	P for Empty, E for full
  U	I	E	I	I	Not defined ??
  (inbound carrier visit)
  *
  * (R) means I/B VV = O/B VV, and restow type is Restow, otherwise, do not report to RTO (RTO category=null)
  * GEN_BARGE - this is a defined dummy visit for barge
  * GEN_RAIL, GEN_TRUCK, GEN_VESSEL are treated as R, T, and V
  * Use the Vessel Visit details: classification (optional field) =
  * Deepsea, Feeder, Barge to determine a vessel or barge
  * The RTO categories expected are
  I   = Import container
  E  = Export container
  D  = Domestic
  R  = Restow
  T  = Transhipment
  P = Empty container Pool (depot) (N4 Storage)
  * The calculated RTO category will be stored in ufvFlexString07 field
  *
  * *
  **
   Modifier: K Sundar
   Date: 2015.07.22
   Description: CSDV-3085 NonOperational Port validation was tried to cehck the null POD of Booking included
				Not null condition to avoid Null Pointer Exception
  **
  **
   Modifier: Allen Hsieh
   Date: 2013.02.21
   Description: Overloaded execute method by adding a boolean argument that will be passed onto Formatter
				script to report UNIT_PROPERTY_UPDATE event from RTO category calculation script.
  **
  *
  **
	Modifier: Allen Hsieh
	Date: 2013.04.16, 2013.04.22
	Description: Modified script to handle carrier mode TRAIN when an export is pre-advised. Converted the
				 TRAIN mode to RAIL mode so that the RTO category can be evaluated properly.
   **
 **
  Modifier: Danny Holthuijsen
  Date: 2013.06.28
  changed unitFlexString01 to ufvFlexString07
 *
 **
  Modifier: Amos Parlante
  Date: April 22, 2015
  Baseline from production at APMT MVII.
 *
  */
 
 import com.navis.apex.business.model.GroovyInjectionBase
 import com.navis.argo.ArgoEntity
 import com.navis.argo.ArgoField
 import com.navis.argo.ContextHelper
 import com.navis.argo.business.api.ArgoUtils
 import com.navis.argo.business.api.IEventType
 import com.navis.argo.business.api.ServicesManager
 import com.navis.argo.business.atoms.FreightKindEnum
 import com.navis.argo.business.atoms.LocTypeEnum
 
 import com.navis.argo.business.atoms.UnitCategoryEnum
 import com.navis.argo.business.model.CarrierVisit
 import com.navis.argo.business.model.Complex
 import com.navis.argo.business.model.Facility
 import com.navis.argo.business.model.GeneralReference
 import com.navis.argo.business.reference.RoutingPoint
 import com.navis.framework.business.Roastery
 import com.navis.framework.metafields.MetafieldId
 import com.navis.framework.metafields.MetafieldIdFactory
 import com.navis.framework.portal.QueryUtils
 import com.navis.framework.portal.UserContext
 import com.navis.framework.portal.query.DomainQuery
 import com.navis.framework.portal.query.PredicateFactory
 import com.navis.inventory.business.atoms.UfvTransitStateEnum
 import com.navis.inventory.business.atoms.UnitVisitStateEnum
 import com.navis.inventory.business.units.EqBaseOrder
 import com.navis.inventory.business.units.Unit
 import com.navis.inventory.business.units.UnitFacilityVisit
 import com.navis.orders.business.eqorders.EquipmentOrder
 import com.navis.services.business.event.EventFieldChange
 import com.navis.services.business.event.GroovyEvent
 import com.navis.vessel.business.atoms.VesselClassificationEnum
 import org.apache.commons.lang.StringUtils
 
 public class RTOCalculateRTOCategory extends GroovyInjectionBase {
   public void execute(event) {
	 this.log("Groovy: RTOCalculateRTOCategory - starts ");
	 UserContext context = ContextHelper.getThreadUserContext();
	 Complex complex = ContextHelper.getThreadComplex();
	 // CSDV-1356: dickspa 03/10/2013: Set the facility to be the Facility in the General Reference entry.
	 Facility facility = getFacilityFromGeneralReference(ACTIVITY_MESSAGE_GR, FACILITY_ID1_GR, GEN_REF_FACILITY_FIELD);
	 // Do need to check that the facility has been supplied correctly, otherwise we'll be setting the Unit's facility to null each time
	 if (facility == null) {
	   log("Please add a General Reference entry [" + ACTIVITY_MESSAGE_GR + "/" + FACILITY_ID1_GR +
			   "] with the current facility given in Value-1.");
	   return;
	 }
	 // CSDV-1450: kumarsu4 21/10/2013: Set the GR_DESTINATION_VALUE to be the non operational terminal code from the General Reference entry data value 1.
	 String nonOperationalTerminalCode = findValueFromGeneralReferenceForNonOperationalTerminal(RTO_CATEGORY_TYPE, NON_OPR_FACILITY_ID1_GR, CONVERT_DOMESTIC_ID2_GR);
	 if (nonOperationalTerminalCode == null) {
	   this.log("Please add a General Reference entry [" + RTO_CATEGORY_TYPE + "/" + NON_OPR_FACILITY_ID1_GR + "/" + CONVERT_DOMESTIC_ID2_GR +
			   "] with the non operation terminal code given in Value-1.");
	   return;
	 }
	 this.log("Non-Operational termainal code : [ $nonOperationalTerminalCode ]");
 
 
	 if (event == null) {
	   this.log("Groovy: RTOCalculateRTOCategory - exits due to event iss null.");
	   return;
	 }
 
	 GroovyEvent theEvent = (GroovyEvent) event;
	 String eventType = theEvent.getEvent().getEventTypeId();
	 //this.log("Event type= " + eventType);
	 //Only evaluate RTO category when an UNIT_CREATE or UNIT_REROUTE event is recorded
	 //Removed the restriction on what events should this groovy be triggered as requested by
	 //Danny Holthuijsen on 1/10/2013.
	 /*
	 if (!eventType.equals(UNIT_CREATE) && !eventType.equals(UNIT_REROUTE) &&
		 !eventType.equals(UNIT_RECTIFY)) {
	   this.log("Groovy: RTOCalculateRTOCategory - exits due to event is not either UNIT_CREATE, UNIT_REROUTE, or UNIT_RECTIFY.");
	   return;
	 }
	 */
 
	 Unit unit = (Unit) theEvent.getEntity();
	 if (unit == null) {
	   this.log("Groovy: RTOCalculateRTOCategory - exits due to unit is null.");
	   return;
	 }
	 //this.log("Unit Id= " + unit.getUnitId());
	 UnitFacilityVisit ufv = null;
	 if (unit.unitVisitState == UnitVisitStateEnum.DEPARTED) {
	   ufv = this.getDepartedUfvForUnit(unit);
	 } else if (unit.unitVisitState == UnitVisitStateEnum.RETIRED) {
	   ufv = this.getRetiredUfvForUnit(unit);
	 } else {
	   ufv = unit.getUnitActiveUfvNowActive();
	 }
 
	 // CSDV-1356: dickspa 03/10/2013: Facility will now always be non-null.
 
	 this.log("Facility Id= " + facility.getFcyId());
	 if (ufv == null) {
	   ufv = this.getUfvForFacility(unit, facility);
 
 
 
	 }
 
 
	 if (ufv == null) {
	   this.log("Groovy: RTOCalculateRTOCategory - exits due to ufv is null.");
	   return;
	 }
 
	 //Save existing RTO category if it's calculated previously
	 //String savedRtoCategory = unit.getufvFlexString07();
	 String savedRtoCategory = ufv.getUfvFlexString07();
	 this.log("Existing RTO Category= " + savedRtoCategory);
 
	 //Get inbound and outbound modality strings
	 String inModalityId = this.getInboundModalityId(unit, ufv);
	 String outModalityId = this.getOutboundModalityId(unit, ufv);
 
	 //get the calculated RTO category by inbound and outbound modalities
	 String rtoCategory = this.evaluateAndGetRTOCategory(unit, ufv, inModalityId, outModalityId, nonOperationalTerminalCode);
	 this.log("return RTO Category= " + rtoCategory);
	 //Task 1: Set RTO Category to ufvFlexString07 field
	 //unit.setUfvFlexString07(rtoCategory);
	 ufv.setUfvFlexString07(rtoCategory);
 
	 Set changes = this.getFieldChangesForEvent(event);
	 //Task 2: Evaluate if reporting of RTO category changes to RTO is needed
	 //Inset a custom event EVENT_RTO_CATEGORY_CHANGE if needed
	 boolean notifyRto = this.shouldReportActivityMessage(unit, eventType, rtoCategory,
			 savedRtoCategory, theEvent, changes);
	 String reportEventId = null;
	 boolean rtoCatPropertyUpdate = false;
	 if (notifyRto) {
	   //Task 3: generate an activity message and report it to RTO
	   //this.log("Post an activity message from RTOCalculateRTOCategory");
	   //this.log("Unit Property Update by RTO Cal Calculation. rtoCatPropertyUpdate= " +rtoCatPropertyUpdate);
	   this.postActivityMessage(unit, event, rtoCatPropertyUpdate);
	 } else {
	   if (StringUtils.equals(eventType, UNIT_REROUTE)) {
		 //Report UNIT_PROPERTY_UPDATE event instead when UNIT_REROUTE event is recorded
		 reportEventId = this.fieldChangesExcludeUnitReroute(eventType, changes, unit, ufv, complex, facility);
		 //this.log("Field changes include UNIT REROUTE");
	   } else if (StringUtils.equals(eventType, UNIT_RECTIFY)) {
		 //Report UNIT_REROUTE or UNIT_PROPERTY_UPDATE event instead when UNIT_ROLL event is recorded
		 reportEventId = this.fieldChangesExcludeUnitRectify(eventType, changes, unit, ufv, complex, facility);
		 //this.log("Field changes include UNIT ROLL");
	   }
 
	   if (StringUtils.equals(reportEventId, UNIT_PROPERTY_UPDATE)) {
		 rtoCatPropertyUpdate = true;
		 //this.log("Unit Property Update by RTO Cal Calculation. rtoCatPropertyUpdate= " + rtoCatPropertyUpdate);
		 this.postActivityMessage(unit, event, rtoCatPropertyUpdate);
	   }
	 }
 
	 this.log("Groovy: RTOCalculateRTOCategory - ends ");
   }
 
   /**
	* CSDV-1356: dickspa 03/10/2013: Get the facility field from the General Reference entry.
	* Lookup this to find a Facility recognised by this String.
	* If facility is not found, returns null.
	* @param genRefType
	* @param genRefID1
	* @param facilityNameField
	* @return
	*/
   private Facility getFacilityFromGeneralReference(String genRefType, String genRefID1, MetafieldId facilityNameField) {
	 // Try to load the General Reference entry. Return null if it does not exist and warn the user.
	 // Note that the warning will be given after this method is called (if null is returned), so we do not need to give it here during any checks.
	 GeneralReference facilityGeneralReference = GeneralReference.findUniqueEntryById(genRefType, genRefID1);
	 if (facilityGeneralReference == null) {
	   return null;
	 }
 
	 // Get the Facility value given as a string. Check that the value is supplied.
	 String facilityString = facilityGeneralReference.getFieldString(facilityNameField);
	 if (StringUtils.isBlank(facilityString)) {
	   return null;
	 }
 
	 // Lookup the facility. It would make more sense to just just return it without checking whether it exists.
	 // This is so that if this does return null, we can ensure that the main execution is halted, which we cannot do from here.
	 return Facility.findFacility(facilityString);
   }
 
   /**
	*
	* @param eventType
	* @param changes
	* @param inUnit
	* @param ufv
	* @param complex
	* @param facility
	* @return
	*/
   private String fieldChangesExcludeUnitRectify(String eventType, Set changes,
												 Unit inUnit, UnitFacilityVisit ufv,
												 Complex complex, Facility facility) {
 
	 String reportEventId = null;
	 if (!StringUtils.equals(eventType, UNIT_RECTIFY)) {
	   return reportEventId;
	 }
 
	 if (!this.fieldChangesIncludeOutboundCarrierMode(changes, inUnit, ufv, complex, facility) &&
			 !this.fieldChangesIncludeInboundCarrierMode(changes, inUnit, ufv, complex, facility)) {
	   //Report UNIT_PROPERTY_UPDATE event otherwise
	   reportEventId = UNIT_PROPERTY_UPDATE;
	   this.log("UNIT_PROPERTY_UPDATE reported instead of UNIT_RECTIFY");
	 }
 
	 return reportEventId;
   }
 
   /**
	*
	* @param changes
	* @param inUnit
	* @param ufv
	* @param complex
	* @param facility
	* @return
	*/
   private boolean fieldChangesIncludeInboundCarrierMode(Set changes, Unit inUnit, UnitFacilityVisit ufv,
														 Complex complex, Facility facility) {
 
	 boolean includeIbCv = false;
	 //build inbound carrier field changes list
	 INBOUND_CV_LIST.add("unitDeclaredIbCv");    //Changes inbound carrier
	 INBOUND_CV_LIST.add("ufvActualIbCv");
	 MetafieldId mfId = null;
	 String prev = null;
	 String post = null;
	 if (changes.size() > 0) {
	   for (Object aFc : changes) {
		 EventFieldChange fc = (EventFieldChange) aFc;
		 if (fc.getEvntfcMetafieldId() != null &&
				 INBOUND_CV_LIST.contains(fc.getMetafieldId())) {
		   //this.log("FC meta field Id= " + fc.getEvntfcMetafieldId());
		   mfId = MetafieldIdFactory.valueOf(fc.getEvntfcMetafieldId());
		   prev = ArgoUtils.getPropertyValueAsUiString(mfId, fc.getEvntfcPrevVal());
		   post = ArgoUtils.getPropertyValueAsUiString(mfId, fc.getEvntfcNewVal());
		   includeIbCv = true;
		   //this.log("FC meta field Id matched - includeIbCv= " + includeIbCv);
		   break;
		 }
	   }
	 }
 
	 if (includeIbCv) {
	   //Reset this boolean as it will be used as indicator
	   //whether inbound carrier mode is changed.
	   includeIbCv = false;
	   String prevCarrierMode = null;
	   String postCarrierMode = null;
	   CarrierVisit cvPrev = null;
	   CarrierVisit cvPost = null;
	   //this.log("prev= " + prev);
	   //this.log("post= " + post);
	   if (this.isGenericCarrierId(prev)) {
		 prevCarrierMode = prev.substring(4);
		 //this.log("new prevCarrierMode= " + prevCarrierMode);
	   } else {
		 cvPrev = this.findFacilityCvById(complex, facility, prev)
	   }
	   if (this.isGenericCarrierId(post)) {
		 postCarrierMode = post.substring(4);
		 //this.log("new postCarrierMode= " + postCarrierMode);
	   } else {
		 cvPost = this.findFacilityCvById(complex, facility, post)
	   }
	   if (cvPrev != null) {
		 prevCarrierMode = cvPrev.getCvCarrierMode().getKey();
		 //this.log("cvPrev is not null " + prevCarrierMode);
	   }
	   if (cvPost != null) {
		 postCarrierMode = cvPost.getCvCarrierMode().getKey();
		 //this.log("cvPost is not null " + postCarrierMode);
	   }
 
	   if (!StringUtils.equals(prevCarrierMode, postCarrierMode)) {
		 //this.log("includeIbCv = true");
		 includeIbCv = true;
	   }
	 }
 
	 return includeIbCv;
   }
 
   /**
	*
	* @param eventType
	* @param changes
	* @param inUnit
	* @param ufv
	* @param complex
	* @param facility
	* @return
	*/
   private String fieldChangesExcludeUnitReroute(String eventType, Set changes,
												 Unit inUnit, UnitFacilityVisit ufv,
												 Complex complex, Facility facility) {
 
	 String reportEventId = null;
	 if (!StringUtils.equals(eventType, UNIT_REROUTE)) {
	   return reportEventId;
	 }
 
	 if (!this.fieldChangesIncludeOutboundCarrierMode(changes, inUnit, ufv, complex, facility)) {
	   //Still report UNIT_PROPERTY_UPDATE event
	   reportEventId = UNIT_PROPERTY_UPDATE;
	   this.log("UNIT_PROPERTY_UPDATE reported instead of EVENT_UNIT_REROUTE");
	 }
 
	 return reportEventId;
   }
 
   private boolean fieldChangesIncludeOutboundCarrierMode(Set changes, Unit inUnit, UnitFacilityVisit ufv,
														  Complex complex, Facility facility) {
 
	 boolean includeObCv = false;
	 //build outbound carrier field changes list
	 OUTBOUND_CV_LIST.add("ufvIntendedObCv");    //Changes outbound carrier
	 OUTBOUND_CV_LIST.add("ufvActualObCv");
	 MetafieldId mfId = null;
	 String prev = null;
	 String post = null;
	 if (changes.size() > 0) {
	   for (Object aFc : changes) {
		 EventFieldChange fc = (EventFieldChange) aFc;
		 if (fc.getEvntfcMetafieldId() != null &&
				 OUTBOUND_CV_LIST.contains(fc.getMetafieldId())) {
		   //this.log("FC meta field Id= " + fc.getEvntfcMetafieldId());
		   mfId = MetafieldIdFactory.valueOf(fc.getEvntfcMetafieldId());
		   prev = ArgoUtils.getPropertyValueAsUiString(mfId, fc.getEvntfcPrevVal());
		   post = ArgoUtils.getPropertyValueAsUiString(mfId, fc.getEvntfcNewVal());
		   includeObCv = true;
		   //this.log("FC meta field Id matched - includeObCv= " + includeObCv);
		   break;
		 }
	   }
	 }
 
	 if (includeObCv) {
	   //Reset this boolean as it will be used as indicator
	   //whether inbound carrier mode is changed.
	   includeObCv = false;
	   String prevCarrierMode = null;
	   String postCarrierMode = null;
	   CarrierVisit cvPrev = null;
	   CarrierVisit cvPost = null;
	   //this.log("prev= " + prev);
	   //this.log("post= " + post);
	   if (this.isGenericCarrierId(prev)) {
		 prevCarrierMode = prev.substring(4);
		 //this.log("new prevCarrierMode= " + prevCarrierMode);
	   } else {
		 cvPrev = this.findFacilityCvById(complex, facility, prev)
	   }
	   if (this.isGenericCarrierId(post)) {
		 postCarrierMode = post.substring(4);
		 //this.log("new postCarrierMode= " + postCarrierMode);
	   } else {
		 cvPost = this.findFacilityCvById(complex, facility, post)
	   }
 
	   if (cvPrev != null) {
		 prevCarrierMode = cvPrev.getCvCarrierMode().getKey();
		 //this.log("cvPrev is not null " + prevCarrierMode);
	   }
	   if (cvPost != null) {
		 postCarrierMode = cvPost.getCvCarrierMode().getKey();
		 //this.log("cvPost is not null " + postCarrierMode);
	   }
 
	   if (!StringUtils.equals(prevCarrierMode, postCarrierMode)) {
		 //The inbound carrier mode is changed
		 //this.log("includeObCv = true");
		 includeObCv = true;
	   }
	 }
 
	 return includeObCv;
   }
 
   /**
	*
	* @param inComplex
	* @param inFacility
	* @param inCvId
	* @return
	*/
   private CarrierVisit findFacilityCvById(Complex inComplex, Facility inFacility, String inCvId) {
 
	 DomainQuery dq = QueryUtils.createDomainQuery(ArgoEntity.CARRIER_VISIT)
			 .addDqPredicate(PredicateFactory.eq(ArgoField.CV_COMPLEX, inComplex.getCpxGkey()))
			 .addDqPredicate(PredicateFactory.eq(ArgoField.CV_ID, inCvId));
 
	 if (inFacility == null) {
	   dq.addDqPredicate(PredicateFactory.isNull(ArgoField.CV_FACILITY));
	 } else {
	   dq.addDqPredicate(PredicateFactory.eq(ArgoField.CV_FACILITY, inFacility.getFcyGkey()));
	 }
 
 
	 return (CarrierVisit) Roastery.getHibernateApi().getUniqueEntityByDomainQuery(dq);
   }
 
   /**
	*
	* @param prev
	* @return
	*/
   private boolean isGenericCarrierId(String prev) {
 
	 if (StringUtils.isBlank(prev)) {
	   return false;
	 }
 
	 if (StringUtils.equals(prev, "GEN_TRAIN") ||
			 StringUtils.equals(prev, "GEN_VESSEL") ||
			 StringUtils.equals(prev, "GEN_TRUCK") ||
			 StringUtils.equals(prev, "GEN_CARRIER")) {
	   return true;
	 }
 
	 return false;
   }
 
   /**
	*
	* @param inRtoCatChangedEvent
	* @param inUnit
	* @param inEventNote
	*/
   private void insertCatChangeEvent(String inRtoCatChangedEvent, Unit inUnit, String inEventNote) {
 
	 if (StringUtils.isBlank(inRtoCatChangedEvent) || inUnit == null) {
	   return;
	 }
 
	 ServicesManager srvcMgr = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID);
	 IEventType rtoCatChangedEvent = srvcMgr.getEventType(inRtoCatChangedEvent);
	 if (rtoCatChangedEvent != null) {
	   srvcMgr.recordEvent(rtoCatChangedEvent, inEventNote, null, null, inUnit);
	   //this.log("RTOCalculateRTOCategory - Event  inserted " + inRtoCatChangedEvent);
	 }
   }
 
   /**
	*
	* @param eventType
	* @param rtoCategory
	* @param savedRtoCategory
	* @return
	*/
   private boolean shouldReportActivityMessage(Unit unit, String eventType, String rtoCategory,
											   String savedRtoCategory, GroovyEvent inEvent, Set changes) {
 
	 boolean notifyRto = false;
	 boolean rtoCatChanged = false;
	 if (StringUtils.isNotBlank(rtoCategory)) {
	   // CSDV-1652: dickspa 20/02/2014: If the Event Type is UNIT_CREATE, no longer notify RTO.
 
 
 
 
 
 
 
	   if ((eventType.equals(UNIT_REROUTE) || eventType.equals(UNIT_RECTIFY)) &&
			   (StringUtils.isNotBlank(savedRtoCategory) &&
					   !StringUtils.equals(savedRtoCategory, rtoCategory))) {
		 //For UNIT_REROUTE event, report an activity message if RTO category is changed
		 notifyRto = true;
		 rtoCatChanged = true;
		 //this.log("UNIT_REROUTE for RTO cat change and notifyRto = " + notifyRto);
	   }
 
	   if (!notifyRto && eventType.equals(UNIT_REROUTE)) {
		 //Populate fieldChanges set
		 if (this.fieldChangesIncludePODs(changes)) {
		   notifyRto = true;
		   //this.log("UNIT_REROUTE for PODs change and notifyRto = " + notifyRto);
		 }
	   }
	 }
 
	 if (rtoCatChanged) {
	   //Insert a custom event for the purpose of tracing RTO category change
	   //this.log("Insert a custom event for RTO category changes");
	   this.insertCatChangeEvent(EVENT_RTO_CATEGORY_CHANGE, unit,
			   "RTO category changed: " + savedRtoCategory + " -> " + rtoCategory);
	 }
	 //this.log("notifyRto = " + notifyRto);
	 //this.log("rtoCatChanged = " + rtoCatChanged);
	 return notifyRto;
   }
 
   /**
	*  Check if field changes happened in PODs fields
	*/
   private boolean fieldChangesIncludePODs(Set changes) {
 
	 boolean includePODs = false;
	 //build POD field changes list
	 POD_LIST.add("rtgOPT1");
	 POD_LIST.add("rtgOPT2");
	 POD_LIST.add("rtgOPT3");
	 POD_LIST.add("rtgPOD1");
	 POD_LIST.add("rtgPOD2");
	 if (changes.size() > 0) {
	   for (Object aFc : changes) {
		 EventFieldChange fc = (EventFieldChange) aFc;
		 if (fc.getEvntfcMetafieldId() != null &&
				 POD_LIST.contains(fc.getMetafieldId())) {
		   //this.log("FC meta field Id= " + fc.getEvntfcMetafieldId());
		   includePODs = true;
		   //this.log("FC meta field Id matched - includePODs= " + includePODs);
		   break;
		 }
	   }
	 }
 
	 return includePODs;
   }
 
   /**
	*
	* @param inEvent
	* @return
	*/
   private Set getFieldChangesForEvent(GroovyEvent inEvent) {
	 Set result = new HashSet();
	 if (inEvent == null) {
	   return result;
	 }
 
	 if (inEvent.getEvent() == null) {
	   return result;
	 }
 
	 //Set changes = inEvent.event.evntFieldChanges
	 Set changes = inEvent.getEvent().getEvntFieldChanges();
 
	 if (changes == null) {
	   return result;
	 }
 
	 if (changes.size() == 0) {
	   return result;
	 }
	 //Set result = new HashSet();
	 for (Object aFc : changes) {
	   EventFieldChange fc = (EventFieldChange) aFc;
	   if (fc.getEvntfcMetafieldId() != null &&
			   (fc.getEvntfcPrevVal() != null || fc.getEvntfcNewVal() != null)) {
		 //this.log("FieldChange metaField Id= " + fc.getEvntfcMetafieldId());
		 //this.log("FieldChange Pre value= " + fc.getEvntfcPrevVal());
		 //this.log("FieldChange New value= " + fc.getEvntfcNewVal());
		 result.add(fc);
	   }
	 }
 
	 return result
   }
 
   /**
	*
	* @param unit
	* @param event
	*/
   private void postActivityMessage(Unit unit, GroovyEvent event, boolean rtoCatPropertyUpdate) {
 
	 //Call JMS processor to report this event to RTO via an activity message
	 Object msProcessor = this.getGroovyClassInstance(JMS_MESSAGE_PROCESSOR);
	 if (msProcessor != null) {
	   msProcessor.execute(event, rtoCatPropertyUpdate);
	 }
   }
 
   /**
	* Return a calculated RTO category based on inbound and outbound modalities
	* @param inUnit
	* @param inUfv
	* @param inModalityId
	* @param outModalityId
	* @return
	*/
   private String evaluateAndGetRTOCategory(Unit inUnit, UnitFacilityVisit inUfv,
											String inModalityId, String outModalityId, String nonOperationalTerminalCode) {
 
	 if (inUnit == null) {
	   return null;
	 }
 
	 String theInModStr = inModalityId;
	 String theOutModStr = outModalityId;
	 //this.log("theInModStr= " + theInModStr);
	 //this.log("theOutModStr= " + theOutModStr);
	 //construct a Map containing RTO category matrix
	 Map<String, String> catMatrix = this.buildRTOCategoryMatrixMap();
	 //for (Iterator itr = catMatrix.keySet().iterator(); itr.hasNext();) {
	 //  String keyStr = (String) itr.next();
	 //this.log("keyStr= " + keyStr);
	 //this.log(" Key String value= " + catMatrix.get(keyStr));
	 // }
 
	 // CSDV-1450: kumarsu4 22/10/2013: find if POL Or POD is Non Operational when OBCarrier is vessel Or Unknown.
	 // If the Port of Load is a non-operational facility ? set RTO Category to D
	 // If the Port of Load is a ?regular? operational facility retrieve the Port Of Discharge from the booking means Unit Booking ( Departure Order )
	 // If the Port Of Discharge  is set to the non-operational facility ? set the RTO category to D
	 boolean flag = findIfPOLOrPODisANonOperationalWhenOBCarrierIsVesOrUnknown(inUnit, inUfv, nonOperationalTerminalCode, outModalityId);
	 if (flag) {
	   this.log("RTO Cateogy is found 'D' becuase POL Or POD is a non Operational Terminal and OBCarrier is Vessel Or Unknown");
	   //Retrieve calculated RTO category from Map by key comprised by inbound and outbound modalities
	   return catMatrix.get(NON_OP_TERMINAL_CODE);
	 } else {
 
	   if (StringUtils.equals(TRAIN_MODE, theInModStr)) {
		 theInModStr = RAIL_MODE;
	   }
 
 
	   if (StringUtils.equals(TRAIN_MODE, theOutModStr)) {
		 theOutModStr = RAIL_MODE;
	   }
 
 
	   FreightKindEnum unitFreightKind = inUnit.getUnitFreightKind();
	   if (UNKNOWN_MODE.equals(theOutModStr)) {
		 if (FreightKindEnum.MTY.equals(unitFreightKind)) {
		   theOutModStr = theOutModStr + "M";
		 } else {
		   theOutModStr = theOutModStr + "F";
		 }
	   }
 
	   if ((VESSEL_MODE.equals(theInModStr) && VESSEL_MODE.equals(theOutModStr)) ||
			   (BARGE_MODE.equals(theInModStr) && BARGE_MODE.equals(theOutModStr))) {
		 if (isSameVesselOrBargeInOutCarrierVisit(inUnit, inUfv, inModalityId, outModalityId)) {
		 // CSDV-1432: dickspa 24/06/2014: When unit is rerouted to be a restow, but XPS Restow type is not yet set,
		 // should still report this as a "R" (reroute).
		 final boolean inAndOutCvSame = isSameVesselOrBargeInOutCarrierVisit(inUnit, inUfv, inModalityId, outModalityId);
 
 
 
 
 
 
 
		 log(String.format("inAndOutCVSame? %b", inAndOutCvSame));
		 final UfvTransitStateEnum ufvTState = inUfv.getUfvTransitState();
		 final UnitCategoryEnum unitCat = inUnit.getUnitCategory();
		 log(String.format("Ufv %s has Transit state %s and Category %s", inUnit.getUnitId(), ufvTState, unitCat));
		 // CSDV-1432: dickspa 24/06/2014: Remove the check on UFV-Restow attribute.
		 // If the Unit's IB and OB carrier are the same, and the Category is THROUGH, mark this as RTO-RESTOW
		 // Should set this Unit as a RTO-RESTOW if all these conditions are true:
		 // - UFV transit state is Yard/ECIN/ECOUT
		 // - UFV I/B and O/B carrier are the same
		 // - Unit Category is THROUGH
		 if ((UfvTransitStateEnum.S30_ECIN.equals(ufvTState) ||
				 UfvTransitStateEnum.S40_YARD.equals(ufvTState) ||
				 UfvTransitStateEnum.S50_ECOUT.equals(ufvTState)) &&
				 inAndOutCvSame && unitCat.equals(UnitCategoryEnum.THROUGH)) {
 
 
 
 
 
		   //only when in & out are same vessel visit or barge visit
		   theOutModStr = theOutModStr + "S";
 
		 }
 
		 else {
		   //If it is not Restow, do not report it to RTO
		   theOutModStr = theOutModStr + "N";
		 }
	   }
	 }
 
	   this.log("Key String theInModStr+theOutModStr= " + theInModStr + theOutModStr);
	   //Retrieve calculated RTO category from Map by key comprised by inbound and outbound modalities
	   return catMatrix.get(theInModStr + theOutModStr);
	 }
 
   }
 
   /**
	* Find if the Unit POL or POD is non operational facility  when Outbound carrier mode is Vessel or Unknown
	*/
   private boolean findIfPOLOrPODisANonOperationalWhenOBCarrierIsVesOrUnknown(Unit inUnit, UnitFacilityVisit inUfv, String nonOperationalTerminalCode, String outModalityId) {
	 if (UnitCategoryEnum.EXPORT.equals(inUnit.getUnitCategory()) && UfvTransitStateEnum.S20_INBOUND.equals(inUfv.getUfvTransitState())) {
	   if (StringUtils.equals(outModalityId, VESSEL_MODE) || StringUtils.equals(outModalityId, UNKNOWN_MODE)) {
		 EqBaseOrder eqBaseOrder = inUnit.getDepartureOrder();
		 if (eqBaseOrder != null) {
		   EquipmentOrder equipmentOrder = EquipmentOrder.resolveEqoFromEqbo(eqBaseOrder);
		   RoutingPoint routingPointPOL = equipmentOrder.getEqoPol();
		   this.log(" RoutingPoint POL :[ " + routingPointPOL.getPointId() + " ]");
		   RoutingPoint routingPointPOD = equipmentOrder.getEqoPod1();
		   if (routingPointPOD != null) { //CSDV-3085
		   this.log(" RoutingPoint POD1 : [ " + routingPointPOD.getPointId() + " ]");}
		   else{this.log("RoutingPoint POD1 is null") } //CSDV-3085
 
		   if (!StringUtils.isEmpty(nonOperationalTerminalCode)) {
			 if (nonOperationalTerminalCode.equalsIgnoreCase(routingPointPOL.getPointId())) {
			   return true;
			 } else if (!nonOperationalTerminalCode.equalsIgnoreCase(routingPointPOL.getPointId())) {
			   if (routingPointPOD != null && nonOperationalTerminalCode.equalsIgnoreCase(routingPointPOD.getPointId())) { //CSDV-3085
				 return true;
			   }
			 } else {
			   return false;
			 }
		   } else {
			 this.log("Non OperationalTerminal Code not found");
		   }
		 } else {
		   this.log("Booking not found with unit : $inUnit");
		 }
	   }
	 }
	 return false;
 
 
 
   }
 
   /**
	* Build a matrix of RTO category based on in and out modalities
	* @return
	*/
   private Map buildRTOCategoryMatrixMap() {
	 Map<String, String> catMatrix = new HashMap<String, String>();
	 //In by TRUCK
	 catMatrix.put(TRUCK_MODE + TRUCK_MODE, D_CATEGORY);
	 catMatrix.put(TRUCK_MODE + VESSEL_MODE, E_CATEGORY);
	 catMatrix.put(TRUCK_MODE + BARGE_MODE, D_CATEGORY);
	 catMatrix.put(TRUCK_MODE + RAIL_MODE, D_CATEGORY);
	 catMatrix.put(TRUCK_MODE + UNKNOWN_MODE + "M", P_CATEGORY);   //P for Empty
	 catMatrix.put(TRUCK_MODE + UNKNOWN_MODE + "F", E_CATEGORY);   //E for Full
	 //In by VESSEL
	 catMatrix.put(VESSEL_MODE + TRUCK_MODE, I_CATEGORY);
	 catMatrix.put(VESSEL_MODE + VESSEL_MODE, T_CATEGORY);
	 catMatrix.put(VESSEL_MODE + VESSEL_MODE + "S", R_CATEGORY);  //in & out vessel visits are the same and Restoe type
	 catMatrix.put(VESSEL_MODE + VESSEL_MODE + "N", "");  //in & out vessel visits are the same and not Restoe type , don't report
	 catMatrix.put(VESSEL_MODE + BARGE_MODE, I_CATEGORY);
	 catMatrix.put(VESSEL_MODE + RAIL_MODE, I_CATEGORY);
	 catMatrix.put(VESSEL_MODE + UNKNOWN_MODE + "M", P_CATEGORY);  //P for Empty
	 catMatrix.put(VESSEL_MODE + UNKNOWN_MODE + "F", I_CATEGORY);  //I for Full
	 //In by BARGE
	 catMatrix.put(BARGE_MODE + TRUCK_MODE, D_CATEGORY);
	 catMatrix.put(BARGE_MODE + VESSEL_MODE, E_CATEGORY);
	 catMatrix.put(BARGE_MODE + BARGE_MODE, D_CATEGORY);
	 catMatrix.put(BARGE_MODE + BARGE_MODE + "S", R_CATEGORY);  //in & out vessel visits are the same and Restow type
	 catMatrix.put(BARGE_MODE + BARGE_MODE + "N", "");  //in & out vessel visits are the same and Restow type, don't report
	 catMatrix.put(BARGE_MODE + RAIL_MODE, D_CATEGORY);
	 catMatrix.put(BARGE_MODE + UNKNOWN_MODE + "M", P_CATEGORY);  //P for Empty
	 catMatrix.put(BARGE_MODE + UNKNOWN_MODE + "F", E_CATEGORY);  //E for Full
	 //In by Rail
	 catMatrix.put(RAIL_MODE + TRUCK_MODE, D_CATEGORY);
	 catMatrix.put(RAIL_MODE + VESSEL_MODE, E_CATEGORY);
	 catMatrix.put(RAIL_MODE + BARGE_MODE, D_CATEGORY);
	 catMatrix.put(RAIL_MODE + RAIL_MODE, D_CATEGORY);
	 catMatrix.put(RAIL_MODE + UNKNOWN_MODE + "M", P_CATEGORY);   //P for Empty
	 catMatrix.put(RAIL_MODE + UNKNOWN_MODE + "F", E_CATEGORY);   //E for Full
	 //In by Unknown
	 catMatrix.put(UNKNOWN_MODE + TRUCK_MODE, I_CATEGORY);
	 catMatrix.put(UNKNOWN_MODE + VESSEL_MODE, E_CATEGORY);
	 catMatrix.put(UNKNOWN_MODE + BARGE_MODE, I_CATEGORY);
	 catMatrix.put(UNKNOWN_MODE + RAIL_MODE, I_CATEGORY);
	 catMatrix.put(UNKNOWN_MODE + UNKNOWN_MODE, "");   //Unknown + Unknown is undefined value
 
	 // CSDV-1450: kumarsu4 22/10/2013: find if POL Or POD is Non Operational when OBCarrier is vessel Or Unknown.
	 // For Non-operational Terminal
	 catMatrix.put(NON_OP_TERMINAL_CODE, D_CATEGORY);
	 return catMatrix;
   }
 
   /**
	* Method to return whether unit's inbound and outbound carrier visits are the same
	* @param inUnit
	* @param inUfv
	* @return
	*/
   private boolean isSameVesselOrBargeInOutCarrierVisit(Unit inUnit, UnitFacilityVisit inUfv,
														String inModalityId, String outModalityId) {
	 boolean isSameVisit = false;
	 if (!StringUtils.isBlank(inModalityId) && !StringUtils.isBlank(outModalityId)) {
	   if ((StringUtils.equals(inModalityId, VESSEL_MODE) && StringUtils.equals(outModalityId, VESSEL_MODE)) ||
			   (StringUtils.equals(inModalityId, BARGE_MODE) && StringUtils.equals(outModalityId, BARGE_MODE))) {
		 if (inUnit != null && inUfv != null) {
		   CarrierVisit inboundDeclaredIbCv = inUnit.getUnitDeclaredIbCv();
		   CarrierVisit inboundActualIbCv = inUfv.getUfvActualIbCv();
		   CarrierVisit outboundIntendedObCv = inUfv.getUfvIntendedObCv();
		   CarrierVisit outboundActualObCv = inUfv.getUfvActualObCv();
		   CarrierVisit inCv = (inboundActualIbCv != null) ? inboundActualIbCv : inboundDeclaredIbCv;
		   CarrierVisit outCv = (outboundActualObCv != null) ? outboundActualObCv : outboundIntendedObCv;
		   if (inCv != null && outCv != null) {
			 //GEN_BARGE/GEN_BARGE or GEN_VESSEL/GEN_VESSEL pairs will be counted as same visit??
			 isSameVisit = (StringUtils.equals(inCv.getCvId(), outCv.getCvId()));
		   }
		 }
	   }
	 }
 
	 return isSameVisit;
   }
 
   /**
	* Return Unit's inbound modality
	* @param inUnit
	* @param inUfv
	* @return
	*/
   private String getInboundModalityId(Unit inUnit, UnitFacilityVisit inUfv) {
	 //this.log("In getInboundModalityId");
	 String inModalityStr = null;
	 if (inUnit == null) {
	   return UNKNOWN_MODE;
	 }
 
	 CarrierVisit inboundCv = null;
	 if (inUfv != null) {
	   inboundCv = inUfv.getUfvActualIbCv();
	 }
 
	 if (inboundCv == null) {
	   inboundCv = inUnit.getUnitDeclaredIbCv();
	 }
 
	 if (inboundCv == null) {
	   //this.log("In getInboundModalityId, inboundCv == null");
	   return UNKNOWN_MODE;
	 }
 
	 //Special case to determine barge mode. Terminal will define a GEN_BARGE
	 //carrier visit id that represents a defined dummy visit for barge
	 if (GEN_BARGE.equalsIgnoreCase(inboundCv.getCvId())) {
	   inModalityStr = BARGE_MODE;
	 } else if (inboundCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL) &&
			 VesselClassificationEnum.BARGE.equals(inboundCv.getCvClassification())) {
	   inModalityStr = BARGE_MODE;
	 } else {
	   inModalityStr = inboundCv.getCvCarrierMode().getKey();
	 }
 
	 //this.log("Carrier visit inbound Mode= " + inboundCv.getCvCarrierMode());
	 //this.log("Carrier visit inbound Mode String= " + inboundCv.getCvCarrierMode().getKey());
	 //this.log("Carrier visit ID= " + inboundCv.getCvId());
	 //this.log("Returned inModalityStr= " + inModalityStr);
	 return inModalityStr;
   }
 
   /**
	* Return Unit's outbound modality
	* @param inUnit
	* @param inUfv
	* @return
	*/
   private String getOutboundModalityId(Unit inUnit, UnitFacilityVisit inUfv) {
	 //this.log("In getOutboundModalityId");
	 String outModalityStr = null;
	 if (inUnit == null) {
	   return UNKNOWN_MODE;
	 }
 
	 CarrierVisit outboundCv = null;
	 if (inUfv != null) {
	   outboundCv = inUfv.getUfvActualObCv();
	   if (outboundCv == null) {
		 outboundCv = inUfv.getUfvIntendedObCv();
	   }
	 }
 
	 if (outboundCv == null && inUnit.getUnitRouting() != null) {
	   outboundCv = inUnit.getUnitRouting().getRtgDeclaredCv();
	   this.log("Getting outbound routing from Unit Routing");
	 }
 
	 if (outboundCv == null) {
	   //this.log("In getOutboundModalityId, outboundCv == null");
	   return UNKNOWN_MODE;
	 }
 
	 //Special case to determine barge mode. Terminal will define a GEN_BARGE
	 //carrier visit id that represents a defined dummy visit for barge
	 if (GEN_BARGE.equalsIgnoreCase(outboundCv.getCvId())) {
	   outModalityStr = BARGE_MODE;
	 } else if (outboundCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL) &&
			 outboundCv.getCvClassification() != null &&
			 VesselClassificationEnum.BARGE.equals(outboundCv.getCvClassification())) {
	   outModalityStr = BARGE_MODE;
	 } else {
	   outModalityStr = outboundCv.getCvCarrierMode().getKey();
	 }
 
	 //this.log("Carrier visit outbound Mode= " + outboundCv.getCvCarrierMode());
	 //this.log("Carrier visit outbound Mode String= " + outboundCv.getCvCarrierMode().getKey());
	 //this.log("Carrier visit ID= " + outboundCv.getCvId());
	 //this.log("Returned outModalityStr= " + outModalityStr);
	 return outModalityStr;
   }
 
   /**
	* Get the Ufv of underlined unit that has latest departure time
	* @param inUnit
	* @return
	*/
   private UnitFacilityVisit getDepartedUfvForUnit(Unit inUnit) {
	 //this.log("getDepartedUfvForUnit unit.unitVisitState=$inUnit.unitVisitState")
 
	 if (!UnitVisitStateEnum.DEPARTED.equals(inUnit.getUnitVisitState())) {
	   return null;
	 }
 
	 UnitFacilityVisit resultUfv = null
	 Set<UnitFacilityVisit> ufvSet = inUnit.getUnitUfvSet();
	 if (ufvSet != null) {
	   for (UnitFacilityVisit testUfv : ufvSet) {
		 if (resultUfv == null) {
		   if (testUfv.getUfvTimeOut() != null) {
			 resultUfv = testUfv;
		   }
		 } else {
		   if (testUfv.getUfvTimeOut() && testUfv.getUfvTimeOut().after(resultUfv.getUfvTimeOut())) {
			 resultUfv = testUfv;
		   }
		 }
	   }
	 }
 
	 return resultUfv;
   }
 
   /**
	*
	* @param inUnit
	* @return
	*/
   private UnitFacilityVisit getRetiredUfvForUnit(Unit inUnit) {
	 //this.log("getRetiredUfvForUnit unit.unitVisitState=$inUnit.unitVisitState")
 
	 if (!UnitVisitStateEnum.RETIRED.equals(inUnit.getUnitVisitState())) {
	   return null;
	 }
 
	 UnitFacilityVisit resultUfv = null
	 Set<UnitFacilityVisit> ufvSet = inUnit.getUnitUfvSet();
	 if (ufvSet != null) {
	   for (UnitFacilityVisit testUfv : ufvSet) {
		 if (resultUfv == null) {
		   if (testUfv.getUfvTimeOut() != null) {
			 resultUfv = testUfv;
		   }
		 } else {
		   if (testUfv.getUfvTimeOut() && testUfv.getUfvTimeOut().after(resultUfv.getUfvTimeOut())) {
			 resultUfv = testUfv;
		   }
		 }
	   }
	 }
 
	 return resultUfv;
   }
 
   /**
	*
	* @param inUnit
	* @param inFacility
	* @return
	*/
   private UnitFacilityVisit getUfvForFacility(Unit inUnit, Facility inFacility) {
	 UnitFacilityVisit bestUfv = null;
	 final Set ufvSet = inUnit.getUnitUfvSet();
	 if (ufvSet != null) {
	   for (Iterator iterator = ufvSet.iterator(); iterator.hasNext();) {
		 final UnitFacilityVisit thisUfv = (UnitFacilityVisit) iterator.next();
		 if (thisUfv.isForFacility(inFacility)) {
		   if (bestUfv == null) {
			 bestUfv = thisUfv;
		   }
		 }
	   }
	 }
 
	 return bestUfv;
   }
 
   /*
	CSDV-1450: kumarsu4 21/10/13: Need to add in the General Reference Type/ID1/ID2 where Data Value 1 will be added to
   determine if the destination terminal is a domestic.
   */
 
   private String findValueFromGeneralReferenceForNonOperationalTerminal(final String type, final String ID1, final String ID2) {
	 final GeneralReference generalReference = GeneralReference.findUniqueEntryById(type, ID1, ID2);
	 if (generalReference != null) {
	   if (StringUtils.isNotBlank(generalReference.refValue1)) {
		 return generalReference.refValue1;
	   }
	 }
	 return null;
   }
   /**
	*
	* @param inStr
	* @return
	*/
   private String getNotNullValue(String inStr) {
	 return inStr != null ? inStr : StringUtils.EMPTY;
   }
 
   private final String JMS_MESSAGE_PROCESSOR = "RTOUnitEventMessageProcessor";
   private final String UNIT_CREATE = "UNIT_CREATE";
   private final String UNIT_REROUTE = "UNIT_REROUTE";
   private final String UNIT_RECTIFY = "UNIT_RECTIFY";
   private final String UNIT_PROPERTY_UPDATE = "UNIT_PROPERTY_UPDATE";
   private final String GEN_RAIL = "GEN_RAIL";
   private final String GEN_TRUCK = "GEN_TRUCK";
   private final String GEN_VESSEL = "GEN_VESSEL";
   private final String GEN_BARGE = "GEN_BARGE";
   private final String TRUCK_MODE = "TRUCK";
   private final String RAIL_MODE = "RAIL";
   private final String TRAIN_MODE = "TRAIN";
   private final String VESSEL_MODE = "VESSEL";
   private final String BARGE_MODE = "BARGE";
   private final String UNKNOWN_MODE = "UNKNOWN";
   private final String NON_OP_TERMINAL_CODE = "NON_OP_TERMINAL_CODE";
   private final String I_CATEGORY = "I";
   private final String E_CATEGORY = "E";
   private final String D_CATEGORY = "D";
   private final String R_CATEGORY = "R";
   private final String T_CATEGORY = "T";
   private final String P_CATEGORY = "P";
   private final String EVENT_RTO_CATEGORY_CHANGE = "RTO_CATEGORY_CHANGE";
   private final List<String> POD_LIST = new ArrayList<String>();
   private final List<String> OUTBOUND_CV_LIST = new ArrayList<String>();
   private final List<String> INBOUND_CV_LIST = new ArrayList<String>();
   // CSDV-1356: dickspa 03/10/2013: Need to add in the General Reference Type/ID1 where the Facility has been given.
   // This is added as a temporary fix, originally done by Sophia, to try to fix a missing/incorrect RTO issue.
   private final String ACTIVITY_MESSAGE_GR = "ACTIVITY_MESSAGE";
   private final String FACILITY_ID1_GR = "FACILITY_ID";
   private final MetafieldId GEN_REF_FACILITY_FIELD = ArgoField.REF_VALUE1;
 
   // CSDV-1450: kumarsu4 21/10/13: Need to add in the General Reference Type/ID1/ID2 where Data Value 1 will be added to
   // determine if the destination terminal is a domestic.
   private final String RTO_CATEGORY_TYPE = "RTOCATEGORY";
   private final String NON_OPR_FACILITY_ID1_GR = "NON_OPR_FACILITY";
   private final String CONVERT_DOMESTIC_ID2_GR = "CONVERT_DOMESTIC";
 }
 
 