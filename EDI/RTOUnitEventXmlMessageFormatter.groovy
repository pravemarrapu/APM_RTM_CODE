
/*
  $Id: RTOUnitEventXmlMessageFormatter.groovy 225337 2015-08-04 11:10:05Z extneefpa $

  Author: Allen Hsieh
  Date: 2013.01.29
  FDR: FDR_APMTMVII+RWG_RTO_20121128.doc
  CSDV-676: Generate event-triggered activity messages in a custom format to RTO and post to JMS queue.
			This is a joint development project with APMT MVII and RWG.

  This groovy script will be deployed via Groovy Plug-ins view
  Installation Instructions:
   Load this groovy script to N4:
		  1. Go to Administration --> System --> Groovy Plug-ins
		  2. Click Add (+)
		  3. Copy and paste groovy script to Groovy Code area
			 enter "RTOUnitEventXmlMessageFormatter" in Short Description field
		  4. Click Save button
  **
  *
  *
  Merger : Paul de Neef
  Date     : 2015.08.03
  Modifier: K.Sundar
  Date: 2015.05.13
  CSDV-2803: New Event EVENT_DRAY_OUT_SET_RTO_CATEGORY is added to buildSupportedEventList()
  UNIT_CREATE event will be sent to RTO if DRAY_OUT_SET_RTO_CATEGORY is added to a unit
  To incorporate this fix RTOUnitEventMessageProcessor needs to be called in
  General notices with filter "Dray Status = Dray Out and Back"
  *
  Modifier : Paul de Neef
  Date     : 2015.07.30
  CSDV-3128:  Modified buildUfvRelatedElements()
  For the events UNIT_RECEIVE, if the unit in-time is null, then get the
  timestamp of event itself. Similar things have been done in CSDV-2988.
  *
  Modifier: Paul de Neef
  Date    : 2015.06.30
  Modification for CSDV-2819 and further including: CSDV-2821, CSDV-2822, CSDV-2988.
  *
  Modifier: Paul de Neef
  Date: 2015.06.12
  Starting from revision: 220207
  + CSDV-2988: Included (from revision 220984 )
  - CSDV-2803: Not included in this revision to get back to a consistent and stable customer delivery
   This is because CSDV-2988 is a high priority case and the UNIT_DRAY_IN_OUT still gives us errors.
  **
  *
  Modifier: Paul Kingsly
  Date: 2015.06.03
  CSDV-2988: Modified buildUfvRelatedElements()
  For the events UNIT_DISCH and UNIT_DERAMP, if the unit in-time is null, then get the
  timestamp of event itself.
  **
  *
  -- Modifier: K.Sundar
  -- Date: 2015.05.13
  -- CSDV-2803: New Event EVENT_DRAY_OUT_SET_RTO_CATEGORY is added to buildSupportedEventList()
  -- UNIT_CREATE event will be sent to RTO if DRAY_OUT_SET_RTO_CATEGORY is added to a unit
  -- To incorporate this fix RTOUnitEventMessageProcessor needs to be called in
  -- General notices with filter "Dray Status = Dray Out and Back"
  *
  *
  **
  *
  Modifier: Paul Kingsly(CSDV-2821) and K.Sundar(CSDV-2822)
  Date: 2015.05.25
  If rtoCategory is Null or “” then No other parameters will be checked/validated
  and No activity message will be sent to RTO for that selected UNIT
  **
  *
  Modifier: Paul Kingsly
  Date: 2015.05.13
  CSDV-2819: Modified buildInboundElement function.  Added code to get ERO if the Unit is of category STORAGE.
  **
  *
  Modifier: Chennai Merger: PdN
  Date: 22/04/2015
  Description: CSDV-2789. In-time and out-time
  *
  Modifier: Amos Parlante
  Date: 19/3/2015
  Description: Fixed CSDV-2796. There was a typo in the Groovy.
			   Fixed CSDV-2619. The field-changes had some bad logic in it that needed cleaning up.
  *
  **
  Modifier: Allen Hsieh
  Date: 2013.02.01
  Description: Fixed null value exception resulted from null operator-name
 *
 **
   Modifier: Allen Hsieh
   Date: 2013.02.06
   Description: Added support of freight kind change. Report UNIT_STUFF event when freight kind
				is changed from empty to other kind. Otherwise, report UNIT_STRIP event.
  *
  **
   Modifier: Allen Hsieh
   Date: 2013.02.14
   Description: Modified scritp to address issue of not reporting UNIT_ACTIVATE event
   *
   **
	Modifier: Allen Hsieh
	Date: 2013.02.15
	Description: Added UNIT_ROLL event that the UNIT_REROUTE will be reported as eventName
				 if the mode of outbound transport changes, else send the
				 UNIT_PROPERTY_UPDATE as eventName in the activity message
	*
   **
   Modifier: Allen Hsieh
   Date: 2013.02.18
   Description:
	UNIT_RECTIFY where the Inbound or Outbound carrier has been changed. If the (inbound or outbound) mode
	  of transport has changed report UNIT_REROUTE in the activity message, else report the UNIT_PROPERTY_UPDATE.
	UNIT_REROUTE. If the outbound mode of transport has changed report UNIT_REROUTE in the activity message,
	  else report the UNIT_PROPERTY_UPDATE.
   *
 **
 **
  Modifier: Allen Hsieh
  Date: 2013.02.21
  Description: Overloaded execute method by adding a boolean argument that will be passed onto Formatter
			   script to report UNIT_PROPERTY_UPDATE event from RTO category calculation script.
  **
  **
  Modifier: Allen Hsieh
  Date: 2013.03.01
  Description: Modified script in buildContentsElement method to properly handle situations
			   when some of fields' value is null.
  **
 **
 Modifier: Allen Hsieh
 Date: 2013.04.06
 Description: RTO is expecting the Time-out attribute to be populated with the load onto barge/vessel/rail timestamps
			  when an activity message is sent and the T-state = Loaded/Departed.
			  Any other T-state and the value should not be populated (=blanked out).
**
**
 Modifier: Allen Hsieh
 Date: 2013.04.17
 Description: Map truck license nbr to inbound or outbound voyage nbr
*
**
*
**
 Modifier: Allen Hsieh
 Date: 2013.05.14
 Description: Added a general reference "ACTIVITY_MESSAGE" and "FACILITY_ID" to get facility Id
			  in case it is missing.
*
**
 Modifier: Allen Hsieh
 Date: 2013.05.17
 CSDV-1106
 Description: the ActivityMessageFormatter script is populating the contents.gross-weight with the wrong value.
	   Currently the value is populated by the getUnitGoodsAndCtrWtKg in code part.
	   The expected N4 value to be populated in this field is the unitCargoWeight in Kg.
*
* **
 Modifier: Allen Hsieh
 Date: 2013.05.17
 CSDV-1108
 Description:
1. for carrier mode rail/train the inbound-actual-carrier-visit.crn / outbound-actual-carrier-visit.crn attributes
 need to be populated by cvdCv.cvCustomsId
*
2. for carrier mode truck inbound-actual-carrier-visit.callsign / outbound-actual-carrier-visit.callsign
 needs to be populated by the truck License plate
*
3. for carrier mode truck inbound-actual-carrier-visit.inboundvoyage / outbound-actual-carrier-visit.outboundvoyage
 needs to be populated by the truck license plate
*
4. for carrier mode vessel the inbound-actual-carrier-visit.id / outbound-actual-carrier-visit.id
 needs to be populated by the vessel id(vesId) instead of the vessel visit id
*
**

 Modifier: Danny Holthuijsen
 Date: 2013.05.17
 CSDV-1108 bug fixing changed object cvCustomerId to cvCustomsId
*
**
 Modifier: Allen Hsieh
 Date: 2013.05.21
 CSDV-1108: Populate vvd in method getCrnValue for non-barge vessel
**
*
**
 Modifier: Danny Holthuijsen
 Date: 2013.06.28
 changed unitFlexString01 to ufvFlexString07
*
**

 Modifier: Allen hsieh
 Date: 2013.07.01
 CSDV-1148: Implemented a special case where outbound carrier is changed from deep-sea to barge vessel or vice versa
*
**
 Modifier: Paul Dickson
 Date: 2013.09.05
 CSDV-1121: Implemented field-change name lookups, plus three other special field-change cases.
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
import com.navis.argo.business.api.GroovyApi
import com.navis.argo.business.api.IEvent
import com.navis.argo.business.api.ServicesManager
import com.navis.argo.business.atoms.CarrierVisitPhaseEnum
import com.navis.argo.business.atoms.FlagPurposeEnum
import com.navis.argo.business.atoms.FreightKindEnum
import com.navis.argo.business.atoms.LocTypeEnum
import com.navis.argo.business.atoms.UnitCategoryEnum
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.model.Complex
import com.navis.argo.business.model.Facility
import com.navis.argo.business.model.GeneralReference
import com.navis.argo.business.reference.Commodity
import com.navis.argo.business.reference.Equipment
import com.navis.argo.business.reference.RoutingPoint
import com.navis.argo.business.reference.UnLocCode
import com.navis.argo.util.XmlUtil
import com.navis.framework.business.Roastery
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.UserContext
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.inventory.InventoryEntity
import com.navis.inventory.InventoryField
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.atoms.UfvTransitStateEnum
import com.navis.inventory.business.atoms.UnitVisitStateEnum
import com.navis.inventory.business.imdg.HazardItem
import com.navis.inventory.business.imdg.Hazards
import com.navis.inventory.business.units.*
import com.navis.orders.business.eqorders.EquipmentOrder
import com.navis.rail.business.entity.Railroad
import com.navis.rail.business.entity.TrainVisitDetails
import com.navis.reference.business.locale.RefCountry
import com.navis.reference.business.locale.RefState
import com.navis.road.RoadEntity
import com.navis.road.RoadField
import com.navis.road.business.atoms.TranStatusEnum
import com.navis.road.business.model.TruckTransaction
import com.navis.services.ServicesField
import com.navis.services.business.event.Event
import com.navis.services.business.event.EventFieldChange
import com.navis.services.business.event.GroovyEvent
import com.navis.services.business.rules.EventType
import com.navis.services.business.rules.Flag
import com.navis.services.business.rules.FlagType
import com.navis.vessel.business.atoms.VesselClassificationEnum
import com.navis.vessel.business.atoms.VesselTypeEnum
import com.navis.vessel.business.schedule.VesselVisitDetails
import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.lang.StringUtils
import org.jdom.Element
import org.jdom.Namespace
import org.jetbrains.annotations.NotNull
import org.jetbrains.annotations.Nullable

import java.text.DecimalFormat
import java.text.MessageFormat
import java.text.SimpleDateFormat

public class RTOUnitEventXmlMessageFormatter extends GroovyInjectionBase {
	public String format(Unit inUnit, GroovyEvent inEvent, boolean rtoCatPropertyUpdate) {
		UserContext context = ContextHelper.getThreadUserContext();
		// CSDV-1356: dickspa 03/10/2013: Set the facility to be the Facility in the General Reference entry.
		Facility facility = getFacilityFromGeneralReference(ACTIVITY_MESSAGE_GR, FACILITY_ID1_GR, GEN_REF_FACILITY_FIELD);
		// Do need to check that the facility has been supplied correctly, otherwise we'll be setting the Unit's facility to null each time
		if (facility == null) {
			log("Please add a General Reference entry [" + ACTIVITY_MESSAGE_GR + "/" + FACILITY_ID1_GR +
					"] with the current facility given in Value-1.");
			return;
		}
		Complex complex = ContextHelper.getThreadComplex();
		Date timeNow = ArgoUtils.convertDateToLocalDateTime(ArgoUtils.timeNow(), context.getTimeZone());
		this.log("Groovy: RTOUnitEventXmlMessageFormatter - starts " + timeNow);

		if (inUnit == null) {
			this.log("Groovy: RTOUnitEventXmlMessageFormatter - unit is null ");
			return null;
		}

		if (inEvent == null) {
			this.log("Groovy: RTOUnitEventXmlMessageFormatter - event is null ");
			return null;
		}

		String eventType = inEvent.getEvent().getEventTypeId();

		//Build a list of events that need to report to RTO via activity message
		this.buildSupportedEventList();
		//Filter out unwanted or unsupported events
		if (EVENT_LIST.size() > 0 && !EVENT_LIST.contains(eventType)) {
			this.log("Groovy: RTOUnitEventXmlMessageFormatter - Event type " + eventType + " is not supported in activity message.");
			//this.log("Total supported event types= " + EVENT_LIST.size());
			return null;
		}
		this.log("Formatter Processing event type= " + eventType);

		UnitFacilityVisit ufv = null;
		//this.log("RTOUnitEventXmlMessageFormatter - Unit visit state= " + inUnit.getUnitVisitState().getKey());
		if (inUnit.unitVisitState == UnitVisitStateEnum.DEPARTED) {
			ufv = this.getDepartedUfvForUnit(inUnit);
		} else if (inUnit.unitVisitState == UnitVisitStateEnum.RETIRED) {
			ufv = this.getRetiredUfvForUnit(inUnit);
	} else if (inUnit.unitVisitState == UnitVisitStateEnum.ACTIVE && StringUtils.equals(eventType, EVENT_UNIT_OUT_GATE)) {
	  ufv = findDepartedUfv(inUnit);
	}
	else {
			ufv = inUnit.getUnitActiveUfvNowActive();
		}

		// CSDV-1356: dickspa 03/10/2013: Facility will now always be non-null.
		this.log("Facility Id= " + facility.getFcyId());
		if (ufv == null) {
			ufv = this.getUfvForFacility(inUnit, facility);
		}

		if (ufv == null) {
			this.log("Groovy: RTOUnitEventXmlMessageFormatter - ufv is null ");
			return null;
		}

		//Populate fieldChanges set
		Set changes = this.getFieldChangesForEvent(inEvent);
		String reportedEventType = null;
		//Evaluate if need to report another event type name that is known to RTO
		//this.log("Before calling evaluateReportedEventTypeId");
		reportedEventType = this.evaluateReportedEventTypeId(inEvent, inUnit, ufv, eventType,
				changes, complex, facility, rtoCatPropertyUpdate);
		if (reportedEventType != null) {
			this.log("Evaluated reported Event Type = " + reportedEventType);
		}

		//CSDV -2821 / CSDV-2822 - Start
		String rtoCategory = ufv.getUfvFlexString07();

		if ((rtoCategory == "") || StringUtils.isBlank(rtoCategory)) {
		  this.log("Unit:"+ inUnit.getUnitId() +" "+ eventType + " ignored as RTO category is not either Import, Transship, Storage or Restow");
		  return null;
		}
		//CSDV -2821 / CSDV-2822 - End
		//this.log("After calling evaluateReportedEventTypeId");
		//Filter out UNIT_PROPERTY_UPDATE event generated by changes of field that is not interested by RTO
		if (reportedEventType == null &&
				StringUtils.equals(eventType, EVENT_UNIT_PROPERTY_UPDATE)) {
			//Stop processing for UNIT_PROPERTY_UPDATE event as unit's property change that
			//caused the recording of this event is not included or supported
			this.log("UNIT_PROPERTY_UPDATE ignored as the property change is not wanted in RTO");
			return null;
		}

		//Filter out EVENT_UNIT_ACTIVATE event if RTO category is not wanted
		if (reportedEventType == null &&
				StringUtils.equals(eventType, EVENT_UNIT_ACTIVATE)) {
			//Stop processing for EVENT_UNIT_ACTIVATE event as it will need to be reported as
			//UNIT_CREATE event when RTO category is either Import, Transship, Storage or Restow
			this.log("EVENT_UNIT_ACTIVATE ignored as RTO category is not either Import, Transship, Storage or Restow");
			return null;
		}

		//Start building the activity message
		//Add ActivityMsg element as root element
		//Namespace sNS = Namespace.getNamespace("sn4", "http://www.w3.org/2000/xmlns");
		Namespace sNS = Namespace.getNamespace("sn4", "http://www.navis.com/sn4");
		Element eActivityMsg = new Element("ActivityMsg", sNS);
		//Build Root and header elements
		this.buildRootAndHeaderElements(eActivityMsg, inEvent, reportedEventType, sNS);

		//Add event element
		Element eEvent = new Element("event", sNS);
		this.buildEventElement(eActivityMsg, eEvent, inEvent, reportedEventType);

		//Add field changes element
		//CSDV-1121 : Special case POD where only the last POD requires to be translated to the expected value, by
		//PChidambaram <ext.priya.chidambaram@navis.com> , Aug 30 2013
		// CSDV-1121: dickspa 05/09/2013: Need to pass the Unit in to check for RTO_CATEGORY_CHANGE events. Removed the Routing parameter.
		if (changes.size() != 0) {
			this.buildFieldChanges(eEvent, changes, sNS, inUnit);
		} else {
			this.log("There is no field change for event " + eventType);
		}

		//Build unit element
		Element eUnit = new Element("unit", sNS);
		//Namespace sNS1 = Namespace.getNamespace("sn4", "someNamespace");
		eEvent.addContent(eUnit);
		this.buildUnitAndStatusRemarksAgentsElements(eUnit, inUnit, ufv, sNS);

		//build primaryEq-1 element under unit element
		this.buildPrimaryEqElement(inUnit, eUnit, sNS);

		GoodsBase goods = inUnit.getUnitGoods();
		//Adding contents element under unit element
		this.buildContentsElement(inUnit, eUnit, goods, sNS);

		//Adding damages element under unit element
		List<UnitEquipDamageItem> damages = this.getDamagesForUnit(inUnit)
		if (damages.size() != 0) {
			//damages element
			this.buildDamagesElement(damages, eUnit, sNS);
		} //end of adding damages

		// CSDV-1121: dickspa 05/09/2013: Moved this routing declaration back here as this is where it is first required.
		Routing routing = inUnit.getUnitRouting();
		if (routing != null) {
			//build routing element under unit element
			Element eRouting = new Element("routing", sNS);
			eUnit.addContent(eRouting);
			//Adding inbound element under routing element
			this.buildInboundElement(inUnit, ufv, eRouting, routing, goods, sNS);
			//Adding outbound element under routing element
			this.buildOutboundElement(inUnit, ufv, eRouting, routing, goods, sNS);
			//Adding ports element under routing element
			this.buildPortsElement(eRouting, routing, sNS);
			//Done with routing element
		}

		//Adding impediments element under unit element
		this.buildImpedimentsElement(inUnit, eUnit, sNS);

		//Adding stops element under unit element
		this.buildStopsElement(inUnit, eUnit, sNS);

		//Add active-holds element under unit element
		Set<Flag> activeHolds = this.getActiveHoldsForUnit(inUnit);
		if (activeHolds.size() > 0) {
			this.buildActiveHoldsElement(inUnit, eUnit, activeHolds, sNS);
		}

		//Add BL, Consignee, shiper elements under unit element
		if (goods != null) {
			this.buildGoodsBLConsigneeShipperElements(goods, eUnit, sNS);
		}

		//Add Unit and Ufv flex-fields elements under unit element
		this.buildFlexFieldsElement(inUnit, ufv, eUnit, sNS);

		//Add time-in, time-out, time-load, last-known-position, arrive position elements under unit element
		if (ufv != null) {
			this.buildUfvRelatedElements(inEvent, ufv, eUnit, sNS);
		}

		this.log("Groovy: RTOUnitEventXmlMessageFormatter - ends " + timeNow);
		//this.log("generated activity message: " + XmlUtil.XML_HEADER + XmlUtil.toString(eActivityMsg, true));
		String response	 = XmlUtil.XML_HEADER + XmlUtil.toString(eActivityMsg, false);
		log("response:$response");
		return response;
	}

//CSDV-2789  To find the
  private UnitFacilityVisit findDepartedUfv(final Unit inCurrentUnit) {

	Equipment equipment = inCurrentUnit.getPrimaryEq();
	if (equipment == null) {
	  this.log("No equipment is attached to the Unit");
	  return null;
	}

	DomainQuery dq = QueryUtils.createDomainQuery(InventoryEntity.UNIT_FACILITY_VISIT)
			.addDqPredicate(PredicateFactory.eq(UnitField.UFV_UNIT_GKEY, inCurrentUnit.getUnitGkey()))
			.addDqPredicate(PredicateFactory.eq(UnitField.UFV_VISIT_STATE, UnitVisitStateEnum.DEPARTED))
			.addDqPredicate(PredicateFactory.eq(UnitField.UFV_TRANSIT_STATE, UfvTransitStateEnum.S70_DEPARTED))
			.addDqOrdering(Ordering.desc(UnitField.UFV_GKEY))
			.setDqMaxResults(1);
	dq.setRequireTotalCount(false);  // improves performance
	List<UnitFacilityVisit> ufvList = Roastery.getHibernateApi().findEntitiesByDomainQuery(dq);
	if (ufvList.isEmpty()) {
	  return null;
	} else {
	  return ufvList.get(0);
	}
  }
//CSDV-2789 - End

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
	 * @param inEvent
	 * @param inUnit
	 * @param eventType
	 * @return
	 */
	private String evaluateReportedEventTypeId(GroovyEvent inEvent, Unit inUnit,
											   UnitFacilityVisit ufv, String eventType, Set changes,
											   Complex complex, Facility facility, boolean rtoCatPropertyUpdate) {
		//UNIT_REROUTE and UNIT_CREATE events are evaluated in MVIICalculateRTOCategory.groovy class
		if (inEvent == null || inUnit == null || StringUtils.isBlank(eventType)) {
			return null;
		}

		//this.log("In event type= " + eventType);
		String reportEventId = null;
		//CSDV-2803
		if (StringUtils.equals(eventType, EVENT_UNIT_ACTIVATE) ||
				StringUtils.equals(eventType, EVENT_DRAY_OUT_SET_RTO_CATEGORY) ) {
			//RTO category should be calculated and stored in ufvFlexString07 field
			//String rtoCategory = inUnit.getUnitFlexString01();
			String rtoCategory = ufv.getUfvFlexString07();
			// If the RTO category has not been set, then calculate it.
			if (StringUtils.isBlank(rtoCategory)) {
				Object rtoCatCalculation = this.getGroovyClassInstance(RTO_CATEGORY_CALCULATION_CLASS);
				if (rtoCatCalculation != null) {
					rtoCatCalculation.execute(inEvent);
				}
				rtoCategory = ufv.getUfvFlexString07();
//        this.log("rtoCategory after recalculation= " + rtoCategory);
			}
			// By this point, maybe the RTO category was already set, and maybe it was just set.
			// Either way, the RTO category should probably not be blank at this point.. but should not remove
			// this check until I'm sure that the RTO_CATEGORY_CALCULATION_CLASS always sets the RTO Category.
			// CSDV-1652: dickspa 20/02/2014: A UNIT_ACTIVATE event should always be reported as UNIT_CREATE if the RTO Category is set.
			if (StringUtils.isNotBlank(rtoCategory)) {
				//If RTO category is Import, Transship, or Storage, report the event type
				//as UNIT_CREATE instead of UNIT_ACTIVATE
				reportEventId = EVENT_UNIT_CREATE;
			}
		} else if (StringUtils.equals(eventType, EVENT_UNIT_REROUTE)) {
			//if (this.fieldChangesIncludePODs(changes)) {
			//Report UNIT_PROPERTY_UPDATE event instead if PODs changes in UNIT_REROUTE event
			//reportEventId = EVENT_UNIT_PROPERTY_UPDATE;
			//this.log("Field changes include PODs");
			//}
			//Report UNIT_REROUTE or UNIT_PROPERTY_UPDATE event instead when UNIT_REROUTE event is recorded
			reportEventId = this.fieldChangesIncludeUnitReroute(eventType, changes, inUnit, ufv, complex, facility);
			//this.log("Field changes include UNIT REROUTE");
		} else if (StringUtils.equals(eventType, EVENT_UNIT_PROPERTY_UPDATE)) {
			//evaluate for different field changes
			if (rtoCatPropertyUpdate) {
				this.log("Unit Property Update by RTO Cal Calculation[rtoCatPropertyUpdate]= " + rtoCatPropertyUpdate);
				reportEventId = EVENT_UNIT_PROPERTY_UPDATE;
			} else {
				//create a new method to handle all this field changes
				reportEventId = this.handleUnitPropertyUpdateEvent(eventType, changes);
			}
		} else if (StringUtils.equals(eventType, EVENT_UNIT_STRIP)) {
			if (inUnit.getUnitFreightKind().equals(FreightKindEnum.BBK)) {
				//Send UNIT_DISCH if freight kind is B-bulk
				reportEventId = EVENT_UNIT_DISCH;
			}
		} else if (StringUtils.equals(eventType, EVENT_UNIT_ROLL)) {
			//Report UNIT_REROUTE or UNIT_PROPERTY_UPDATE event instead when UNIT_ROLL event is recorded
			reportEventId = this.fieldChangesIncludeUnitRoll(eventType, changes, inUnit, ufv, complex, facility);
			//this.log("Field changes include UNIT ROLL");
		} else if (StringUtils.equals(eventType, EVENT_UNIT_RECTIFY)) {
			//Report UNIT_REROUTE or UNIT_PROPERTY_UPDATE event instead when UNIT_ROLL event is recorded
			reportEventId = this.fieldChangesIncludeUnitRectify(eventType, changes, inUnit, ufv, complex, facility);
			//this.log("Field changes include UNIT ROLL");
		}

		//this.log("Report event type= " + reportEventId);
		return reportEventId;
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
	private String fieldChangesIncludeUnitReroute(String eventType, Set changes,
												  Unit inUnit, UnitFacilityVisit ufv,
												  Complex complex, Facility facility) {

		String reportEventId = null;
		if (!StringUtils.equals(eventType, EVENT_UNIT_REROUTE)) {
			return reportEventId;
		}

		// CSDV-1121: dickspa 05/09/2013: As agreed with Danny (confirmed on CSDV-1121), if the
		// Unit has had its RTO Category changed (RTO_CATEGORY_CHANGE event just been applied),
		// then return UNIT_REROUTE. Else return UNIT_PROPERTY_UPDATE.
		if (this.hasUnitRTOCategoryChanged(inUnit)) {
			reportEventId = EVENT_UNIT_REROUTE;
			this.log("Unit RTO Category just changed so UNIT_REROUTE reported as UNIT_REROUTE");
		}
		else {
			reportEventId = EVENT_UNIT_PROPERTY_UPDATE;
			this.log("Unit RTO Category has not changed so UNIT_PROPERTY_UPDATE reported instead of UNIT_REROUTE");
		}

		return reportEventId;
	}

	/**
	 * CSDV-1121: dickspa 05/09/2013: Looks for an RTO_CATEGORY_CHANGE event that has been applied to the given Unit
	 * within the last minute. If it exists, then return it. Else return null.
	 * @param unit
	 * @return
	 */
	@Nullable
	private IEvent getUnitRTOCategoryChangeJustApplied(@NotNull final Unit unit) {
		IEvent appliedRtoCategoryChangeEvent = null;
		final ServicesManager servicesManager = (ServicesManager)Roastery.getBean(ServicesManager.BEAN_ID);
		final String rtoCategoryChangeEventName = "RTO_CATEGORY_CHANGE";
		final EventType rtoCategoryChangeEvent = EventType.findEventType(rtoCategoryChangeEventName);
		// If this Event has not been defined in N4, should log out an error here and return.
		if (rtoCategoryChangeEvent == null) {
			log("Bad Configuration: Event Type " + rtoCategoryChangeEventName + " not defined in N4. " + EVENT_UNIT_REROUTE +
					" may not be reported correctly.");
			return null;
		}

		// Now, find the most recent applied instance of this RTO_CATEGORY_CHANGE
		final IEvent mostRecentRTOCategoryChangeEvent = servicesManager.getMostRecentEvent(rtoCategoryChangeEvent, unit);
		// If a RTO_CATEGORY_CHANGE has not been recorded, then the RTO Category has not changed
		if (mostRecentRTOCategoryChangeEvent == null) {
			log("No " + rtoCategoryChangeEventName + " event has been recorded. Unit's RTO Category has not changed (ever).");
			return null;
		}

		// So now a RTO Category change has occurred at some point. Need to check if is has been recorded recently (within the last minute)
		final Date eventAppliedTime = mostRecentRTOCategoryChangeEvent.getEventTime();
		// Need to add a minute on to the time the event was applied. If this is now after the current time, then the event was applied
		// within the last minute
		// CSDV-1702: dickspa 17/03/2014: Change this to 5 seconds  (since one minute gives too much time for mis-reporting)
		final Date eventAppliedTimePlusFiveSeconds = getDateWithModification(eventAppliedTime, 5, Calendar.SECOND);
		if (eventAppliedTimePlusFiveSeconds.after(ArgoUtils.timeNow())) {
			appliedRtoCategoryChangeEvent = mostRecentRTOCategoryChangeEvent;
		}
		else {
			log(String.format("When checking if Unit RTO Category just changed, did find %s event but it was applied at %s (current time is %s).",
					rtoCategoryChangeEventName, eventAppliedTime.toString(), ArgoUtils.timeNow().toString()));
		}

		return appliedRtoCategoryChangeEvent;
	}

	/**
	 * CSDV-1121: dickspa 05/09/2013: If the Unit has had a RTO_CATEGORY_CHANGE event applied
	 * in the last minute, then return true. Else return false.
	 * @param unit
	 * @return
	 */
	@NotNull
	private boolean hasUnitRTOCategoryChanged(@NotNull final Unit unit) {
		final IEvent appliedRtoCategoryChangeEvent = getUnitRTOCategoryChangeJustApplied(unit);
		return appliedRtoCategoryChangeEvent != null;
	}

	/**
	 * CSDV-1121: dickspa 05/09/2013: Adds the supplied amount to the given field, using the Calendar class.
	 * @param date
	 * @param amountToChange
	 * @param fieldToChange
	 * @return
	 */
	@NotNull
	private Date getDateWithModification(@NotNull final Date date, @NotNull final int amountToChange, @NotNull final int fieldToChange) {
		final Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		// CSDV-1702: dickspa 17/03/2014: Switch the input fields to be correct order
		calendar.add(fieldToChange, amountToChange);
		return calendar.getTime();
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
	private String fieldChangesIncludeUnitRectify(String eventType, Set changes,
												  Unit inUnit, UnitFacilityVisit ufv,
												  Complex complex, Facility facility) {

		String reportEventId = null;
		if (!StringUtils.equals(eventType, EVENT_UNIT_RECTIFY)) {
			return reportEventId;
		}

		if (this.fieldChangesIncludeOutboundCarrierMode(changes, inUnit, ufv, complex, facility) ||
				this.fieldChangesIncludeInboundCarrierMode(changes, inUnit, ufv, complex, facility)) {
			//Report UNIT_REROUTE instead when inbound or outbound transport mode is changed
			reportEventId = EVENT_UNIT_REROUTE;
			this.log("UNIT_REROUTE reported instead of UNIT_RECTIFY");
		} else {
			//Report UNIT_PROPERTY_UPDATE event otherwise
			reportEventId = EVENT_UNIT_PROPERTY_UPDATE;
			this.log("UNIT_PROPERTY_UPDATE reported instead of UNIT_RECTIFY");
		}

		return reportEventId;
	}

	/**
	 *
	 * @param eventType
	 * @param changes
	 * @return
	 */
	private String fieldChangesIncludeUnitRoll(String eventType, Set changes,
											   Unit inUnit, UnitFacilityVisit ufv,
											   Complex complex, Facility facility) {

		String reportEventId = null;
		if (!StringUtils.equals(eventType, EVENT_UNIT_ROLL)) {
			return reportEventId;
		}

		if (this.fieldChangesIncludeOutboundCarrierMode(changes, inUnit, ufv, complex, facility)) {
			//Report UNIT_REROUTE instead when outbound transport mode is changed
			reportEventId = EVENT_UNIT_REROUTE;
			//this.log("UNIT_REROUTE reported instead of UNIT_ROLL");
		} else {
			//Report UNIT_PROPERTY_UPDATE event otherwise
			reportEventId = EVENT_UNIT_PROPERTY_UPDATE;
			//this.log("UNIT_PROPERTY_UPDATE reported instead of UNIT_ROLL");
		}

		return reportEventId;
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
			if (this.isGenericCarrierId(prev))  {
				prevCarrierMode = prev.substring(4);
				//this.log("new prevCarrierMode= " + prevCarrierMode);
			} else {
				cvPrev = this.findFacilityCvById(complex, facility, prev)
			}
			if (this.isGenericCarrierId(post))  {
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
	 * @param changes
	 * @return
	 */
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
			VesselClassificationEnum preVslClass = null;
			VesselClassificationEnum postVslClass = null;
			//this.log("prev= " + prev);
			//this.log("post= " + post);
			if (this.isGenericCarrierId(prev))  {
				prevCarrierMode = prev.substring(4);
				//this.log("new prevCarrierMode= " + prevCarrierMode);
			} else {
				cvPrev = this.findFacilityCvById(complex, facility, prev)
			}
			if (this.isGenericCarrierId(post))  {
				postCarrierMode = post.substring(4);
				//this.log("new postCarrierMode= " + postCarrierMode);
			} else {
				cvPost = this.findFacilityCvById(complex, facility, post)
			}

			if (cvPrev != null) {
				prevCarrierMode = cvPrev.getCvCarrierMode().getKey();
				//this.log("cvPrev is not null " + prevCarrierMode);
				preVslClass = (VesselClassificationEnum) cvPrev.getCvClassification();
				//this.log("preVslClass= " + preVslClass);
			}
			if (cvPost != null) {
				postCarrierMode = cvPost.getCvCarrierMode().getKey();
				//this.log("cvPost is not null " + postCarrierMode);
				postVslClass = (VesselClassificationEnum) cvPost.getCvClassification();
				this.log("postVslClass= " + postVslClass);
			}

			if (!StringUtils.equals(prevCarrierMode, postCarrierMode)) {
				//The inbound carrier mode is changed
				//this.log("includeObCv = true.1");
				includeObCv = true;
			} else {
				//this.log("else");
				if (StringUtils.equalsIgnoreCase("VESSEL", prevCarrierMode) &&
						StringUtils.equalsIgnoreCase("VESSEL", postCarrierMode)) {
					if (this.isVesselClassificationChangedToBarge(preVslClass, postVslClass)) {
						includeObCv = true;
						//this.log("includeObCv = true.2");
					}
				}
			}
		}

		return includeObCv;
	}

	/**
	 *
	 * @param preVslClass
	 * @param postVslClass
	 * @return
	 */
	private boolean isVesselClassificationChangedToBarge(VesselClassificationEnum preVslClass,
														 VesselClassificationEnum postVslClass) {

		boolean vslChangedToBarge = false;
		if (preVslClass == null && postVslClass == null) {
			vslChangedToBarge = false;
			//this.log("1");
		} else if (preVslClass == null) {
			if (postVslClass.equals(VesselClassificationEnum.BARGE)) {
				vslChangedToBarge = true;
				//this.log("2");
			}
		} else if (postVslClass == null) {
			if (preVslClass.equals(VesselClassificationEnum.BARGE)) {
				vslChangedToBarge = true;
				//this.log("3");
			}
		} else {
			//this.log("4");
			if (preVslClass.equals(VesselClassificationEnum.BARGE) &&
					!postVslClass.equals(VesselClassificationEnum.BARGE)) {
				vslChangedToBarge = true;
				//this.log("5");
			} else if (postVslClass.equals(VesselClassificationEnum.BARGE) &&
					!preVslClass.equals(VesselClassificationEnum.BARGE)) {
				vslChangedToBarge = true;
				//this.log("6");
			}
		}
		this.log("vslChangedToBarge= " + vslChangedToBarge);
		return vslChangedToBarge;
	}

	/**
	 *
	 * @param eventType
	 * @param EVENT_UNIT_PROPERTY_UPDATE
	 * @param changes
	 * @return
	 */
	private String handleUnitPropertyUpdateEvent(String eventType, Set changes) {

		String reportEventId = null;
		if (!StringUtils.equals(eventType, EVENT_UNIT_PROPERTY_UPDATE)) {
			return null;
		}

		if (this.fieldChangesIncludeSeals(changes)) {
			//Report UNIT_SEAL event instead if the UNIT_PROPERTY_UPDATE event is triggered by
			//Seal changes
			reportEventId = EVENT_UNIT_SEAL;
			//this.log("Field changes include Seals");
		} else if (this.fieldChangesIncludeInTime(changes)) {
			//Report UNIT_RECTIFY event instead if the UNIT_PROPERTY_UPDATE event is triggered by
			//in time changes
			reportEventId = EVENT_UNIT_RECTIFY;
			//this.log("Field changes include in time");
		} else if (this.fieldChangesIncludeRTOCategory(changes)) {
			//CSDV-1121 : Special Case RTO Category : report the RTO_CATEGORY_CHANGE as UNIT_REROUTE ,
			// This change is already in place - PChidambaram <ext.priya.chidambaram@navis.com> , Aug 30 2013

			//Report UNIT_REROUTE event if the UNIT_PROPERTY_UPDATE event is triggered by
			//RTO category changes
			reportEventId = EVENT_UNIT_REROUTE;
			//this.log("Field changes include unit RTO category");
		} else if (this.fieldChangesIncludeFreightKind(changes)) {
			//Report UNIT_REROUTE event if the UNIT_PROPERTY_UPDATE event is triggered by
			//freight kind changes
			if (StringUtils.equalsIgnoreCase("MTY", this.getPreviousFieldChangeValue(changes))) {
				reportEventId = EVENT_UNIT_STUFF;
			} else {
				reportEventId = EVENT_UNIT_STRIP;
			}
			//this.log("Field changes include unit Freight Kind");
		} else if (this.fieldChangesIncludeGenericFields(changes)) {
			//Report UNIT_PROPERTY_UPDATE event if the UNIT_PROPERTY_UPDATE event is triggered by
			//generic property changes.
			reportEventId = EVENT_UNIT_PROPERTY_UPDATE;
			//this.log("Field changes include unit generic property");
		}

		return reportEventId;
	}

	/**
	 *
	 * @param changes
	 * @return
	 */
	private String getPreviousFieldChangeValue(Set changes) {

		String returnValue = null;
		FREIGHT_KIND_LIST.clear();
		//build freight kind list
		FREIGHT_KIND_LIST.add("unitFreightKind");
		if (changes.size() > 0) {
			for (Object aFc : changes) {
				EventFieldChange fc = (EventFieldChange) aFc;
				if (fc.getEvntfcMetafieldId() != null &&
						FREIGHT_KIND_LIST.contains(fc.getMetafieldId())) {
					//this.log("FC meta field Id= " + fc.getEvntfcMetafieldId());
					//if (StringUtils.equalsIgnoreCase(fc.getEvntfcPrevVal(), "MTY")) {
					returnValue = fc.getEvntfcPrevVal();
					//}
					//this.log("FC meta field Id matched - includeFreightKind= " + includeFreightKind);
					break;
				}
			}
		}

		return returnValue;
	}

	/**
	 *
	 * @param changes
	 * @return
	 */
	private boolean fieldChangesIncludeFreightKind(Set changes) {

		boolean includeFreightKind = false;
		//build freight kind list
		FREIGHT_KIND_LIST.add("unitFreightKind");
		if (changes.size() > 0) {
			for (Object aFc : changes) {
				EventFieldChange fc = (EventFieldChange) aFc;
				if (fc.getEvntfcMetafieldId() != null &&
						FREIGHT_KIND_LIST.contains(fc.getMetafieldId())) {
					//this.log("FC meta field Id= " + fc.getEvntfcMetafieldId());
					includeFreightKind = true;
					//this.log("FC meta field Id matched - includeFreightKind= " + includeFreightKind);
					break;
				}
			}
		}

		return includeFreightKind;
	}

	/**
	 *
	 * @param changes
	 * @return
	 */
	private boolean fieldChangesIncludeRTOCategory(Set changes) {

		boolean includeRTOCategory = false;
		//build generic property list
		FLEX_STRING_FIELDS_LIST.add("ufvFlexString07");  //RTO category stored in ufvFlexString07 field
		if (changes.size() > 0) {
			for (Object aFc : changes) {
				EventFieldChange fc = (EventFieldChange) aFc;
				if (fc.getEvntfcMetafieldId() != null &&
						FLEX_STRING_FIELDS_LIST.contains(fc.getMetafieldId())) {
					//this.log("FC meta field Id= " + fc.getEvntfcMetafieldId());
					includeRTOCategory = true;
					//this.log("FC meta field Id matched - includeRTOCategory= " + includeRTOCategory);
					break;
				}
			}
		}

		return includeRTOCategory;
	}

	/**
	 *
	 * @param changes
	 * @return
	 */
	private boolean fieldChangesIncludeGenericFields(Set changes) {

		boolean includeGenericField = false;
		//build generic property list
		GENERIC_PROPERTY_LIST.add("eqEquipType");  //eq type (iso code changes
		GENERIC_PROPERTY_LIST.add("unitGoodsAndCtrWtKg");  //unit gross weight changes
		if (changes.size() > 0) {
			for (Object aFc : changes) {
				EventFieldChange fc = (EventFieldChange) aFc;
				if (fc.getEvntfcMetafieldId() != null &&
						GENERIC_PROPERTY_LIST.contains(fc.getMetafieldId())) {
					//this.log("FC meta field Id= " + fc.getEvntfcMetafieldId());
					includeGenericField = true;
					//this.log("FC meta field Id matched - includeGenericField= " + includeGenericField);
					break;
				}
			}
		}

		return includeGenericField;
	}

	/**
	 *
	 * @param changes
	 * @return
	 */
	private boolean fieldChangesIncludeInTime(Set changes) {

		boolean includeInTime = false;
		//build in time list
		IN_TIME_LIST.add("ufvTimeIn");   //Changes in unit's in time
		IN_TIME_LIST.add("ufvTimeEcIn");
		if (changes.size() > 0) {
			for (Object aFc : changes) {
				EventFieldChange fc = (EventFieldChange) aFc;
				//Consider there was an in time already to avoid UNIT_RECTIFY from being sent
				//after stripping a uni
				if (fc.getEvntfcMetafieldId() != null &&
						IN_TIME_LIST.contains(fc.getMetafieldId()) &&
						fc.getEvntfcPrevVal() != null) {
					//this.log("FC meta field Id= " + fc.getEvntfcMetafieldId());
					includeInTime = true;
					//this.log("FC meta field Id matched - includeInTime= " + includeInTime);
					break;
				}
			}
		}

		return includeInTime;
	}

	/**
	 *  Check if field changes happened in seal field
	 */
	private boolean fieldChangesIncludeSeals(Set changes) {

		boolean includeSeals = false;
		//build Seal field changes list
		SEAL_LIST.add("unitSealNbr1");    //Changes in seals
		SEAL_LIST.add("unitSealNbr2");
		SEAL_LIST.add("unitSealNbr3");
		SEAL_LIST.add("unitSealNbr4");
		if (changes.size() > 0) {
			for (Object aFc : changes) {
				EventFieldChange fc = (EventFieldChange) aFc;
				if (fc.getEvntfcMetafieldId() != null &&
						SEAL_LIST.contains(fc.getMetafieldId())) {
					//this.log("FC meta field Id= " + fc.getEvntfcMetafieldId());
					includeSeals = true;
					//this.log("FC meta field Id matched - includeSeals= " + includeSeals);
					break;
				}
			}
		}

		return includeSeals;
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
	 *  Build list of events that is supported in activity message
	 */
	private void buildSupportedEventList() {
		EVENT_LIST.add(EVENT_CANCELLED_DEPARTURE);  //1
		EVENT_LIST.add(EVENT_CANCELLED_ARRIVAL);    //2
		EVENT_LIST.add(EVENT_CONTAINER_MOVE);       //3
		EVENT_LIST.add(EVENT_FORCED_REPAIR);         //4
		//EVENT_LIST.add(EVENT_UNIT_CANCEL_RESERVE);   //No need to report this event
		EVENT_LIST.add(EVENT_UNIT_CREATE);           //5
		EVENT_LIST.add(EVENT_UNIT_DERAMP);           //6
		EVENT_LIST.add(EVENT_UNIT_DISCH);            //7
		EVENT_LIST.add(EVENT_UNIT_HAZARDS_DELETE);   //8
		EVENT_LIST.add(EVENT_UNIT_HAZARDS_INSERT);   //9
		EVENT_LIST.add(EVENT_UNIT_HAZARDS_UPDATE);   //10
		EVENT_LIST.add(EVENT_UNIT_LOAD);             //11
		EVENT_LIST.add(EVENT_UNIT_OPERATOR_CHANGE);  //12
		EVENT_LIST.add(EVENT_UNIT_OUT_GATE);         //13
		EVENT_LIST.add(EVENT_UNIT_PROPERTY_UPDATE);  //14
		EVENT_LIST.add(EVENT_UNIT_RAMP);             //15
		EVENT_LIST.add(EVENT_UNIT_RECEIVE);          //16
		EVENT_LIST.add(EVENT_UNIT_RECTIFY);          //17
		EVENT_LIST.add(EVENT_UNIT_RENUMBER);         //18
		EVENT_LIST.add(EVENT_UNIT_REROUTE);          //29
		EVENT_LIST.add(EVENT_UNIT_SEAL);             //20
		EVENT_LIST.add(EVENT_UNIT_STRIP);            //21
		EVENT_LIST.add(EVENT_UNIT_STUFF);            //22
		EVENT_LIST.add(EVENT_UNIT_ACTIVATE);         //23
		EVENT_LIST.add(EVENT_UNIT_ROLL);             //24
		EVENT_LIST.add(EVENT_DRAY_OUT_SET_RTO_CATEGORY); //25 CSDV-2803
	}

	/**
	 *
	 * @param ufv
	 * @param eUnit
	 * @param sNS
	 */
	private void buildUfvRelatedElements(GroovyEvent inEvent, UnitFacilityVisit ufv, Element eUnit, Namespace sNS) {
		//CSDV-2988 - Start
		Date timeIn = ufv.getUfvTimeIn();

		if (timeIn == null) {
			Event event = inEvent.getEvent();
			String eventId = event.getEvntEventType().getId();
			if ("UNIT_DISCH".equals(eventId) || "UNIT_DERAMP".equals(eventId) || "UNIT_RECEIVE".equals(eventId) ) {
			  timeIn = event.getEventTime();
			  this.log("For the UFV: " + ufv.toString() + " Using the Time-in from event " + eventId);
			}
		}

		if (timeIn != null) {
			Element eTimeIn = new Element("time-in", sNS);
			eUnit.addContent(eTimeIn);
			eTimeIn.addContent("");
			eTimeIn.setAttribute("value", this.getXmlTimestamp(timeIn));
		}
	  //CSDV-2988 - End
		Date ufvLoadedDepTime = this.getUfvLoadedOrDepartedTime(inEvent, ufv);
		if (ufvLoadedDepTime != null) {
			Element eTimeOut = new Element("time-out", sNS);
			eUnit.addContent(eTimeOut);
			eTimeOut.addContent("");
			eTimeOut.setAttribute("value", this.getXmlTimestamp(ufvLoadedDepTime));
		}

		if (ufv.getUfvLastKnownPosition() != null) {
			Element eUfvLastKnownPos = new Element("last-known-position", sNS);
			eUnit.addContent(eUfvLastKnownPos);
			eUfvLastKnownPos.addContent("");
			eUfvLastKnownPos.setAttribute("value", ufv.getUfvLastKnownPosition().toString());
		}
		if (ufv.getUfvArrivePosition() != null) {
			Element eUfvArrivePos = new Element("arrive-position", sNS);
			eUnit.addContent(eUfvArrivePos);
			eUfvArrivePos.addContent("");
			eUfvArrivePos.setAttribute("value", ufv.getUfvArrivePosition().toString());
		}
	}

	/**
	 *
	 * @param inEvent
	 * @param ufv
	 * @return
	 */
	private Date getUfvLoadedOrDepartedTime(GroovyEvent inEvent, UnitFacilityVisit ufv) {

		Date unitLoadedOrDepTime = null;
		if (ufv == null || inEvent == null) {
			return null;
		}

		UfvTransitStateEnum transitState = ufv.getUfvTransitState();
		//Don't set time-out if T-state is not either in Loaded nor Departed
		if (!transitState.equals(UfvTransitStateEnum.S60_LOADED) &&
				!transitState.equals(UfvTransitStateEnum.S70_DEPARTED)) {
			return null;
		}

		if (StringUtils.equals(inEvent.getEvent().getEventTypeId(), EVENT_UNIT_LOAD) ||
				StringUtils.equals(inEvent.getEvent().getEventTypeId(), EVENT_UNIT_RAMP) ||
				StringUtils.equals(inEvent.getEvent().getEventTypeId(), EVENT_UNIT_OUT_GATE)) {
			unitLoadedOrDepTime = inEvent.getEvent().getEventTime();
		}

		return unitLoadedOrDepTime;
	}

	/**
	 *
	 * @param inUnit
	 * @param ufv
	 * @param eUnit
	 * @param sNS
	 */
	private void buildFlexFieldsElement(Unit inUnit, UnitFacilityVisit ufv, Element eUnit, Namespace sNS) {
		Element eFlexFields = new Element("flex-fields", sNS);
		eUnit.addContent(eFlexFields);
		//Adding unit flex string fields
		Element eUnitFlexFields = new Element("unit-flex-fields", sNS);
		eFlexFields.addContent(eUnitFlexFields);
		if (StringUtils.isNotBlank(inUnit.getUnitFlexString02())) {
			Element eUnitFlexField2 = new Element("unit-flex-string-02", sNS);
			eUnitFlexFields.addContent(eUnitFlexField2);
			eUnitFlexField2.addContent("");
			eUnitFlexField2.setAttribute("value", inUnit.getUnitFlexString02());
		}
		if (StringUtils.isNotBlank(inUnit.getUnitFlexString03())) {
			Element eUnitFlexField3 = new Element("unit-flex-string-03", sNS);
			eUnitFlexFields.addContent(eUnitFlexField3);
			eUnitFlexField3.addContent("");
			eUnitFlexField3.setAttribute("value", inUnit.getUnitFlexString03());
		}
		if (StringUtils.isNotBlank(inUnit.getUnitFlexString04())) {
			Element eUnitFlexField4 = new Element("unit-flex-string-04", sNS);
			eUnitFlexFields.addContent(eUnitFlexField4);
			eUnitFlexField4.addContent("");
			eUnitFlexField4.setAttribute("value", inUnit.getUnitFlexString04());
		}
		if (StringUtils.isNotBlank(inUnit.getUnitFlexString05())) {
			Element eUnitFlexField5 = new Element("unit-flex-string-05", sNS);
			eUnitFlexFields.addContent(eUnitFlexField5);
			eUnitFlexField5.addContent("");
			eUnitFlexField5.setAttribute("value", inUnit.getUnitFlexString05());
		}
		if (StringUtils.isNotBlank(inUnit.getUnitFlexString06())) {
			Element eUnitFlexField6 = new Element("unit-flex-string-06", sNS);
			eUnitFlexFields.addContent(eUnitFlexField6);
			eUnitFlexField6.addContent("");
			eUnitFlexField6.setAttribute("value", inUnit.getUnitFlexString06());
		}
		if (StringUtils.isNotBlank(inUnit.getUnitFlexString07())) {
			Element eUnitFlexField7 = new Element("unit-flex-string-07", sNS);
			eUnitFlexFields.addContent(eUnitFlexField7);
			eUnitFlexField7.addContent("");
			eUnitFlexField7.setAttribute("value", inUnit.getUnitFlexString07());
		}
		if (StringUtils.isNotBlank(inUnit.getUnitFlexString08())) {
			Element eUnitFlexField8 = new Element("unit-flex-string-08", sNS);
			eUnitFlexFields.addContent(eUnitFlexField8);
			eUnitFlexField8.addContent("");
			eUnitFlexField8.setAttribute("value", inUnit.getUnitFlexString08());
		}
		if (StringUtils.isNotBlank(inUnit.getUnitFlexString09())) {
			Element eUnitFlexField9 = new Element("unit-flex-string-09", sNS);
			eUnitFlexFields.addContent(eUnitFlexField9);
			eUnitFlexField9.addContent("");
			eUnitFlexField9.setAttribute("value", inUnit.getUnitFlexString09());
		}
		if (StringUtils.isNotBlank(inUnit.getUnitFlexString10())) {
			Element eUnitFlexField10 = new Element("unit-flex-string-10", sNS);
			eUnitFlexFields.addContent(eUnitFlexField10);
			eUnitFlexField10.addContent("");
			eUnitFlexField10.setAttribute("value", inUnit.getUnitFlexString10());
		}
		if (StringUtils.isNotBlank(inUnit.getUnitFlexString01())) {
			Element eUnitFlexField1 = new Element("unit-flex-string-01", sNS);
			eUnitFlexFields.addContent(eUnitFlexField1);
			eUnitFlexField1.addContent("");
			eUnitFlexField1.setAttribute("value", inUnit.getUnitFlexString01());
		}

		//Adding Ufv flex string fields
		Element eUfvFlexFields = new Element("ufv-flex-fields", sNS);
		eFlexFields.addContent(eUfvFlexFields);
		if (StringUtils.isNotBlank(ufv.getUfvFlexString01())) {
			Element eUfvField01 = new Element("ufv-flex-string-01", sNS);
			eUfvFlexFields.addContent(eUfvField01);
			eUfvField01.addContent("");
			eUfvField01.setAttribute("value", ufv.getUfvFlexString01());
		}
		if (StringUtils.isNotBlank(ufv.getUfvFlexString02())) {
			Element eUfvField02 = new Element("ufv-flex-string-02", sNS);
			eUfvFlexFields.addContent(eUfvField02);
			eUfvField02.addContent("");
			eUfvField02.setAttribute("value", ufv.getUfvFlexString02());
		}
		if (StringUtils.isNotBlank(ufv.getUfvFlexString03())) {
			Element eUfvField03 = new Element("ufv-flex-string-03", sNS);
			eUfvFlexFields.addContent(eUfvField03);
			eUfvField03.addContent("");
			eUfvField03.setAttribute("value", ufv.getUfvFlexString03());
		}
		if (StringUtils.isNotBlank(ufv.getUfvFlexString04())) {
			Element eUfvField04 = new Element("ufv-flex-string-04", sNS);
			eUfvFlexFields.addContent(eUfvField04);
			eUfvField04.addContent("");
			eUfvField04.setAttribute("value", ufv.getUfvFlexString04());
		}
		if (StringUtils.isNotBlank(ufv.getUfvFlexString05())) {
			Element eUfvField05 = new Element("ufv-flex-string-05", sNS);
			eUfvFlexFields.addContent(eUfvField05);
			eUfvField05.addContent("");
			eUfvField05.setAttribute("value", ufv.getUfvFlexString05());
		}
		if (StringUtils.isNotBlank(ufv.getUfvFlexString06())) {
			Element eUfvField06 = new Element("ufv-flex-string-06", sNS);
			eUfvFlexFields.addContent(eUfvField06);
			eUfvField06.addContent("");
			eUfvField06.setAttribute("value", ufv.getUfvFlexString06());
		}
		if (StringUtils.isNotBlank(ufv.getUfvFlexString07())) {
			Element eUfvField07 = new Element("ufv-flex-string-07", sNS);
			eUfvFlexFields.addContent(eUfvField07);
			eUfvField07.addContent("");
			eUfvField07.setAttribute("value", ufv.getUfvFlexString07());
		}
		if (StringUtils.isNotBlank(ufv.getUfvFlexString08())) {
			Element eUfvField08 = new Element("ufv-flex-string-08", sNS);
			eUfvFlexFields.addContent(eUfvField08);
			eUfvField08.addContent("");
			eUfvField08.setAttribute("value", ufv.getUfvFlexString08());
		}
		if (StringUtils.isNotBlank(ufv.getUfvFlexString09())) {
			Element eUfvField09 = new Element("ufv-flex-string-09", sNS);
			eUfvFlexFields.addContent(eUfvField09);
			eUfvField09.addContent("");
			eUfvField09.setAttribute("value", ufv.getUfvFlexString09());
		}
		if (StringUtils.isNotBlank(ufv.getUfvFlexString10())) {
			Element eUfvField10 = new Element("ufv-flex-string-10", sNS);
			eUfvFlexFields.addContent(eUfvField10);
			eUfvField10.addContent("");
			eUfvField10.setAttribute("value", ufv.getUfvFlexString10());
		}
		if (ufv.getUfvFlexDate01() != null) {
			Element eUfvDate01 = new Element("ufv-flex-date-01", sNS);
			eUfvFlexFields.addContent(eUfvDate01);
			eUfvDate01.addContent("");
			eUfvDate01.setAttribute("value", this.getXmlTimestamp(ufv.getUfvFlexDate01()));
		}
		if (ufv.getUfvFlexDate02() != null) {
			Element eUfvDate02 = new Element("ufv-flex-date-02", sNS);
			eUfvFlexFields.addContent(eUfvDate02);
			eUfvDate02.addContent("");
			eUfvDate02.setAttribute("value", this.getXmlTimestamp(ufv.getUfvFlexDate02()));
		}
		if (ufv.getUfvFlexDate03() != null) {
			Element eUfvDate03 = new Element("ufv-flex-date-03", sNS);
			eUfvFlexFields.addContent(eUfvDate03);
			eUfvDate03.addContent("");
			eUfvDate03.setAttribute("value", this.getXmlTimestamp(ufv.getUfvFlexDate03()));
		}
		if (ufv.getUfvFlexDate04() != null) {
			Element eUfvDate04 = new Element("ufv-flex-date-04", sNS);
			eUfvFlexFields.addContent(eUfvDate04);
			eUfvDate04.addContent("");
			eUfvDate04.setAttribute("value", this.getXmlTimestamp(ufv.getUfvFlexDate04()));
		}
		if (ufv.getUfvFlexDate05() != null) {
			Element eUfvDate05 = new Element("ufv-flex-date-05", sNS);
			eUfvFlexFields.addContent(eUfvDate05);
			eUfvDate05.addContent("");
			eUfvDate05.setAttribute("value", this.getXmlTimestamp(ufv.getUfvFlexDate05()));
		}
		if (ufv.getUfvFlexDate06() != null) {
			Element eUfvDate06 = new Element("ufv-flex-date-06", sNS);
			eUfvFlexFields.addContent(eUfvDate06);
			eUfvDate06.addContent("");
			eUfvDate06.setAttribute("value", this.getXmlTimestamp(ufv.getUfvFlexDate06()));
		}
		if (ufv.getUfvFlexDate07() != null) {
			Element eUfvDate07 = new Element("ufv-flex-date-07", sNS);
			eUfvFlexFields.addContent(eUfvDate07);
			eUfvDate07.addContent("");
			eUfvDate07.setAttribute("value", this.getXmlTimestamp(ufv.getUfvFlexDate07()));
		}
		if (ufv.getUfvFlexDate08() != null) {
			Element eUfvDate08 = new Element("ufv-flex-date-08", sNS);
			eUfvFlexFields.addContent(eUfvDate08);
			eUfvDate08.addContent("");
			eUfvDate08.setAttribute("value", this.getXmlTimestamp(ufv.getUfvFlexDate08()));
		}
	}

	/**
	 *
	 * @param goods
	 * @param eUnit
	 * @param sNS
	 */
	private void buildGoodsBLConsigneeShipperElements(GoodsBase goods, Element eUnit, Namespace sNS) {
		if (StringUtils.isNotBlank(goods.getGdsBlNbr())) {
			Element eBL = new Element("bill-of-lading-number", sNS);
			eUnit.addContent(eBL);
			eBL.setAttribute("value", goods.getGdsBlNbr());
		}

		if (goods.getGdsConsigneeBzu() != null) {
			Element eConsignee = new Element("consignee", sNS);
			eUnit.addContent(eConsignee);
			eConsignee.addContent("");
			eConsignee.setAttribute("value", this.stringNullCheck(goods.getGdsConsigneeBzu().getBzuId()));
		}

		if (goods.getGdsShipperBzu() != null) {
			Element eShiper = new Element("shipper", sNS);
			eUnit.addContent(eShiper);
			eShiper.addContent("");
			eShiper.setAttribute("value", this.stringNullCheck(goods.getGdsShipperBzu().getBzuId()));
		}
	}

	/**
	 *
	 * @param inUnit
	 * @param eUnit
	 * @param activeHolds
	 * @param sNS
	 */
	private void buildActiveHoldsElement(Unit inUnit, Element eUnit, Set activeHolds, Namespace sNS) {
		Element eActiveHolds = new Element("active-holds", sNS);
		eUnit.addContent(eActiveHolds);
		for (Object aHold : activeHolds) {
			Flag flag = (Flag) aHold;
			Element eHold = new Element("hold", sNS);
			eActiveHolds.addContent(eHold);
			eHold.setAttribute("gkey", this.longNullCheck(flag.getFlagGkey().toString()));
			eHold.setAttribute("id", this.stringNullCheck(flag.getFlagFlagType().getFlgtypId()));
			eHold.setAttribute("description", this.stringNullCheck(flag.getFlagFlagType().getFlgtypDescription()));
			eHold.setAttribute("applied-date", this.getXmlTimestamp(flag.getFlagAppliedDate()));
			eHold.setAttribute("applied-by", this.stringNullCheck(flag.getFlagAppliedBy()));
			eHold.setAttribute("reference", this.stringNullCheck(flag.getFlagReferenceId()));
			eHold.setAttribute("note", this.stringNullCheck(flag.getFlagNote()));
		}
	}

	/**
	 *
	 * @param inUnit
	 * @param eUnit
	 * @param sNS
	 */
	private void buildStopsElement(Unit inUnit, Element eUnit, Namespace sNS) {
		Element eStops = new Element("stops", sNS);
		eUnit.addContent(eStops);
		eStops.setAttribute("vessel", this.booleanNullCheck(inUnit.getUnitStoppedVessel()));
		eStops.setAttribute("road", this.booleanNullCheck(inUnit.getUnitStoppedRoad()));
		eStops.setAttribute("rail", this.booleanNullCheck(inUnit.getUnitStoppedRail()));
	}

	/**
	 *
	 * @param inUnit
	 * @param eUnit
	 * @param sNS
	 */
	private void buildImpedimentsElement(Unit inUnit, Element eUnit, Namespace sNS) {
		Element eImpediments = new Element("impediments", sNS);
		eUnit.addContent(eImpediments);
		if (StringUtils.isNotBlank(inUnit.getUnitImpedimentVessel())) {
			Element eVessel = new Element("vessel", sNS);
			eImpediments.addContent(eVessel);
			eVessel.setAttribute("value", inUnit.getUnitImpedimentVessel());
		}

		if (StringUtils.isNotBlank(inUnit.getUnitImpedimentRoad())) {
			Element eRoad = new Element("road", sNS);
			eImpediments.addContent(eRoad);
			eRoad.setAttribute("value", inUnit.getUnitImpedimentRoad());
		}

		if (StringUtils.isNotBlank(inUnit.getUnitImpedimentRail())) {
			Element eRail = new Element("rail", sNS);
			eImpediments.addContent(eRail);
			eRail.setAttribute("value", inUnit.getUnitImpedimentRail());
		}
	}

	/**
	 *
	 * @param eRouting
	 * @param routing
	 * @param sNS
	 */
	private void buildPortsElement(Element eRouting, Routing routing, Namespace sNS) {

		Element ePorts = new Element("ports", sNS);
		eRouting.addContent(ePorts);
		if (routing.getRtgOPT1() != null) {
			Element eOpt1 = new Element("opt-1", sNS);
			ePorts.addContent(eOpt1);
			eOpt1.addContent("");
			ePorts.setAttribute("value", this.stringNullCheck(routing.getRtgOPT1().getPointUnlocId()));
		}
		if (routing.getRtgOPT2() != null) {
			Element eOpt2 = new Element("opt-2", sNS);
			ePorts.addContent(eOpt2);
			eOpt2.addContent("");
			ePorts.setAttribute("value", this.stringNullCheck(routing.getRtgOPT2().getPointUnlocId()));
		}
		if (routing.getRtgOPT3() != null) {
			Element eOpt3 = new Element("opt-3", sNS);
			ePorts.addContent(eOpt3);
			eOpt3.addContent("");
			ePorts.setAttribute("value", this.stringNullCheck(routing.getRtgOPT3().getPointUnlocId()));
		}
	}

	/**
	 *
	 * @param inValue
	 * @return
	 */
	private String stringNullCheck(String inValue) {
		return inValue ? inValue : "";
	}

	/**
	 *
	 * @param inValue
	 * @return
	 */
	private String doubleNullCheck(String inValue) {
		return inValue ? inValue : "0.0";
	}

	/**
	 *
	 * @param inValue
	 * @return
	 */
	private String longNullCheck(String inValue) {
		return inValue ? inValue : "0";
	}

	/**
	 *
	 * @param inValue
	 * @return
	 */
	private String booleanNullCheck(Object inValue) {
		//this.log("booleanNullCheck - inValue= " + inValue);
		return inValue ? "true" : "false";
	}

	/**
	 *
	 * @param mode
	 * @return
	 */
	private String formatCarrierVisitMode(LocTypeEnum mode) {
		if (mode == null) {
			return "null";
		}

		if (mode.getKey() == null) {
			return "null";
		}

		return mode.getKey().toLowerCase();
	}

	/**
	 *
	 * @param state
	 * @return
	 */
	private String formatCarrierVisitState(CarrierVisitPhaseEnum state) {
		if (state == null) {
			return "null";
		}

		if (state.getKey() == null) {
			return "null";
		}

		return state.getKey().substring(2).toLowerCase();
	}

	/**
	 *
	 * @param inState
	 * @return
	 */
	private String formatUnitVisitState(UnitVisitStateEnum inState) {
		if (inState == null) {
			return null;
		}

		if (inState.getKey() == null) {
			return null;
		}

		return inState.getKey().substring(1).toLowerCase();
	}

	/**
	 *
	 * @param inState
	 * @return
	 */
	private String formatUfvTransitState(UfvTransitStateEnum inState) {
		if (inState == null) {
			return null;
		}

		if (inState.getKey() == null) {
			return null;
		}

		return inState.getKey().substring(4).toLowerCase();
	}

	/**
	 *
	 * @param inUnit
	 * @return
	 */
	private Commodity getCommodiyForUnit(Unit inUnit) {
		if (inUnit == null) {
			return null;
		}

		GoodsBase goods = inUnit.getUnitGoods();
		if (goods == null) {
			return null;
		}

		return goods.getGdsCommodity();
	}

	/**
	 *
	 * @param inUnit
	 * @return
	 */
	private List getDamagesForUnit(Unit inUnit) {
		List<UnitEquipDamageItem> items = new ArrayList<UnitEquipDamageItem>();
		if (inUnit == null) {
			return items;
		}

		if (inUnit.getPrimaryEq() == null) {
			return items;
		}

		UnitEquipDamages damages = inUnit.getDamages(inUnit.getPrimaryEq())
		if (damages == null) {
			return items;
		}

		return damages.getDmgsItems() == null ? items : damages.getDmgsItems();
	}

	/**
	 *
	 * @param inUnit
	 * @return
	 */
	private List getHazardItemsForUnit(Unit inUnit) {
		List<HazardItem> items = new ArrayList<HazardItem>();
		if (inUnit == null) {
			return items;
		}

		GoodsBase goods = inUnit.getUnitGoods();
		if (goods == null) {
			return items;
		}

		Hazards hazards = goods.getGdsHazards();
		if (hazards == null) {
			return items;
		}

		return hazards.getHzrdItems() == null ? items : hazards.getHzrdItems();
	}

	/**
	 *
	 * @param inUnit
	 * @return
	 */
	private Set getActiveHoldsForUnit(Unit inUnit) {

		Set result = new HashSet();
		if (inUnit == null) {
			return result;
		}

		Collection<Flag> activeFlags = FlagType.findActiveFlagsOnEntity(inUnit)
		if (activeFlags == null) {
			return result;
		}

		if (activeFlags.size() == 0) {
			return result;
		}

		Iterator activeFlagsItemItr = activeFlags.iterator();
		while (activeFlagsItemItr.hasNext()) {
			Flag flag = (Flag) activeFlagsItemItr.next();
			if (flag.getFlagFlagType().getFlgtypPurpose() == FlagPurposeEnum.HOLD) {
				result.add(flag);
			}
		}

		return result
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
	 * @return
	 */
	private String getXmlNowTime() {
		return MessageFormat.format("{0,date,HH:mm:ss}", new Date())
	}

	/**
	 *
	 * @return
	 */
	private String getXmlNowDate() {
		return MessageFormat.format("{0,date,yyyy-MM-dd}", new Date())
	}

	/**
	 *
	 * @param inDate
	 * @return
	 */
	private String getXmlTimestamp(Date inDate) {
		if (inDate == null)
			return null;

		SimpleDateFormat ISO8601Local = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
		TimeZone timeZone = TimeZone.getDefault();

		ISO8601Local.setTimeZone(timeZone)

		int offset = timeZone.getOffset(inDate.getTime())

		String sign = "+"

		if (offset < 0) {
			offset = -offset
			sign = "-"
		}

		double hours = offset / 3600000
		double minutes = (offset - hours * 3600000) / 60000

		if (offset != hours * 3600000 + minutes * 60000) {
			// E.g. TZ=Asia/Riyadh87
			throw new RuntimeException("TimeZone offset (" + sign + offset + " ms) is not an exact number of minutes")
		}

		DecimalFormat twoDigits = new DecimalFormat("00")

		return ISO8601Local.format(inDate) + sign + twoDigits.format(hours) + ":" + twoDigits.format(minutes)
	}

	/**
	 * CSDV-1144: dickspa 10/10/2013: Return true if the given UnitFacilityVisit has an RTO Category which is
	 * set to "E"(Export) or "D"(Domestic). Previously, a check was only done for E, but when an Export Unit which is
	 * leaving on a Barge is created, it is created as a "D"(Domestic). This new method should resolve this
	 * and ensure that the Inbound Order is reported correctly in the RTO message.
	 * @param ufv
	 * @return
	 */
	private boolean isExportOrDomestic(final UnitFacilityVisit ufv) {
		boolean exportOrDomestic = false;
		final String rtoCategory = ufv.getUfvFlexString07();

		// If this is not set, then should log a warning
		if (StringUtils.isEmpty(rtoCategory)) {
			log("Tried to evaluate if Unit is Export/Domestic based on RTO Category, but the RTO Category field is not set.");
		}

		// If the RTO Category is "E" for Export, or "D" for Domestic, then return true (instead of default false)
		if (StringUtils.equals(rtoCategory, E_CATEGORY) || StringUtils.equals(rtoCategory, D_CATEGORY)) {
			exportOrDomestic = true;
		}

		return exportOrDomestic;
	}

	/**
	 * Return true if unit arrives by landside modality, ie truck, rail, and barge
	 * @param inCv
	 * @return
	 */
	private boolean isLandSideArrivalModality(CarrierVisit inActualCv, CarrierVisit inDeclaredCv) {
		if (inActualCv == null && inDeclaredCv == null) {
			return false;
		}

		CarrierVisit inCv = (inActualCv != null) ? inActualCv : inDeclaredCv;
		boolean isLandSideArrival = false;
		if (inCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN) ||
				inCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR)||
				inCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
			isLandSideArrival = true;
		} else if (inCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL)) {
			if ((inCv.getCvClassification() != null &&
					VesselClassificationEnum.BARGE.equals(inCv.getCvClassification())) ||
					(inCv.getCarrierVesselClassType() != null &&
							VesselTypeEnum.BARGE.equals(inCv.getCarrierVesselClassType()))) {
				//If arrives by vessel, only barge counts
				isLandSideArrival = true;
			}
		}

		return isLandSideArrival;
	}

	/**
	 * Return true if unit arrives or departs by landside modality, ie truck, rail, and barge
	 * @param inCv
	 * @return
	 */
	private boolean isLandSideModality(CarrierVisit inActualCv, CarrierVisit inDeclaredCv) {
		//The modality will only be determined by actual visit, not by declared or intended carrier visit
		if (inActualCv == null) {
			return false;
		}

		//CarrierVisit inCv = (inActualCv != null) ? inActualCv : inDeclaredCv;
		//The modality will only be determined by actual visit, not by declared or intended carrier visit
		CarrierVisit inCv = inActualCv;
		//this.log("Vessel Class= " + inCv.getCarrierVesselClassType() ? inCv.getCarrierVesselClass().toString() : "Vessel class is null");
		boolean isLandSideModality = false;
		if (inCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN) ||
				inCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR) ||
				inCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
			isLandSideModality = true;
		} else if (inCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL) &&
				((inCv.getCvClassification() != null &&
						VesselClassificationEnum.BARGE.equals(inCv.getCvClassification())) ||
						(inCv.getCarrierVesselClassType() != null &&
								VesselTypeEnum.BARGE.equals(inCv.getCarrierVesselClassType())))) {
			// If arrives by vessel, only barge counts
			//If the vessel classification is not set, the default value is deep-sea
			isLandSideModality = true;
		} else if (GEN_BARGE.equalsIgnoreCase(inCv.getCvId())) {
			//Special case to determine barge mode. Terminal will define a GEN_BARGE
			//carrier visit id that represents a defined dummy visit for barge
			isLandSideModality = true;
		}

		/*
	if (isLandSideModality) {
	  this.log("It is landside modality");
	} else {
	  this.log("It is not landside modality");
	}
	*/
		return isLandSideModality;
	}

	/**
	 * Return inbound or outbound voyage. For truck, return truck license nbr.
	 * For vessel, barge, and rail, return vessel or train id
	 * CSDV-1622: dickspa 10/02/2014: Changed this to only return I/B voyage number.
	 * @param actualCv
	 * @return
	 */
	private String getInboundActualVoyage(CarrierVisit actualCv) {

		if (actualCv == null) {
			return null;
		}

		String voyageStr = null;
		//For truck, the retuned value is blank
		if (actualCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL) ||
				GEN_BARGE.equalsIgnoreCase(actualCv.getCvId()) ||
				actualCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN) ||
				actualCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR)) {
			if (actualCv.getCvCvd() != null) {
				//currently Vessel=vvdIbVygNbr, Truck=null, Train=rvdtlsId
				voyageStr = actualCv.getCvCvd().getCarrierIbVoyNbrOrTrainId();
			}
		} else if (actualCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
			//find the latest truck transaction associated with the unit
			//CSDV-1108
			voyageStr = actualCv.getCarrierVehicleId();
			//TruckTransaction tran = this.findTransactionByUnitGkey(inUnit);
			//if (tran != null && tran.getTranTruckVisit() != null) {
			//  voyageStr = tran.getTranTruckVisit().getTvdtlsTruckLicenseNbr();
			//}
		}

		return voyageStr;
	}

	/**
	 * Return inbound or outbound voyage. For truck, return truck license nbr.
	 * For vessel, barge, and rail, return vessel or train id
	 * CSDV-1622: dickspa 10/02/2014: Get specifically the outbound voyage number.
	 * @param actualCv
	 * @return
	 */
	private String getOutboundActualVoyage(CarrierVisit actualCv) {

		if (actualCv == null) {
			return null;
		}

		String voyageStr = null;
		//For truck, the retuned value is blank
		if (actualCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL) ||
				GEN_BARGE.equalsIgnoreCase(actualCv.getCvId()) ||
				actualCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN) ||
				actualCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR)) {
			if (actualCv.getCvCvd() != null) {
				//currently Vessel=vvdIbVygNbr, Truck=null, Train=rvdtlsId
				voyageStr = actualCv.getCvCvd().getCarrierObVoyNbrOrTrainId();
			}
		} else if (actualCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
			//find the latest truck transaction associated with the unit
			//CSDV-1108
			voyageStr = actualCv.getCarrierVehicleId();
			//TruckTransaction tran = this.findTransactionByUnitGkey(inUnit);
			//if (tran != null && tran.getTranTruckVisit() != null) {
			//  voyageStr = tran.getTranTruckVisit().getTvdtlsTruckLicenseNbr();
			//}
		}

		return voyageStr;
	}

	/**
	 *  Return the full name for barge and vessel. For truck and rail are not needed
	 * @param declaredIbCv
	 * @param actualIbCv
	 * @param inUnit
	 * @param ufv
	 * @return
	 */
	private String getInboundActualCarrierName(CarrierVisit declaredIbCv, CarrierVisit actualIbCv,
											   Unit inUnit, UnitFacilityVisit ufv) {
		if (inUnit == null) {
			return null;
		}

		String carrierName = null;
		if (actualIbCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL) ||
				GEN_BARGE.equalsIgnoreCase(actualIbCv.getCvId())) {
			if (actualIbCv.getCvCvd() != null) {
				//carrierName = actualIbCv.getCvId();
				String carrierOpr = actualIbCv.getCvCvd().getCarrierOperator().getBzuId();  //Carrier Operator
				//this.log("carrierOpr= " + carrierOpr);
				String AnotherOpr = actualIbCv.getCarrierOperator().getBzuId();
				//this.log("AnotherOpr= " + AnotherOpr);
				//For vesseel, it should be the full vessel name
				carrierName = actualIbCv.getCvCvd().getCarrierVehicleName();
				//this.log("carrierName= " + carrierName);
			}
		} //It not needed to truck and rail
		/* else if (actualIbCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN) ||
			actualIbCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR)) {
	  //For train it is the train id
	  TrainVisitDetails trv = TrainVisitDetails.resolveTvdFromCv(actualIbCv);
	  trv.getRvdtlsBizu().getBzuId();
	  if (trv != null) {
		//carrierName = trv.getRvdtlsId();
		carrierName = trv.getCvdCv().getCvId();
	  }
	} else if (actualIbCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
	  //This is a receival transaction. The truck license number is stored as ufvActualIbCv
	  carrierName = actualIbCv.getCvId();
	}
	*/

		return carrierName;
	}

	/**
	 *
	 * @param declaredIbCv
	 * @param actualIbCv
	 * @param inUnit
	 * @param ufv
	 * @return
	 */
	private String getInboundActualCarrierOperator(CarrierVisit declaredIbCv, CarrierVisit actualIbCv,
												   Unit inUnit, UnitFacilityVisit ufv) {
		if (inUnit == null || actualIbCv == null) {
			return null;
		}

		String carrierOprator = null;
		if (GEN_BARGE.equalsIgnoreCase(actualIbCv.getCvId())) {
			//This is a dummy barge viswit terminal defined
			if (actualIbCv.getCarrierOperator() != null) {
				//For barge, return the full name of operator name
				carrierOprator = actualIbCv.getCarrierOperator().getBzuName();
				//this.log("Barge carrierOprator= " + carrierOprator);
			}
		} else if (actualIbCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL) &&
				actualIbCv.getCvClassification() != null) {
			if (actualIbCv.getCvClassification().equals(VesselClassificationEnum.BARGE)) {
				if (actualIbCv.getCarrierOperator() != null) {
					//For barge, return the full name of operator name
					carrierOprator = actualIbCv.getCarrierOperator().getBzuName();
					//this.log("Barge carrierOprator= " + carrierOprator);
				}
			} else {
				if (actualIbCv.getCarrierOperator() != null) {
					//For vessel, it is the line operator defined in Vessel view
					carrierOprator = actualIbCv.getCvCvd().getCarrierOperator().getBzuId();  //Carrier Operator
					//this.log("Vessel carrierOprator1= " + carrierOprator);
					carrierOprator = actualIbCv.getCarrierOperator().getBzuId();
					//this.log("Vessel carrierOprator2= " + carrierOprator);
				}
			}
		} else if (actualIbCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN) ||
				actualIbCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR)) {
			//For train it is the the full name of railroad operator
			TrainVisitDetails trv = TrainVisitDetails.resolveTvdFromCv(actualIbCv);
			if (trv != null) {
				String railRoadId = trv.getRvdtlsId();
				//this.log("railRoadId= " + railRoadId);
				//this.log("railroad Id= " + trv.getRvdtlsBizu().getBzuId());
				Railroad railroad = Railroad.findRailroadById(railRoadId);
				carrierOprator = railroad.getBzuName();
			}
		} else if (actualIbCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
			//This is a receival transaction. The truck license number is stored as ufvActualIbCv
			carrierOprator = actualIbCv.getCvId();
		}

		return carrierOprator;
	}

	/**
	 * For rail, the call sign will be blank. For truck, it'll be truck license number
	 * For barge and vessel, it will be populated from call sign field
	 * @param actualCv
	 * @param inUnit
	 * @return
	 */
	private String getInboundOrOutboundCallSign(CarrierVisit actualCv, Unit inUnit) {

		if (inUnit == null || actualCv == null) {
			return null;
		}

		String callSign = null;
		if (actualCv.getCvCvd() != null && GEN_BARGE.equalsIgnoreCase(actualCv.getCvId())) {
			//This is a dummy barge viswit terminal defined
			callSign = actualCv.getCvCvd().getRadioCallSign();
		} else if (actualCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL)) {
			if (actualCv.getCvCvd() != null) {
				callSign = actualCv.getCvCvd().getRadioCallSign();
			}
		} else if (actualCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
			//find the latest truck transaction associated with the unit
			//CSDV-1108
			callSign = actualCv.getCarrierVehicleId();
			//TruckTransaction tran = this.findTransactionByUnitGkey(inUnit);
			//if (tran != null && tran.getTranTruckVisit() != null) {
			//  callSign = tran.getTranTruckVisit().getTvdtlsTruckLicenseNbr();
			//}
		}  //No need to report for rail

		return callSign;
	}

	/**
	 * Return line operator for vessel, but full name for rail, truck, and barge mode
	 * @param actualCv
	 * @param inUnit
	 * @return
	 */
	private String getInboundOrOutboundActualCarrierOperator(CarrierVisit actualCv, Unit inUnit) {

		if (inUnit == null || actualCv == null) {
			return null;
		}

		String carrierOpr = null;
		String carrierName = null;
		if (GEN_BARGE.equalsIgnoreCase(actualCv.getCvId())) {
			//This is a dummy barge viswit terminal defined
			if (actualCv.getCarrierOperator() != null) {
				//For barge, return the full name of operator name
				carrierOpr = actualCv.getCarrierOperator().getBzuName();
				//this.log("Barge carrierOprator1= " + carrierOpr);
			}
		} else if (actualCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL)) {
			if ((actualCv.getCvClassification() != null &&
					actualCv.getCvClassification().equals(VesselClassificationEnum.BARGE)) ||
					(actualCv.getCarrierVesselClassType() != null &&
							VesselTypeEnum.BARGE.equals(actualCv.getCarrierVesselClassType()))) {
				if (actualCv.getCarrierOperator() != null) {
					//For barge, return the full name of operator name
					carrierOpr = actualCv.getCarrierOperator().getBzuName();
					//this.log("Barge carrierOprator2= " + carrierOpr);
				}
			} else {
				if (actualCv.getCarrierOperator() != null) {
					//For vessel, it is the line operator defined in Vessel view.
					//But get the SCAC code first, then opearator Id
					//this.log("Vessel carrierOprator1= " + carrierOpr);
					carrierOpr = actualCv.getCarrierOperator().getBzuScac();
					if (StringUtils.isBlank(carrierOpr)) {
						carrierOpr = actualCv.getCarrierOperator().getBzuId();
					}
					//this.log("Vessel carrierOprator2= " + carrierOpr);
				}
			}
		} else if (actualCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN) ||
				actualCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR)) {
			//For train it is the the full name of railroad operator
			TrainVisitDetails trv = TrainVisitDetails.resolveTvdFromCv(actualCv);
			if (trv != null) {
				// CSDV-1149: dickspa 10/09/2013: Get the railroad operator for this train visit
				final Railroad railroadOperator = trv.getRvdtlsRR();
				// CSDV-1149: dickspa 10/09/2013: If it exists, then return the railroad operator Name as requested in CSDV.
				if (railroadOperator != null) {
					carrierOpr = railroadOperator.getBzuName();
				}
				else {
					// CSDV-1149: dickspa 10/09/2013: Log an error if the Railroad Operator has not been set for this Train Visit
					log("Railroad Operator not set for Train Visit [" + actualCv.getCvId() + "].");
				}
			}
		} else if (actualCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
			// CSDV-1143: dickspa 26/09/2013: Find the most recent Gate Transaction for the Unit and Truck.
			TruckTransaction tran = this.findMostRecentTransactionByUnitAndTruck(inUnit, actualCv.getCarrierVehicleId());
			if (tran != null && tran.getTranTruckVisit() != null &&
					tran.getTranTruckVisit().getTvdtlsTrkCompany() != null) {
				//For truck, return the name of trucking company
				carrierOpr = tran.getTranTruckVisit().getTvdtlsTrkCompany().getBzuName();
			}
		}

		return carrierOpr;
	}

	/**
	 *  Return the full name for barge and vessel. For truck and rail are not needed
	 * @param declaredObCv
	 * @param actualObCv
	 * @param inUnit
	 * @param ufv
	 * @return
	 */
	private String getOutboundActualCarrierName(CarrierVisit declaredObCv, CarrierVisit actualObCv,
												Unit inUnit, UnitFacilityVisit ufv) {
		if (inUnit == null) {
			return null;
		}

		String carrierName = null;
		if (actualObCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL) ||
				GEN_BARGE.equalsIgnoreCase(actualObCv.getCvId())) {
			if (actualObCv.getCvCvd() != null) {
				//carrierName = actualIbCv.getCvId();
				String carrierOpr = actualObCv.getCvCvd().getCarrierOperator().getBzuId();  //Carrier Operator
				//this.log("carrierOpr= " + carrierOpr);
				//For vesseel, it should be the full vessel name
				carrierName = actualObCv.getCvCvd().getCarrierVehicleName();
			}
		}  //It not needed to truck and rail
		/* else if (actualObCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN) ||
			actualObCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR)) {
	  //For train it is the train id
	  TrainVisitDetails trv = TrainVisitDetails.resolveTvdFromCv(actualObCv);
	  if (trv != null) {
		//carrierName = trv.getRvdtlsId();
		carrierName = trv.getCvdCv().getCvId();
	  }
	} else if (actualObCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
	  //This is a delivery transaciton. Get the truck license number from truck visit details
	  TruckTransaction tran = this.findTransactionByUnitGkey(inUnit);
	  if (tran != null && tran.getTranTruckVisit() != null) {
		carrierName = tran.getTranTruckVisit().getTvdtlsTruckLicenseNbr();
	  }
	}
	*/

		return carrierName;
	}

	/**
	 *  For truck - truck license plate. For Vessel & Barge vessel ID, for train, it is train ID
	 * @param declaredIbCv
	 * @param actualIbCv
	 * @param inUnit
	 * @param ufv
	 * @return
	 */
	private String getInboundActualCvId(CarrierVisit declaredIbCv, CarrierVisit actualIbCv,
										Unit inUnit, UnitFacilityVisit ufv) {
		if (inUnit == null) {
			return null;
		}
		/*
	if (actualIbCv != null) {
	  String actualIbMode = actualIbCv.getCvCarrierMode().getKey();
	  this.log("actualIbMode= " + actualIbMode);
	  this.log("actualIbCv.getCvId()= " + actualIbCv.getCvId());
	  if (actualIbCv.getCvId().length() > 4) {
		this.log("actualIbCv.getCvId(4)= " + actualIbCv.getCvId().substring(0, 4));
	  }
	  if (actualIbCv.getCvCvd() != null) {
		this.log("actualIbCv.getCvCvd().getCarrierVehicleName()= " + actualIbCv.getCvCvd().getCarrierVehicleName());
	  }
	}
	*/
		String cvId = null;
		if (actualIbCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL) ||
				GEN_BARGE.equalsIgnoreCase(actualIbCv.getCvId())) {
			//cvId = actualIbCv.getCvId();
			cvId = actualIbCv.getCarrierVehicleId();
		} else if (actualIbCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN) ||
				actualIbCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR)) {
			TrainVisitDetails trv = TrainVisitDetails.resolveTvdFromCv(actualIbCv);
			if (trv != null) {
				cvId = trv.getRvdtlsId();
			}
		} else if (actualIbCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
			//This is a receival transaction. The truck license number is stored as ufvActualIbCv
			cvId = actualIbCv.getCvId();
		}

		return cvId;
	}

	/**
	 * For truck - truck license plate. For Vessel & Barge vessel ID, for train, it is train ID
	 * @param declaredObCv
	 * @param actualObCv
	 * @param inUnit
	 * @param ufv
	 * @return
	 */
	private String getOutboundActualCvId(CarrierVisit declaredObCv, CarrierVisit actualObCv,
										 Unit inUnit, UnitFacilityVisit ufv) {
		if (inUnit == null) {
			return null;
		}
		/*
	if (actualObCv != null) {
	  String actualObMode = actualObCv.getCvCarrierMode().getKey();
	  this.log("actualObMode= " + actualObMode);
	  this.log("actualObMode= " + actualObMode);inbound-actual-carrier-visit
	  this.log("actualObCv.getCvId()= " + actualObCv.getCvId());
	  if (actualObCv.getCvId().length() > 4) {
		this.log("actualObCv.getCvId(4)= " + actualObCv.getCvId().substring(0, 4));
	  }
	  if (actualObCv.getCvCvd() != null) {
		this.log("actualObCv.getCvCvd().getCarrierVehicleName()= " + actualObCv.getCvCvd().getCarrierVehicleName());
	  }
	}
	*/

		String cvId = null;
		if (actualObCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL) ||
				GEN_BARGE.equalsIgnoreCase(actualObCv.getCvId())) {
			//cvId = actualObCv.getCvId();
			cvId = actualObCv.getCarrierVehicleId();
		} else if (actualObCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN) ||
				actualObCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR)) {
			TrainVisitDetails trv = TrainVisitDetails.resolveTvdFromCv(actualObCv);
			if (trv != null) {
				cvId = trv.getRvdtlsId();
			}
		} else if (actualObCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
			//This is a delivery transaciton. Get the truck license number from truck visit details
			TruckTransaction tran = this.findTransactionByUnitGkey(inUnit);
			if (tran != null && tran.getTranTruckVisit() != null) {
				cvId = tran.getTranTruckVisit().getTvdtlsTruckLicenseNbr();
			}
		} else if (actualObCv.getCvCarrierMode().equals(LocTypeEnum.UNKNOWN)) {
			cvId = actualObCv.getCvId();
		}

		return cvId;
	}

	/**
	 * find latest truck transacoitn by unit
	 * @param inUnit
	 * @return
	 */
	private TruckTransaction findTransactionByUnitGkey(Unit inUnit) {
		TruckTransaction activeTran = null;
		if (inUnit != null) {
			DomainQuery dq = QueryUtils.createDomainQuery(RoadEntity.TRUCK_TRANSACTION)
					.addDqPredicate(PredicateFactory.eq(RoadField.TRAN_UNIT, inUnit.getUnitGkey()))
					.addDqPredicate(PredicateFactory.ne(RoadField.TRAN_STATUS, TranStatusEnum.CANCEL))
					.addDqOrdering(Ordering.desc(RoadField.TRAN_CREATED));

			List trans = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
			if (!trans.isEmpty()) {
				//Return the latest truck transaction associated with this unit
				activeTran = (TruckTransaction) trans.get(0);
			}
		}

		return activeTran;
	}

	/**
	 * CSDV-1143: dickspa 26/09/2013: Finds the most recent Gate Transaction (TruckTransaction) entity which matches the conditions:
	 * 1. The Gate Transaction's Unit matches the supplied Unit.
	 * 2. The Gate Transaction's Truck Visit Truck Licence field matches the supplied Truck Licence.
	 * 3. The Status is not "CANCELLED".
	 * This has been created to work alongside the existing "findTransactionByUnitGkey()"  method. The Truck Licence argument
	 * has been added to fix an issue where when the Truck arrives as an Export on a Truck, and then leaves as a Dray Off also on a Truck.
	 * The old method mistakenly identifies the Dray Off transaction as both the inbound and outbound carrier.
	 * @param unit
	 * @param truckLicence
	 * @return
	 */
	@Nullable
	private TruckTransaction findMostRecentTransactionByUnitAndTruck(@NotNull final Unit unit, @Nullable final String truckLicence) {
		// Should first check that the truckLicence has been supplied. If is has not, then should log an error and return null
		if (StringUtils.isBlank(truckLicence)) {
			log("Could not identify a Gate Transaction with a blank Truck Licence.");
			return null;
		}

		// This will be either returned as null, or set to the first matching result.
		TruckTransaction mostRecentGateTransaction = null;

		// Create the query as explained in the header of this method
		final DomainQuery gateTransactionQuery = QueryUtils.createDomainQuery(RoadEntity.TRUCK_TRANSACTION);
		gateTransactionQuery.addDqPredicate(PredicateFactory.eq(RoadField.TRAN_UNIT, unit.getUnitGkey()));

		gateTransactionQuery.addDqPredicate(PredicateFactory.ne(RoadField.TRAN_STATUS, TranStatusEnum.CANCEL));
		gateTransactionQuery.addDqOrdering(Ordering.desc(RoadField.TRAN_CREATED));
		// Create the query for the Truck Licence Number, and add this to the Gate Transaction entity.
		final DomainQuery truckVisitLicenceNumberQuery = createBasicQuery(RoadEntity.TRUCK_VISIT_DETAILS, RoadField.TVDTLS_TRUCK_LICENSE_NBR, truckLicence);
		gateTransactionQuery.addDqPredicate(PredicateFactory.subQueryIn(truckVisitLicenceNumberQuery, RoadField.TRAN_TRUCK_VISIT));

		// Load the list, and if one is found, the list will already be sorted so return the first value
		final List<TruckTransaction> foundGateTransactions = Roastery.getHibernateApi().findEntitiesByDomainQuery(gateTransactionQuery);
		if (!foundGateTransactions.isEmpty()) {
			mostRecentGateTransaction = foundGateTransactions.get(0);
		}

		return mostRecentGateTransaction;
	}

	/**
	 * CSDV-1143: dickspa 26/09/2013: Create a basic domain query: one entity, with one field being compared as equal to a given value.
	 * @param entityName
	 * @param fieldName
	 * @param truckLicence
	 * @return
	 */
	@NotNull
	private DomainQuery createBasicQuery(@NotNull final String entityName, @NotNull final MetafieldId fieldName, @NotNull final String fieldValue) {
		final DomainQuery basicQuery = QueryUtils.createDomainQuery(entityName);
		basicQuery.addDqPredicate(PredicateFactory.eq(fieldName, fieldValue));
		return basicQuery;
	}

	/**
	 * Method that will return a booking number or ERO
	 * @param inUnit
	 * @return
	 */
	private String getUnitEqboNbr(Unit inUnit) {
		if (inUnit == null) {
			return null;
		}

		String inboundOrder = null;
		if (inUnit.getUnitPrimaryUe() != null) {
			EquipmentOrder equipmentOrder = null;
			EqBaseOrder eqBaseOrder = null;
			if (inUnit.getUnitPrimaryUe().getUeDepartureOrderItem() != null) {
				//It's a booking
				eqBaseOrder = inUnit.getUnitPrimaryUe().getUeDepartureOrderItem().getEqboiOrder();
			} else if (inUnit.getUnitPrimaryUe().getUeArrivalOrderItem() != null) {
				//It's a ERO
				eqBaseOrder = inUnit.getUnitPrimaryUe().getUeArrivalOrderItem().getEqboiOrder();
			}
			equipmentOrder = EquipmentOrder.resolveEqoFromEqbo(eqBaseOrder);
			if (equipmentOrder != null) {
				inboundOrder = equipmentOrder.getEqboNbr();
			}
		}

		return inboundOrder;
	}

	private String getUnitInboundOrder(Unit inUnit) {

		if (inUnit == null) {
			return null;
		}

		String inboundOrder = null;
		EqBaseOrder eqBaseOrder = null;
		EquipmentOrder equipmentOrder = null;
		//For full and empty container, get the inbound order from booking first
		if (!(UnitCategoryEnum.STORAGE.equals(inUnit.getUnitCategory()))) {
		  if (inUnit.getUnitPrimaryUe() != null &&
				  inUnit.getUnitPrimaryUe().getUeDepartureOrderItem() != null) {
			eqBaseOrder = inUnit.getUnitPrimaryUe().getUeDepartureOrderItem().getEqboiOrder();
			equipmentOrder = EquipmentOrder.resolveEqoFromEqbo(eqBaseOrder);
			if (equipmentOrder != null) {
			  inboundOrder = equipmentOrder.getEqboNbr();
			}
		  }
		}

		//If inbound order is not found and it is an empty, then try to get the order from ERO
		if (StringUtils.isBlank(inboundOrder) &&
				inUnit.getUnitFreightKind().equals(FreightKindEnum.MTY)) {
			if (inUnit.getUnitPrimaryUe() != null &&
					inUnit.getUnitPrimaryUe().getUeArrivalOrderItem() != null) {
				eqBaseOrder = inUnit.getUnitPrimaryUe().getUeArrivalOrderItem().getEqboiOrder();
				equipmentOrder = EquipmentOrder.resolveEqoFromEqbo(eqBaseOrder);
				if (equipmentOrder != null) {
					inboundOrder = equipmentOrder.getEqboNbr();
				}
			}
		}

		return inboundOrder;
	}

	private UnitFacilityVisit getAdvisedUfvForUnit(Unit inUnit) {
		//this.log("getDepartedUfvForUnit unit.unitVisitState=$inUnit.unitVisitState")

		if (!UnitVisitStateEnum.ADVISED.equals(inUnit.getUnitVisitState())) {
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
	 * Return the point terminal based on POD1 or POL
	 * @param inRouting
	 * @param inDirection
	 * @return
	 */
	private String getRoutingPointTerminalId(Routing inRouting, String inDirection) {
		if (inRouting == null) {
			return "";
		}

		RoutingPoint routingPoint = null;
		if (StringUtils.equalsIgnoreCase(inDirection, "outbound")) {
			//Get the to terminal from POD1
			routingPoint = inRouting.getRtgPOD1();
		} else {
			//Get the from terminal from POL
			routingPoint = inRouting.getRtgPOL();
		}

		String pointTerminal = null;
		if (routingPoint != null) {
			//Get the terminal id based on POD1 or POL
			pointTerminal = routingPoint.getPointTerminal();
		}

		return pointTerminal;
	}

	/**
	 * the pod will be getting from the last port of discharge field.
	 * @param inRouting
	 * @return
	 */
	private String getRoutingPodUnLocId(Routing inRouting) {
		if (inRouting == null) {
			return "";
		}
		RoutingPoint routingPoint = null;
		String pod = null;
		if (inRouting.getRtgOPT3() != null) {
			routingPoint = inRouting.getRtgOPT3();
		} else if (inRouting.getRtgOPT2() != null) {
			routingPoint = inRouting.getRtgOPT2();
		} else if (inRouting.getRtgOPT1() != null) {
			routingPoint = inRouting.getRtgOPT1();
		} else if (inRouting.getRtgPOD2() != null) {
			routingPoint = inRouting.getRtgPOD2();
		} else if (inRouting.getRtgPOD1() != null) {
			routingPoint = inRouting.getRtgPOD1();
		}

		if (routingPoint != null) {
			//pod = routingPoint.getPointId();
			pod = routingPoint.getPointUnlocId();
		}

		return pod;
	}

	/**
	 *  build root element
	 * @param eActivityMsg
	 * @param inEvent
	 * @param sNS
	 */
	private void buildRootAndHeaderElements(Element eActivityMsg, GroovyEvent inEvent,
											String inReportedEventType, Namespace sNS) {

		String responseSchemaName = "RTO_N4.xsd";
		eActivityMsg.addNamespaceDeclaration(XmlUtil.XSI_NAMESPACE);
		XmlUtil.addSchemaLocation(eActivityMsg, sNS /*XmlUtil.ARGO_NAMESPACE */, responseSchemaName);
		//Add header element
		Element eHeader = new Element("header", sNS);
		eActivityMsg.addContent(eHeader);
		eHeader.setAttribute("SenderId", "TOS");
		eHeader.setAttribute("ReceiverId", "RTO");
		eHeader.setAttribute("EventType", inReportedEventType ? inReportedEventType : this.stringNullCheck(inEvent.getEvent().getEventTypeId()));
		//eHeader.setAttribute("EventType", inEvent.getEvent().getEventTypeId());
		eHeader.setAttribute("messageDateTime", this.getXmlTimestamp(new Date()));
	}

	/**
	 *  build event element
	 * @param eActivityMsg
	 * @param eEvent
	 * @param inEvent
	 */
	private void buildEventElement(Element eActivityMsg, Element eEvent,
								   GroovyEvent inEvent, String inReportedEventType) {

		eActivityMsg.addContent(eEvent);
		eEvent.setAttribute("gkey", inEvent.getEvent().getEventGKey().toString());
		eEvent.setAttribute("message-id", RandomStringUtils.random(15, false, true));
		eEvent.setAttribute("created", this.getXmlTimestamp(inEvent.getEvent().getEventTime()));
		eEvent.setAttribute("recorded", this.getXmlTimestamp(inEvent.getEvent().getEventCreated()));
		eEvent.setAttribute("operator-id", this.stringNullCheck(ContextHelper.getThreadOperator().getOprId()));
		eEvent.setAttribute("operator-name", this.stringNullCheck(ContextHelper.getThreadOperator().getOprName()));
		eEvent.setAttribute("complex-id", this.stringNullCheck(ContextHelper.getThreadComplex().getCpxId()));
		eEvent.setAttribute("complex-name", this.stringNullCheck(ContextHelper.getThreadComplex().getCpxName()));
		eEvent.setAttribute("complex-timezone-id", this.stringNullCheck(ContextHelper.getThreadComplex().getCpxTimeZoneId()));
		String fcyId = ContextHelper.getThreadFacility() ? this.stringNullCheck(ContextHelper.getThreadFacility().getFcyId()) : "";
		if (StringUtils.isBlank(fcyId)) {
			fcyId = this.getFacilityIdFromGeneralReference();
		}
		eEvent.setAttribute("facility-id", this.stringNullCheck(fcyId));
		String facility_name = null;
		if (ContextHelper.getThreadFacility() != null) {
			//this.log("Facility-name= " + ContextHelper.getThreadFacility().getFcyName());
			facility_name = ContextHelper.getThreadFacility().getFcyName();
		}
		eEvent.setAttribute("facility-name", this.stringNullCheck(facility_name));
		eEvent.setAttribute("facility-timezone-id", this.stringNullCheck(ContextHelper.getThreadFacility() ? this.stringNullCheck(ContextHelper.getThreadFacility().getFcyTimeZoneId()) : ""));
		eEvent.setAttribute("facility-is-non-operational", ContextHelper.getThreadFacility() ? this.booleanNullCheck(ContextHelper.getThreadFacility().getFcyIsNonOperational()) : "false");
		eEvent.setAttribute("facility-routing-point-id", ContextHelper.getThreadFacility() ? this.stringNullCheck(ContextHelper.getThreadFacility().getFcyRoutingPoint().getPointId()) : "");
		eEvent.setAttribute("type-name", inReportedEventType ? inReportedEventType : this.stringNullCheck(inEvent.getEvent().getEventTypeId()));
		//eEvent.setAttribute("type-name", inEvent.getEvent().getEventTypeId());
		eEvent.setAttribute("timestamp", this.getXmlTimestamp(new Date()));
		eEvent.setAttribute("type-description", this.stringNullCheck(inEvent.getEvent().getEvntEventType().getEvnttypeDescription()));
		eEvent.setAttribute("type-is-built-in", this.booleanNullCheck(inEvent.getEvent().getEvntEventType().getEvnttypeIsBuiltInEvent()));
		eEvent.setAttribute("type-is-bulkable", this.booleanNullCheck(inEvent.getEvent().getEvntEventType().getEvnttypeCanBulkUpdate()));
		eEvent.setAttribute("type-is-billable", this.booleanNullCheck(inEvent.getEvent().getEvntEventType().getEvnttypeIsBillable()));
		eEvent.setAttribute("type-is-notifiable", this.booleanNullCheck(inEvent.getEvent().getEvntEventType().getEvnttypeIsNotifiable()));
		eEvent.setAttribute("type-applies-to-entity", inEvent.getEvent().getEvntEventType().getEvnttypeAppliesTo().getKey().toLowerCase());
		eEvent.setAttribute("applied-by", this.stringNullCheck(inEvent.getEvent().getEvntAppliedBy()));
		eEvent.setAttribute("creator", this.stringNullCheck(inEvent.getEvent().getEvntCreator()));
	}

	/**
	 *
	 * @return
	 */
	private String getFacilityIdFromGeneralReference() {

		GroovyApi groovyApi = new GroovyApi();
		//Populate the facility Id from general reference
		String definedFcyId = groovyApi.getReferenceValue(ACTIVITY_MESSAGE_GR, FACILITY_ID1_GR, null, null, 1);

		return definedFcyId;
	}

	/**
	 * build field changes element
	 * CSDV-1121: dickspa 05/09/2013: Pass in the Unit as a parameter, and get the Routing from it in here.
	 * @param eEvent
	 * @param changes
	 * @param sNS
	 */
	private void buildFieldChanges(Element eEvent, Set changes, Namespace sNS, Unit unit) {
		final Routing routing = unit.getUnitRouting();
		// CSDV-1121: dickspa 27/08/2013 : Initialise the list of the Routing Point elements in the field changes that
		// should be used when calculating the last POD to send as a new element
		createRequiredRoutingPointsForPODFieldChangeElement();

		//The changes has been validated already
		Element eFieldChanges = new Element("field-changes", sNS);
		boolean fieldsAdded = false;
		eFieldChanges.setAttribute("count", Integer.toString(changes.size()));
		for (Object aFc : changes) {
			EventFieldChange fc = (EventFieldChange) aFc;
			// CSDV-1121: dickspa 27/08/2013, pChidambaram 30/08/2013: If this current Field Change is one of the PODs in the special case defined on CSDV-1121, then
			// should added this with the old and new values to a storage list of the EventFieldChanges.
			if (requiredRoutingPointsForPODFieldChangeElement.contains(fc.getEvntfcMetafieldId())){
				routingPointFieldChanges.add(fc);
			} else {
				fieldsAdded = true;
				Element eFieldChange = new Element("field-change", sNS);
				eFieldChanges.addContent(eFieldChange);
				//this.log("gkey= " + this.longNullCheck(fc.getEvntfcGkey().toString()));
//        this.log("field-name= " + this.stringNullCheck(fc.getEvntfcMetafieldId()));
//        this.log("previous-value= " + this.stringNullCheck(ArgoUtils.getPropertyValueAsUiString(MetafieldIdFactory.valueOf(fc.getEvntfcMetafieldId()), fc.getEvntfcPrevVal())));
//        this.log("new-value= " + this.stringNullCheck(ArgoUtils.getPropertyValueAsUiString(MetafieldIdFactory.valueOf(fc.getEvntfcMetafieldId()), fc.getEvntfcNewVal())));
				//this.log("description= " + this.stringNullCheck(fc.getEvntfcDescription()));

				// CSDV-1121: dickspa 23/08/2013: Slight change to existing logic to make sure this is null-safe.
				// Get the field name, then peform the null-check before attempting to do a lookup on it. Also have moved the existing lookup on ufvFlexString07
				// into the field name translation method for consistency
				String fieldName = fc.getEvntfcMetafieldId();
				fieldName = this.stringNullCheck(fieldName);
				fieldName = this.getFieldChangesFieldNameTranslation(fieldName);
				eFieldChange.setAttribute("gkey", this.longNullCheck(fc.getEvntfcGkey().toString()));
				eFieldChange.setAttribute("field-name", fieldName);
				eFieldChange.setAttribute("previous-value", this.stringNullCheck(ArgoUtils.getPropertyValueAsUiString(MetafieldIdFactory.valueOf(fc.getEvntfcMetafieldId()), fc.getEvntfcPrevVal())));
				eFieldChange.setAttribute("new-value", this.stringNullCheck(ArgoUtils.getPropertyValueAsUiString(MetafieldIdFactory.valueOf(fc.getEvntfcMetafieldId()), fc.getEvntfcNewVal())));
				eFieldChange.setAttribute("description", this.stringNullCheck(fc.getEvntfcDescription()));
			}
		} //end of building field changes

		// CSDV-1121: dickspa 27/08/2013, pChidambaram 30/08/2013: Now if a POD element is required, it will be created by this method, and we
		// should add it to the field-changes element
		if (routingPointFieldChanges.size() > 0){
			// CSDV-1121: dickspa 03/09/2013: Try to write this as cleanly as possible
			final Element specialCasePODElement = createPODElementFromObservedRoutingChanges(sNS, routing);
			if (specialCasePODElement != null) {
				fieldsAdded = true;
				eFieldChanges.addContent(specialCasePODElement);
			}
		}

		// CSDV-1121: dickspa 05/09/2013: Special Case RTO Category. If the RTO Category was changed just before
		// this XMLFormatter class was called, then should add a new field-change element.
		final Element specialCaseRTOCategoryElement = createRTOCategoryElement(unit, sNS);
		if (specialCaseRTOCategoryElement != null) {
			fieldsAdded = true;
			eFieldChanges.addContent(specialCaseRTOCategoryElement);
		}

		if (fieldsAdded == true) {
			eEvent.addContent(eFieldChanges);
		}
	}

	/**
	 * CSDV-1121: dickspa 05/09/2013: If the RTO Category has just changed, then create a new field-change element
	 * which contains the from/to values for this. Can get these values from the RTO_CATEGORY_CHANGED event.
	 * @param unit
	 * @param namespace
	 * @return
	 */
	@Nullable
	private Element createRTOCategoryElement(@NotNull final Unit unit, @NotNull final Namespace namespace) {
		Element specialCaseRTOCategoryElement;

		// Firstly, need to confirm if the RTO Category has changed by checking if an RTO_CATEGORY_CHANGE has just been applied to this Unit
		final IEvent appliedRtoCategoryChangeEvent = getUnitRTOCategoryChangeJustApplied(unit);

		// If no such event has been applied, then do not need to create this new field-change element and can return null from here
		if (appliedRtoCategoryChangeEvent == null) {
			return null;
		}

		// Now need to gather the data from this event and create the new element
		final String elementName = "field-change";
		final String unitGkeyString = Long.toString(unit.getUnitGkey());
		final String fieldName = "unit.rtocategory";
		// So, the Event's notes field will look like this:
		// RTO category changed: E -> D
		// From this field, need to get the prev value (in this case, E), the new value (D) and the change (E -> D).
		// Need to do some string manipulation to work this out
		final String eventNote = appliedRtoCategoryChangeEvent.getEventNote();
		// Need to strip out part of the Note to reveal the part which should be added to this element
		final String descriptionWithWhitespace = StringUtils.remove(eventNote, "RTO category changed: ");
		// The description will contain some unwanted whitespace, something like:
		// E -> D
		// Remove the spaces
		final String description = StringUtils.remove(descriptionWithWhitespace, " ");
		// the "description" will be:
		// E->D
		// Now need to split the String on the "->", and get the previous and new values
		final String[] previousAndNewValues = StringUtils.split(description, "->");
		// I expect that splitting "E->D" on the "->" will produce two values, "E" and "D". Should confirm this
		if (previousAndNewValues == null || previousAndNewValues.length != 2) {
			log("Tried to calculate the old/new RTO Category to add the field-change element for it, but was unable to get values.");
			if (previousAndNewValues == null) {
				log("When splitting the event Note " + eventNote + ", then splitting the changes " + description + ", got no values.");
			}
			else {
				log("When splitting the event Note " + eventNote + ", then splitting the changes " + description + ", expected 2 values " +
						"but got " + previousAndNewValues.length + ".");
				for (final String eachValue : previousAndNewValues) {
					log("\t" + eachValue);
				}
			}
			return null;
		}

		// So now we definitely have the old and new values, can just store these
		final String previousValue = previousAndNewValues[0];
		final String newValue = previousAndNewValues[1];
		specialCaseRTOCategoryElement = createNewElement("field-change", namespace, unitGkeyString, fieldName, description, previousValue, newValue, null);

		return specialCaseRTOCategoryElement;
	}

	/**
	 * CSDV-1121: dickspa 03/09/2013: Create the Special Case POD element using the logic described in the CSDV-1121.
	 * This will create the Element if possible, but will return null if not, according to the logic.
	 * @return
	 */
	@Nullable
	private Element createPODElementFromObservedRoutingChanges(@NotNull final Namespace namespace, @NotNull final Routing routing) {
		Element podElement;
		final String podElementName = "pod.value";
//    log(String.format("Starting logic to create the %s Element from the field changes.", podElementName));

		// Need to first do a bit of a data fix to make sure this gives the correct output.
		// Fixing the scenario where POD1 might be set in N4 earlier. User than sets POD2 and OPT1 to a new value.
		// POD1 is in the Unit Routing, but will not be seen as a Field Change.
		// As POD2 and OPT1 have new values but no previous, this method will report that no previous value exists and so will
		// not add the POD element.
		// To fix this, need to go through the list of valid Routing values from the Unit. If any are in the Routing but
		// are not in the Field Changes, should add these as "previous" values only.
		addExistingRoutingPointsWhichHaveNotBeenUpdatedToFieldChanges(routing);

		// So, should go through the list of observed POD-related field changes
		// Should start from the latest (rtgOPT3) and go through to the earliest (rtgPOD1).
		Collections.sort(routingPointFieldChanges, new RTOPODListComparator());

		// So now, should go through each one. Find the first one that has a non-null value for "new value", and store it.
		final EventFieldChange fieldChangeWithValidNewValue = getFirstEventFieldChangeWithValidNewValue();

		// So at this point, may have found a new value which is not-null, but may not have
		// If none of the fields have a new value supplied, should return null here
		if (fieldChangeWithValidNewValue == null) {
			log(String.format("Could not identify any POD-related field changes with a new value. Not adding %s element.", podElementName));
			return null;
		}

		// So now we need to find the correct "previous" value to report back.
		final EventFieldChange fieldChangeWithValidPreviousValue = getFirstEventFieldChangeWithValidPreviousValue(fieldChangeWithValidNewValue);

		// If this was not identified, then we should output a warning and return null
		if (fieldChangeWithValidPreviousValue == null) {
			log(String.format("Could not identify any POD-related field changes with a valid previous value. Not adding %s element.", podElementName));
			return null;
		}

		// So now, both previous and new values have been found. Can create the element with these values.
		podElement = createNewElement("field-change", namespace, null, podElementName, null, fieldChangeWithValidPreviousValue.getPrevVal(),
				fieldChangeWithValidNewValue.getNewVal(), fieldChangeWithValidNewValue.getMetafieldId());
		log(String.format("Successfully created the new %s element. Got new value %s from field %s, and got previous value %s from field %s.",
				podElementName, fieldChangeWithValidNewValue.getNewVal(), fieldChangeWithValidNewValue.getMetafieldId(),
				fieldChangeWithValidPreviousValue.getPrevVal(), fieldChangeWithValidPreviousValue.getMetafieldId()));
		return podElement;
	}

	/**
	 * CSDV-1121: dickspa 03/09/2013: Add Routing Points which are in the Unit Routing, but are not in the Field Changes, to the Field Changes
	 * as "previous" values only.
	 * @param routing
	 */
	private void addExistingRoutingPointsWhichHaveNotBeenUpdatedToFieldChanges(@NotNull final Routing routing) {
		// Go through every POD-related field that we are looking for
		for (final String eachPODField : requiredRoutingPointsForPODFieldChangeElement) {
			// Check to see if this field has changed. If it has, take no action. But if it is not, look to see if the routing entity
			// has this field.
			if (!isFieldInObservedPODFieldChanges(eachPODField)) {

				// Routing does not seem to support getField(). So will have to get the field value via a method
				final RoutingPoint currentPODFieldValueInRouting = getRoutingPointField(routing, eachPODField);

				// If it does have a value, add this to the observed POD-related Field Changes
				if (currentPODFieldValueInRouting != null) {
					final EventFieldChange newPODFieldChange = new EventFieldChange();
					newPODFieldChange.setFieldValue(ServicesField.EVNTFC_METAFIELD_ID, eachPODField);
					newPODFieldChange.setFieldValue(ServicesField.EVNTFC_PREV_VAL, Long.toString(currentPODFieldValueInRouting.getPointGkey()));
					routingPointFieldChanges.add(newPODFieldChange);
//          log(String.format("Field %s did not change, but previous value %s is set. Ensuring this is examined correctly when setting pod.value.",
//                              eachPODField, currentPODFieldValueInRouting.getPointId()));
				}
			}
		}
	}

	/**
	 * CSDV-1121: dickspa 03/09/2013: As the RoutingPoint entity seems to not support the getField() method, need to write this
	 * myself below.
	 * @param routingPointField
	 * @return
	 */
	@NotNull
	private RoutingPoint getRoutingPointField(@NotNull final Routing routing, @NotNull final String routingPointField) {
		RoutingPoint fieldValue = null;

		if (StringUtils.equals(routingPointField, InventoryField.RTG_P_O_D1.getFieldId())) {
			fieldValue = routing.getRtgPOD1();
		}
		else if (StringUtils.equals(routingPointField, InventoryField.RTG_P_O_D2.getFieldId())) {
			fieldValue = routing.getRtgPOD2();
		}
		else if (StringUtils.equals(routingPointField, InventoryField.RTG_O_P_T1.getFieldId())) {
			fieldValue = routing.getRtgOPT1();
		}
		else if (StringUtils.equals(routingPointField, InventoryField.RTG_O_P_T2.getFieldId())) {
			fieldValue = routing.getRtgOPT2();
		}
		else if (StringUtils.equals(routingPointField, InventoryField.RTG_O_P_T3.getFieldId())) {
			fieldValue = routing.getRtgOPT3();
		}
		else {
			log(String.format("ERROR! Could not identify Routing Field %s.", routingPointField));
		}

		return fieldValue;
	}

	/**
	 * CSDV-1121: dickspa 03/09/2013: Returns true if the observed POD-related Field Changes contains an entry that
	 * has this metafield. False if not.
	 * @param podField
	 * @return
	 */
	@NotNull
	private boolean isFieldInObservedPODFieldChanges(@NotNull final String podField) {
		boolean isInFieldChanges = false;

		// For each field change, look at the metafield and compare to input
		for (final EventFieldChange currentFieldChange : routingPointFieldChanges) {
			final String fieldname = currentFieldChange.getMetafieldId();
			if (StringUtils.equals(fieldname, podField)) {
				isInFieldChanges = true;
			}
		}

		return isInFieldChanges;
	}

	/**
	 * CSDV-1121: dickpa 03/09/2013: Create new Element. The elementName must be supplied, but if the attributes will each be added if supplied.
	 * @param elementName
	 * @param gkey
	 * @param fieldName
	 * @param description
	 * @param previousValue
	 * @param newValue
	 * @return
	 */
	@NotNull
	private Element createNewElement(@NotNull final String elementName, @NotNull final Namespace namespace, @Nullable final String gkey,
									 @Nullable final String fieldName, @Nullable final String description, @Nullable final String previousValue,
									 @Nullable final String newValue, @Nullable final String metafieldIdString) {
		final Element element = new Element(elementName, namespace);

		// For both the previous value and new value, if the metafield ID was supplied, should do a UI lookup
		String outputPreviousValue;
		String outputNewValue;
		if (StringUtils.isNotBlank(metafieldIdString)) {
			final MetafieldId metafieldId = MetafieldIdFactory.valueOf(metafieldIdString);
			outputPreviousValue = ArgoUtils.getPropertyValueAsUiString(metafieldId, previousValue);
			outputNewValue = ArgoUtils.getPropertyValueAsUiString(metafieldId, newValue);
		}
		// Else just output them as they are
		else {
			outputPreviousValue = previousValue;
			outputNewValue = newValue;
		}

		// Add each of these attributes if they are supplied
		if (StringUtils.isNotEmpty(gkey)) {
			element.setAttribute("gkey", gkey);
		}
		if (StringUtils.isNotEmpty(fieldName)) {
			element.setAttribute("field-name", fieldName);
		}
		if (StringUtils.isNotEmpty(outputPreviousValue)) {
			element.setAttribute("previous-value", outputPreviousValue);
		}
		if (StringUtils.isNotEmpty(outputNewValue)) {
			element.setAttribute("new-value", outputNewValue);
		}
		if (StringUtils.isNotEmpty(description)) {
			element.setAttribute("description", description);
		}

		return element;
	}

	/**
	 * CSDV-1121: dickspa 03/09/2013: Get the POD-related EventFieldChange which contains the previous value which should be reported.
	 * This can be the same as the input parameter if the conditions are met.
	 *
	 * 2.1 First priority is to use the current rtgPOD's current value, but only if:
	 * 2.1.1 The rtgPOD's current value is not null, AND
	 * 2.1.2 The rtgPOD's current value is not equal to the rtgPOD's new value.
	 * 2.2 If selected rtgPOD's current value did not meet the criteria,
	 * 2.2.1 Go through the current value of all rtgPOD's from latest to oldest.
	 * 2.2.2 Identify the latest rtgPOD with a non-null current value. Select it as the POD-element's old value if:
	 * 2.2.3 The selected latest current value is not equal to the current rtgPOD's new value selected in step 1.
	 * @param fieldChangeWithValidNewValue
	 * @return
	 */
	@Nullable
	private EventFieldChange getFirstEventFieldChangeWithValidPreviousValue(@NotNull final EventFieldChange fieldChangeWithValidNewValue) {
		EventFieldChange fieldChangeWithValidPreviousValue;

		// So 2.1, try to use input field Change's previous value if valid and not equal to the new value
		final String previousValueOfSameFieldAsNewValue = fieldChangeWithValidNewValue.getPrevVal();
		final String identifiedNewValue = fieldChangeWithValidNewValue.getNewVal();
		if (isStringNotBlankAndNotEqualToReferenceString(previousValueOfSameFieldAsNewValue, identifiedNewValue)) {
			log(String.format("Found that the new field change value %s is different to the valid previous value %s, so using these as new/previous values.",
					identifiedNewValue, previousValueOfSameFieldAsNewValue));
			fieldChangeWithValidPreviousValue = fieldChangeWithValidNewValue;
		}

		// If we did not identify that the same field old/new values can be used, we do now need to try and find the appropriate previous value
		// from the whole list of POD-related field changes
		if (fieldChangeWithValidPreviousValue == null) {

			// So now need to iterate through the list
			for (final EventFieldChange currentFieldChange : routingPointFieldChanges) {
				// Can select this field change if:
				// If has a non-null previous value
				// AND the previous value does not equal the new value of the "new" field change
				final String currentFieldChangePreviousValue = currentFieldChange.getPrevVal();
				if (isStringNotBlankAndNotEqualToReferenceString(currentFieldChangePreviousValue, identifiedNewValue)) {
					fieldChangeWithValidPreviousValue = currentFieldChange;
					log(String.format("Found that field %s has a previous value %s which is suitable.", currentFieldChange.getEvntfcMetafieldId(),
							currentFieldChangePreviousValue));
					break;
				}
			}
		}

		return fieldChangeWithValidPreviousValue;
	}

	/**
	 * CSDV-1121: dickspa 03/09/2013: Returns true if the inputString is NOT null AND is NOT equal to the reference String.
	 * @param inputString
	 * @param referenceString
	 * @return
	 */
	@NotNull
	private boolean isStringNotBlankAndNotEqualToReferenceString(@Nullable final String inputString, @Nullable final String referenceString) {
		return StringUtils.isNotBlank(inputString) && !StringUtils.equals(inputString, referenceString);
	}

	/**
	 * CSDV-1121: dickspa 03/09/2013: goes through the sorted list of observed POD-related field changes,
	 * and returns the first one with a valid (non-null) new value.
	 * Returns null if none are identified.
	 * @return
	 */
	@Nullable
	private EventFieldChange getFirstEventFieldChangeWithValidNewValue() {
		EventFieldChange fieldChangeWithValidNewValue = null;

		// Go through the list: as it is already sorted, can just iterate through, going from best (most recent) to worst.
		for (final EventFieldChange currentFieldChange : routingPointFieldChanges) {
			final String currentFieldChangeNewValue = currentFieldChange.getEvntfcNewVal();
			// If non-null, use it and break out of this loop!
			if (StringUtils.isNotBlank(currentFieldChangeNewValue)) {
				fieldChangeWithValidNewValue = currentFieldChange;
				log(String.format("Identified that field %s has valid new value %s. Using this as new Value!", currentFieldChange.getMetafieldId(),
						currentFieldChangeNewValue));
				break;
			}
		}

		return fieldChangeWithValidNewValue;
	}

	/**
	 * CSDV-1121: pChidambaram 30/08/2013: This initialises the List of all Routing Points for which we need to store the exact POD field,
	 * along with the new and old value. This will add:
	 * rtgPOD1
	 * rtgPOD2
	 * rtgOPT1
	 * rtgOPT2
	 * rtgOPT3 IN REVERSE ORDER
	 * Note that they need to be stored as Strings to be comparable with the EventFieldChange metafield attribute.
	 * Note They are added in reverse order (though since the refactor there is no advantage/disadvantage to this.
	 */
	private void createRequiredRoutingPointsForPODFieldChangeElement() {
		requiredRoutingPointsForPODFieldChangeElement.add(InventoryField.RTG_O_P_T3.getFieldId());
		requiredRoutingPointsForPODFieldChangeElement.add(InventoryField.RTG_O_P_T2.getFieldId());
		requiredRoutingPointsForPODFieldChangeElement.add(InventoryField.RTG_O_P_T1.getFieldId());
		requiredRoutingPointsForPODFieldChangeElement.add(InventoryField.RTG_P_O_D2.getFieldId());
		requiredRoutingPointsForPODFieldChangeElement.add(InventoryField.RTG_P_O_D1.getFieldId());
	}

	/**
	 * CSDV-1121: dickspa 23/08/2013: Map the supplied field-name from N4 value to RTO value. If no mapping required, return original value.
	 * @param currentFieldName The N4 field-name before mapping.
	 * @return The mapped field-name (or if no mapping for the field-name is required, the original input).
	 */
	private String getFieldChangesFieldNameTranslation(final String currentFieldName) {
		String translatedFieldName;
		// The Map is initialised when declared, so it will be empty rather than null if not set up yet.
		// Only set it up the first time, after that it will be already populated.
		if (fieldChangeFieldNameTranslations.isEmpty()) {
			createFieldChangesFieldNameTranslations();
		}

		// If there is a mapping for this field-name, then lookup the RTO value and return this
		if (fieldChangeFieldNameTranslations.containsKey(currentFieldName)) {
			translatedFieldName = fieldChangeFieldNameTranslations.get(currentFieldName);
			log(String.format("Translated field-changes field-name from %s to %s", currentFieldName, translatedFieldName));
		}
		// else if there is no mapping, just return the current field-name.
		else {
			translatedFieldName = currentFieldName;
		}

		return translatedFieldName;
	}

	/**
	 * CSDV-1121: dickspa 23/08/2013: Create a Map full of the lookup values to translate from N4 names to RTO names.
	 * The key will be the N4 fieldname, and the value is the RTO fieldname.
	 * Danny Holthuijsen 01/10/2013: added translation for eqIdFull
	 */
	private void createFieldChangesFieldNameTranslations() {
		fieldChangeFieldNameTranslations.put("ufvTime", "Intime-in");
		fieldChangeFieldNameTranslations.put("unitSealNbr1", "seal-number-1");
		fieldChangeFieldNameTranslations.put("unitSealNbr2", "seal-number-2");
		fieldChangeFieldNameTranslations.put("unitSealNbr3", "seal-number-3");
		fieldChangeFieldNameTranslations.put("unitSealNbr4", "seal-number-4");
		fieldChangeFieldNameTranslations.put("hzrdiImdgClass", "hazard-item.imdg-class");
		fieldChangeFieldNameTranslations.put("unitLineOperator", "primary-equipment.line-operator");
		fieldChangeFieldNameTranslations.put("unitId", "unit.id");
		fieldChangeFieldNameTranslations.put("eqIdFull", "unit.id");
		fieldChangeFieldNameTranslations.put("eqEquipType", "primary-equipment.equipment-type");
		fieldChangeFieldNameTranslations.put("posName", "last-known-position");
		fieldChangeFieldNameTranslations.put("unitFreightKind", "unit.freight-kind");
		fieldChangeFieldNameTranslations.put("unitGoodsAndCtrWtKg", "unit.weight");

		// This one was previously implemented in an "if" just before the field-name was set. Moved it into here for consistency.
		fieldChangeFieldNameTranslations.put("ufvFlexString07", "RTO-Category");

	}

	/**
	 * build unit element
	 * @param eUnit
	 * @param inUnit
	 * @param ufv
	 * @param sNS
	 */
	private void buildUnitAndStatusRemarksAgentsElements(Element eUnit, Unit inUnit, UnitFacilityVisit ufv, Namespace sNS) {

		eUnit.setAttribute("id", this.stringNullCheck(inUnit.getUnitId()));
		eUnit.setAttribute("gkey", this.longNullCheck(inUnit.getUnitGkey().toString()));
		eUnit.setAttribute("created", this.getXmlTimestamp(inUnit.getUnitCreateTime()));
		double grossWeight = inUnit.getUnitGoodsAndCtrWtKg() ? inUnit.getUnitGoodsAndCtrWtKg().doubleValue() : 0.0;
		eUnit.setAttribute("weight", this.doubleNullCheck(grossWeight.toString()));
		eUnit.setAttribute("category", this.stringNullCheck(inUnit.getUnitCategory().getKey().toLowerCase()));
		//RTO category is stored in ufvFlexString07 field
		eUnit.setAttribute("rtocategory", this.stringNullCheck(ufv.getUfvFlexString07()));
		String rtoFreightKind = this.getRtoFreightKind(inUnit);
		eUnit.setAttribute("freight-kind", this.stringNullCheck(rtoFreightKind));

		//Add status element under unit element
		Element eStatus = new Element("status", sNS);
		eUnit.addContent(eStatus);
		//vAdd isit-state element under unit element
		Element eStatusVisitState = new Element("visit-state", sNS);
		eStatus.addContent(eStatusVisitState);
		eStatusVisitState.setAttribute("value", this.formatUnitVisitState(inUnit.getUnitVisitState()));
		//Add transit-state element under unit element
		Element eStatusTransitState = new Element("transit-state", sNS);
		eStatus.addContent(eStatusTransitState);
		eStatusTransitState.setAttribute("value", this.formatUfvTransitState(ufv.getUfvTransitState()));
		//Add remark element under unit element
		Element eRemark = new Element("remark", sNS);
		eUnit.addContent(eRemark);
		eRemark.setAttribute("value", this.stringNullCheck(inUnit.getUnitRemark()));
		if (inUnit.getUnitAgent1() != null) {
			//Add agent-1 element under unit element
			Element eAgent1 = new Element("agent-1", sNS);
			eUnit.addContent(eAgent1);
			eAgent1.setAttribute("value", this.stringNullCheck(inUnit.getUnitAgent1().getBzuId()));
		}
		if (inUnit.getUnitAgent2() != null) {
			//Add agent-2 element under unit element
			Element eAgent2 = new Element("agent-2", sNS);
			eUnit.addContent(eAgent2);
			eAgent2.setAttribute("value", this.stringNullCheck(inUnit.getUnitAgent2().getBzuId()));
		}
	}

	/**
	 * return F  for FCL, LCL, B-bulk.  E for MTY
	 * @param inUnit
	 * @return
	 */
	private String getRtoFreightKind(Unit inUnit) {

		String rtoFreightKind = "E";
		if (inUnit == null || inUnit.getUnitFreightKind() == null) {
			return rtoFreightKind;
		}

		if (StringUtils.equals(inUnit.getUnitFreightKind().getKey(), "MTY")) {
			rtoFreightKind = "E";
		} else {
			rtoFreightKind = "F";
		}

		return rtoFreightKind;
	}

	/**
	 * build damages element
	 * @param damages
	 * @param eUnit
	 * @param sNS
	 */
	private void buildDamagesElement(List damages, Element eUnit, Namespace sNS) {

		Element eDamages = new Element("damages", sNS);
		eUnit.addContent(eDamages);
		eDamages.setAttribute("count", Integer.toString(damages.size()));
		Iterator damageItemItr = damages.iterator();
		while (damageItemItr.hasNext()) {
			UnitEquipDamageItem damageItem = (UnitEquipDamageItem) damageItemItr.next();
			//amage-item element
			Element eDamagedItem = new Element("damage-item", sNS);
			eDamages.addContent(eDamagedItem);
			eDamagedItem.addContent("");
			eDamagedItem.setAttribute("gkey", this.longNullCheck(damageItem.getDmgitemGkey().toString()));
			eDamagedItem.setAttribute("type", this.stringNullCheck(damageItem.getDmgitemType().getEqdmgtypId()));
			eDamagedItem.setAttribute("component", this.stringNullCheck(damageItem.getDmgitemComponent().getEqcmpId()));
			eDamagedItem.setAttribute("severity", this.stringNullCheck(damageItem.getDmgitemSeverity().getName()));
			eDamagedItem.setAttribute("date-reported", this.getXmlTimestamp(damageItem.getDmgitemReported()));
		}
	}

	private Double getUnitCargoWeight(Unit inUnit) {
		double grossWeight = inUnit.getUnitGoodsAndCtrWtKg().doubleValue();
		UnitEquipment ue = inUnit.getUnitPrimaryUe();
		double tareWeight = (ue == null) ? 0.0 : ue.getUeEquipment().getEqTareWeightKg().doubleValue();
		double cargoWeight = Math.max(0.0, grossWeight - tareWeight);
		cargoWeight = Math.round(cargoWeight*1)/1.0d;
		return new Double(cargoWeight);
	}

	/**
	 * build contents element
	 * @param inUnit
	 * @param eUnit
	 * @param goods
	 * @param sNS
	 */
	private void buildContentsElement(Unit inUnit, Element eUnit, GoodsBase goods, Namespace sNS) {

		Element eContents = new Element("contents", sNS);
		eUnit.addContent(eContents);
		eContents.addContent("");
		//It's cargo weight in kg that needs to report to RTO
		Double cargoWeight = this.getUnitCargoWeight(inUnit);
		String cargoWtStr = null;
		if (cargoWeight != null) {
			cargoWtStr = cargoWeight.toString();
		} else {
			cargoWtStr = new Double(0.0).toString();
		}
		eContents.setAttribute("gross-weight", this.doubleNullCheck(cargoWtStr));
		eContents.setAttribute("is-hazardous", this.booleanNullCheck(inUnit.getUnitIsHazard()));
		eContents.setAttribute("is-oog", this.booleanNullCheck(inUnit.getUnitIsOog()));
		eContents.setAttribute("requires-power", this.booleanNullCheck(inUnit.getUnitRequiresPower()));
		eContents.setAttribute("is-powered", this.booleanNullCheck(inUnit.getUnitIsPowered()));
		eContents.setAttribute("wants-power", this.booleanNullCheck(inUnit.getUnitWantPowered()));
		if (inUnit.getUnitInbond() != null) {
			eContents.setAttribute("bond", this.stringNullCheck(inUnit.getUnitInbond().getKey()));
		} else {
			eContents.setAttribute("bond", this.stringNullCheck(inUnit.getUnitInbond().toString()));
		}

		if (inUnit.getUnitExam() != null) {
			eContents.setAttribute("exam", this.stringNullCheck(inUnit.getUnitExam().getKey()));
		} else {
			eContents.setAttribute("exam", this.stringNullCheck(inUnit.getUnitExam().toString()));
		}

		//oog-left-cm element
		if (inUnit.getUnitOogLeftCm() != null) {
			Element eOGLeft = new Element("oog-left-cm", sNS);
			eContents.addContent(eOGLeft);
			eOGLeft.addContent("sn4:oog-left-cm");
			eOGLeft.setAttribute("value", this.doubleNullCheck(inUnit.getUnitOogLeftCm().toString()));
		}

		//oog-right-cm element
		if (inUnit.getUnitOogRightCm() != null) {
			Element eOGRight = new Element("oog-right-cm", sNS);
			eContents.addContent(eOGRight);
			eOGRight.addContent("sn4:oog-right-cm");
			eOGRight.setAttribute("value", this.doubleNullCheck(inUnit.getUnitOogRightCm().toString()));
		}

		//oog-top-cm element
		if (inUnit.getUnitOogTopCm() != null) {
			Element eOGTop = new Element("oog-top-cm", sNS);
			eContents.addContent(eOGTop);
			eOGTop.addContent("sn4:oog-top-cm");
			eOGTop.setAttribute("value", this.doubleNullCheck(inUnit.getUnitOogTopCm().toString()));
		}

		//oog-back-cm element
		if (inUnit.getUnitOogBackCm() != null) {
			Element eOGBack = new Element("oog-back-cm", sNS);
			eContents.addContent(eOGBack);
			eOGBack.addContent("sn4:oog-back-cm");
			eOGBack.setAttribute("value", this.doubleNullCheck(inUnit.getUnitOogBackCm().toString()));
		}

		//oog-front-cm element
		if (inUnit.getUnitOogFrontCm() != null) {
			Element eOGFront = new Element("oog-front-cm", sNS);
			eContents.addContent(eOGFront);
			eOGFront.addContent("sn4:oog-front-cm");
			eOGFront.setAttribute("value", this.doubleNullCheck(inUnit.getUnitOogFrontCm().toString()));
		}

		Commodity commodity = this.getCommodiyForUnit(inUnit)
		if (commodity) {
			//commodity element
			Element eCommodity = new Element("commodity", sNS);
			eContents.addContent(eCommodity);
			eCommodity.addContent("");
			eCommodity.setAttribute("gkey", this.longNullCheck(commodity.getCmdyGkey().toString()));
			eCommodity.setAttribute("id", this.stringNullCheck(commodity.getCmdyId()));
			eCommodity.setAttribute("short-name", this.stringNullCheck(commodity.getCmdyShortName()));
			eCommodity.setAttribute("one-character-code", this.stringNullCheck(commodity.getCmdyOneCharCode()));
		}

		List hazards = this.getHazardItemsForUnit(inUnit)
		if (hazards.size() != 0) {
			//hazards element
			Element eHazards = new Element("hazards", sNS);
			eContents.addContent(eHazards);
			eHazards.setAttribute("count", Integer.toString(hazards.size()));
			Iterator hazardsItr = hazards.iterator();
			while (hazardsItr.hasNext()) {
				HazardItem hazard = (HazardItem) hazardsItr.next();
				//hazard-item element
				Element eHazard = new Element("hazard-item", sNS);
				eHazards.addContent(eHazard);
				eHazard.addContent("");
				eHazard.setAttribute("gkey", this.longNullCheck(hazard.getHzrdiGkey().toString()));
				eHazard.setAttribute("imdg-class", this.stringNullCheck(hazard.getHzrdiImdgClass().getName()));
				eHazard.setAttribute("un-number", this.stringNullCheck(hazard.getHzrdiUNnum()));
			}
		}

		//Add fire-code element
		Element eFireCode = new Element("fire-code", sNS);
		eContents.addContent(eFireCode);
		if (goods != null && goods.getGdsHazards() != null &&
				goods.getGdsHazards().getHzrdWorstFireCode() != null) {
			eFireCode.setAttribute("code", this.stringNullCheck(goods.getGdsHazards().getHzrdWorstFireCode().getFirecodeFireCode()));
			eFireCode.setAttribute("class", this.stringNullCheck(goods.getGdsHazards().getHzrdWorstFireCode().getFirecodeFireCodeClass()));
		} else {
			eFireCode.setAttribute("code", "");
			eFireCode.setAttribute("class", "");
		}//end of adding contents element
	}

	/**
	 * return eq type
	 * @param inUnit
	 * @return
	 */
	private String getUnitEqType(Unit inUnit) {
		if (inUnit == null) {
			return null;
		}

		String eqType = null;
		if (inUnit.getUnitFreightKind().equals(FreightKindEnum.BBK)) {
			//Return BB is unit freight kind is break-bulk
			eqType = BB_CODE;
		} else if (inUnit.getPrimaryEq() != null &&
				inUnit.getPrimaryEq().getEqEquipType() != null) {
			eqType = inUnit.getPrimaryEq().getEqEquipType().getEqtypId();
		}

		return eqType;
	}

	/**
	 *  build primary Eq element
	 * @param inUnit
	 * @param eUnit
	 * @param sNS
	 */
	private void buildPrimaryEqElement(Unit inUnit, Element eUnit, Namespace sNS) {
		Element ePrimaryEq = new Element("primary-equipment", sNS);
		eUnit.addContent(ePrimaryEq);
		String eqType = this.getUnitEqType(inUnit);
		ePrimaryEq.setAttribute("equipment-type", this.stringNullCheck(eqType));
		ePrimaryEq.setAttribute("line-operator", this.stringNullCheck(inUnit.getUnitLineOperator().getBzuId()));
		UnitEquipment ue = inUnit.getUnitPrimaryUe();
		double tareWeight = (ue == null) ? 0.0 : ue.getUeEquipment().getEqTareWeightKg().doubleValue();
		ePrimaryEq.setAttribute("tare-weight", this.doubleNullCheck(tareWeight.toString()));
		//Adding seal elements
		if (inUnit.getUnitSealNbr1()) {
			//Seal-1 element
			Element eSeal1 = new Element("seal-number-1", sNS);
			ePrimaryEq.addContent(eSeal1);
			eSeal1.addContent("");
			eSeal1.setAttribute("value", inUnit.getUnitSealNbr1());
		}

		if (inUnit.getUnitSealNbr2()) {
			//Seal-2 element
			Element eSeal2 = new Element("seal-number-2", sNS);
			ePrimaryEq.addContent(eSeal2);
			eSeal2.addContent("");
			eSeal2.setAttribute("value", inUnit.getUnitSealNbr2());
		}

		if (inUnit.getUnitSealNbr3()) {
			//Seal-3 element
			Element eSeal3 = new Element("seal-number-3", sNS);
			ePrimaryEq.addContent(eSeal3);
			eSeal3.addContent("");
			eSeal3.setAttribute("value", inUnit.getUnitSealNbr3());
		}

		if (inUnit.getUnitSealNbr4()) {
			//Seal-4 element
			Element eSeal4 = new Element("seal-number-4", sNS);
			ePrimaryEq.addContent(eSeal4);
			eSeal4.addContent("");
			eSeal4.setAttribute("value", inUnit.getUnitSealNbr4());
		} //end of adding primary-equipment element
	}

	/**
	 *  build inbound element
	 * @param inUnit
	 * @param ufv
	 * @param eRouting
	 * @param routing
	 * @param goodsBase
	 * @param sNS
	 */
	private void buildInboundElement(Unit inUnit, UnitFacilityVisit ufv, Element eRouting,
									 Routing routing, GoodsBase goodsBase, Namespace sNS) {
		Element eInbound = new Element("inbound", sNS);
		eRouting.addContent(eInbound);
		CarrierVisit inboundDeclaredIbCv = null;
		if (inUnit != null) {
			inboundDeclaredIbCv = inUnit.getUnitDeclaredIbCv();
		}
		CarrierVisit inboundActualIbCv = null;
		if (ufv != null) {
			inboundActualIbCv = ufv.getUfvActualIbCv();
		}

		String inboundOrder = null;
		//String inboundFacilityId = null;
		if (inboundActualIbCv != null || inboundDeclaredIbCv != null) {
			// CSDV-1144: dickspa 10/10/2013: Check for both Export and Domestic categories, rather than just Export. Fix issue where a Unit leaving
			// on a Barge is reported as Domestic and so the Order is not populated.
		  //CSDV-2819: Get ERO for inbound EXPORT/DOMESTIC and STORAGE category.
			if ((this.isExportOrDomestic(ufv) || (UnitCategoryEnum.STORAGE.equals(inUnit.getUnitCategory()))) &&
					this.isLandSideModality(inboundActualIbCv, inboundDeclaredIbCv)) {
				//For export the inbound order is the booking nbr and blank if it is an import.
				//It just apply to landside arrival
				//inboundOrder = this.getUnitEqboNbr(inUnit);
				inboundOrder = this.getUnitInboundOrder(inUnit);
			}
		}
		eInbound.setAttribute("order", this.stringNullCheck(inboundOrder));
		//Get the inbound facility-id based on the point terminal of POL
		String fromTerminal = this.getRoutingPointTerminalId(routing, "inbound");
		String fcyId = ContextHelper.getThreadFacility() ? this.stringNullCheck(ContextHelper.getThreadFacility().getFcyId()) : "";
		if (StringUtils.isBlank(fcyId)) {
			fcyId = this.getFacilityIdFromGeneralReference();
		}
		log("Facility:$fcyId + fromTerminal:$fromTerminal");
		if ( fromTerminal != null) {
			eInbound.setAttribute("facility-id", this.stringNullCheck(fromTerminal));
			log("Found fromTerminal based on POD = " + fromTerminal);
		} else {
			eInbound.setAttribute("facility-id", this.stringNullCheck(fcyId));
			log("Found fromTerminal from Current Facility Id = " + fcyId) ;
		}
		//Adding inbound-declared-carrier-visit element
		if (inboundDeclaredIbCv != null) {
			Element eInboundDecCv = new Element("inbound-declared-carrier-visit", sNS);
			eInbound.addContent(eInboundDecCv);
			eInboundDecCv.addContent("");
			eInboundDecCv.setAttribute("id", this.stringNullCheck(inboundDeclaredIbCv.getCvId()));
			String modeValue = this.getModeValue(inboundDeclaredIbCv);
			eInboundDecCv.setAttribute("mode", this.stringNullCheck(modeValue));
			eInboundDecCv.setAttribute("phase", this.stringNullCheck(inboundDeclaredIbCv.getCvVisitPhase().getKey().substring(2)));
		}

		//Adding inbound-actual-carrier-visit element
		if (inboundActualIbCv != null) {
			Element eInboundActCv = new Element("inbound-actual-carrier-visit", sNS);
			eInbound.addContent(eInboundActCv);
			eInboundActCv.addContent("");
			//For truck - truck license plate. For Vessel & Barge vessel ID, for train, it is train ID
			String cvId = this.getInboundActualCvId(inboundDeclaredIbCv, inboundActualIbCv, inUnit, ufv);
			eInboundActCv.setAttribute("id", this.stringNullCheck(cvId));
			String modeValue = this.getModeValue(inboundActualIbCv);
			eInboundActCv.setAttribute("mode", this.stringNullCheck(modeValue));
			//Vessel and barge is the Full name of carrier. For truck is the license plate number, for train, is the train id
			String carrierName = this.getInboundActualCarrierName(inboundDeclaredIbCv, inboundActualIbCv, inUnit, ufv);
			eInboundActCv.setAttribute("carriername", this.stringNullCheck(carrierName));
			String callSign = this.getInboundOrOutboundCallSign(inboundActualIbCv, inUnit);
			//this.log("Inbound Call Sign = " + callSign);
			eInboundActCv.setAttribute("callsign", this.stringNullCheck(callSign));
			String carrierOpr = this.getInboundOrOutboundActualCarrierOperator(inboundActualIbCv, inUnit);
			//this.log("Inbound carrieroperator = " + carrierOpr);
			eInboundActCv.setAttribute("carrieroperator", this.stringNullCheck(carrierOpr));
			//For truck it's license nbr, for vessel and barge, it's voyage nbr, for train, it's train Id.
			// CSDV-1622: dickspa 10/02/2014: Functionality stays the same, but changed the method name so
			// the inbound voyage number is returned.
			String inboundVoyage = this.getInboundActualVoyage(inboundActualIbCv);
			eInboundActCv.setAttribute("inboundvoyage", this.stringNullCheck(inboundVoyage));

			//Getting CRN from vvFlexString01 field
			String crnValue = this.getCrnValue(inboundActualIbCv);
			eInboundActCv.setAttribute("crn", this.stringNullCheck(crnValue));
			eInboundActCv.setAttribute("phase", "Actual"); //Always be "Actual" is actual carrier visit is set
			String etdString = this.getXmlTimestamp(inboundActualIbCv.getCvCvd() ? inboundActualIbCv.getCvCvd().getCvdETD() : null);
			if (StringUtils.isNotBlank(etdString)) {
				//if (etdString != null) {
				eInboundActCv.setAttribute("etd", etdString);
			}
			//Add EAN attributes
			//Landside operator EAN number  (truck, rail, barge)
			String receiverEanNumber = null;
			if (this.isLandSideModality(inboundActualIbCv, inboundDeclaredIbCv)) {
				receiverEanNumber = this.getInAndOutLandSideReceiverEanNumber(inboundActualIbCv, inUnit);
			}
			eInboundActCv.setAttribute("receiverEanNumber", this.stringNullCheck(receiverEanNumber));
			//EAN number box operator (container operator id)
			String cntrOperatorId = this.getContainerOperatorId(inUnit);
			eInboundActCv.setAttribute("receiverCargoHandlingAgent", this.stringNullCheck(cntrOperatorId));
		}

		//Adding POL
		RoutingPoint routingPol = routing.getRtgPOL();
		if (routingPol != null) {
			Element ePol = new Element("pol", sNS);
			eInbound.addContent(ePol);
			ePol.addContent("");
			//ePol.setAttribute("value", this.stringNullCheck(routingPol.getPointId()));
			ePol.setAttribute("value", this.stringNullCheck(routingPol.getPointUnlocId()));
		}
		//Adding OPL
		RoutingPoint routingOpl = routing.getRtgOPL();
		if (routingOpl != null) {
			Element eOpl = new Element("opl", sNS);
			eInbound.addContent(eOpl);
			eOpl.addContent("");
			//eOpl.setAttribute("value", this.stringNullCheck(routingOpl.getPointId()));
			eOpl.setAttribute("value", this.stringNullCheck(routingOpl.getPointUnlocId()));
		}
		//Adding origin
		if (goodsBase != null) {
			Element eOrigin = new Element("origin", sNS);
			eInbound.addContent(eOrigin);
			eOrigin.addContent("");
			eOrigin.setAttribute("value", this.stringNullCheck(goodsBase.getGdsOrigin()));
		}
	}

	/**
	 * This is only applied to landside modalities and return operator Id if found. return blank for vessel.
	 * @param actualCv
	 * @param inUnit
	 * @return
	 */
	private String getInAndOutLandSideReceiverEanNumber(CarrierVisit actualCv, Unit inUnit) {
		if (inUnit == null || actualCv == null) {
			return null;
		}

		//this.log("getInAndOutLandSideReceiverEanNumber=");
		String eanNumberStr = null;
		if (GEN_BARGE.equalsIgnoreCase(actualCv.getCvId()) ||
				(actualCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL) &&
						((actualCv.getCvClassification() != null &&
								actualCv.getCvClassification().equals(VesselClassificationEnum.BARGE)) ||
								(actualCv.getCarrierVesselClassType() != null &&
										VesselTypeEnum.BARGE.equals(actualCv.getCarrierVesselClassType()))))) {
			if (actualCv.getCarrierOperator() != null) {
				//For barge or a dummy barge viswit terminal defined, return the line operator Id
				eanNumberStr = actualCv.getCarrierOperator().getBzuId();
				//this.log("Barge eanNumberStr= " + eanNumberStr);
			} //blank value for vessel
		} else if (actualCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN) ||
				actualCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR)) {
			//For train it is the ailroad operator Id
			TrainVisitDetails trv = TrainVisitDetails.resolveTvdFromCv(actualCv);
			if (trv != null) {
				String trainVisitId = trv.getRvdtlsId();
				Railroad railRoad = trv.getRvdtlsRR();
				//this.log("trainVisitId= " + trainVisitId);
				//if (trv.getRvdtlsBizu() != null) {
				//  this.log("railroad Id= " + trv.getRvdtlsBizu().getBzuId());
				//}
				//Railroad railroad = Railroad.findRailroadById(railRoadId);
				if (railRoad != null) {
					eanNumberStr = railRoad.getBzuId();
					//this.log("eanNumberStr.1= " + eanNumberStr);
				}
			}
		} else if (actualCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
			// CSDV-1143: dickspa 26/09/2013: Find the most recent Gate Transaction for the Unit and Truck.
			TruckTransaction tran = this.findMostRecentTransactionByUnitAndTruck(inUnit, actualCv.getCarrierVehicleId());
			if (tran != null && tran.getTranTruckVisit() != null &&
					tran.getTranTruckVisit().getTvdtlsTrkCompany() != null) {
				//For truck, return the name of trucking company Id
				eanNumberStr = tran.getTranTruckVisit().getTvdtlsTrkCompany().getBzuId();
			}
		}

		//this.log("eanNumberStr.2= " + eanNumberStr);
		return eanNumberStr;
	}

	/**
	 * Return container operator id if the SCSC code is not defined
	 * @param inUnit
	 * @return
	 */
	private String getContainerOperatorId(Unit inUnit) {

		if (inUnit == null) {
			return null;
		}

		String containerOprId = null;
		if (inUnit.getUnitLineOperator() != null) {
			containerOprId = inUnit.getUnitLineOperator().getBzuScac();
			if (StringUtils.isBlank(containerOprId)) {
				//If SCAC code is not defined, return the operator Id
				containerOprId = inUnit.getUnitLineOperator().getBzuId();
			}
		}
		//this.log("containerOprId= " + containerOprId);

		return containerOprId;
	}

	/**
	 * Return the CRN that is supposed to store in VvFlexString01 field
	 * @param theActualCv
	 * @return
	 */
	private String getCrnValue(CarrierVisit theActualCv) {
		//The CRN is only meant for Vessel only, not barge
		//Now include TRAIN requested in CSDV-1108 and the value should be populated
		//from cvdCv.cvCustomsId
		if (theActualCv == null) {
			this.log("getCrnValue= null ");
			return null;
		}

		VesselVisitDetails vvd = null;
		String crnValue = null;
		boolean getValueFromCvCustomsId = false;
		if (theActualCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL)) {
			//If vessel classification is not defined, it's not barge by default
			if (theActualCv.getCvClassification() == null ||
					!VesselClassificationEnum.BARGE.equals(theActualCv.getCvClassification())) {
				vvd = VesselVisitDetails.resolveVvdFromCv(theActualCv);
				if (vvd != null) {
					//Getting CRN from vvFlexString01 field
					crnValue = vvd.getVvFlexString01();
				}
			}
		} else if ((theActualCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR) ||
				theActualCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN))) {
			getValueFromCvCustomsId = true;
		}

		//CSDV-1108 for VESSEL and TRAIN, get this value from cvCustomsId field
		if (getValueFromCvCustomsId) {
			crnValue = theActualCv.getCvCustomsId();
		}

		return crnValue;
	}

	/**
	 * Return transport mode accepted by RTO
	 * @param inCv
	 * @return
	 */
	private String getModeValue(CarrierVisit inCv) {
		if (inCv == null) {
			return null;
		}

		VesselVisitDetails vvd = null;
		String modeValue = "VS";
		if (GEN_BARGE.equalsIgnoreCase(inCv.getCvId())) {
			//This is a dummy barge visit set up in terminal
			modeValue = "BG";
		} else if (inCv.getCvCarrierMode().equals(LocTypeEnum.VESSEL)) {
			if ((inCv.getCvClassification() != null &&
					VesselClassificationEnum.BARGE.equals(inCv.getCvClassification())) ||
					(inCv.getCarrierVesselClassType() != null &&
							VesselTypeEnum.BARGE.equals(inCv.getCarrierVesselClassType()))) {
				modeValue = "BG";
			} else {
				//If vessel classification is not defined, it is not a barge by default
				modeValue = "VS";
			}
		} else if (inCv.getCvCarrierMode().equals(LocTypeEnum.TRUCK)) {
			modeValue = "TR";
		} else if (inCv.getCvCarrierMode().equals(LocTypeEnum.RAILCAR) ||
				inCv.getCvCarrierMode().equals(LocTypeEnum.TRAIN)) {
			modeValue = "RL";
		}


		return modeValue;
	}

	/**
	 * build outbound element
	 * @param inUnit
	 * @param ufv
	 * @param eRouting
	 * @param routing
	 * @param goodsBase
	 * @param sNS
	 */
	private void buildOutboundElement(Unit inUnit, UnitFacilityVisit ufv, Element eRouting,
									  Routing routing, GoodsBase goodsBase, Namespace sNS) {
		Element eOutbound = new Element("outbound", sNS);
		eRouting.addContent(eOutbound);
		CarrierVisit outboundActualObCv = null;
		if (ufv != null) {
			outboundActualObCv = ufv.getUfvActualObCv();
		}

		CarrierVisit outboundIntendedObCv = null;
		if (inUnit.getUnitRouting() != null) {
			outboundIntendedObCv = inUnit.getUnitRouting().getRtgDeclaredCv();
		}
		if (outboundIntendedObCv == null) {
			outboundIntendedObCv = ufv.getUfvIntendedObCv();
		}
		String outboundOrder = null;
		if (outboundIntendedObCv != null || outboundActualObCv != null) {
			if (this.isLandSideModality(outboundActualObCv, outboundIntendedObCv)) {
				if (this.pinOrBookingExists(inUnit)) {
					outboundOrder = "Released by shipping line";
				}
			}
		}
		eOutbound.setAttribute("order", this.stringNullCheck(outboundOrder));
		//Get the outbound facility-id based on the point terminal of POD1
		String toTerminal = this.getRoutingPointTerminalId(routing, "outbound");
		log("toTerminal:$toTerminal");
		eOutbound.setAttribute("facility-id", this.stringNullCheck(toTerminal));
		//Adding outbound-intended-carrier-visit element
		if (outboundIntendedObCv != null) {
			Element eOutboundIntCv = new Element("outbound-intended-carrier-visit", sNS);
			eOutbound.addContent(eOutboundIntCv);
			eOutboundIntCv.addContent("");
			eOutboundIntCv.setAttribute("id", this.stringNullCheck(outboundIntendedObCv.getCvId()));
			String modeValue = this.getModeValue(outboundIntendedObCv);
			eOutboundIntCv.setAttribute("mode", this.stringNullCheck(modeValue));
			eOutboundIntCv.setAttribute("phase", this.stringNullCheck(outboundIntendedObCv.getCvVisitPhase().getKey().substring(2)));
		}
		//Adding outbound-actual-carrier-visit element
		if (outboundActualObCv != null) {
			Element eOutboundActCv = new Element("outbound-actual-carrier-visit", sNS);
			eOutbound.addContent(eOutboundActCv);
			eOutboundActCv.addContent("");
			//For truck - truck license plate. For Vessel & Barge vessel ID, for train, it is train ID
			String cvId = this.getOutboundActualCvId(outboundIntendedObCv, outboundActualObCv, inUnit, ufv);
			eOutboundActCv.setAttribute("id", this.stringNullCheck(cvId));
			//eOutboundActCv.setAttribute("id", this.stringNullCheck(outboundActualObCv.getCvId()));
			String modeValue = this.getModeValue(outboundActualObCv);
			eOutboundActCv.setAttribute("mode", this.stringNullCheck(modeValue));
			//Vessel and barge is the Full name of carrier. But it is not needed for truck and train.
			String carrierName = this.getOutboundActualCarrierName(outboundIntendedObCv, outboundActualObCv, inUnit, ufv);
			eOutboundActCv.setAttribute("carriername", this.stringNullCheck(carrierName));
			String callSign = this.getInboundOrOutboundCallSign(outboundActualObCv, inUnit);
			//this.log("Outbound Call Sign = " + callSign);
			eOutboundActCv.setAttribute("callsign", this.stringNullCheck(callSign));
			String carrierOpr = this.getInboundOrOutboundActualCarrierOperator(outboundActualObCv, inUnit);
			eOutboundActCv.setAttribute("carrieroperator", this.stringNullCheck(carrierOpr));
			//For truck it's license nbr, for vessel and barge, it's voyage nbr, for train, it's train Id.
			// CSDV-1622: dickspa 10/02/2014: For outbound, get specifically the outbound voyage number
			String outboundVoyage = this.getOutboundActualVoyage(outboundActualObCv);
			eOutboundActCv.setAttribute("outboundvoyage", this.stringNullCheck(outboundVoyage));
			//Getting CRN from vvFlexString01 field
			String crnValue = this.getCrnValue(outboundActualObCv);
			eOutboundActCv.setAttribute("crn", this.stringNullCheck(crnValue));
			eOutboundActCv.setAttribute("phase", "Actual"); //Always be "Actual" is actual carrier visit is set
			String etdString = this.getXmlTimestamp(outboundActualObCv.getCvCvd() ? outboundActualObCv.getCvCvd().getCvdETD() : null);
			if (StringUtils.isNotBlank(etdString)) {
				eOutboundActCv.setAttribute("etd", etdString);
			}

			//Add EAN attributes
			//Landside operator EAN number  (truck, rail, barge)
			String receiverEanNumber = null;
			if (this.isLandSideModality(outboundActualObCv, outboundIntendedObCv)) {
				receiverEanNumber = this.getInAndOutLandSideReceiverEanNumber(outboundActualObCv, inUnit);
			}
			eOutboundActCv.setAttribute("receiverEanNumber", this.stringNullCheck(receiverEanNumber));
			//EAN number box operator (container operator id)
			String cntrOperatorId = this.getContainerOperatorId(inUnit);
			eOutboundActCv.setAttribute("receiverCargoHandlingAgent", this.stringNullCheck(cntrOperatorId));
		} else {
			this.log("Actual outbound Cv is null");
		}
		//Adding pod element
		//Getting the pod from the last port of discharge field as requested by customer
		String pod = this.getRoutingPodUnLocId(routing);
		String portName = null;
		String countryName = null;
		boolean EUport = false;
		//this.log("pod= " + pod);
		if (StringUtils.isNotBlank(pod)) { //pod is 3 characters pod
			UnLocCode unLocCode = UnLocCode.findUnLocCode(pod);
			if (unLocCode != null) { //5 characters unLocCode
				//this.log("unLocCode= " + unLocCode);
				portName = unLocCode.getUnlocPlaceName();
				if (unLocCode.getUnlocCntry() != null) {
					countryName = unLocCode.getUnlocCntry().getCntryName();
					//this.log("countryName= " + countryName);
				}
				String stateCode = StringUtils.substring(pod, 0, 2); // 2 characters country code
				//this.log("stateCode= " + stateCode);
				//this.log("EUport1= " + EUport);
				if (StringUtils.isNotBlank(stateCode)) {
					EUport = this.checkForEUPort(stateCode);
					//Need to insert EUport to a unit flex string field?
				}
			}
		}
		//this.log("EUport2= " + EUport);
		Element ePod = new Element("pod", sNS);
		eOutbound.addContent(ePod);
		ePod.addContent("");
		XmlUtil.setOptionalAttribute(ePod, "value", pod, null);
		//this.log("Before calling booleanNullCheck");
		//this.log("Call to booleanNullCheck= " + this.booleanNullCheck(EUport));
		ePod.setAttribute("EUport", this.booleanNullCheck(EUport));
		XmlUtil.setOptionalAttribute(ePod, "portName", portName, null);
		XmlUtil.setOptionalAttribute(ePod, "countryName", countryName, null);

		//Adding pod-1
		String pod1 = null;
		if (routing.getRtgPOD1() != null) {
			pod1 = routing.getRtgPOD1().getPointUnlocId();
		}
		Element ePod1 = new Element("pod-1", sNS);
		eOutbound.addContent(ePod1);
		ePod1.addContent("");
		if (pod1 != null) {
			ePod1.setAttribute("value", this.stringNullCheck(pod1));
		}

		//Adding pod-2
		String pod2 = null;
		if (routing.getRtgPOD2() != null) {
			pod2 = routing.getRtgPOD2().getPointUnlocId();
		}
		if (StringUtils.isNotBlank(pod2)) {
			Element ePod2 = new Element("pod-2", sNS);
			eOutbound.addContent(ePod2);
			ePod2.addContent("");
			ePod2.setAttribute("value", this.stringNullCheck(pod2));
		}

		//Adding destination
		if (goodsBase != null) {
			Element eDest = new Element("destination", sNS);
			eOutbound.addContent(eDest);
			eDest.addContent("");
			eDest.setAttribute("value", this.stringNullCheck(goodsBase.getGdsDestination()));
		} //finish adding outbound element
	}

	/**
	 * return true indicating outbound order status if pin or booking exists
	 * @param inUnit
	 * @return
	 */
	private boolean pinOrBookingExists(Unit inUnit) {
		if (inUnit == null) {
			return false;
		}
		boolean exists = false;
		String pinNbr = null;
		if (inUnit.getUnitFreightKind().equals(FreightKindEnum.MTY)) {
			//For empty, check if it has booking or EDO to determine whether it's released
			if (inUnit.getUnitPrimaryUe() != null &&
					inUnit.getUnitPrimaryUe().getUeDepartureOrderItem() != null) {
				EqBaseOrder eqBaseOrder = inUnit.getUnitPrimaryUe().getUeDepartureOrderItem().getEqboiOrder();
				EquipmentOrder equipmentOrder = EquipmentOrder.resolveEqoFromEqbo(eqBaseOrder);
				if (equipmentOrder != null) {
					exists = (StringUtils.isNotBlank(equipmentOrder.getEqboNbr()));
				}
			}
		} else {
			//For full, check if it has PIN or IDO to determine whether is's released
			if (inUnit.getUnitRouting() != null) {
				//Check PIN first
				pinNbr = inUnit.getUnitRouting().getRtgPinNbr();
				exists = (pinNbr != null);
			}

			if (!exists && inUnit.getUnitImportDeliveryOrder() != null) {
				//Check IDO nbr
				exists = StringUtils.isNotBlank(inUnit.getUnitImportDeliveryOrder().getIdoId());
			}
		}

		return exists;
	}

	/**
	 * Evaluae EUport
	 * @param stateCode
	 * @return
	 */
	private boolean checkForEUPort(String stateCode) {
		//this.log("stateCode= " + stateCode);
		boolean isEUport = false;
		if (StringUtils.isBlank(stateCode)) {
			return false;
		}
		//Populate country object for EU. The states defined under EU will be used to verify whether
		//the POD is an EUport by comparing the country code of POD against the state defined in EU
		RefCountry refCountry = RefCountry.findCountry(EU_COUNTRY_CODE);
		if (refCountry != null) {
			Set<RefState> stateList = refCountry.getStateList();
			if (stateList != null && stateList.size() > 0) {
				for (Object aSt : stateList) {
					RefState state = (RefState) aSt;
					//this.log("The stateCode= " + stateCode);
					//this.log("State Code= " + state.getStateCode());
					if (StringUtils.equals(state.getStateCode(), stateCode)) {
						//this.log("stateCode= " + stateCode+" equals to " + state.getStateCode());
						isEUport = true;
						break;
					}
				}
			}
		}

		return isEUport;
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
			for (Iterator iterator = ufvSet.iterator(); iterator.hasNext(); ) {
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

	/**
	 *
	 * @param inStr
	 * @return
	 */
	private String getNotNullValue(String inStr) {
		return inStr != null ? inStr : StringUtils.EMPTY;
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

	private final String EU_COUNTRY_CODE = "EU";
	private final String GEN_BARGE = "GEN_BARGE";
	private final String BB_CODE = "BB";
	private final String I_CATEGORY = "I";
	private final String E_CATEGORY = "E";
	private final String D_CATEGORY = "D";
	private final String R_CATEGORY = "R";
	private final String T_CATEGORY = "T";
	private final String P_CATEGORY = "P";
	private final String EVENT_UNIT_ACTIVATE = "UNIT_ACTIVATE";    //1
	private final String EVENT_UNIT_DISCH = "UNIT_DISCH";        //2
	private final String EVENT_UNIT_DERAMP = "UNIT_DERAMP";      //3
	private final String EVENT_UNIT_RECEIVE = "UNIT_RECEIVE";    //4
	private final String EVENT_UNIT_LOAD = "UNIT_LOAD";          //5
	private final String EVENT_UNIT_RAMP = "UNIT_RAMP";          //6
	private final String EVENT_UNIT_OUT_GATE = "UNIT_OUT_GATE";    //7
	private final String EVENT_UNIT_CREATE = "UNIT_CREATE";       //8
	private final String EVENT_UNIT_STUFF = "UNIT_STUFF";         //9
	private final String EVENT_UNIT_STRIP = "UNIT_STRIP";          //10
	private final String EVENT_UNIT_RENUMBER = "UNIT_RENUMBER";      //11
	private final String EVENT_CONTAINER_MOVE = "CONTAINER_MOVE";         //12
	private final String EVENT_CANCELLED_ARRIVAL = "CANCELLED_ARRIVAL";  //13
	private final String EVENT_CANCELLED_DEPARTURE = "CANCELLED_DEPARTURE";    //14
	private final String EVENT_UNIT_OPERATOR_CHANGE = "UNIT_OPERATOR_CHANGE";  //15
	private final String EVENT_UNIT_HAZARDS_INSERT = "UNIT_HAZARDS_INSERT";  //16
	private final String EVENT_UNIT_HAZARDS_UPDATE = "UNIT_HAZARDS_UPDATE";          //17
	private final String EVENT_UNIT_HAZARDS_DELETE = "UNIT_HAZARDS_DELETE";      //18
	private final String EVENT_FORCED_REPAIR = "FORCED_REPAIR";         //19
	private final String EVENT_UNIT_RECTIFY = "UNIT_RECTIFY";  //20
	private final String EVENT_UNIT_SEAL = "UNIT_SEAL";    //21
	private final String EVENT_UNIT_REROUTE = "UNIT_REROUTE";  //22
	private final String EVENT_UNIT_PROPERTY_UPDATE = "UNIT_PROPERTY_UPDATE";  //23
	private final String EVENT_UNIT_ROLL = "UNIT_ROLL";  //24
	private final String EVENT_DRAY_OUT_SET_RTO_CATEGORY = "DRAY_OUT_SET_RTO_CATEGORY";
														//25 CSDV-2803
	private final List<String> POD_LIST = new ArrayList<String>();
	private final List<String> SEAL_LIST = new ArrayList<String>();
	private final List<String> OUTBOUND_CV_LIST = new ArrayList<String>();
	private final List<String> INBOUND_CV_LIST = new ArrayList<String>();
	private final List<String> EVENT_LIST = new ArrayList<String>();
	private final List<String> IN_TIME_LIST = new ArrayList<String>();
	private final List<String> GENERIC_PROPERTY_LIST = new ArrayList<String>();
	private final List<String> FLEX_STRING_FIELDS_LIST = new ArrayList<String>();
	private final List<String> FREIGHT_KIND_LIST = new ArrayList<String>();
	// CSDV-1121: dickspa 23/08/2013: Create the lookup of known field names that require mapping to an RTO value
	private final Map<String, String> fieldChangeFieldNameTranslations = new HashMap<String, String>();
	// CSDV-1121: dickspa 27/08/2013: Create a List of the EventFieldChanges that are for a Routing Point for use in
	// determining the latest POD. More information on CSDV-1121.
	private final List<EventFieldChange> routingPointFieldChanges = new ArrayList<EventFieldChange>();
	// CSDV-1121: dickspa 27/08/2013: Make the list of all known Routing Point PODs which need to be stored in the above List.
	private final List<String> requiredRoutingPointsForPODFieldChangeElement = new ArrayList<String>();
	private final String RTO_CATEGORY_CALCULATION_CLASS = "RTOCalculateRTOCategory";
	private final String ACTIVITY_MESSAGE_GR = "ACTIVITY_MESSAGE";
	private final String FACILITY_ID1_GR = "FACILITY_ID";
	// CSDV-1356: dickspa 03/10/2013: The Value-1 field of the Gen Ref entry that defines the Facility for this Unit.
	private final MetafieldId GEN_REF_FACILITY_FIELD = ArgoField.REF_VALUE1;
}

/**
 * CSDV-1121: dickspa 03/09/2013: Sort the EventFieldChange's metafield ID field into reverse order as defined:
 * - rtgOPT3
 * - rtgOPT2
 * - rtgOPT1
 * - rtgPOD2
 * - rtgPOD1
 */
public class RTOPODListComparator implements Comparator<EventFieldChange> {

	/**
	 * Returns a negative integer,
	 * zero, or a positive integer as the first argument is less than, equal
	 * to, or greater than the second.
	 * @param first
	 * @param second
	 * @return
	 */
	@NotNull
	public int compare(@NotNull final EventFieldChange first, @NotNull final EventFieldChange second) {
		// Want to compare the fields, so get these values
		final String firstField = first.getEvntfcMetafieldId();
		final String secondField = second.getEvntfcMetafieldId();

		// They should never be null, and also should never equal each other, since the same field
		// cannot be altered twice
		// This means that if firstField is OPT3, it is always going to be the greatest
		if (StringUtils.equals(firstField, OPT3)) {
			return FIRST_IS_GREATER;
		}

		// Else if it is OPT2, it is the greatest unless Value 2 is OPT3
		else if (StringUtils.equals(firstField, OPT2)) {
			if (!StringUtils.equals(secondField, OPT3)) {
				return FIRST_IS_GREATER;
			}
			else {
				return SECOND_IS_GREATER;
			}
		}

		// Else if first is OPT1, it is the greatest unless Value 2 is OPT2 or OPT3
		else if (StringUtils.equals(firstField, OPT1)) {
			if (!StringUtils.equals(secondField, OPT2) && !StringUtils.equals(secondField, OPT3)) {
				return FIRST_IS_GREATER;
			}
			else {
				return SECOND_IS_GREATER;
			}
		}

		// Now the first is POD2, the second lowest. We can now say that if the second is anything other than POD1,
		// Second is greater
		else if (StringUtils.equals(firstField, POD2)) {
			if (!StringUtils.equals(secondField, POD1)) {
				return SECOND_IS_GREATER;
			}
			else {
				return FIRST_IS_GREATER;
			}
		}

		// Finally, if the first field is POD1, then we can assume that second is greater
		else if (StringUtils.equals(firstField, POD1)) {
			return SECOND_IS_GREATER;
		}

		// Error case. just return 0
		else {
			return 0;
		}
	}

	private final String OPT3 = "rtgOPT3";
	private final String OPT2 = "rtgOPT2";
	private final String OPT1 = "rtgOPT1";
	private final String POD2 = "rtgPOD2";
	private final String POD1 = "rtgPOD1";

	private final int FIRST_IS_GREATER = -1;
	private final int SECOND_IS_GREATER = 1;

}