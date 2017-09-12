/*
 * Copyright (c) 2013 Navis LLC. All Rights Reserved.
 *
 * Date 25/06/14 CSDV-2152 use GateAppointment dynamic flex fields instead of gapptUfv and gapptUnit flex fields for prean specific values
 *
 * 11/Feb/2015 CSDV-2731 added NOK_CANCEL_DELAY
 */


import com.navis.external.framework.AbstractExtensionCallback;
import com.navis.framework.metafields.MetafieldId;
import com.navis.framework.metafields.MetafieldIdFactory;
import com.navis.inventory.business.api.UnitField;
import com.navis.road.RoadApptsField;

public class PANFields extends AbstractExtensionCallback {

	//Preannouncement Fields

	public static MetafieldId PREAN_LANDSIDE_CARRIER_VISIT =  MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFlandsideCV");

	public static MetafieldId PREAN_PIN =  RoadApptsField.GAPPT_IMPORT_RELEASE_NBR;

	public static MetafieldId EDI_TRANS_REF_NBR =  RoadApptsField.GAPPT_REFERENCE_NBR;

	public static MetafieldId PREAN_STATUS = MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFpreanStatus");

	public static MetafieldId PREAN_EQO_NBR  = MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFpreanEqoNbr");

	public static MetafieldId RESPONSE_MSG_TYPE =  MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFresponseMsgType");   //Values: REJECT/STATUS_UPDATE/null

	public static MetafieldId EDI_TRAN_GKEY =  MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFediTranGkey");

	public static MetafieldId PREAN_VALIDATION_RUN_ID =  MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFvalidationRunId");          //Helper Field

	public static MetafieldId EDI_ORIGINAL_REQUEST_DATETIME = MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFediOrigRequestDate");

	public static MetafieldId PREAN_LAST_VALIDATION_DATE   = MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFpreanValidationDate");  //Helper Field

	public static MetafieldId SEND_MSG   = MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFsendMsg");

	public static MetafieldId EDI_PARTNER_NAME   = MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFediPartnerName");

	private  static MetafieldId NOK_CANCEL_DELAY = RoadApptsField.GAPPT_CHS_IS_OWNERS;

  //Unit flex fields
	public static MetafieldId UFV_PREAN_RECEIVAL_STATUS = UnitField.UFV_FLEX_STRING01;
	public static MetafieldId UFV_PREAN_DELIVERY_STATUS = UnitField.UFV_FLEX_STRING02;


}