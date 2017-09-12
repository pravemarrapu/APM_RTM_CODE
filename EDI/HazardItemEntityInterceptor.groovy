/*
 **
 Author: Allen Hsieh
 Date: 2012.09.13
 
 LAST MODIFIED by Amos Parlante: November 25, 2015. Added code for CSDV-3133. See CSDV for details.
 
 FDR link: http://confluence.navis.com/display/FD/FDR+Gap+OG01+Hazard+Validations
 CSDV-575: For unit hazards, regardless of how the information enters the system:
   1) Require UN code with IMDG class (both fields mandatory).
   2) Validate combination of UN code and IMDG class against N4 hazard reference data.
 
 This groovy script will be deployed in Code Extensions view of ENTITY_LIFECYCLE_INTERCEPTOR type
 and triggered by persistent update on HazardItem.
 
 Installation Instructions:
  Load Code Extension to N4:
		 1. Go to Administration --> System --> Code Extensions
		 2. Click Add (+)
		 3. Enter the values as below:
			 Code Extension Name:  ValidateHazardItemForUNNumberIMDGClass
			 Groovy Code: Copy and paste the contents of groovy code.
		 4. Click Save button
 
  Attach code extension to Extension Trigger View:
		 1. Go to Administration-->System-->Extension Triggers
		 2. Select ENTITY_LIFECYCLE_INTERCEPTION in Extension Type tab
		 3. Select the extension in "Extensions" tab
		 5. Click on save
 
  CSDV-3133 (Mantis 4393 / 4509) - We set the ufvFlexString10 with YES if the UFV must be grounded because of special rules definined on the Hazardous
  goods.
 
 *
 */
 
 import com.navis.argo.ContextHelper
 import com.navis.argo.business.api.ArgoUtils
 import com.navis.external.framework.entity.AbstractEntityLifecycleInterceptor
 import com.navis.external.framework.entity.EEntityView
 import com.navis.external.framework.util.EFieldChanges
 import com.navis.external.framework.util.EFieldChangesView
 import com.navis.framework.metafields.MetafieldId
 import com.navis.framework.metafields.MetafieldIdFactory
 import com.navis.framework.persistence.HibernateApi
 import com.navis.framework.portal.QueryUtils
 import com.navis.framework.portal.query.DomainQuery
 import com.navis.framework.portal.query.PredicateFactory
 import com.navis.framework.util.BizFailure
 import com.navis.inventory.InventoryEntity
 import com.navis.inventory.InventoryField
 import com.navis.inventory.business.imdg.HazardItem
 import com.navis.inventory.business.imdg.HazardousGoods
 import com.navis.inventory.business.imdg.Hazards
 import com.navis.inventory.business.imdg.ImdgClass
 import com.navis.inventory.business.units.GoodsBase
 import com.navis.inventory.business.units.Unit
 import com.navis.inventory.web.InventoryGuiMetafield
 import com.navis.road.business.util.RoadBizUtil
 import org.apache.commons.lang.StringUtils
 import org.apache.log4j.Logger
 
 public class HazardItemEntityInterceptor extends AbstractEntityLifecycleInterceptor {
 
   @Override
   public void onCreate(EEntityView inEntity, EFieldChangesView inOriginalFieldChanges, EFieldChanges inMoreFieldChanges) {
	 this.log("Recorded from onCreate");
	 this.onCreateOrUpdate(inEntity, inOriginalFieldChanges, inMoreFieldChanges);
   }
 
   public void onUpdate(EEntityView inEntity, EFieldChangesView inOriginalFieldChanges, EFieldChanges inMoreFieldChanges) {
	 this.log("Recorded from onUpdate");
	 this.onCreateOrUpdate(inEntity, inOriginalFieldChanges, inMoreFieldChanges);
   }
 
   private void onCreateOrUpdate(EEntityView inEntity, EFieldChangesView inOriginalFieldChanges, EFieldChanges inMoreFieldChanges) {
	 TimeZone timeZone = ContextHelper.getThreadUserTimezone();
	 Date timeNow = ArgoUtils.convertDateToLocalDateTime(ArgoUtils.timeNow(), timeZone);
	 this.log("Groovy: HazardItemEntityInterceptor onCreateOrUpdate - starts " + timeNow);
 
	 HazardItem hazardItem = (HazardItem) inEntity._entity;
	 if (hazardItem == null) {
	   this.log("hazardItem is null");
	   this.log("Groovy: HazardItemEntityInterceptor onCreateOrUpdate - ends " + timeNow)
	   return;
	 }
 
	 String unNumber = hazardItem.getHzrdiUNnum();
	 if (StringUtils.isBlank(unNumber)) {
	   this.reportUserError("The UN Number is not defined!");
	   this.log("Groovy: HazardItemEntityInterceptor onCreateOrUpdate - ends " + timeNow)
	   return;
	 }
 
	 ImdgClass hzrdiImdgClass = hazardItem.getHzrdiImdgClass();
	 if (hzrdiImdgClass == null) {
	   this.reportUserError("The Hazard class is not defined!");
	   this.log("Groovy: HazardItemEntityInterceptor onCreateOrUpdate - ends " + timeNow)
	   return;
	 }
 
	 String imdgClassString = hazardItem.getHzrdiImdgClass().getKey();
	 HazardousGoods hazardousGoods = this.findHazardousGoodsByUnNumberIMDGClass(unNumber, hzrdiImdgClass);
	 if (hazardousGoods == null) {
	   this.reportUserError("The Hazard item inserting with UN number: " + unNumber + " and IMDG Class: " + imdgClassString + " is not valid!");
	   this.log("Groovy: HazardItemEntityInterceptor onCreateOrUpdate - ends " + timeNow)
	   return;
	 }
 
	 Unit unit = findUnitFromHazards(hazardItem.getHzrdiHazards());
 
	 if ( unit != null ) {
		 updateGroundedFlagFromGoods(unit, null, hazardItem);
	 }
 
	 this.log("Groovy: HazardItemEntityInterceptor onCreateOrUpdate - ends and passed " + timeNow)
   }
 
	 @Override
	 public void validateDelete(EEntityView inEntity) {
		 this.log("Recorded from validateDelete");
		 HazardItem hazardItem = (HazardItem) inEntity._entity;
		 Unit unit = findUnitFromHazards(hazardItem.getHzrdiHazards());
		 if ( unit != null ) {
			 updateGroundedFlagFromGoods(unit, hazardItem, null);
		 }
	 }
 
	 private static Unit findUnitFromHazards(Hazards inHazards) {
		 if (inHazards == null ) return null;
		 Unit result = null;
		 if (InventoryEntity.GOODS_BASE.equals(inHazards.getHzrdOwnerEntityName())) {
			 Long gkey = inHazards.getHzrdOwnerEntityGkey();
			 if (gkey != null) {
				 GoodsBase gds = (GoodsBase) HibernateApi.getInstance().get(GoodsBase.class, gkey);
				 if (gds != null) {
					 result = gds.getGdsUnit();
				 }
			 }
		 }
		 return result;
	 }
 
	 private void updateGroundedFlagFromGoods(Unit unit, HazardItem inHazardBeingDeleted, HazardItem inHazardItemBeingAdded) {
		 Hazards hazards = unit.getUnitGoods().getGdsHazards();
 
		 boolean isTank = unit.isPrimaryEqTank();
		 String groundRule = null;
		 Long deletedHazardGkey = inHazardBeingDeleted != null ? inHazardBeingDeleted.getHzrdiGkey() : null;
 
		 if ( hazards != null) {
			 Iterator<HazardItem> hazardsIterator = hazards.getHazardItemsIterator();
			 while ( hazardsIterator.hasNext() ) {
				 HazardItem item = hazardsIterator.next();
				 String unNumber = item.getHzrdiUNnum();
				 ImdgClass imdgClass = item.getHzrdiImdgClass();
				 HazardousGoods hazardousGoods = this.findHazardousGoodsByUnNumberIMDGClass(unNumber, imdgClass);
 
				 if (deletedHazardGkey == item.getHzrdiGkey()) {
					 continue
				 };
				 String ground = (String) hazardousGoods.getField(getCustomGroundMetafieldId());
				 if ( ground != null) {
					 if ("Tanks".equals(ground) && isTank || "All".equals(ground)) {
						 groundRule = "YES";
					 }
				 }
			 }
		 }
 
		 // check the created hazard
		 if (inHazardItemBeingAdded != null) {
			 String unNumber = inHazardItemBeingAdded.getHzrdiUNnum();
			 ImdgClass imdgClass = inHazardItemBeingAdded.getHzrdiImdgClass();
			 HazardousGoods hazardousGoods = this.findHazardousGoodsByUnNumberIMDGClass(unNumber, imdgClass);
			 String ground = (String) hazardousGoods.getField(getCustomGroundMetafieldId());
			 if ( ground != null) {
				 if ("Tanks".equals(ground) && isTank || "All".equals(ground)) {
					 groundRule = "YES";
				 }
			 }
		 }
 
		 unit.setFieldValue(InventoryGuiMetafield.UNIT_FLEX_STRING01, groundRule);
		 HibernateApi.getInstance().update(unit);
		 HibernateApi.getInstance().flush();
	 }
 
   /**
	* Method to find Hazardous Goods by UN number and IMDG class. It may return null is not found
	* @param inUnNumber
	* @param inImdgClass
	* @return
	*/
   public HazardousGoods findHazardousGoodsByUnNumberIMDGClass(String inUnNumber, ImdgClass inImdgClass) {
 
	 DomainQuery dq = QueryUtils.createDomainQuery(InventoryEntity.HAZARDOUS_GOODS)
			 .addDqField(getCustomGroundMetafieldId())
			 .addDqPredicate(PredicateFactory.eq(InventoryField.HZGOODS_UN_NBR, inUnNumber))
			 .addDqPredicate(PredicateFactory.eq(InventoryField.HZGOODS_IMDG_CLASS, inImdgClass));
 
	 return (HazardousGoods) HibernateApi.getInstance().getUniqueEntityByDomainQuery(dq);
   }
 
	 private MetafieldId getCustomGroundMetafieldId() {
		 return MetafieldIdFactory.valueOf("customFlexFields.hzgoodsCustomDFFGround");
	 }
 
   /**
	* Recorded an error message that will stop processing
	* @param message
	*/
   private void reportUserError(message) {
	 RoadBizUtil.messageCollector.appendMessage(BizFailure.create(message))
   }
 
	 private static final Logger LOGGER = Logger.getLogger(HazardItemEntityInterceptor.class);
 }
 