	package com.weserve.APM.EDI
	
	import groovy.sql.InParameter;
	
	import java.util.Map;
	
	import org.jdom.Element;
	import org.apache.log4j.Level;
	import org.apache.log4j.Logger;
	
	import com.navis.argo.business.atoms.EquipClassEnum;
	import com.navis.argo.business.portal.EdiExtractDao;
	import com.navis.external.edi.entity.AbstractEdiExtractInterceptor;
	import com.navis.external.edi.entity.EdiExtractFilter;
import com.navis.external.edi.entity.EdiExtractPredicate;
	import com.navis.framework.SimpleSavedQueryEntity;
	import com.navis.framework.SimpleSavedQueryField;
	import com.navis.framework.business.atoms.PredicateParmEnum;
	import com.navis.framework.business.atoms.PredicateVerbEnum;
	import com.navis.framework.metafields.MetafieldId;
	import com.navis.framework.metafields.MetafieldIdFactory;
	import com.navis.framework.query.business.SavedPredicate;
	import com.navis.framework.util.ValueHolder;
import com.navis.framework.util.ValueObject;
	
	/**
	 * This code extension is used to fetch the data based on the custom Flex field for ITT
	 * Author : mpraveen
	 */
	
	class RTMCodecoExtractInterceptor extends AbstractEdiExtractInterceptor{
		Logger LOGGER = Logger.getLogger(this.class);
		private SavedPredicate _savedPredicate;
		
	
		public SavedPredicate get_savedPredicate() {
			return _savedPredicate;
		}
	
		public void set_savedPredicate(SavedPredicate _savedPredicate) {
			this._savedPredicate = _savedPredicate;
		}
	
		@Override
		public void beforeEdiExtract(Map inParams) {
			
			LOGGER.setLevel(Level.DEBUG);
			LOGGER.debug("Inside RTMCodecoExtractInterceptor :: START");
			SavedPredicate sp = (SavedPredicate) inParams.get("PREDICATE");
			LOGGER.debug("Current Saved Predicate :: "+sp.getExecutablePredicate());
			
			ValueObject vaoAll = createVaoEntry(0, null, PredicateVerbEnum.AND, null);
			ValueObject currentVO = sp.getPredicateVao();
			ValueHolder[] childPredicates = new ValueHolder[1];
			childPredicates[0] = createVaoEntry(0, MetafieldIdFactory.valueOf("customFlexFields.unitCustomDFFITTMTS"), PredicateVerbEnum.NOT_NULL, null);
			/*childPredicates[1] = createVaoEntry(1, MetafieldIdFactory.valueOf("evntEventType"), PredicateVerbEnum.EQ, "411");
			childPredicates[2] = createVaoEntry(2, MetafieldIdFactory.valueOf("ufvUnit.unitLineOperator"), PredicateVerbEnum.EQ, "2");*/
			
			vaoAll.setFieldValue(SimpleSavedQueryField.PRDCT_CHILD_PRDCT_LIST, childPredicates);
			
			SavedPredicate savedPred = new SavedPredicate(vaoAll);
			LOGGER.debug("New Saved Predicate :: "+savedPred.getExecutablePredicate());
			set_savedPredicate(savedPred);
			LOGGER.debug("Inside RTMCodecoExtractInterceptor :: END");
		}
		
		@Override
		public EdiExtractPredicate addFilterToGroup(MetafieldId inMetafieldId, PredicateVerbEnum inVerb, Object inValue, EdiExtractPredicate inEdiExtractPredicate) {
			// TODO Auto-generated method stub
			return super.addFilterToGroup(inMetafieldId, inVerb, inValue,
					inEdiExtractPredicate);
		}

		private ValueObject createVaoEntry(long inOrder, MetafieldId inMetafieldId, PredicateVerbEnum inVerbEnum, Object inValue) {
			ValueObject vao = new ValueObject(SimpleSavedQueryEntity.SAVED_PREDICATE);
			vao.setFieldValue(SimpleSavedQueryField.PRDCT_METAFIELD, inMetafieldId);
			vao.setFieldValue(SimpleSavedQueryField.PRDCT_VERB, inVerbEnum);
			vao.setFieldValue(SimpleSavedQueryField.PRDCT_VALUE, inValue);
			vao.setFieldValue(SimpleSavedQueryField.PRDCT_UI_VALUE, null);
			vao.setFieldValue(SimpleSavedQueryField.PRDCT_ORDER, new Long(inOrder));
			vao.setFieldValue(SimpleSavedQueryField.PRDCT_NEGATED, Boolean.FALSE);
			vao.setFieldValue(SimpleSavedQueryField.PRDCT_PARAMETER_TYPE, PredicateParmEnum.NO_PARM);
			return vao;
		}
	
		@Override
		public ValueHolder savedPredicate() {
			return get_savedPredicate().getPredicateVao();
		}
	}
