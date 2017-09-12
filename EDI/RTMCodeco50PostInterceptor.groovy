import com.navis.argo.ActivityTransactionDocument.ActivityTransaction
import com.navis.argo.ActivityTransactionsDocument
import com.navis.argo.ActivityTransactionsDocument.ActivityTransactions
import com.navis.argo.ArgoConfig
import com.navis.argo.ContextHelper
import com.navis.argo.business.atoms.LocTypeEnum
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.model.Facility
import com.navis.argo.webservice.external.ServiceLocator
import com.navis.argo.webservice.external.ServiceSoap
import com.navis.edi.business.edimodel.EdiConsts
import com.navis.edi.business.entity.EdiBatch
import com.navis.external.edi.entity.AbstractEdiPostInterceptor
import com.navis.framework.business.Roastery
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.util.BizFailure
import com.navis.framework.util.internationalization.PropertyKey
import com.navis.road.RoadPropertyKeys
import com.navis.road.business.appointment.api.AppointmentFinder
import com.navis.road.business.appointment.model.GateAppointment
import com.navis.road.business.atoms.AppointmentStateEnum
import com.navis.road.business.atoms.TranStatusEnum
import com.navis.road.business.atoms.TruckVisitStatusEnum
import com.navis.road.business.atoms.TruckerFriendlyTranSubTypeEnum
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckVisitDetails
import groovy.xml.MarkupBuilder
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.xmlbeans.XmlObject
import org.jdom.Document
import org.jdom.Element
import org.jdom.input.SAXBuilder

import javax.xml.rpc.Stub

/**
 * This groovy is used to process the transaction XML that is recieved and validate the
 * 1. Truck Visit and the gate transaction
 * 2. If they are in open state then create the submit-transaction web service request.
 * 3. Else, create the truck visit, submit the transaction and stage done.
 * 4. Enable the SKIP_POST to true, so that the post of the transaction is skipped.
 * @author Praveen Babu M
 *
 */
class RTMCodeco50PostInterceptor extends AbstractEdiPostInterceptor {

    private Logger LOGGER = Logger.getLogger(this.class);
    private ServiceLocator _externalServiceLocator = new ServiceLocator();


    @Override
    public void beforeEdiPost(XmlObject inXmlTransactionDocument, Map inParams) {
        LOGGER.setLevel(Level.DEBUG);
        LOGGER.debug("Inside the RTMCodeco50PostInterceptor :: START");

        Serializable batchGKey = inParams.get("BATCH_GKEY");
        EdiBatch ediBatch = (EdiBatch) HibernateApi.getInstance().load(EdiBatch.class, batchGKey);

        inParams.put(EdiConsts.SKIP_POSTER, Boolean.TRUE);

        List ediBatchTranList = new ArrayList();
        if (ediBatch.getEdibatchNotes()) {
            ediBatchTranList = ediBatch.getEdibatchNotes().split(",");
        }

        ActivityTransactionsDocument activityDocument = (ActivityTransactionsDocument) inXmlTransactionDocument;
        ActivityTransactions activityTransactions = activityDocument.getActivityTransactions();
        ActivityTransaction[] activityTransactionArray = activityTransactions.getActivityTransactionArray();

        if (activityTransactionArray.length != 1) {
            throw BizFailure.create("expected exactly one ActivityTransactionDocument, but inXmlObject contained " + activityTransactionArray.length);
        }

        ActivityTransaction activityTransaction = activityTransactionArray[0];
        String txnSetNbr = activityTransaction.getTransactionSetNbr();


        Facility currentFacility = Facility.findFacility(activityTransaction.getFacilityId(), ContextHelper.getThreadComplex());
        if (activityTransaction.getEdiInboundVisit() != null && activityTransaction.getEdiInboundVisit().getEdiTruckVisit() != null) {
            String carrierId;
            String truckPosition;

            String truckId = activityTransaction.getEdiInboundVisit().getEdiTruckVisit().getTruckId();
            String truckLicNbr = activityTransaction.getEdiInboundVisit().getEdiTruckVisit().getTruckLicenseNbr();

            carrierId = truckId != null ? truckId : truckLicNbr;
            if (carrierId != null && carrierId.contains(".")) {
                int indexPosition = carrierId.indexOf(".");
                carrierId = carrierId.substring(0, indexPosition);
                truckPosition = truckId != null ? truckId.substring(indexPosition + 1) : truckLicNbr.substring(indexPosition + 1);
            }
            if (!truckPosition) {
                truckPosition = "0"
            }
            LOGGER.debug("Current Carrier ID :: " + carrierId + " TRuck Position :: " + truckPosition);
            CarrierVisit cv;
            TruckVisitDetails truckVisit;
            if (carrierId != null) {
                cv = CarrierVisit.findNewestNumberedCarrierVisit(currentFacility, LocTypeEnum.TRUCK, carrierId);
                LOGGER.debug("After fetching the CV from Newest Number :: " + cv);
                boolean createNewTruckVisit = true;
                boolean isCurrentActivityProcessed = false;
                if (cv != null) {
                    truckVisit = HibernateApi.getInstance().downcast(cv.getCvCvd(), TruckVisitDetails.class);
                    LOGGER.debug("Current Truck Visit Status :: " + truckVisit.getTvdtlsStatus() + " Truck Visit Next Stage ID :: " + truckVisit.getTvdtlsNextStageId());
                    if ((TruckVisitStatusEnum.OK == truckVisit.getTvdtlsStatus() || TruckVisitStatusEnum.TROUBLE == truckVisit.getTvdtlsStatus())
                            && ("ingate").equalsIgnoreCase(truckVisit.getTvdtlsNextStageId())) {
                        // if the truck visit is in OK status, check for the available transactions and if any of the transaction is in OK, then post submit-transaction
                        createNewTruckVisit = false;
                        if (truckVisit.getTvdtlsTruckTrans() != null && truckVisit.getTvdtlsTruckTrans().size() > 0) {
                            for (TruckTransaction truckTransaction : truckVisit.getTvdtlsTruckTrans()) {
                                LOGGER.debug("Current Truck transaction Status :: " + truckTransaction.getTranStatus());
                                if (!TranStatusEnum.COMPLETE.equals(truckTransaction.getTranStatus())
                                        && isCurrentTransactionMatchingActivity(activityTransaction, truckTransaction)) {
                                    isCurrentActivityProcessed = true;
                                    if (TranStatusEnum.OK.equals(truckTransaction.getTranStatus()) || TranStatusEnum.TROUBLE.equals(truckTransaction.getTranStatus())) {
                                        String webServiceRequest = frameSubmitTransactionRequest(activityTransaction, null, carrierId, truckPosition, inParams);
                                        if (webServiceRequest != null) {
                                            String webServiceResponse = invokeWebServiceRequest(webServiceRequest);
                                            if (parseWebServiceResponse(webServiceResponse, inParams, true)) {
                                                //if(isLastTransaction(activityTransaction, ediBatch)){
                                                if (ediBatchTranList.contains(txnSetNbr)) {
                                                    webServiceResponse = invokeWebServiceRequest(frameStageDoneRequest(activityTransaction, carrierId, truckPosition));
                                                    parseWebServiceResponse(webServiceResponse, inParams, false);
                                                    break;
                                                }
                                            }
                                        }
                                    } else {
                                        String webServiceRequest = frameSubmitTransactionRequest(activityTransaction, null, carrierId, truckPosition, inParams);
                                        if (webServiceRequest != null) {
                                            String webServiceResponse = invokeWebServiceRequest(webServiceRequest);
                                            if (parseWebServiceResponse(webServiceResponse, inParams, true)) {
                                                //if(isLastTransaction(activityTransaction, ediBatch)){
                                                if (ediBatchTranList.contains(txnSetNbr)) {
                                                    webServiceResponse = invokeWebServiceRequest(frameStageDoneRequest(activityTransaction, carrierId, truckPosition));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if (!isCurrentActivityProcessed) {
                                String webServiceRequest = frameSubmitTransactionRequest(activityTransaction, null, carrierId, truckPosition, inParams);
                                if (webServiceRequest != null) {
                                    String webServiceResponse = invokeWebServiceRequest(webServiceRequest);
                                    if (parseWebServiceResponse(webServiceResponse, inParams, true)) {
                                        //if(isLastTransaction(activityTransaction, ediBatch)){
                                        if (ediBatchTranList.contains(txnSetNbr)) {
                                            webServiceResponse = invokeWebServiceRequest(frameStageDoneRequest(activityTransaction, carrierId, truckPosition));
                                        }
                                    }
                                }
                            }
                        } else {
                            String webServiceRequest = frameSubmitTransactionRequest(activityTransaction, null, carrierId, truckPosition, inParams);
                            if (webServiceRequest != null) {
                                String webServiceResponse = invokeWebServiceRequest(webServiceRequest);
                                if (parseWebServiceResponse(webServiceResponse, inParams, true)) {
                                    //if(isLastTransaction(activityTransaction, ediBatch)){
                                    if (ediBatchTranList.contains(txnSetNbr)) {
                                        webServiceResponse = invokeWebServiceRequest(frameStageDoneRequest(activityTransaction, carrierId, truckPosition));
                                    }
                                }
                            }
                        }
                    } else if ((TruckVisitStatusEnum.OK == truckVisit.getTvdtlsStatus() || TruckVisitStatusEnum.TROUBLE == truckVisit.getTvdtlsStatus())
                            && !("ingate").equalsIgnoreCase(truckVisit.getTvdtlsNextStageId())) {
                        createBizFaiureMessage(RoadPropertyKeys.LABEL_ACTIVE_TRUCK_VISIT_EXIST, inParams, null);
                        createBizFaiureMessage(RoadPropertyKeys.GATE__TRUCK_VISIT_HAS_INCOMPLETE_TRANSACTIONS, inParams, null);
                        createBizFaiureMessage(RoadPropertyKeys.GATE__ERROR_UPDATING_TRUCK_VISIT, inParams, null);
                    }
                }
                LOGGER.debug("Current status of the create new truck visit flag :: " + createNewTruckVisit)
                if (createNewTruckVisit) {
                    /**
                     *  1. Frame create truck visit.
                     *  2. Frame submit-transaction
                     *  3. Frame stage-done..
                     */
                    LOGGER.debug("Before creating the gate transactions New Truck visit:: ")
                    try {
                        String webServiceResponse = invokeWebServiceRequest(frameCreateTruckVisitWSRequest(activityTransaction, carrierId, truckPosition))
                        if (parseWebServiceResponse(webServiceResponse, inParams, true)) {
                            String webServiceRequest = frameSubmitTransactionRequest(activityTransaction, null, carrierId, truckPosition, inParams);
                            if (webServiceRequest != null) {
                                webServiceResponse = invokeWebServiceRequest(webServiceRequest);
                                if (parseWebServiceResponse(webServiceResponse, inParams, false)) {
                                    //if(isLastTransaction(activityTransaction, ediBatch)){
                                    if (ediBatchTranList.contains(txnSetNbr)) {
                                        webServiceResponse = invokeWebServiceRequest(frameStageDoneRequest(activityTransaction, carrierId, truckPosition));
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage());
                    } finally {
                        inParams.put(EdiConsts.SKIP_POSTER, Boolean.TRUE);
                    }
                }
            } else {
                //If either of the truck id or license nbr is not found in the transaction XML, then fail the batch and skip post process.
                createBizFaiureMessage(RoadPropertyKeys.TRUCK_ID_OR_LICENSE_REQUIRED, inParams, null);
                inParams.put(EdiConsts.SKIP_POSTER, Boolean.TRUE);
            }
        }
        /*if(ediBatch.getEdibatchNotes() != null) {
            ediBatch.setEdibatchNotes("");
        }*/

        LOGGER.debug("Inside the RTMCodeco50PostInterceptor :: END")
    }

    private boolean isCurrentTransactionMatchingActivity(ActivityTransaction inActivityTransaction, TruckTransaction inTruckTransaction) {
        LOGGER.debug("Container type from Transaction :: " + inTruckTransaction.getTranCtrTypeId() + " for tran ctr nbr :: " + inTruckTransaction.getTranCtrNbr())
        if (inActivityTransaction.getEdiContainer() && inActivityTransaction.getEdiContainer().getContainerNbr()
                && inActivityTransaction.getEdiContainer().getContainerISOcode()
                && inTruckTransaction && inTruckTransaction.getTranCtrNbr() && inTruckTransaction.getTranCtrTypeId()) {
            return (inActivityTransaction.getEdiContainer().getContainerNbr().equalsIgnoreCase(inTruckTransaction.getTranCtrNbr())
                    && inActivityTransaction.getEdiContainer().getContainerISOcode().equalsIgnoreCase(inTruckTransaction.getTranCtrTypeId()));
        }
        return false;
    }

    /*private boolean isLastTransaction(ActivityTransaction activityTransaction, EdiBatch ediBatch){
        for(EdiTransaction ediTransaction : ediBatch.getEdiTransactionSet()){
            LOGGER.debug("current transaction is the last transaction :: "+ediTransaction.getEditranSetNbr() + " :: "+activityTransaction.getTransactionSetNbr() +" TXN Count :: "+ediBatch.getEdibatchTransactionCount())
            if(ediTransaction.getEditranSetNbr() && ediTransaction.getEditranSetNbr().equalsIgnoreCase(activityTransaction.getTransactionSetNbr())){
                if(ediBatch.getEdibatchTransactionCount().equals(ediTransaction.getEditranSeqWithinBatch())){
                    LOGGER.debug("Inside the check for Last transaction :: current transaction is the last transaction :: "+ediTransaction.getEditranSetNbr());
                    return true;
                }
            }
        }
        return false;
    }*/

    private String frameCreateTruckVisitWSRequest(ActivityTransaction activityTransaction, String carrierId, String currentPosition) {
        StringWriter strWriter = new StringWriter();
        strWriter.write('<?xml version="1.0" encoding="utf-8" ?>\n');
        MarkupBuilder markUpBuilder = new MarkupBuilder(strWriter);
        markUpBuilder.setDoubleQuotes(true);

        markUpBuilder.'gate' {
            'create-truck-visit' {
                'gate-id'("RAIL_ITT")
                'stage-id'("ingate")
                'truck'('license-nbr': carrierId,
                        'id': carrierId)

            }
        }
        return strWriter.toString()
    }


    private String frameSubmitTransactionRequest(ActivityTransaction inActivityTransaction, TruckTransaction inTruckTransaction, String carrierId, String currentPosition, Map inParams) {

        String containerNbr;
        String containerISOCode;
        if (inTruckTransaction != null) {
            containerNbr = inTruckTransaction.getTranCtrNbr();
            containerISOCode = inTruckTransaction.getTranCtrTypeId();
        } else if (inActivityTransaction.getEdiContainer() != null) {
            containerNbr = inActivityTransaction.getEdiContainer().getContainerNbr();
            containerISOCode = inActivityTransaction.getEdiContainer().getContainerISOcode();
        }

        AppointmentFinder apptFinder = (AppointmentFinder) Roastery.getBean("appointmentFinder");
        TruckerFriendlyTranSubTypeEnum[] tranSubTypes = new TruckerFriendlyTranSubTypeEnum[10]
        List<TruckerFriendlyTranSubTypeEnum> truckList = (List<TruckerFriendlyTranSubTypeEnum>) TruckerFriendlyTranSubTypeEnum.getList();
        tranSubTypes = truckList.toArray(tranSubTypes)
        GateAppointment gateAppointment = apptFinder.findAppointmentByContainerId(containerNbr, tranSubTypes, AppointmentStateEnum.CREATED)

        LOGGER.debug("Current Gate Appointment that is fetched :: " + gateAppointment);
        StringWriter strWriter = new StringWriter();
        if (gateAppointment != null) {
            String preanStatus = (String) gateAppointment.getField(MetafieldIdFactory.valueOf("customFlexFields.gapptCustomDFFpreanStatus"));
            Long apptNbr = gateAppointment.getApptNbr()
            if (preanStatus != null && preanStatus != null && preanStatus.equalsIgnoreCase("OK")) {
                strWriter.write('<?xml version="1.0" encoding="utf-8" ?>\n');
                MarkupBuilder markUpBuilder = new MarkupBuilder(strWriter);
                markUpBuilder.setDoubleQuotes(true);
                markUpBuilder.'gate' {
                    'submit-transaction' {
                        'gate-id'("RAIL_ITT")
                        'stage-id'("ingate")
                        'truck'('license-nbr': carrierId)
                        'truck-transaction'('tran-type': gateAppointment.getGapptTranType().getKey(), 'truck-position': currentPosition, 'appointment-nbr': apptNbr) {
                            'container'('eqid': containerNbr, 'type': containerISOCode)
                        }

                    }
                }
            } else {
                createBizFaiureMessage(null, inParams, "Prean ($apptNbr) is not in OK state.")
                return null;
            }
        } else {
            createBizFaiureMessage(null, inParams, "Prean is not available.")
            return null;
        }
        return strWriter.toString();
    }

    private String frameStageDoneRequest(ActivityTransaction inActivityTransaction, String carrierId, String currentPosition) {
        StringWriter strWriter = new StringWriter();
        strWriter.write('<?xml version="1.0" encoding="utf-8" ?>\n');
        MarkupBuilder markUpBuilder = new MarkupBuilder(strWriter);
        markUpBuilder.setDoubleQuotes(true);

        markUpBuilder.'gate' {
            'stage-done' {
                'gate-id'("RAIL_ITT")
                'stage-id'("ingate")
                'truck'('license-nbr': carrierId)
            }
        }
        return strWriter.toString();
    }

    private boolean parseWebServiceResponse(String inWebServiceResponse, Map inParams, boolean resetMC) {
        boolean isSuccess = true;
        InputStream is = new ByteArrayInputStream(inWebServiceResponse.getBytes());
        SAXBuilder saxBuilder = new SAXBuilder(false);
        try {
            Document wsResponseDoc = saxBuilder.build(is);
            Element rootElement = wsResponseDoc.getRootElement();
            String status = wsResponseDoc.getRootElement().getAttribute("status").getValue();
            if (status && status.equalsIgnoreCase("3")) {
                isSuccess = false;
                Element ctvResponseElement = wsResponseDoc.getRootElement().getChild("messages");
                if (ctvResponseElement) {
                    Iterator iterator = ctvResponseElement.getChildren().iterator();
                    while (iterator.hasNext()) {
                        Element currentMessageElement = (Element) iterator.next();
                        createBizFaiureMessage(null, inParams, currentMessageElement.getAttribute("message-text").getValue())
                    }
                }
            }

            if (rootElement.getChildren() != null) {
                Iterator itr = rootElement.getChildren().iterator()
                while (itr.hasNext()) {
                    Element currentChild = (Element) itr.next()
                    if (currentChild.getChildren() != null) {
                        Iterator itr1 = currentChild.getChildren().iterator()
                        while (itr1.hasNext()) {
                            Element currentSubChild = (Element) itr1.next();
                            Element truckTransactionsChild = currentSubChild.getChild("truck-transactions");
                            if (truckTransactionsChild != null) { //truck-transaction
                                LOGGER.debug("name1: " + truckTransactionsChild.getName())
                                /** *************************/
                                if (truckTransactionsChild.getChildren() != null) {
                                    Iterator itr2 = truckTransactionsChild.getChildren().iterator();
                                    while (itr2.hasNext()) {
                                        Element truckTransactionChild = (Element) itr2.next();
                                        LOGGER.debug("name2: " + truckTransactionChild.getName())
                                        Element ctvResponseElement = truckTransactionChild.getChild("messages");
                                        if (ctvResponseElement) {
                                            Iterator iterator = ctvResponseElement.getChildren().iterator();
                                            while (iterator.hasNext()) {
                                                Element currentMessageElement = (Element) iterator.next();
                                                currentMessageElement.getAttribute("message-text").getValue();

                                                createBizFaiureMessage(null, inParams, currentMessageElement.getAttribute("message-text").getValue());
                                                if (currentMessageElement.getAttribute("message-severity") &&
                                                        currentMessageElement.getAttribute("message-severity").getValue().equalsIgnoreCase("SEVERE")) {
                                                    isSuccess = false;
                                                }
                                            }
                                        }
                                        /** *************************/
                                        LOGGER.debug("isSuccess: " + isSuccess);
                                        if (truckTransactionChild.getAttribute("tran-key"))
                                            LOGGER.debug("tran key: " + truckTransactionChild.getAttribute("tran-key").getValue());

                                        if (truckTransactionChild.getAttribute("status"))
                                            LOGGER.debug("tran status: " + truckTransactionChild.getAttribute("status").getValue());

                                        LOGGER.debug("tran status--: " + truckTransactionChild.getAttribute("status"));

                                        if (isSuccess && truckTransactionChild.getAttribute("status") && "TROUBLE".equals(truckTransactionChild.getAttribute("status").getValue())) {
                                            LOGGER.debug("record trouble ");
                                            //isSuccess = false;
                                            createBizFaiureMessage(RoadPropertyKeys.TROUBLETRAN, inParams, null);
                                            LOGGER.debug("record trouble tran error");
                                        }
                                        /** *************************/
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
        return isSuccess;
    }

    private String invokeWebServiceRequest(String inWebServiceRequest) {
        String businessCords;
        if (ContextHelper.getThreadUserContext().getScopeCoordinate() != null && ContextHelper.getThreadUserContext().getScopeCoordinate().getBusinessCoords() != null
                && !ContextHelper.getThreadUserContext().getScopeCoordinate().getBusinessCoords().isEmpty()) {
            LOGGER.debug("Inside IF condition for Biz co-ordinates:: " + ContextHelper.getThreadUserContext().getScopeCoordinate().getBusinessCoords());
            businessCords = ContextHelper.getThreadUserContext().getScopeCoordinate().getBusinessCoords();
        } else {
            LOGGER.debug("Inside ELSE for finding BIZ cordinates :: ");
            businessCords = "APMT/NLRTM/RTM/RTM";
        }
        LOGGER.debug("Incoming web Service Request :: " + inWebServiceRequest + "Business Cords :: " + businessCords)
        String url = ArgoConfig.N4_WS_ARGO_URL.getSetting(ContextHelper.getThreadUserContext());
        url = url.replace("argoservice", "argobasicservice");
        ServiceSoap port = this._externalServiceLocator.getServiceSoap(new URL(url));
        Stub stub = (Stub) port;
        stub._setProperty("javax.xml.rpc.security.auth.username", "admin");
        stub._setProperty("javax.xml.rpc.security.auth.password", "admin");
        String webServiceResponse = port.basicInvoke(businessCords, inWebServiceRequest);
        LOGGER.debug("WebService Response recieved :: " + webServiceResponse);
        return webServiceResponse;

    }

    private createBizFaiureMessage(PropertyKey inPropertyKey, Map inParams, String errorMessage) {
        BizFailure bv;
        if (inPropertyKey) {
            bv = BizFailure.create(inPropertyKey, null);
        } else if (errorMessage) {
            bv = BizFailure.createProgrammingFailure(errorMessage)
        }

        ContextHelper.getThreadMessageCollector().appendMessage(bv);
        inParams.put(EdiConsts.SKIP_POSTER, Boolean.TRUE);
    }

    private boolean isEmpty(String inString) {
        return (inString == null || inString.trim().isEmpty());
    }

}
