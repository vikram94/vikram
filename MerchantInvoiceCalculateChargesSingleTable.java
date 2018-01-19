package com.repo.billdesk.admin.demon;
import java.sql.Connection;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.repo.billdesk.admin.config.AdminConstants;
import com.repo.billdesk.admin.util.InvoiceHelper;
import com.repo.billdesk.admin.util.MICRDatabase;
import com.repo.billdesk.businessobjects.BinDetails;
import com.repo.billdesk.businessobjects.CitiMerchantMaster;
import com.repo.billdesk.businessobjects.GroupMerchant;
import com.repo.billdesk.businessobjects.MercInvDetails;
import com.repo.billdesk.businessobjects.MercInvRateTypeMaster;
import com.repo.billdesk.businessobjects.MercInvoiceTotalDetails;
import com.repo.billdesk.businessobjects.MerchantMaster;
import com.repo.billdesk.businessobjects.MerchantRateConfig;
import com.repo.billdesk.businessobjects.MicrBranchName;
import com.repo.billdesk.businessobjects.SipConfMaster;
import com.repo.billdesk.businessobjects.TaxMaster;
import com.repo.billdesk.businessobjects.TransactionDetail;
import com.repo.billdesk.services.ServiceLocator;
import com.repo.billdesk.services.config.AmountUtil;
import com.repo.billdesk.services.config.Data;
import com.repo.billdesk.services.config.DateUtil;
import com.repo.billdesk.services.config.PropertyFileReader;
import com.repo.billdesk.services.config.SendMailMessage;
import com.repo.billdesk.services.config.ViralConstants;
import com.repo.billdesk.services.datasource.DBConnectionManager;
import com.repo.billdesk.services.datasource.util.DBUtility;
import com.repo.billdesk.services.servicedef.IdGeneratorDefinition;
import com.repo.billdesk.services.servicedef.InvoiceDefinition;
import com.repo.billdesk.services.servicedef.MerchantDefinition;

public class MerchantInvoiceCalculateChargesSingleTable {

	String CLASS_NAME=this.getClass().getName();
	Logger logger=Logger.getLogger(CLASS_NAME);

	private ServiceLocator servicelocator;
	private IdGeneratorDefinition idGeneratorDefinition;
	private MerchantDefinition merchantDefinition;
	private InvoiceDefinition invoiceDefinition;
	PropertyFileReader CITIMerchantChargesConfig=null;
	private DBConnectionManager manager = DBConnectionManager.getInstance();
	private StringBuffer configErrorReportBuffer = new StringBuffer();
	private StringBuffer zeroChargesReportBuffer = new StringBuffer();
	private ArrayList<TaxMaster>  taxMasters;
	private List<String> mnemonicListCard;
	
	public MerchantInvoiceCalculateChargesSingleTable() {

		servicelocator=new ServiceLocator();
		idGeneratorDefinition = (IdGeneratorDefinition)servicelocator.getBean("idGeneratorService");
		merchantDefinition=(MerchantDefinition)servicelocator.getBean("merchantService");
		invoiceDefinition=(InvoiceDefinition)servicelocator.getBean("invoiceService");
		CITIMerchantChargesConfig = new PropertyFileReader("CITIMerchantCharges.properties");
		configErrorReportBuffer = new StringBuffer();
		zeroChargesReportBuffer = new StringBuffer();
	}

	public static void main(String[] args) {

		try {
			args = new String[1];
			args[0] = "201608";
			String dateStr = null;
			int countTotal = 20;
			if(args.length > 0){
				//dateStr = args[0];
				try {
					
					Timestamp grossInvoiceMonth = DateUtil.getUDFTimestamp(args[0],"yyyyMM");
					
					String monthYearStr = DateUtil.getUDFDateString(grossInvoiceMonth, "MMMyy").toUpperCase();
					
					AdminConstants.TRANSACTION_DETAILS = "TRANSACTION_DETAILS_"+monthYearStr;
					
					AdminConstants.MERC_INV_DETAILS = "MERC_INV_DETAILS_"+monthYearStr;
					
					AdminConstants.TXN_CUST_DETAILS = "TXN_CUST_DETAILS_"+monthYearStr;
					
				} catch (Exception e) {
					
				}
			}else{
				try {
					
					Calendar now = Calendar.getInstance();
					
					String monthYearStr = DateUtil.getMonthYearFormat(now);
					
					AdminConstants.TRANSACTION_DETAILS = "TRANSACTION_DETAILS_"+monthYearStr;
					
					AdminConstants.MERC_INV_DETAILS = "MERC_INV_DETAILS_"+monthYearStr;
					
					AdminConstants.TXN_CUST_DETAILS = "TXN_CUST_DETAILS_"+monthYearStr;
					
				} catch (Exception e) {
					
				}
			}
			if(args.length > 1){
				countTotal = Integer.parseInt(args[1]);
			}

			MerchantInvoiceCalculateChargesSingleTable merchantInvoiceCalculateCharges = new MerchantInvoiceCalculateChargesSingleTable();

			merchantInvoiceCalculateCharges.calculateCharges(dateStr, countTotal);

		} finally {


		}
	}
	
	public void startCalculationForMonth(String month){
		
		int countTotal = 20;
		
		try {
		
			Timestamp grossInvoiceMonth = DateUtil.getUDFTimestamp(month,"yyyyMM");
			
			String monthYearStr = DateUtil.getUDFDateString(grossInvoiceMonth, "MMMyy").toUpperCase();
			
			AdminConstants.TRANSACTION_DETAILS = "TRANSACTION_DETAILS_"+monthYearStr;
			
			AdminConstants.MERC_INV_DETAILS = "MERC_INV_DETAILS_"+monthYearStr;
			
			AdminConstants.TXN_CUST_DETAILS = "TXN_CUST_DETAILS_"+monthYearStr;
			
			MerchantInvoiceCalculateChargesSingleTable merchantInvoiceCalculateCharges = new MerchantInvoiceCalculateChargesSingleTable();

			merchantInvoiceCalculateCharges.calculateCharges(null, countTotal);
			
		} catch (Exception e) {
			
		}
	}

	private void getTxnCustDetails(ArrayList<TransactionDetail> transactionDetailList, Connection dbConnection) throws Exception{

		invoiceDefinition.getTxnCustDetails(transactionDetailList, dbConnection);

	}

	private void calculateCharges(String dateStr, int countTotal) {

		Connection dbConnection=null;

		//ArrayList<TransactionDetail> transactionDetailRefundList = new ArrayList<TransactionDetail>();


		try {

			logger.info("Starting Invoice Creation for dateStr: [" +dateStr+ "]");
			dbConnection = manager.getConnection();
			dbConnection.setAutoCommit(false);
			
			//Getting monthly cuttoff date
			
			Timestamp invoiceMonth = DateUtil.getUDFTimestamp(AdminConstants.TRANSACTION_DETAILS.replace("TRANSACTION_DETAILS_", ""),"MMMyy");
			
			String monthCutOff = invoiceDefinition.getMonthlyCutOffDate(DateUtil.getUDFDateString(invoiceMonth, "yyyyMM"), dbConnection);
			
			if(monthCutOff==null){
				logger.info("Month Cutoff not found in database for month ="+DateUtil.getUDFDateString(invoiceMonth, "yyyyMM"));
				return;
			}else{				
				try {
					logger.info("Month Cutoff date for "+invoiceMonth+" is "+monthCutOff);
					AdminConstants.CUTOFF_MONTH_DATE_PLUS_ONE = Integer.parseInt(monthCutOff)+1;
				} catch (Exception e) {
					logger.info("Month Cutoff not proper in  database");
				}			
			}

			//Getting List of Group Merchant into hashmap...
			HashMap<String, GroupMerchant> hashtableGroupMerchant = merchantDefinition.getGroupMerchantHashtable(dbConnection);	
			logger.debug("hashtableGroupMerchant, size: [" +hashtableGroupMerchant.size()+ "]");


			GroupMerchant groupMerchant;
			
			//TODO: need to check below logic and set accordingly...
			/*
			if(groupMerchant.getRefundNet().equalsIgnoreCase("Y")){
				//Getting Refund Transaction List...
				transactionDetailRefundList = invoiceDefinition.getMercTransactionList(subMerchantMasterArr, month, true, dbConnection);
				ArrayList<String> transactionIdList = getTransactionIdList(transactionDetailList);
				getActualTransactionDetailsRefundList(transactionIdList, transactionDetailRefundList);
			}
			 */
			
			logger.info("Starting getTaxMaster....");
			taxMasters =  invoiceDefinition.getTaxMasterArrayList(dbConnection);
			logger.info("Finished getTaxMaster....");

			//Getting Full Rate Type Master...
			HashMap<String, ArrayList<MercInvRateTypeMaster>> mercInvRateTypeMasterHashMap = merchantDefinition.getMerchantInvRateTypeMaster(dbConnection);

			//Getting Full Card Bin Details List...
			HashMap<String, BinDetails> binDetailsLinkedHashMap = invoiceDefinition.getCardBinDetailsList(dbConnection);

			//Getting list of merchants...
			ArrayList<MerchantMaster> merchantMasterList = merchantDefinition.getMerchantMasterList(dbConnection);
			HashMap<String, MerchantMaster> hashtableMerchantMaster = merchantDefinition.getMerchantMasterHashtable(merchantMasterList);

			//Getting list of citi merchants...
			ArrayList<CitiMerchantMaster> citiMerchantMasterList = merchantDefinition.getCitiMerchantMasterList(dbConnection);
			HashMap<String, CitiMerchantMaster> hashtableCitiMerchantMaster = merchantDefinition.getCitiMerchantMasterHashtable(citiMerchantMasterList);
			
			//Getting Pay Config...
			logger.info("Starting getMerchantRateCofig....");
			HashMap<String, MerchantRateConfig> merchantRateConfigHashMap = merchantDefinition.getMerchantRateCofig(dbConnection);
			logger.info("Finished getMerchantRateCofig....");

			MerchantRateConfig merchantRateConfig;
			TransactionDetail transactionDetail;
			String payConfigKey;
			double surcharge_charge, surcharge_service_tax,surcharge_charge_inclusive, net_charge, net_net_charge;
			String rateTypeMnemonic, rateTypeMnemonicOrig, cardBin;
			ArrayList<MercInvRateTypeMaster> mercInvRateTypeMasterList;
			BinDetails binDetails;
			Timestamp timestampBase, timestampBaseGross, timestampFromDateBase = null;

			String merchantHashKey, groupMerchantId, sipDay;
			MerchantMaster merchantMaster;


			/*
			logger.info("Starting getMercInvoiceTotalDetails....");
			HashMap<String, MercInvoiceTotalDetails> mercInvoiceTotalDetailsHashMap = invoiceDefinition.getMercInvoiceTotalDetails(month, dbConnection);
			ArrayList<String> invoiceTotalExtraGroupMerchantList = new ArrayList<String>();
			logger.info("Finished getMercInvoiceTotalDetails....");
			 */

			/*HashMap<String, MercInvoiceTotalDetails> mercInvoiceTotalDetailsHashMapCITI = new HashMap<String, MercInvoiceTotalDetails>();
			ArrayList<String> invoiceTotalExtraGroupMerchantListCITI = new ArrayList<String>();
			String rateTypeNarrationKeys = CITIMerchantChargesConfig.getValue("RATE_TYPE_NARRATION_KEYS");
			String[] rateTyepNarrationKeysArray = null;
			
			if(rateTypeNarrationKeys == null) {
				logger.info("There is no rate type narrations found");
			} else {
				rateTyepNarrationKeysArray = rateTypeNarrationKeys.split(AdminConstants.SPLIT_CHAR);
			}
			
			String slabKeys = CITIMerchantChargesConfig.getValue("SLAB_KEYS");
			String[] slabKeysArray = null;
			if(slabKeys == null){
				logger.info("There is no slab type narrations found");
			}else{
				slabKeysArray = slabKeys.split(AdminConstants.SPLIT_CHAR);
			}*/

			//Getting SIP Config...
			HashMap<String, SipConfMaster> sipConfMasterHash = merchantDefinition.getSIPConfMaster(dbConnection);
			int count = 1;
			ArrayList<TransactionDetail> transactionDetailList = null;
			String subject = "";
			String messageBody = "";
			ArrayList<TransactionDetail> transactionDetailListSingle = null;
			ArrayList<String> transactionIdList = null;
			ArrayList<String> transactionConfigErrorIdList = null;
			ArrayList<String> transactionCitiMerchantIdList = null;
			ArrayList<String> transactionProcessingErrorList = null;
			ArrayList<String> transactionNotValidIdList = null;
			ArrayList<String> transactionTaxesNotValidIdList = null;
			ArrayList<String> transactionCardPayUBPTypeList = null;
			HashMap<String, MercInvoiceTotalDetails> mercInvoiceTotalDetailsHashMap = null;
			ArrayList<String> invoiceTotalExtraGroupMerchantList = null;
			TaxMaster taxMaster;
			mnemonicListCard = getCardMnemonicInitials();
			do {
				
				transactionDetailListSingle = new ArrayList<TransactionDetail>();
				transactionIdList = new ArrayList<String>();
				transactionConfigErrorIdList = new ArrayList<String>();
				transactionCitiMerchantIdList = new ArrayList<String>();
				transactionProcessingErrorList = new ArrayList<String>();
				transactionNotValidIdList = new ArrayList<String>();
				transactionTaxesNotValidIdList = new ArrayList<String>();
				transactionCardPayUBPTypeList = new ArrayList<String>();
				invoiceTotalExtraGroupMerchantList = new ArrayList<String>();
				mercInvoiceTotalDetailsHashMap = new HashMap<String, MercInvoiceTotalDetails>();
				
				//Getting Transaction List...
				logger.info("Starting getTransactionList Query...");
				transactionDetailList = invoiceDefinition.getMercTransactionList(false, dateStr,dbConnection);
				logger.info("Ending getTransactionList Query, size: [" +transactionDetailList.size()+ "]");

				if(transactionDetailList.size()==0) {

					logger.warn("No transactions found for dateStr: ["+dateStr+"]");
					return;
				}

				//Getting TxnCustDetails
				logger.info("Starting gettting txnCustDetails ...");
				getTxnCustDetails(transactionDetailList, dbConnection);
				logger.info("Ending gettting txnCustDetails ...");
				
				for(int txnCount=0; txnCount<transactionDetailList.size(); txnCount++) {


					transactionDetail = (TransactionDetail) transactionDetailList.get(txnCount);

					taxMaster = getTransactionTax(transactionDetail, AdminConstants.TAX_BASE);
					
					if(taxMaster==null){
						logger.info("TaxMaster not found for RepoTxn Id: [" +transactionDetail.getRepoTxnId()+"] and Transaction Timestamp: [" +transactionDetail.getTransactionTimestamp()+ "]");
						transactionTaxesNotValidIdList.add(transactionDetail.getRepoTxnId());
						continue;
					}
					
					transactionDetail.setTaxMaster(taxMaster);
					
					//Ignoring Transactions...
					if(transactionDetail.getSystemId().equals("CardPay") && (transactionDetail.getTransactionStatus().equals(AdminConstants.SUCCESS) || transactionDetail.getTransactionStatus().equals(AdminConstants.FAILED))){

					}else if(transactionDetail.getSystemId().equals("BMS") && transactionDetail.getTransactionStatus().equals(AdminConstants.SUCCESS)){

					}else if(transactionDetail.getSystemId().equals("PGI") && transactionDetail.getTransactionStatus().equals(AdminConstants.SUCCESS)){

					}/*else if(transactionDetail.getSystemId().equals("ECOM") && transactionDetail.getTransactionStatus().equals(AdminConstants.SUCCESS)){

					}*/
					else{
						//Transaction not valid for Invoicing...
						logger.info("Transaction not valid for Invoicing for SystemId: [" +transactionDetail.getSystemId()+"] and Transaction Status: [" +transactionDetail.getTransactionStatus()+ "]");
						transactionNotValidIdList.add(transactionDetail.getRepoTxnId());
						continue;
					}

					if(transactionDetail.getSystemId().equals("CardPay") && transactionDetail.getMerchantId().endsWith("UBP") && 
							transactionDetail.getSourceId().endsWith("UBP")) {

						//Transaction is UBP txn where processing is through CardPay...
						logger.info("Transaction not valid for Invoicing for SystemId: [" +transactionDetail.getSystemId()+"], Merchant Id: [" +transactionDetail.getMerchantId()+ "] and Source Id: [" +transactionDetail.getSourceId()+ "]");
						transactionCardPayUBPTypeList.add(transactionDetail.getRepoTxnId());
						continue;
					}

					if(transactionDetail.getSystemId().equals("CardPay") && transactionDetail.getMerchantId().endsWith("VBP") && 
							transactionDetail.getSourceId().endsWith("VBP")) {

						//Transaction is VBP txn where processing is through CardPay...
						logger.info("Transaction not valid for Invoicing for SystemId: [" +transactionDetail.getSystemId()+"], Merchant Id: [" +transactionDetail.getMerchantId()+ "] and Source Id: [" +transactionDetail.getSourceId()+ "]");
						transactionCardPayUBPTypeList.add(transactionDetail.getRepoTxnId());
						continue;
					}

					if(transactionDetail.getSystemId().equals("CardPay") && transactionDetail.getMerchantId().endsWith("UBP") && 
							transactionDetail.getSourceId().equals("UBPBANK")) {

						//Transaction is VBP txn where processing is through CardPay...
						logger.info("Transaction not valid for Invoicing for SystemId: [" +transactionDetail.getSystemId()+"], Merchant Id: [" +transactionDetail.getMerchantId()+ "] and Source Id: [" +transactionDetail.getSourceId()+ "]");
						transactionCardPayUBPTypeList.add(transactionDetail.getRepoTxnId());
						continue;
					}

					//Key for Pay Config...
					payConfigKey = transactionDetail.getSystemId()+AdminConstants.KEY_CHAR+transactionDetail.getMerchantId()+
							AdminConstants.KEY_CHAR+transactionDetail.getBankId()+AdminConstants.KEY_CHAR+transactionDetail.getPayeeBankId()+
							AdminConstants.KEY_CHAR+transactionDetail.getProductCode()+AdminConstants.KEY_CHAR+transactionDetail.getMerchantIdBank();

					merchantRateConfig = merchantRateConfigHashMap.get(payConfigKey);

					if(merchantRateConfig==null) {

						//TODO: need to create report for these exceptions...
						//No rate config found, ignoring...
						logger.info("Config_error: No Rate config found for key: [" +payConfigKey+ "] and RateType:["+transactionDetail.getTomerchantRateTypeMnemonic()+"]");
						configErrorReportBuffer.append("No Rate config found for key: " +payConfigKey+ "~"+transactionDetail.getTomerchantRateTypeMnemonic()+System.getProperty("line.separator"));
						transactionConfigErrorIdList.add(transactionDetail.getRepoTxnId());
						continue;
					}



					//Now check whether gross charges are present, store model for gross invoice generation...
					//Get Rate Type Mnemonic...
					rateTypeMnemonic = merchantRateConfig.getRateTypeMnemonic();

					//Get revised Rate Type Mnemonic based on card bin...
					binDetails=null;
					cardBin=null;
					rateTypeMnemonicOrig = rateTypeMnemonic; 

					if(transactionDetail.getTxnCustDetails() != null) {

						cardBin = transactionDetail.getTxnCustDetails().getCardBin();

						if(cardBin!=null && !cardBin.trim().equals("") && !cardBin.trim().equalsIgnoreCase("NA")) {

							binDetails = binDetailsLinkedHashMap.get(cardBin);
						}

					} else {
						logger.info("Processing_Error: No TxnCustDetails found for: [" +transactionDetail.getRepoTxnId()+ "]");
						transactionProcessingErrorList.add(transactionDetail.getRepoTxnId());
						continue;
					}


					if(rateTypeMnemonic.startsWith("C_")) {

						//card bin is present...and mnemonic also starts with C_, hence card bin logic will be applied...
						if(binDetails!=null) {
							rateTypeMnemonic = binDetails.getCardProductId()+ "_" +rateTypeMnemonic;
						}

					}


					//Get Merchant Inv Rate Type Master Model...
					mercInvRateTypeMasterList = mercInvRateTypeMasterHashMap.get(rateTypeMnemonic);
					if(mercInvRateTypeMasterList==null) {

						//could not get the rate master for revised mnemonic which came based on card bin...hence shifting to the original mnemonic...
						mercInvRateTypeMasterList = mercInvRateTypeMasterHashMap.get(rateTypeMnemonicOrig);
						rateTypeMnemonic = rateTypeMnemonicOrig;
					}

					if(mercInvRateTypeMasterList==null) {

						//TODO: need to create report for these exceptions...
						//No rate master found, ignoring...
						logger.info("Config_error: No Rate Type Master found for key: [" +rateTypeMnemonic+ "] and RateType:["+transactionDetail.getTomerchantRateTypeMnemonic()+"]");
						configErrorReportBuffer.append("No Rate Type Master found for key: " +rateTypeMnemonic+ "~"+transactionDetail.getTomerchantRateTypeMnemonic()+System.getProperty("line.separator"));
						transactionConfigErrorIdList.add(transactionDetail.getRepoTxnId());
						continue;
					}

					transactionDetail.setMerchantRateConfig(merchantRateConfig);
					transactionDetail.setInvRateTypeMnemonic(rateTypeMnemonic);
					transactionDetail.setMercInvRateTypeMasterList(mercInvRateTypeMasterList);

					//transactionDetail.setChannelId(merchantRateConfig.getChannelId());
					//transactionDetail.setMerchantFn(merchantMaster.getMerchantFn());
					//transactionDetail.setSubMerchantFn(merchantMaster.getSubMerchantFn());
					//transactionDetail.setIndustry(merchantMaster.getIndustry());


					merchantHashKey = transactionDetail.getSystemId()+AdminConstants.KEY_CHAR+transactionDetail.getMerchantId()+
							AdminConstants.KEY_CHAR+transactionDetail.getSubMerchantId();

					merchantMaster = hashtableMerchantMaster.get(merchantHashKey);
					if(merchantMaster==null) {

						//TODO: need to create report for these exceptions...
						//No merchantMaster entry found , ignoring...
						logger.info("Config_error: No merchantMaster entry found for key: [" +merchantHashKey+ "] and Group Merchant ID:["+transactionDetail.getGroupMerchantId()+"]");
						configErrorReportBuffer.append("No merchantMaster entry found for key: " +merchantHashKey+System.getProperty("line.separator"));
						transactionConfigErrorIdList.add(transactionDetail.getRepoTxnId());
						continue;
					}
					transactionDetail.setMerchantMaster(merchantMaster);

					groupMerchantId = merchantMaster.getGroupMerchantId();

					//For SBIELRCECS where invoices split on the basis of Authenticator2(MICR Branch Code)
					if(groupMerchantId.equals(AdminConstants.GROUP_MERCHANT_MICR_CODE_BASE_SPLIT_SBIELRCESB) && transactionDetail.getAdditionalInfo2() != null){

						groupMerchantId = groupMerchantId+"_"+transactionDetail.getAdditionalInfo2();					

					}

					//For SBIELRC where invoices split on the basis of Authenticator3(MICR Branch Code)
					if(groupMerchantId.equals(AdminConstants.GROUP_MERCHANT_MICR_CODE_BASE_SPLIT_SBIELRC) && transactionDetail.getAdditionalInfo3() != null){

						groupMerchantId = groupMerchantId+"_"+transactionDetail.getAdditionalInfo3();					

					}

					//For MTNLDELCRM where invoices split on the basis of Authenticator2(MICR Branch Code) and is 900
					if(groupMerchantId.equals(AdminConstants.GROUP_MERCHANT_MICR_CODE_900_BASE) && transactionDetail.getAdditionalInfo2() != null){

						if(transactionDetail.getAdditionalInfo2().startsWith("900")){
							groupMerchantId = groupMerchantId+"_900";
						}

					}

					//To check is there a split in invoice at merchant rate config level
					if(merchantRateConfig.getGroupMerchantId().equals("NA")){

						groupMerchant = hashtableGroupMerchant.get(groupMerchantId);

					}else{

						groupMerchantId = merchantRateConfig.getGroupMerchantId();
						groupMerchant = hashtableGroupMerchant.get(groupMerchantId);

					}			

					transactionDetail.setGroupMerchantId(groupMerchantId);

					if(groupMerchant == null) {
						//throw new Exception("GroupMerchant not found for repo_txn : [" +transactionDetail.getRepoTxnId()+ "] and Merchant Id ["+transactionDetail.getMerchantId()+"]");
						logger.info("Config_error: GroupMerchant Id : [" +groupMerchantId+ "] not found in GROUP_MERCHANT table, combination: [" +merchantHashKey+ "]");
						configErrorReportBuffer.append("GROUP_MERCHANT_ID not found :"+groupMerchantId+System.getProperty("line.separator"));
						transactionConfigErrorIdList.add(transactionDetail.getRepoTxnId());
						continue;
					}				


					if(groupMerchant.getDetailFormat()==null) {
						logger.info("Config_error: Detail Format not found for Group Merchant ID: [" +transactionDetail.getGroupMerchantId()+ "]");
						configErrorReportBuffer.append("Detail Format not found for Group Merchant ID :"+transactionDetail.getGroupMerchantId()+System.getProperty("line.separator"));
						transactionConfigErrorIdList.add(transactionDetail.getRepoTxnId());
						continue;
					}
					
					
					timestampBase = null;

					try {

						//For Gross...
						timestampBase = getInvMonthFromDateBase("G", groupMerchant, transactionDetail);
						transactionDetail.setMercGrossInvoiceMonth(timestampBase);
						//For Start Date and End Date check in config master
						timestampFromDateBase = getTimestampFromDateBase("G", groupMerchant, transactionDetail);


					} catch(Exception e) {
						logger.info("Exception: timestampBase", e);
						logger.info(e.getMessage());
						transactionConfigErrorIdList.add(transactionDetail.getRepoTxnId());
						continue;
					}
					
					//This is for CITI Merchant and will go below for calulation
					/*if(groupMerchant.getDetailFormat().equalsIgnoreCase(AdminConstants.CITI_MERCHANT_DETAIL_FORMAT)){
						try {
							calculateAndSetConfigForCITI(transactionDetail, mercInvoiceTotalDetailsHashMap, hashtableCitiMerchantMaster, mercInvRateTypeMasterList,  
									invoiceTotalExtraGroupMerchantList, timestampFromDateBase, dbConnection);

						} catch(Exception e) {
							logger.info("Exception: calculateAndSetConfigForCITI", e);
							logger.info(e.getMessage());
							transactionConfigErrorIdList.add(transactionDetail.getRepoTxnId());
							continue;
						}
						logger.info("CITI Merchant for Group Merchant ID: [" +transactionDetail.getGroupMerchantId()+ "]");
						transactionCitiMerchantIdList.add(transactionDetail.getRepoTxnId());
						continue;
					}*/

					//Check If merchant is Axamfb and SIP is D
					if(groupMerchant.getGroupMerchantId().equals(AdminConstants.GROUP_MERCHANT_D_SIP)){
						sipDay = InvoiceHelper.getTxnSipDay(sipConfMasterHash, transactionDetail);
						if(sipDay.equals("D")){
							rateTypeMnemonic = transactionDetail.getInvRateTypeMnemonic()+"_D";
							//Get Merchant Inv Rate Type Master Model...
							mercInvRateTypeMasterList = mercInvRateTypeMasterHashMap.get(rateTypeMnemonic);
							if(mercInvRateTypeMasterList!=null) {

								transactionDetail.setInvRateTypeMnemonic(rateTypeMnemonic);
								transactionDetail.setMercInvRateTypeMasterList(mercInvRateTypeMasterList);

							}else{

								//could not get the rate master for revised mnemonic which came based on sip day...hence shifting to the original mnemonic...
								mercInvRateTypeMasterList = transactionDetail.getMercInvRateTypeMasterList();
							}
						}
					}


					timestampBase = null;			

					try {

						
						calculateInvoiceCharges(transactionDetail, mercInvRateTypeMasterList, mercInvoiceTotalDetailsHashMap, 
								invoiceTotalExtraGroupMerchantList, timestampFromDateBase, false, dbConnection);


					} catch(Exception e) {
						//logger.info("Exception: ", e);
						logger.info(e.getMessage());
						transactionConfigErrorIdList.add(transactionDetail.getRepoTxnId());
						continue;
					}

					if(groupMerchant.getSTaxApplicable() != null && groupMerchant.getSTaxApplicable().equals("N")){

						transactionDetail.setInvoiceTransactionStax(Data.lpad(String.valueOf(0l), '0', 18));

						transactionDetail.setEcsChargeServiceTax(Data.lpad(String.valueOf(0l), '0', 18));
						
						transactionDetail.setGstAppliedNet("N");
						transactionDetail.setGstAppliedGross("N");
						transactionDetail.setGstAppliedSurcharge("N");

					}
					timestampBaseGross=transactionDetail.getMercGrossInvoiceMonth();
					transactionDetail.setMercGrossInvoiceMonth(null);


					//if there is gross-charges...
					if(AmountUtil.hasValue(transactionDetail.getInvoiceTransactionCharges()) || 
							AmountUtil.hasValue(transactionDetail.getEcsSuccessCharge()) || 
							AmountUtil.hasValue(transactionDetail.getEcsFailureCharge())) {

						transactionDetail.setMercGrossInvoiceMonth(timestampBaseGross);

					} else if(!AmountUtil.hasValue(transactionDetail.getTomerchantCharges()) && !AmountUtil.hasValue(transactionDetail.getTransactionCharges())) {

						if(transactionDetail.getMerchantRateConfig().getDefaultInvoiceType() != null && transactionDetail.getMerchantRateConfig().getDefaultInvoiceType().equals("N")){

							//Means, transaction has no surcharge, no net charges and no gross charges, and default invoice type is net
							timestampBase = getInvMonthFromDateBase("N", groupMerchant, transactionDetail);
							transactionDetail.setMercNetInvoiceMonth(timestampBase);

						}else if(transactionDetail.getMerchantRateConfig().getDefaultInvoiceType() != null && transactionDetail.getMerchantRateConfig().getDefaultInvoiceType().equals("S")){

							//Means, transaction has no surcharge, no net charges and no gross charges, and default invoice type is surcharge
							timestampBase = getInvMonthFromDateBase("S", groupMerchant, transactionDetail);
							transactionDetail.setMercSurchargeInvoiceMonth(timestampBase);

							transactionDetail.setTransactionCharges(Data.lpad("0", '0', 16));
							transactionDetail.setTransactionServiceTax(Data.lpad("0", '0', 16));

						}else{

							//Means, transaction has no surcharge, no net charges and no gross charges, and default invoice type is gross or nothing then those transactions in the gross invoicing...
							transactionDetail.setMercGrossInvoiceMonth(timestampBaseGross);

						}
						if(!checkWhiteListingOfZeroCharges(transactionDetail.getSystemId()+AdminConstants.KEY_CHAR+transactionDetail.getMerchantId()+
								AdminConstants.KEY_CHAR+transactionDetail.getBankId())){
							//SYSTEM_ID~MERCHANT_ID~BANK_ID~GROUP_MERCHANT_ID~DEFAULT_INVOIVE_TYPE~TRANSACTION_STATUS~MERC_INV_RATE_TYPE_MNEMONIC
							zeroChargesReportBuffer.append(transactionDetail.getSystemId()+AdminConstants.KEY_CHAR+transactionDetail.getMerchantId()+
									AdminConstants.KEY_CHAR+transactionDetail.getBankId()+AdminConstants.KEY_CHAR+transactionDetail.getGroupMerchantId()+AdminConstants.KEY_CHAR+transactionDetail.getMerchantRateConfig().getDefaultInvoiceType()+AdminConstants.KEY_CHAR+transactionDetail.getTransactionStatus()+AdminConstants.KEY_CHAR+transactionDetail.getInvRateTypeMnemonic()+AdminConstants.KEY_CHAR+transactionDetail.getTomerchantRateTypeMnemonic()+System.getProperty("line.separator"));
						}
					}


					//if there is surcharge ...
					if(AmountUtil.hasValue(transactionDetail.getTransactionCharges())) {

						timestampBase = getInvMonthFromDateBase("S", groupMerchant, transactionDetail);
						transactionDetail.setMercSurchargeInvoiceMonth(timestampBase);
					}

					try{

						//if there is surcharge, store in model for surcharge invoice generation...
						if(AmountUtil.hasValue(transactionDetail.getTransactionCharges())) {

							surcharge_charge_inclusive = (transactionDetail.getTransactionChargesLong()/10000.0);
							// PGI Hack - Surcharge = NET + NET_STAX
							if(transactionDetail.getSystemId().equals(AdminConstants.SYSTEM_ID_PGI)){

								net_charge = (transactionDetail.getTomerchantChargesLong()/10000.0);

								net_net_charge = net_charge - surcharge_charge_inclusive;

								if(net_net_charge==0){

									transactionDetail.setTomerchantCharges(AdminConstants.ZERO_AMOUNT);

								}else{
									//To check the final calculation if in ps or not  
									/*String final_net_charge = String.valueOf(net_net_charge*10000.0);
									int decimal_index = final_net_charge.indexOf('.');
									if (decimal_index > 0) { // Need to check!
										final_net_charge = final_net_charge.substring(0, decimal_index);
									}
									transactionDetail.setTomerchantCharges(final_net_charge);*/
									transactionDetail.setTomerchantCharges(Data.lpad(String.valueOf(AmountUtil.getLongAmountPs(net_net_charge)), '0', 16));

								}

							}
							if(groupMerchant.getSTaxApplicable() != null && groupMerchant.getSTaxApplicable().equals("N")){
								//Do nothing because service tax in not applicable on group merchant and it is excluded in the charges.
								//transactionDetail.setTransactionServiceTax(Data.lpad(String.valueOf(AmountUtil.getLongAmountPs(0)), '0', 16));
							}else{
								boolean notCardOrCardAndGreaterThan2K = true;
								double payAmount = (transactionDetail.getTransactionAmountLong()/10000.0);
								if(payAmount<=AdminConstants.NO_GST_CARD_LIMIT && isCardMnemonic(transactionDetail.getTomerchantRateTypeMnemonic())){
									notCardOrCardAndGreaterThan2K = false;
								}
								boolean isAmexCardTransactionInclusive = true;
								isAmexCardTransactionInclusive = isAmexCardTransactionInclusive(transactionDetail);
								if(isAmexCardTransactionInclusive || notCardOrCardAndGreaterThan2K){
									//a+a*(rate/100)=c
									//a(1+rate/100)=c
									//a=c/(1+rate/100)
									surcharge_charge = AmountUtil.roundTwoDecimal(surcharge_charge_inclusive/(1+(transactionDetail.getTaxMaster().getTotalServiceTaxInPercentage().doubleValue()/100.0)));
									surcharge_service_tax = AmountUtil.roundTwoDecimal(surcharge_charge_inclusive - surcharge_charge); //Service Tax
									transactionDetail.setTransactionCharges(Data.lpad(String.valueOf(AmountUtil.getLongAmountPs(surcharge_charge)), '0', 16));
									transactionDetail.setTransactionServiceTax(Data.lpad(String.valueOf(AmountUtil.getLongAmountPs(surcharge_service_tax)), '0', 16));
								}else{
									transactionDetail.setGstAppliedSurcharge("N");
								}
							}

						}
						//For NET billers whose card transaction is upto 2K
						if(AmountUtil.hasValue(transactionDetail.getTomerchantCharges())){
							double payAmount = (transactionDetail.getTransactionAmountLong()/10000.0);
							if(payAmount<=AdminConstants.NO_GST_CARD_LIMIT && isCardMnemonic(transactionDetail.getTomerchantRateTypeMnemonic())){
								transactionDetail.setGstAppliedNet("N");
							}
						}

					}catch (Exception e){					
						logger.info(e.getMessage());
						logger.info("Config_error: Charges issue For Repo Txn Id ["+transactionDetail.getRepoTxnId()+"]");
						configErrorReportBuffer.append("Charges issue For Repo Txn Id :"+transactionDetail.getRepoTxnId()+System.getProperty("line.separator"));
						transactionConfigErrorIdList.add(transactionDetail.getRepoTxnId());
						continue;

					}
					//if there are net charges...
					if(AmountUtil.hasValue(transactionDetail.getTomerchantCharges())) {

						timestampBase = getInvMonthFromDateBase("N", groupMerchant, transactionDetail);
						transactionDetail.setMercNetInvoiceMonth(timestampBase);
					}

					//Check if invoice has to be excluded
					//logger.info("Kamal debug ["+transactionDetail.getMercInvRateTypeMaster().getRateTypeMnemonic()+"]["+transactionDetail.getMercInvRateTypeMaster().getInvExclude()+"]["+transactionDetail.getMercInvRateTypeMaster().getInvExclude().trim()+"]["+transactionDetail.getTransactionStatus()+"]");
					if ((transactionDetail.getMercInvRateTypeMaster().getInvExclude() != null) && (transactionDetail.getMercInvRateTypeMaster().getInvExclude().trim().equals("F")) && (transactionDetail.getTransactionStatus().equals(AdminConstants.FAILED))) {
						// Failed transaction, not to be included and transaction status is also failed
						// Nullify all the 3 merc months
						// Transaction - not to be included in any invoice but should be present in merc_inv_details for MIS pusposes
						transactionDetail.setMercGrossInvoiceMonth(null);
						transactionDetail.setMercNetInvoiceMonth(null);
						transactionDetail.setMercSurchargeInvoiceMonth(null);
					}

					
					//Check if invoice has to be excluded for Net, Surcharge or Gross
					//This change is for Amex biller right now
					transactionDetail.setNetInvExcluded(merchantRateConfig.getNetInvExcluded());
					transactionDetail.setGrossInvExcluded(merchantRateConfig.getGrossInvExcluded());
					transactionDetail.setSurchargeInvExcluded(merchantRateConfig.getSurchargeInvExcluded());
					
					//CITI Merchant Charges calcuation method will come here from above
					if(groupMerchant.getDetailFormat().equalsIgnoreCase(AdminConstants.CITI_MERCHANT_DETAIL_FORMAT)){
						try {
							calculateAndSetConfigForCITI(transactionDetail, mercInvoiceTotalDetailsHashMap, hashtableCitiMerchantMaster, mercInvRateTypeMasterList,  
									invoiceTotalExtraGroupMerchantList, timestampFromDateBase, dbConnection);
							transactionDetailListSingle.add(transactionDetail);
							transactionIdList.add(transactionDetail.getRepoTxnId());
						} catch(Exception e) {
							logger.info("Exception: calculateAndSetConfigForCITI", e);
							logger.info(e.getMessage());
							transactionConfigErrorIdList.add(transactionDetail.getRepoTxnId());
							continue;
						}
						/*logger.info("CITI Merchant for Group Merchant ID: [" +transactionDetail.getGroupMerchantId()+ "]");
						transactionCitiMerchantIdList.add(transactionDetail.getRepoTxnId());
						continue;*/
					}else{
						transactionDetailListSingle.add(transactionDetail);
						transactionIdList.add(transactionDetail.getRepoTxnId());
					}
				}

				logger.warn("Inserting transactionDetailList, size: [" +transactionDetailListSingle.size()+ "]");
				insertInvoiceDetails(transactionDetailListSingle, hashtableGroupMerchant, sipConfMasterHash, dbConnection);
				logger.warn("Finished transactionDetailList, size: [" +transactionDetailListSingle.size()+ "]");

				logger.warn("Updating MercInvoiceStatus A, size: [" +transactionIdList.size()+ "]");
				invoiceDefinition.updateMercInvoiceStatus("A", transactionIdList, dbConnection);
				logger.warn("Finished MercInvoiceStatus A, size: [" +transactionIdList.size()+ "]");

				if(transactionConfigErrorIdList.size()>0){
					logger.warn("Updating MercInvoiceStatus L, size: [" +transactionConfigErrorIdList.size()+ "]");
					invoiceDefinition.updateMercInvoiceStatus("L", transactionConfigErrorIdList, dbConnection);
					logger.warn("Finished MercInvoiceStatus L, size: [" +transactionConfigErrorIdList.size()+ "]");
				}

				if(transactionNotValidIdList.size()>0){
					logger.warn("Updating MercInvoiceStatus X, size: [" +transactionNotValidIdList.size()+ "]");
					invoiceDefinition.updateMercInvoiceStatus("X", transactionNotValidIdList, dbConnection);
					logger.warn("Finished MercInvoiceStatus X, size: [" +transactionNotValidIdList.size()+ "]");
				}
				if(transactionCardPayUBPTypeList.size()>0){
					logger.warn("Updating MercInvoiceStatus V, size: [" +transactionCardPayUBPTypeList.size()+ "]");
					invoiceDefinition.updateMercInvoiceStatus("V", transactionCardPayUBPTypeList, dbConnection);
					logger.warn("Finished MercInvoiceStatus V, size: [" +transactionCardPayUBPTypeList.size()+ "]");
				}

				if(transactionCitiMerchantIdList.size()>0){
					logger.warn("Updating MercInvoiceStatus B, size: [" +transactionCitiMerchantIdList.size()+ "]");
					invoiceDefinition.updateMercInvoiceStatus("B", transactionCitiMerchantIdList, dbConnection);
					logger.warn("Finished MercInvoiceStatus B, size: [" +transactionCitiMerchantIdList.size()+ "]");
				}
				
				if(transactionProcessingErrorList.size()>0){
					logger.warn("Updating transactionProcessingList P, size: [" +transactionProcessingErrorList.size()+ "]");
					invoiceDefinition.updateMercInvoiceStatus("P", transactionProcessingErrorList, dbConnection);
					logger.warn("Finished transactionProcessingList P, size: [" +transactionProcessingErrorList.size()+ "]");
				}
				if(transactionTaxesNotValidIdList.size()>0){
					logger.warn("Updating transactionTaxesNotValidIdList T, size: [" +transactionTaxesNotValidIdList.size()+ "]");
					invoiceDefinition.updateMercInvoiceStatus("T", transactionTaxesNotValidIdList, dbConnection);
					logger.warn("Finished transactionTaxesNotValidIdList T, size: [" +transactionTaxesNotValidIdList.size()+ "]");
				}

				dbConnection.commit();
				try {
					subject = "Invoice proccess successfully executed for "+dateStr+" on "+new Date()+" with count = "+count;
					messageBody = "Invoice proccess successfully executed for "+dateStr+" on "+new Date()+" with count = "+count;
					SendMailMessage.sendMailMsg("victor@billdesk.com|kishor.chauhan@billdesk.com|rahul.dere@billdesk.com|amir.sheikh@billdesk.com|shreechand.rawat@billdesk.com|sathaiah.g@billdesk.com", subject, null, messageBody);	
					//SendMailMessage.sendMailMsg("kishore.chauhan@gmail.com", subject, null, messageBody);
				} catch (Exception e) {
					e.printStackTrace();
				}
				count++;
			} while (countTotal >= count);					
		} catch (Exception e) {

			logger.error("Exception: ", e);

		} finally {
			//transactionDetailRefundList = null;
			try {
				invoiceDefinition.updateMercInvStatusFromTo("L", "R", dbConnection);
				dbConnection.commit();
			} catch (Exception e2) {
				logger.error("Exception updateMercInvStatusFromTo: ", e2);
			}
			
			DBUtility.rollbackConnection(dbConnection);
			DBUtility.closeConnection(dbConnection);
			
			try {
				if(configErrorReportBuffer.length()>0){
					configErrorReportBuffer = stripDuplicates(configErrorReportBuffer.toString());
					CreateTxnReport.createConfigErrorReport(configErrorReportBuffer, "Config_Error");
				}
			} catch (Exception e2) {
				logger.error("Exception configErrorReportBuffer: ", e2);
			}
			
			try {
				if(zeroChargesReportBuffer.length()>0){
					zeroChargesReportBuffer = stripDuplicates(zeroChargesReportBuffer.toString());
					zeroChargesReportBuffer.insert(0, "SYSTEM_ID~MERCHANT_ID~BANK_ID~GROUP_MERCHANT_ID~DEFAULT_INVOIVE_TYPE~TRANSACTION_STATUS~MERC_INV_RATE_TYPE_MNEMONIC~TOMERCHANT_RATE_TYPE_MNEMONIC"+System.getProperty("line.separator"));
					CreateTxnReport.createConfigErrorReport(zeroChargesReportBuffer, "Zero_Charges");
				}
			} catch (Exception e2) {
				logger.error("Exception zeroChargesReportBuffer: ", e2);
			}	
		}

	}


	private void insertInvoiceDetails(ArrayList<TransactionDetail> transactionDetailList, HashMap<String, GroupMerchant> hashtableGroupMerchant, HashMap<String, SipConfMaster> sipConfMasterHash, Connection dbConnection) throws Exception {

		try {

			SequenceSingleton sequenceSingleton = SequenceSingleton.getInstance();

			String seqName = AdminConstants.MERC_INVOICE_DETAILS_ID;

			String[] seqArray = sequenceSingleton.getSequenceArray(seqName, transactionDetailList.size(), true);

			logger.warn("Starting getInvoiceDetailsList...");
			ArrayList invoiceDetailsList = getInvoiceDetailsList(transactionDetailList, seqArray, hashtableGroupMerchant, sipConfMasterHash, dbConnection);		
			logger.warn("Ending getInvoiceDetailsList...");

			invoiceDefinition.insertMercInvoiceDetails(invoiceDetailsList, dbConnection);

		} finally {
		}
	}

	private Timestamp getTimestampFromDateBase(String invoiceType, GroupMerchant groupMerchant, TransactionDetail transactionDetail) throws Exception{

		Timestamp date = null;
		Timestamp date1 = null;

		if(transactionDetail.getTransactionTimestamp().length()==8){
			date1 = DateUtil.getUDFTimestamp(transactionDetail.getTransactionTimestamp(),"yyyyMMdd");
		}else if(transactionDetail.getTransactionTimestamp().length()==14){
			date1 = DateUtil.getUDFTimestamp(transactionDetail.getTransactionTimestamp(),"yyyyMMddHHmmss");
		}

		Timestamp date2 = transactionDetail.getMerchantTidDate();
		Timestamp reconDate = transactionDetail.getDebitFileDate();
		
		if(invoiceType.equalsIgnoreCase("N")) {
			
			if(groupMerchant.getInvNetDateBase().equalsIgnoreCase(AdminConstants.COLUMN_TRANSACTION_TIMESTAMP)) {
				date = date1;
			} else if(groupMerchant.getInvNetDateBase().equalsIgnoreCase(AdminConstants.COLUMN_MERCHANT_TID_DATE)) {
				date = date2;
			} else if(groupMerchant.getInvNetDateBase().startsWith(AdminConstants.COLUMN_TRANSACTION_RECON_DATE)) {
				
				if(transactionDetail.getSystemId().equalsIgnoreCase(AdminConstants.SYSTEM_ID_CARDPAY)) {
					date = reconDate;
				} else {
				
					int idx = groupMerchant.getInvNetDateBase().indexOf("~");
					if(idx==-1) {
						date = reconDate;
					} else {
						String alternativeBase = groupMerchant.getInvNetDateBase().substring(idx+1);
						if(alternativeBase.equalsIgnoreCase(AdminConstants.COLUMN_TRANSACTION_TIMESTAMP)){
							date = date1;
						} else if(alternativeBase.equalsIgnoreCase(AdminConstants.COLUMN_MERCHANT_TID_DATE)) {
							date = date2;
						} else {
							configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - N"+System.getProperty("line.separator"));
							throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - N ");
						}
					}
				}
			} else {
				configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - N"+System.getProperty("line.separator"));
				throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - N ");
			}			
			
		} else if(invoiceType.equalsIgnoreCase("G")) {
			if(groupMerchant.getInvGrossDateBase().equalsIgnoreCase(AdminConstants.COLUMN_TRANSACTION_TIMESTAMP)) {
				date = date1;
			} else if(groupMerchant.getInvGrossDateBase().equalsIgnoreCase(AdminConstants.COLUMN_MERCHANT_TID_DATE)) {
				date = date2;
			} else if(groupMerchant.getInvGrossDateBase().startsWith(AdminConstants.COLUMN_TRANSACTION_RECON_DATE)) {
				
				if(transactionDetail.getSystemId().equalsIgnoreCase(AdminConstants.SYSTEM_ID_CARDPAY)) {
					date = reconDate;
				} else {
				
					int idx = groupMerchant.getInvGrossDateBase().indexOf("~");
					if(idx==-1) {
						date = reconDate;
					} else {
						String alternativeBase = groupMerchant.getInvGrossDateBase().substring(idx+1);
						if(alternativeBase.equalsIgnoreCase(AdminConstants.COLUMN_TRANSACTION_TIMESTAMP)){
							date = date1;
						} else if(alternativeBase.equalsIgnoreCase(AdminConstants.COLUMN_MERCHANT_TID_DATE)) {
							date = date2;
						} else {
							configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - G"+System.getProperty("line.separator"));
							throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - G ");
						}
					}
				}
			} else {
				configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - G"+System.getProperty("line.separator"));
				throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - G ");
				
			}
		} else if(invoiceType.equalsIgnoreCase("S")) {
			
			if(groupMerchant.getInvSurchargeDateBase().equalsIgnoreCase(AdminConstants.COLUMN_TRANSACTION_TIMESTAMP)){
				date = date1;
			}else if(groupMerchant.getInvSurchargeDateBase().equalsIgnoreCase(AdminConstants.COLUMN_MERCHANT_TID_DATE)){
				date = date2;
			}else if(groupMerchant.getInvSurchargeDateBase().startsWith(AdminConstants.COLUMN_TRANSACTION_RECON_DATE)) {
				
				
				if(transactionDetail.getSystemId().equalsIgnoreCase(AdminConstants.SYSTEM_ID_CARDPAY)) {
					date = reconDate;
				} else {
				
					int idx = groupMerchant.getInvSurchargeDateBase().indexOf("~");
					if(idx==-1) {
						date = reconDate;
					} else {
						String alternativeBase = groupMerchant.getInvSurchargeDateBase().substring(idx+1);
						if(alternativeBase.equalsIgnoreCase(AdminConstants.COLUMN_TRANSACTION_TIMESTAMP)){
							date = date1;
						} else if(alternativeBase.equalsIgnoreCase(AdminConstants.COLUMN_MERCHANT_TID_DATE)) {
							date = date2;
						} else {
							configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - S"+System.getProperty("line.separator"));
							throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - S ");
						}
						
					}
				}
			} else{
				configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - S"+System.getProperty("line.separator"));
				throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - S ");
			}
		}else{
			configErrorReportBuffer.append("Invalid Invoice Type: ["+invoiceType+"]"+System.getProperty("line.separator"));
			throw new Exception("Config_error: Invalid Invoice Type: ["+invoiceType+"]");
		}
		if(date==null){
			configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for  Repo Txn Id ["+transactionDetail.getRepoTxnId()+"]"+System.getProperty("line.separator"));
			throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for Repo Txn Id ["+transactionDetail.getRepoTxnId()+"]");
		}
		return date;
	}

	private Timestamp getInvMonthFromDateBase(String invoiceType, GroupMerchant groupMerchant, TransactionDetail transactionDetail) throws Exception{

		Timestamp date = null;
		String[] array2to1, array31to30, array3to2, arraySunEx;
		List<String> list2to1, list31to30, list3to2, listSunEx;
		boolean isTidBase = true;

		try {
			//logger.info("HERE...");
			String datestr = transactionDetail.getTransactionTimestamp().substring(0, 6);

			Timestamp date1 = DateUtil.getUDFTimestamp(datestr,"yyyyMM");
			Timestamp date2 = DateUtil.getUDFTimestamp(DateUtil.getUDFDateString(transactionDetail.getMerchantTidDate(), "yyyyMM"),"yyyyMM");
			
			Timestamp reconDate = null;
			if(transactionDetail.getDebitFileDate()!=null) {
				reconDate = DateUtil.getUDFTimestamp(DateUtil.getUDFDateString(transactionDetail.getDebitFileDate(), "yyyyMM"),"yyyyMM");
			}
			
			
			if(invoiceType.equalsIgnoreCase("N")) {
				
				if(groupMerchant.getInvNetDateBase().equalsIgnoreCase(AdminConstants.COLUMN_TRANSACTION_TIMESTAMP)){
					date = date1;
					isTidBase = false;
				} else if(groupMerchant.getInvNetDateBase().equalsIgnoreCase(AdminConstants.COLUMN_MERCHANT_TID_DATE)) {
					date = date2;
				} else if(groupMerchant.getInvNetDateBase().startsWith(AdminConstants.COLUMN_TRANSACTION_RECON_DATE)) {
					
					if(transactionDetail.getSystemId().equalsIgnoreCase(AdminConstants.SYSTEM_ID_CARDPAY)) {
						date = reconDate;
					} else {
					
						int idx = groupMerchant.getInvNetDateBase().indexOf("~");
						if(idx==-1) {
							date = reconDate;
						} else {
							String alternativeBase = groupMerchant.getInvNetDateBase().substring(idx+1);
							if(alternativeBase.equalsIgnoreCase(AdminConstants.COLUMN_TRANSACTION_TIMESTAMP)){
								date = date1;
								isTidBase = false;
							} else if(alternativeBase.equalsIgnoreCase(AdminConstants.COLUMN_MERCHANT_TID_DATE)) {
								date = date2;
							} else {
								configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - N"+System.getProperty("line.separator"));
								throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - N ");
							}
							
						}
					}
					
				} else {
					configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - N"+System.getProperty("line.separator"));
					throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - N ");
				}
				
			} else if(invoiceType.equalsIgnoreCase("G")) {
				
				if(groupMerchant.getInvGrossDateBase().equalsIgnoreCase(AdminConstants.COLUMN_TRANSACTION_TIMESTAMP)) {
					date = date1;
					isTidBase = false;
				} else if(groupMerchant.getInvGrossDateBase().equalsIgnoreCase(AdminConstants.COLUMN_MERCHANT_TID_DATE)) {
					date = date2;
				} else if(groupMerchant.getInvGrossDateBase().startsWith(AdminConstants.COLUMN_TRANSACTION_RECON_DATE)) {
					
					if(transactionDetail.getSystemId().equalsIgnoreCase(AdminConstants.SYSTEM_ID_CARDPAY)) {
						date = reconDate;
					} else {
						//logger.info("HERE...date 1: [" +groupMerchant.getInvGrossDateBase()+ "]");					
						int idx = groupMerchant.getInvGrossDateBase().indexOf("~");
						if(idx==-1) {
							//logger.info("HERE...date 1.1 idx: [" +idx+ "]");
							date = reconDate;
						} else {
							String alternativeBase = groupMerchant.getInvGrossDateBase().substring(idx+1);
							//logger.info("HERE...date 1.2 alternativeBase: [" +alternativeBase+ "]");

							if(alternativeBase.equalsIgnoreCase(AdminConstants.COLUMN_TRANSACTION_TIMESTAMP)){
								date = date1;
								isTidBase = false;
							} else if(alternativeBase.equalsIgnoreCase(AdminConstants.COLUMN_MERCHANT_TID_DATE)) {
								date = date2;
							} else {
								configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - G"+System.getProperty("line.separator"));
								throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - G ");
							}
							//logger.info("HERE...date 1.3 alternativeBase: [" +alternativeBase+ "]");
						}
					}
					//logger.info("HERE...date 2: [" +date+ "]");
					
				} else {
					configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - G"+System.getProperty("line.separator"));
					throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - G ");
				}
				
			} else if(invoiceType.equalsIgnoreCase("S")) {
				
				if(groupMerchant.getInvSurchargeDateBase().equalsIgnoreCase(AdminConstants.COLUMN_TRANSACTION_TIMESTAMP)) {
					date = date1;
					isTidBase = false;
				} else if(groupMerchant.getInvSurchargeDateBase().equalsIgnoreCase(AdminConstants.COLUMN_MERCHANT_TID_DATE)) {
					date = date2;
				} else if(groupMerchant.getInvSurchargeDateBase().startsWith(AdminConstants.COLUMN_TRANSACTION_RECON_DATE)) {
					
					
					if(transactionDetail.getSystemId().equalsIgnoreCase(AdminConstants.SYSTEM_ID_CARDPAY)) {
						date = reconDate;
					} else {
					
						int idx = groupMerchant.getInvSurchargeDateBase().indexOf("~");
						if(idx==-1) {
							date = reconDate;
						} else {
							String alternativeBase = groupMerchant.getInvSurchargeDateBase().substring(idx+1);
							if(alternativeBase.equalsIgnoreCase(AdminConstants.COLUMN_TRANSACTION_TIMESTAMP)){
								date = date1;
								isTidBase = false;
							} else if(alternativeBase.equalsIgnoreCase(AdminConstants.COLUMN_MERCHANT_TID_DATE)) {
								date = date2;
							} else {
								configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - S"+System.getProperty("line.separator"));
								throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - S ");
							}
							
						}
					}
				} else {
					configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - S"+System.getProperty("line.separator"));
					throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for invoiceType - S ");
				}
				
			} else {
				configErrorReportBuffer.append("Invalid Invoice Type: ["+invoiceType+"]"+System.getProperty("line.separator"));
				throw new Exception("Config_error: Invalid Invoice Type: ["+invoiceType+"]");
			}
			
			if(date==null) {
				configErrorReportBuffer.append("Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for Repo Txn Id ["+transactionDetail.getRepoTxnId()+"]"+System.getProperty("line.separator"));
				throw new Exception("Config_error: Invoice Date Base is not proper for GroupMerchant Id ["+groupMerchant.getGroupMerchantId()+"] for Repo Txn Id ["+transactionDetail.getRepoTxnId()+"]");
			}
			
			//logger.info("HERE...date: [" +date+ "]");

			//Include transactions happening on 1st of month to previuos month
			array2to1 = AdminConstants.GROUP_MERCHANT_2_TO_1_BILLING_ARRAY;
			list2to1 = Arrays.asList(array2to1);

			//Include transactions happening on last day of previuos month to this month
			//array31to30 = AdminConstants.GROUP_MERCHANT_30_TO_29_BILLING_ARRAY;
			//list31to30 = Arrays.asList(array31to30);

			//Include transactions happening on 2nd of month to previuos month
			array3to2 = AdminConstants.GROUP_MERCHANT_3_TO_2_BILLING_ARRAY;
			list3to2 = Arrays.asList(array3to2);

			//Include transactions happening on 2nd of month to previuos month if 1st is Sunday
			arraySunEx = AdminConstants.GROUP_MERCHANT_WO_SUN_BILLING_ARRAY;
			listSunEx = Arrays.asList(arraySunEx);

			if(list2to1.contains(groupMerchant.getGroupMerchantId()) && (invoiceType.equalsIgnoreCase("G"))) {

				if(!isTidBase){
					datestr = transactionDetail.getTransactionTimestamp().substring(0, 8);
				} else {
					datestr = DateUtil.getUDFDateString(transactionDetail.getMerchantTidDate(), "yyyyMMdd");
				}
				// Check if its first or if its second, then if 1st is monday and its in list of sunday excluded
				if((datestr.substring(6, 8).equals("01")) || (datestr.substring(6, 8).equals("02") 
						&& DateUtil.isMonday(datestr) && listSunEx.contains(transactionDetail.getGroupMerchantId()))){
					date = DateUtil.addMonth2(DateUtil.getUDFDateString(date, "yyyyMMdd"), -1);
				}
				
			} else if(list3to2.contains(groupMerchant.getGroupMerchantId()) && (invoiceType.equalsIgnoreCase("G"))) {
				
				if(!isTidBase) {
					datestr = transactionDetail.getTransactionTimestamp().substring(0, 8);
				} else {
					datestr = DateUtil.getUDFDateString(transactionDetail.getMerchantTidDate(), "yyyyMMdd");
				}
				
				if((datestr.substring(6, 8).equals("01")) || (datestr.substring(6, 8).equals("02"))) {
					date = DateUtil.addMonth2(DateUtil.getUDFDateString(date, "yyyyMMdd"), -1);
				}
			} /*else if(list31to30.contains(groupMerchant.getGroupMerchantId()) && (invoiceType.equalsIgnoreCase("G"))) {
				
				if(!isTidBase) {
					datestr = transactionDetail.getTransactionTimestamp().substring(0, 8);
				} else {
					datestr = DateUtil.getUDFDateString(transactionDetail.getMerchantTidDate(), "yyyyMMdd");
				}
				if(Integer.parseInt(datestr.substring(6, 8)) == DateUtil.getLastDayOfMonth(datestr)) {
					date = DateUtil.addMonth2(DateUtil.getUDFDateString(date, "yyyyMMdd"), 1);
				}
			}*/

			return date;

		} finally {
			
			array2to1 = null;
			array31to30 = null;
			array3to2 = null;
			arraySunEx = null;

			list2to1 = null;
			list31to30 = null;
			list3to2 = null;
			listSunEx = null;
		}
	}


	private ArrayList getInvoiceDetailsList(ArrayList<TransactionDetail> transactionDetailList,  String[] seqArray, 
			HashMap<String, GroupMerchant> hashtableGroupMerchant, HashMap<String, SipConfMaster> sipConfMasterHash, 
			Connection dbConnection) throws Exception {

		ArrayList invoiceDetailsList = new ArrayList();

		TransactionDetail transactionDetail;
		MercInvDetails invoiceDetails;
		GroupMerchant groupMerchant;
		boolean fixedCharges;
		String invoiceDetailsId;
		String minTxn, maxTxn, minAmt, maxAmt, slabDescription, slabRate, schemeCode, micrCode, micrBranchNameStr;

		MicrBranchName micrBranchName;

		MICRDatabase micrDatabase = MICRDatabase.getInstance();
		HashMap<String, MicrBranchName> micrBranchNameHash = micrDatabase.getMicrBranchNameHash();

		//Timestamp invoiceMonth = DateUtil.getUDFTimestamp(AdminConstants.INVOICE_MONTH,"yyyyMM");
		
		Timestamp invoiceMonth = DateUtil.getUDFTimestamp(AdminConstants.TRANSACTION_DETAILS.replace("TRANSACTION_DETAILS_", ""),"MMMyy");
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(invoiceMonth.getTime());
		calendar.add(Calendar.MONTH, -1);
		invoiceMonth.setTime(calendar.getTimeInMillis());
		
		logger.info("INVOICE_MONTH ="+invoiceMonth);
		
		int cutoffMonthDatePlusOne = AdminConstants.CUTOFF_MONTH_DATE_PLUS_ONE;

		Calendar calendarNow = Calendar.getInstance();	
		
		Calendar calendarInvoiceMonth = Calendar.getInstance();
		calendarInvoiceMonth.setTimeInMillis(invoiceMonth.getTime());
		
		Timestamp invoiceMonthDefault = invoiceMonth;
		
		for(int txnCount=0; txnCount<transactionDetailList.size(); txnCount++) {

			transactionDetail = (TransactionDetail) transactionDetailList.get(txnCount);

			calendarNow.setTimeInMillis(transactionDetail.getCreatedOn().getTime());			
			
			invoiceMonthDefault = invoiceMonth;
			
			int dayInt = calendarNow.get(Calendar.DATE);
			
			if(dayInt>cutoffMonthDatePlusOne && calendarNow.equals(calendarInvoiceMonth)){

			}else if(dayInt<=cutoffMonthDatePlusOne && calendarNow.before(calendarInvoiceMonth)){

			}else if(dayInt>cutoffMonthDatePlusOne && calendarNow.after(calendarInvoiceMonth)){
				invoiceMonthDefault =DateUtil.getUDFTimestamp(DateUtil.getUDFDateString(new Timestamp(calendarNow.getTimeInMillis()),"yyyyMM"),"yyyyMM");
			}
			
			//logger.info("INVOICE_MONTH Final ="+invoiceMonthDefault);
			
			groupMerchant = hashtableGroupMerchant.get(transactionDetail.getGroupMerchantId());

			invoiceDetails = new MercInvDetails();

			invoiceDetailsId = seqArray[txnCount];
			invoiceDetails.setMercInvDetailsId(invoiceDetailsId);
			if(transactionDetail.getMercGrossInvoiceMonth() != null){
				invoiceDetails.setFiller1(DateUtil.getDateFromTimestamp(transactionDetail.getMercGrossInvoiceMonth()));
				if(transactionDetail.getMercGrossInvoiceMonth().after(invoiceMonthDefault)){
					invoiceDetails.setMercGrossInvoiceMonth(transactionDetail.getMercGrossInvoiceMonth());
				}else{
					invoiceDetails.setMercGrossInvoiceMonth(invoiceMonthDefault);
				}				
			}
			
			if(transactionDetail.getMercNetInvoiceMonth() != null){
				invoiceDetails.setFiller2(DateUtil.getDateFromTimestamp(transactionDetail.getMercNetInvoiceMonth()));
				if(transactionDetail.getMercNetInvoiceMonth().after(invoiceMonthDefault)){
					invoiceDetails.setMercNetInvoiceMonth(transactionDetail.getMercNetInvoiceMonth());
				}else{
					invoiceDetails.setMercNetInvoiceMonth(invoiceMonthDefault);
				}
			}
			
			if(transactionDetail.getMercSurchargeInvoiceMonth() != null){
				invoiceDetails.setFiller3(DateUtil.getDateFromTimestamp(transactionDetail.getMercSurchargeInvoiceMonth()));
				if(transactionDetail.getMercSurchargeInvoiceMonth().after(invoiceMonthDefault)){
					invoiceDetails.setMercSurchargeInvoiceMonth(transactionDetail.getMercSurchargeInvoiceMonth());
				}else{
					invoiceDetails.setMercSurchargeInvoiceMonth(invoiceMonthDefault);
				}
			}
			//For Reurn Cases Only
			/*invoiceDetails.setMercGrossInvoiceMonth(transactionDetail.getMercGrossInvoiceMonth());
			invoiceDetails.setMercNetInvoiceMonth(transactionDetail.getMercNetInvoiceMonth());
			invoiceDetails.setMercSurchargeInvoiceMonth(transactionDetail.getMercSurchargeInvoiceMonth());*/
			
			invoiceDetails.setGroupMerchantId(transactionDetail.getGroupMerchantId());
			invoiceDetails.setSystemId(transactionDetail.getSystemId());
			invoiceDetails.setMerchantId(transactionDetail.getMerchantId());
			invoiceDetails.setBankId(transactionDetail.getBankId());
			invoiceDetails.setSubBankId(transactionDetail.getPayeeBankId());
			invoiceDetails.setProductId(transactionDetail.getProductCode());
			invoiceDetails.setMerchantIdBank(transactionDetail.getMerchantIdBank());
			invoiceDetails.setMeCode(Data.NullToNA(transactionDetail.getMeCode()));
			invoiceDetails.setSubMerchantId(transactionDetail.getSubMerchantId());
			invoiceDetails.setRepoTxnId(transactionDetail.getRepoTxnId());
			invoiceDetails.setTransactionId(transactionDetail.getTransactionId());
			invoiceDetails.setTransactionTimestamp(transactionDetail.getTransactionTimestamp());
			invoiceDetails.setTransactionAmount(transactionDetail.getTransactionAmount());
			invoiceDetails.setTransactionCurrency(transactionDetail.getTransactionCurrency());
			invoiceDetails.setTransactionStatus(transactionDetail.getTransactionStatus());
			invoiceDetails.setTransactionType(transactionDetail.getTransactionType());
			invoiceDetails.setMerchantTidReport(transactionDetail.getMerchantTidReport());
			invoiceDetails.setMerchantTidDate(transactionDetail.getMerchantTidDate());

			invoiceDetails.setP3File(transactionDetail.getP3File());
			//logger.info("transactionDetail, RepoTxnId: [" +transactionDetail.getRepoTxnId()+ "], MercInvRateTypeMaster: [" +transactionDetail.getMercInvRateTypeMaster()+ "]");
			if(transactionDetail.getMercInvRateTypeMaster() != null){
				invoiceDetails.setRateTypeNarration(transactionDetail.getMercInvRateTypeMaster().getRateTypeNarration());
				invoiceDetails.setSlabType(transactionDetail.getMercInvRateTypeMaster().getSlabType());
	
				slabDescription="";
				if(transactionDetail.getMercInvRateTypeMaster().getSlabType().equals("N")) {
	
					minTxn = transactionDetail.getMercInvRateTypeMaster().getMinTxn();
					maxTxn = transactionDetail.getMercInvRateTypeMaster().getMaxTxn();
					slabDescription=minTxn+" < Txn No < "+maxTxn;
	
				} else {
	
					minAmt = transactionDetail.getMercInvRateTypeMaster().getMinAmount();
					maxAmt = transactionDetail.getMercInvRateTypeMaster().getMaxAmount();
					if(maxAmt.equalsIgnoreCase("9999999999999999")){
						slabDescription = Double.parseDouble(minAmt)/100+" < Txn Amt";
					}else{
						slabDescription = Double.parseDouble(minAmt)/100+" < Txn Amt < "+Double.parseDouble(maxAmt)/100;
					}
				}
	
				invoiceDetails.setSlabDescription(slabDescription);
	
				fixedCharges = false;
				slabRate="NA";
				if (AmountUtil.hasValue(transactionDetail.getMercInvRateTypeMaster().getFixChargesC())) {
	
					slabRate = AmountUtil.getAmountInRsPs(transactionDetail.getMercInvRateTypeMaster().getFixChargesC());
					fixedCharges = true;
				}
	
				if (AmountUtil.hasValue(transactionDetail.getMercInvRateTypeMaster().getPerChargesC())) {
	
					if(fixedCharges) {
	
						slabRate = slabRate + ", " +transactionDetail.getMercInvRateTypeMaster().getPerChargesC()+"%";
	
					} else {
	
						slabRate = transactionDetail.getMercInvRateTypeMaster().getPerChargesC()+"%";
					}
				}
	
				invoiceDetails.setSlabRate(slabRate);
	
				invoiceDetails.setEcsSuccessRate(transactionDetail.getMercInvRateTypeMaster().getEcsSuccessCharge());
	
				invoiceDetails.setEcsfailureRate(transactionDetail.getMercInvRateTypeMaster().getEcsFailureCharge());
			}
			schemeCode = InvoiceHelper.getTransactionScheme(transactionDetail);
			invoiceDetails.setSchemeCode(schemeCode);

			micrCode=null;

			if(transactionDetail.getTxnCustDetails()!=null) {

				micrCode = transactionDetail.getTxnCustDetails().getMicrCode();
			}

			micrBranchNameStr=Data.NullToNA(micrCode);

			if(micrBranchNameHash.containsKey(micrCode)) {

				micrBranchName = micrBranchNameHash.get(micrCode);
				if(micrBranchName !=null){

					micrBranchNameStr = micrBranchName.getBranchName(); 

				}

			}

			invoiceDetails.setMicrCode(micrCode);
			invoiceDetails.setMicrBranchName(micrBranchNameStr);

			InvoiceHelper.getTxnSipDay(sipConfMasterHash, transactionDetail);
			invoiceDetails.setSipDay(transactionDetail.getSip());

			invoiceDetails.setRefund(transactionDetail.getRefundAmount());

			//For Net
			invoiceDetails.setTomerchantRateTypeMnemonic(transactionDetail.getTomerchantRateTypeMnemonic());
			invoiceDetails.setTomerchantCharges(transactionDetail.getTomerchantCharges());
			invoiceDetails.setTomerchantStax(transactionDetail.getTomerchantStax());
			invoiceDetails.setTomerchantTdsAmount(transactionDetail.getTomerchantTdsAmount());
			invoiceDetails.setTomerchantNetamt(transactionDetail.getTomerchantNetamt());
			invoiceDetails.setTomerchantNetamtSign(transactionDetail.getTomerchantNetamtSign());



			//For Gross
			if(transactionDetail.getMercInvRateTypeMaster() != null){
				invoiceDetails.setMercInvRateTypeId(transactionDetail.getMercInvRateTypeMaster().getRateTypeId());
				invoiceDetails.setMercInvRateTypeMnemonic(transactionDetail.getMercInvRateTypeMaster().getRateTypeMnemonic());
				invoiceDetails.setMercInvRateTypeNarration(transactionDetail.getMercInvRateTypeMaster().getRateTypeNarration());
				invoiceDetails.setSlabParam(transactionDetail.getMercInvRateTypeMaster().getSlabParam());
			}
			invoiceDetails.setMercInvCharges(transactionDetail.getInvoiceTransactionCharges());
			invoiceDetails.setMercInvStax(transactionDetail.getInvoiceTransactionStax());
			invoiceDetails.setMercInvTds(Data.NullToNA(transactionDetail.getInvoiceTransactionTdsAmount()));
			invoiceDetails.setEcsSuccessCharge(transactionDetail.getEcsSuccessCharge());
			invoiceDetails.setEcsFailureCharge(transactionDetail.getEcsFailureCharge());
			invoiceDetails.setEcsStax(transactionDetail.getEcsChargeServiceTax());

			//For Surcharge
			invoiceDetails.setTransactionDiscount(transactionDetail.getTransactionDiscount());
			invoiceDetails.setTransactionCharges(transactionDetail.getTransactionCharges());
			invoiceDetails.setTransactionServiceTax(transactionDetail.getTransactionServiceTax());
			invoiceDetails.setDebitAmount(transactionDetail.getDebitAmount());

			//For CITI Merchants
			if(groupMerchant.getDetailFormat().equalsIgnoreCase(AdminConstants.CITI_MERCHANT_DETAIL_FORMAT)){
				slabDescription = "NA";
				invoiceDetails.setCitiBillingRateToCiti(transactionDetail.getCitiBillingRateToCiti());
				invoiceDetails.setCitiBillingRate1(transactionDetail.getCitiBillingRate1());
				invoiceDetails.setCitiBillingSlab(transactionDetail.getCitiBillingSlab());
				invoiceDetails.setCitiBillingSlabRate(transactionDetail.getCitiBillingSlabRate());
				invoiceDetails.setCitiCharges1(transactionDetail.getCitiCharges1());
				invoiceDetails.setCitiCharges1STax(transactionDetail.getCitiCharges1STax());
				invoiceDetails.setCitiBillingRate2(transactionDetail.getCitiBillingRate2());
				invoiceDetails.setCitiBillingRateNarration(transactionDetail.getCitiBillingRateNarration());
				invoiceDetails.setCitiBillingRateRate(transactionDetail.getCitiBillingRateRate());
				invoiceDetails.setCitiBdRevenueSharingRate(transactionDetail.getCitiBdRevenueSharingRate());
				invoiceDetails.setCitiCharges2(transactionDetail.getCitiCharges2());
				invoiceDetails.setCitiCharges2STax(transactionDetail.getCitiCharges2STax());

				if(transactionDetail.getMercInvRateTypeMasterCITI() != null && transactionDetail.getMercInvRateTypeMasterCITI().getSlabType().equals("N")) {

					minTxn = transactionDetail.getMercInvRateTypeMasterCITI().getMinTxn();
					maxTxn = transactionDetail.getMercInvRateTypeMasterCITI().getMaxTxn();
					slabDescription=minTxn+" < Txn No < "+maxTxn;

				} else if (transactionDetail.getMercInvRateTypeMasterCITI() != null){

					minAmt = transactionDetail.getMercInvRateTypeMasterCITI().getMinAmount();
					maxAmt = transactionDetail.getMercInvRateTypeMasterCITI().getMaxAmount();
					if(maxAmt.equalsIgnoreCase("9999999999999999")){
						slabDescription = Double.parseDouble(minAmt)/100+" < Txn Amt";
					}else{
						slabDescription = Double.parseDouble(minAmt)/100+" < Txn Amt < "+Double.parseDouble(maxAmt)/100;
					}
				}

				invoiceDetails.setCitiBillingSlab(slabDescription);
				fixedCharges = false;
				slabRate="0";
				if (transactionDetail.getMercInvRateTypeMasterCITI() !=null && AmountUtil.hasValue(transactionDetail.getMercInvRateTypeMasterCITI().getFixChargesC())) {

					slabRate = AmountUtil.getAmountInRsPs(transactionDetail.getMercInvRateTypeMasterCITI().getFixChargesC());
					fixedCharges = true;
				}

				if (transactionDetail.getMercInvRateTypeMasterCITI() !=null && AmountUtil.hasValue(transactionDetail.getMercInvRateTypeMasterCITI().getPerChargesC())) {

					if(fixedCharges) {

						slabRate = slabRate + ", " +transactionDetail.getMercInvRateTypeMasterCITI().getPerChargesC()+"%";

					} else {

						slabRate = transactionDetail.getMercInvRateTypeMasterCITI().getPerChargesC()+"%";
					}
				}

				invoiceDetails.setCitiBillingSlabRate(slabRate);
			}
			
			//For Amex type billers
			invoiceDetails.setNetInvExcluded(transactionDetail.getNetInvExcluded());
			invoiceDetails.setGrossInvExcluded(transactionDetail.getGrossInvExcluded());
			invoiceDetails.setSurchargeInvExcluded(transactionDetail.getSurchargeInvExcluded());
			
			invoiceDetails.setStatus(AdminConstants.ACTIVE);
			invoiceDetails.setCreatedOn(DateUtil.getSQLTimeStamp());
			invoiceDetails.setCreatedBy("daemon");
			invoiceDetails.setGstAppliedGross(transactionDetail.getGstAppliedGross());
			invoiceDetails.setGstAppliedNet(transactionDetail.getGstAppliedNet());
			invoiceDetails.setGstAppliedSurcharge(transactionDetail.getGstAppliedSurcharge());
			invoiceDetailsList.add(invoiceDetails);

		}

		return invoiceDetailsList;
	}

	/*private void calculateAndSetConfigForCITI(TransactionDetail transactionDetail, String[] slabKeysArray, String[] rateTyepNarrationKeysArray, HashMap<String, MercInvoiceTotalDetails> mercInvoiceTotalDetailsHashMap, 
			ArrayList<String> invoiceTotalExtraGroupMerchantList, Timestamp timestampFromDateBase, Connection dbConnection) throws Exception{

		String slabTypeRate, rateStr;

		String propertyKey =  transactionDetail.getSystemId()+"_"+transactionDetail.getMerchantId();

		// For Billing Rate to Citi
		String billingRateToCITI = CITIMerchantChargesConfig.getValue(propertyKey+"_"+AdminConstants.BILLING_RATE_TO_CITI_PROP_KEY);
		if(billingRateToCITI != null){
			transactionDetail.setCitiBillingRateToCiti(billingRateToCITI);
		}else{
			throw new Exception("Config_error: Billing rate to citi not found in property file for key : [" +propertyKey+"_"+AdminConstants.BILLING_RATE_TO_CITI_PROP_KEY+ "]");
		}

		// For Billing Rate 1
		billingRateToCITI = CITIMerchantChargesConfig.getValue(propertyKey+"_"+AdminConstants.BILLING_RATE_1_PROP_KEY);
		if(billingRateToCITI != null){
			transactionDetail.setCitiBillingRate1(billingRateToCITI);
		}else{
			throw new Exception("Config_error: Billing rate 1 not found in property file for key : [" +propertyKey+"_"+AdminConstants.BILLING_RATE_1_PROP_KEY+ "]");
		}

		// For Billing Rate 2
		billingRateToCITI = CITIMerchantChargesConfig.getValue(propertyKey+"_"+AdminConstants.BILLING_RATE_2_PROP_KEY);
		if(billingRateToCITI != null){
			transactionDetail.setCitiBillingRate2(billingRateToCITI);
		}else{
			throw new Exception("Config_error: Billing rate 2 not found in property file for key : [" +propertyKey+"_"+AdminConstants.BILLING_RATE_2_PROP_KEY+ "]");
		}

		// For BillDesk Revenue Sharing Rate
		billingRateToCITI = CITIMerchantChargesConfig.getValue(propertyKey+"_"+AdminConstants.BILLDESK_REVENUE_SHARING_RATE_PROP_KEY);
		if(billingRateToCITI != null){
			transactionDetail.setCitiBdRevenueSharingRate(billingRateToCITI);
		}else{
			throw new Exception("Config_error: Billdesk revenue sharing rate not found in property file for key : [" +propertyKey+"_"+AdminConstants.BILLDESK_REVENUE_SHARING_RATE_PROP_KEY+ "]");
		}

		ArrayList<MercInvRateTypeMaster> mercInvRateTypeMasterList = new ArrayList<MercInvRateTypeMaster>();
		if(slabKeysArray != null){
			for (int j = 0; j < slabKeysArray.length; j++) {
				slabTypeRate = CITIMerchantChargesConfig.getValue(propertyKey+"_SLAB_RATE_"+(j+1));
				if(slabTypeRate != null){
					mercInvRateTypeMasterList.add(getMercInvRateTypeMaster(slabTypeRate, propertyKey));
					if(!invRateTypeMaster.getFixChargesC().equals("0000000000000000")){
						rateStr = String.valueOf(Long.parseLong(invRateTypeMaster.getFixChargesC()));
					}else{
						rateStr = null;
					}
					if(!invRateTypeMaster.getPerChargesC().equals("0000000000000000") && rateStr!=null){
						rateStr = rateStr+ ", "+invRateTypeMaster.getPerChargesC();
					}else if(!invRateTypeMaster.getPerChargesC().equals("0000000000000000")){
						rateStr = invRateTypeMaster.getPerChargesC();
					}
					if(rateStr!= null){

					}
				}					
			}
		}

		if(mercInvRateTypeMasterList.size()>0){

			calculateInvoiceCharges(transactionDetail, mercInvRateTypeMasterList, mercInvoiceTotalDetailsHashMap, invoiceTotalExtraGroupMerchantList, 
					timestampFromDateBase, true, dbConnection);

		}

		String billingRateType;

		if(rateTyepNarrationKeysArray != null){
			//For Billing Rate RBI, Non RBI and others
			for (int j = 0; j < rateTyepNarrationKeysArray.length; j++) {
				billingRateType = CITIMerchantChargesConfig.getValue(propertyKey+"_"+rateTyepNarrationKeysArray[j]);
				if(billingRateType != null && transactionDetail.getMercInvRateTypeMaster().getRateTypeNarration().contains("NonRBI") && rateTyepNarrationKeysArray[j].contains("_Non_RBI")){
					transactionDetail.setCitiBillingRateNarration(rateTyepNarrationKeysArray[j]);
					transactionDetail.setCitiBillingRateRate(billingRateType);
				}else if(billingRateType != null && transactionDetail.getMercInvRateTypeMaster().getRateTypeNarration().contains("RBI") && rateTyepNarrationKeysArray[j].contains("_RBI")){
					transactionDetail.setCitiBillingRateNarration(rateTyepNarrationKeysArray[j]);
					transactionDetail.setCitiBillingRateRate(billingRateType);
				}
			}			
			// For charge 2
			calculateCharge2CITI(transactionDetail);
		}

	}*/
	
	
	private void calculateAndSetConfigForCITI(TransactionDetail transactionDetail, HashMap<String, MercInvoiceTotalDetails> mercInvoiceTotalDetailsHashMap, HashMap<String, CitiMerchantMaster> hashtableCitiMerchantMaster, ArrayList<MercInvRateTypeMaster> mercInvRateTypeMasterList,  
			ArrayList<String> invoiceTotalExtraGroupMerchantList, Timestamp timestampFromDateBase, Connection dbConnection) throws Exception{

		String propertyKey =  transactionDetail.getMerchantRateConfig().getRateTypeMnemonic();
		
		CitiMerchantMaster citiMerchantMaster =  hashtableCitiMerchantMaster.get(propertyKey);
		
		if(citiMerchantMaster == null){
			configErrorReportBuffer.append("CitiMerchantMaster not found for mnemonic : [" +propertyKey+"]"+System.getProperty("line.separator"));
			throw new Exception("Config_error: CitiMerchantMaster not found for mnemonic : [" +propertyKey+"]");
		}
		
		// For Billing Rate to Citi
		String billingRateToCITI = citiMerchantMaster.getBillingRateToCiti();
		if(billingRateToCITI != null){
			transactionDetail.setCitiBillingRateToCiti(billingRateToCITI);
			logger.info("billingRateToCITI 1: "+billingRateToCITI);
		}else{
			configErrorReportBuffer.append("Billing rate to citi not found for mnemonic : [" +propertyKey+"]"+System.getProperty("line.separator"));
			throw new Exception("Config_error: Billing rate to citi not found for mnemonic : [" +propertyKey+"]");
		}

		// For Billing Rate 1
		billingRateToCITI = citiMerchantMaster.getBillingRate1();
		if(billingRateToCITI != null){
			transactionDetail.setCitiBillingRate1(billingRateToCITI);
			logger.info("billingRateToCITI 2: "+billingRateToCITI);
		}else{
			configErrorReportBuffer.append("Billing rate 1 not found for mnemonic : [" +propertyKey+"]"+System.getProperty("line.separator"));
			throw new Exception("Config_error: Billing rate 1 not found for mnemonic : [" +propertyKey+"]");
		}

		// For Billing Rate 2
		billingRateToCITI = citiMerchantMaster.getBillingRate2();
		if(billingRateToCITI != null){
			transactionDetail.setCitiBillingRate2(billingRateToCITI);
			logger.info("billingRateToCITI 3: "+billingRateToCITI);
		}else{
			configErrorReportBuffer.append("Billing rate 2 not found for mnemonic : [" +propertyKey+"]"+System.getProperty("line.separator"));
			throw new Exception("Config_error: Billing rate 2 not found for mnemonic : [" +propertyKey+"]");
		}

		// For BillDesk Revenue Sharing Rate
		billingRateToCITI = citiMerchantMaster.getBilldeskRevenueSharingRate();
		if(billingRateToCITI != null){
			transactionDetail.setCitiBdRevenueSharingRate(billingRateToCITI);
			logger.info("billingRateToCITI 4: "+billingRateToCITI);
		}else{
			configErrorReportBuffer.append("Billing revenue sharing rate not found for mnemonic : [" +propertyKey+"]"+System.getProperty("line.separator"));
			throw new Exception("Config_error: Billdesk revenue sharing rate not found for mnemonic : [" +propertyKey+"]");
		}

		if(mercInvRateTypeMasterList.size()>0){

			calculateInvoiceCharges(transactionDetail, mercInvRateTypeMasterList, mercInvoiceTotalDetailsHashMap, invoiceTotalExtraGroupMerchantList, 
					timestampFromDateBase, true, dbConnection);

		}

		if(transactionDetail.getMercInvRateTypeMasterCITI().getRateTypeNarration().contains("NonRBI")){
			transactionDetail.setCitiBillingRateNarration("Billing_Rate_Non_RBI");
			transactionDetail.setCitiBillingRateRate(citiMerchantMaster.getBillingRateNonRbi());
		}else if(transactionDetail.getMercInvRateTypeMasterCITI().getRateTypeNarration().contains("RBI")){
			transactionDetail.setCitiBillingRateNarration("Billing_Rate_RBI");
			transactionDetail.setCitiBillingRateRate(citiMerchantMaster.getBillingRateRbi());
		}
				
		// For charge 2
		calculateCharge2CITI(transactionDetail);		

	}

	private void calculateCharge2CITI(TransactionDetail transactionDetail) throws Exception{

		double charges2      = 0;
		double service_tax      = 0;
		try{
			if(transactionDetail.getCitiBillingRateRate() != null){
	
				charges2 += (Double.parseDouble(transactionDetail.getCitiBillingRateRate()) - Double.parseDouble(transactionDetail.getCitiBillingRate1())- Double.parseDouble(transactionDetail.getCitiBillingRateToCiti()))*Double.parseDouble(transactionDetail.getCitiBdRevenueSharingRate().replace("%", ""))/100+(Double.parseDouble(transactionDetail.getCitiBillingRate2())-Double.parseDouble(transactionDetail.getCitiBillingRate1())- Double.parseDouble(transactionDetail.getCitiBillingRateToCiti()))*Double.parseDouble(transactionDetail.getCitiBdRevenueSharingRate().replace("%", ""))/100;
	
			}else{
	
				charges2 = Double.parseDouble(transactionDetail.getCitiBillingRate2())-Double.parseDouble(transactionDetail.getCitiBillingRate1())- Double.parseDouble(transactionDetail.getCitiBillingRateToCiti())*Double.parseDouble(transactionDetail.getCitiBdRevenueSharingRate().replace("%", ""))/100;
	
			}
			logger.info("calculateCharge2CITI charges2: "+charges2);
			charges2 = AmountUtil.roundFourDecimal(charges2);
			service_tax = AmountUtil.roundFourDecimal(charges2*(transactionDetail.getTaxMaster().getTotalServiceTaxInPercentage().doubleValue()/100)); //Service Tax
			logger.info("calculateCharge2CITI service_tax: "+service_tax);
			long per_charges_long      = AmountUtil.getLongAmountMilliPs(charges2);
			long service_tax_long      = AmountUtil.getLongAmountMilliPs(service_tax);	
	
	
			transactionDetail.setCitiCharges2(Data.lpad(String.valueOf(per_charges_long), '0', 18));
			transactionDetail.setCitiCharges2STax(Data.lpad(String.valueOf(service_tax_long), '0', 18));
		}catch(Exception e){
			logger.info("Exception: calculateCharge2CITI", e);
			e.printStackTrace();
			throw new Exception("Config_error: calculateCharge2CITI ["+e.getMessage()+"]");
		}

	}

	private boolean isConfigInDateBaseRange(MercInvRateTypeMaster invRateTypeMaster, Timestamp timestampDateBase, TransactionDetail transactionDetail) throws Exception {
		boolean isConfigInDateRange = false;

		Timestamp startDate;
		Timestamp endDate;
		try{
			startDate = invRateTypeMaster.getStartDate();
			endDate = invRateTypeMaster.getEndDate();
			Long milisecondsInaDay = Long.valueOf(24 * 60 * 60 * 1000);
			endDate = new Timestamp(endDate.getTime() + milisecondsInaDay);
			//logger.info("startDate: [" +startDate+ "] and endDate: [" +endDate+ "] for mnemonic ["+invRateTypeMaster.getRateTypeMnemonic()+"] and repo_txn_id["+transactionDetail.getRepoTxnId()+"] and timestampDateBase ["+timestampDateBase+"]");
			if((startDate.before(timestampDateBase) || startDate.equals(timestampDateBase))  && endDate.after(timestampDateBase)){
				isConfigInDateRange = true;
			}
		}finally{

		}

		return isConfigInDateRange;
	}

	private void calculateInvoiceCharges(TransactionDetail transactionDetail, ArrayList<MercInvRateTypeMaster> mercInvRateTypeMasterListTotal, 
			HashMap<String, MercInvoiceTotalDetails> mercInvoiceTotalDetailsHashMap, 
			ArrayList<String> invoiceTotalExtraGroupMerchantList, Timestamp timestampDateBase, boolean isMercCITI, Connection dbConnection) throws Exception {

		MercInvoiceTotalDetails mercInvoiceTotalDetails = null;
		boolean isConfigInDateRange = false;
		ArrayList<MercInvRateTypeMaster> mercInvRateTypeMasterList = new ArrayList<MercInvRateTypeMaster>();		

		String invInvRateTypeMnemonic = transactionDetail.getInvRateTypeMnemonic();

		if(isMercCITI){
			invInvRateTypeMnemonic = invInvRateTypeMnemonic+"_FOR_CITI";
		}

		MercInvRateTypeMaster mercInvRateTypeMasterFinal=null;
		
		try {

			MercInvRateTypeMaster mercInvRateTypeMaster;			
			for(int i=0; i<mercInvRateTypeMasterListTotal.size(); i++) {

				mercInvRateTypeMaster = mercInvRateTypeMasterListTotal.get(i);

				isConfigInDateRange = isConfigInDateBaseRange(mercInvRateTypeMaster, timestampDateBase, transactionDetail);

				if(isConfigInDateRange){
					mercInvRateTypeMasterList.add(mercInvRateTypeMaster);
				}

			}

			if(mercInvRateTypeMasterList.size()==0) {
				configErrorReportBuffer.append("No Rate Type Master entries found for mnemonic: [" +invInvRateTypeMnemonic+ "] " +
						"for date ["+timestampDateBase+"]"+System.getProperty("line.separator"));
				throw new Exception("Config_error: No Rate Type Master entries found for mnemonic: [" +invInvRateTypeMnemonic+ "] " +
						"for date ["+timestampDateBase+"]");
			}

			MercInvRateTypeMaster mercInvRateTypeMasterTemp = mercInvRateTypeMasterList.get(0);
			String slabTypeTemp, rateApplicable, slabParam;
			long minAmount, maxAmount;
			int minTxnCount, maxTxnCount;
			long txnAmountLong = Long.parseLong(transactionDetail.getTransactionAmount());


			String slabType = mercInvRateTypeMasterTemp.getSlabType();

			slabParam = "NA";

			if(slabType.equalsIgnoreCase("NA")) {

				if(mercInvRateTypeMasterList.size()>2) {
					configErrorReportBuffer.append("Multiple Rate Type Master entries found for mnemonic: [" +invInvRateTypeMnemonic+ "] " +
							"while slab type is NA..."+System.getProperty("line.separator"));
					throw new Exception("Config_error: Multiple Rate Type Master entries found for mnemonic: [" +invInvRateTypeMnemonic+ "] " +
							"while slab type is NA...");
				}
				for(int i=0; i<mercInvRateTypeMasterList.size(); i++) {

					mercInvRateTypeMaster = mercInvRateTypeMasterList.get(i);
					slabTypeTemp = mercInvRateTypeMaster.getSlabType();
					rateApplicable = mercInvRateTypeMaster.getRateApplicable();

					//just comparing slab types to make sure whether config has any mistake...
					if(!slabTypeTemp.equalsIgnoreCase(slabType)) {
						configErrorReportBuffer.append("Slab Types are not same for mnemonic: [" +invInvRateTypeMnemonic+ "]..."+System.getProperty("line.separator"));
						throw new Exception("Config_error: Slab Types are not same for mnemonic: [" +invInvRateTypeMnemonic+ "]...");
					}

					if(transactionDetail.getTransactionStatus().equals(AdminConstants.SUCCESS)) {

						if(Data.isEmpty(rateApplicable) || rateApplicable.equals("B") || rateApplicable.equals("S")) {

							mercInvRateTypeMasterFinal = mercInvRateTypeMaster;
							break;
						}

					} else if(transactionDetail.getTransactionStatus().equals(AdminConstants.FAILED)) {

						if(rateApplicable.equals("B") || rateApplicable.equals("F")) {

							mercInvRateTypeMasterFinal = mercInvRateTypeMaster;
							break;
						}

					} else {
						configErrorReportBuffer.append("Invalid Transaction Status: [" +transactionDetail.getTransactionStatus()+ "], for " +
								"Transaction Master Id: [" +transactionDetail.getRepoTxnId()+ "]..."+System.getProperty("line.separator"));
						throw new Exception("Config_error: Invalid Transaction Status: [" +transactionDetail.getTransactionStatus()+ "], for " +
								"Transaction Master Id: [" +transactionDetail.getRepoTxnId()+ "]...");
					}
				}

			} else {

				//Get Total...
				int totalTxnCount = 0;
				long totalTxnAmount = 0;

				if(slabType.equalsIgnoreCase("N") || slabType.equalsIgnoreCase("T")) {

					if(!invoiceTotalExtraGroupMerchantList.contains(transactionDetail.getGroupMerchantId()
							+AdminConstants.KEY_CHAR+transactionDetail.getMercGrossInvoiceMonth().getTime())) {

						HashMap<String, MercInvoiceTotalDetails> mercInvoiceTotalDetailsHashMapPrevious = 
								invoiceDefinition.getMercInvoiceTotalDetails(transactionDetail.getGroupMerchantId(), 
										DateUtil.getUDFDateString(transactionDetail.getMercGrossInvoiceMonth(), "yyyyMM"), dbConnection);

						mercInvoiceTotalDetailsHashMap.putAll(mercInvoiceTotalDetailsHashMapPrevious);
						invoiceTotalExtraGroupMerchantList.add(transactionDetail.getGroupMerchantId()+AdminConstants.KEY_CHAR+
								transactionDetail.getMercGrossInvoiceMonth().getTime());
					}

					for(int i=0; i<mercInvRateTypeMasterList.size(); i++) {

						mercInvRateTypeMaster = mercInvRateTypeMasterList.get(i);
						rateApplicable = mercInvRateTypeMaster.getRateApplicable();

						if(transactionDetail.getTransactionStatus().equals(AdminConstants.SUCCESS)) {

							if(Data.isEmpty(rateApplicable) || rateApplicable.equals("B") || rateApplicable.equals("S")) {

								slabParam = mercInvRateTypeMaster.getSlabParam();
								break;

							}
						} else if(transactionDetail.getTransactionStatus().equals(AdminConstants.FAILED)) {

							if(rateApplicable.equals("B") || rateApplicable.equals("F")) {

								slabParam = mercInvRateTypeMaster.getSlabParam();
								break;

							}

						} else {
							configErrorReportBuffer.append("Invalid Transaction Status: [" +transactionDetail.getTransactionStatus()+ "], for " +
									"Transaction Master Id: [" +transactionDetail.getRepoTxnId()+ "]..."+System.getProperty("line.separator"));
							throw new Exception("Config_error: Invalid Transaction Status: [" +transactionDetail.getTransactionStatus()+ "], for " +
									"Transaction Master Id: [" +transactionDetail.getRepoTxnId()+ "]...");
						}
					}

					if(slabParam.equals("NA")){
						configErrorReportBuffer.append("Invalid Slab Param Transaction Status: [" +transactionDetail.getTransactionStatus()+ "], for " +
								"Transaction Master Id: [" +transactionDetail.getRepoTxnId()+ "]..."+System.getProperty("line.separator"));
						throw new Exception("Config_error: Invalid Slab Param Transaction Status: [" +transactionDetail.getTransactionStatus()+ "], for " +
								"Transaction Master Id: [" +transactionDetail.getRepoTxnId()+ "]...");
					}

					mercInvoiceTotalDetails = mercInvoiceTotalDetailsHashMap.get(
							DateUtil.getUDFDateString(transactionDetail.getMercGrossInvoiceMonth(), "yyyyMM")+AdminConstants.KEY_CHAR+
							transactionDetail.getGroupMerchantId()+AdminConstants.KEY_CHAR+
							transactionDetail.getInvRateTypeMnemonic()+AdminConstants.KEY_CHAR+slabParam);

					if(mercInvoiceTotalDetails==null) {
						mercInvoiceTotalDetails = new MercInvoiceTotalDetails();
						mercInvoiceTotalDetails.setGroupMerchantId(transactionDetail.getGroupMerchantId());
						mercInvoiceTotalDetails.setRateTypeMnemonic(transactionDetail.getInvRateTypeMnemonic());

						mercInvoiceTotalDetailsHashMap.put(
								DateUtil.getUDFDateString(transactionDetail.getMercGrossInvoiceMonth(), "yyyyMM")+AdminConstants.KEY_CHAR+
								transactionDetail.getGroupMerchantId()+AdminConstants.KEY_CHAR+
								transactionDetail.getInvRateTypeMnemonic()+AdminConstants.KEY_CHAR+slabParam, mercInvoiceTotalDetails);
					}

					mercInvoiceTotalDetails.addTxn(transactionDetail);

					totalTxnCount = mercInvoiceTotalDetails.getTotalCount();
					totalTxnAmount = mercInvoiceTotalDetails.getTotalAmount();
				}


				if(slabType.equalsIgnoreCase("A")) {

					for(int i=0; i<mercInvRateTypeMasterList.size(); i++) {

						mercInvRateTypeMaster = mercInvRateTypeMasterList.get(i);
						slabTypeTemp = mercInvRateTypeMaster.getSlabType();
						rateApplicable = mercInvRateTypeMaster.getRateApplicable();
						minAmount = Long.parseLong(mercInvRateTypeMaster.getMinAmount())*100; // Modified to make in 100ps
						maxAmount = Long.parseLong(mercInvRateTypeMaster.getMaxAmount())*100; // Modified to make in 100ps

						//just comparing slab types to make sure whether config has any mistake...
						if(!slabTypeTemp.equalsIgnoreCase(slabType)) {
							configErrorReportBuffer.append("Slab Types are not same for mnemonic: [" +invInvRateTypeMnemonic+ "]..."+System.getProperty("line.separator"));
							throw new Exception("Config_error: Slab Types are not same for mnemonic: [" +invInvRateTypeMnemonic+ "]...");
						}

						if(transactionDetail.getTransactionStatus().equals(AdminConstants.SUCCESS)) {

							if(Data.isEmpty(rateApplicable) || rateApplicable.equals("B") || rateApplicable.equals("S")) {

								if(txnAmountLong>minAmount && txnAmountLong<=maxAmount) {

									mercInvRateTypeMasterFinal = mercInvRateTypeMaster;
									break;
								}

							}
						} else if(transactionDetail.getTransactionStatus().equals(AdminConstants.FAILED)) {

							if(rateApplicable.equals("B") || rateApplicable.equals("F")) {

								if(txnAmountLong>minAmount && txnAmountLong<=maxAmount) {

									mercInvRateTypeMasterFinal = mercInvRateTypeMaster;
									break;
								}

							}

						} else {
							
							configErrorReportBuffer.append("Invalid Transaction Status: [" +transactionDetail.getTransactionStatus()+ "], for " +
									"Transaction Master Id: [" +transactionDetail.getRepoTxnId()+ "]..."+System.getProperty("line.separator"));
							throw new Exception("Config_error: Invalid Transaction Status: [" +transactionDetail.getTransactionStatus()+ "], for " +
									"Transaction Master Id: [" +transactionDetail.getRepoTxnId()+ "]...");
						}

					}

				} else if(slabType.equalsIgnoreCase("N")) {

					for(int i=0; i<mercInvRateTypeMasterList.size(); i++) {

						mercInvRateTypeMaster = mercInvRateTypeMasterList.get(i);
						slabTypeTemp = mercInvRateTypeMaster.getSlabType();
						rateApplicable = mercInvRateTypeMaster.getRateApplicable();
						minTxnCount = Integer.parseInt(mercInvRateTypeMaster.getMinTxn());
						maxTxnCount = Integer.parseInt(mercInvRateTypeMaster.getMaxTxn());

						//just comparing slab types to make sure whether config has any mistake...
						if(!slabTypeTemp.equalsIgnoreCase(slabType)) {
							configErrorReportBuffer.append("Slab Types are not same for mnemonic: [" +invInvRateTypeMnemonic+ "]..."+System.getProperty("line.separator"));
							throw new Exception("Config_error: Slab Types are not same for mnemonic: [" +invInvRateTypeMnemonic+ "]...");
						}

						if(transactionDetail.getTransactionStatus().equals(AdminConstants.SUCCESS)) {

							if(Data.isEmpty(rateApplicable) || rateApplicable.equals("B") || rateApplicable.equals("S")) {

								if(totalTxnCount>=minTxnCount && totalTxnCount<=maxTxnCount) {

									mercInvRateTypeMasterFinal = mercInvRateTypeMaster;
									break;								
								}

							}
						} else if(transactionDetail.getTransactionStatus().equals(AdminConstants.FAILED)) {

							if(rateApplicable.equals("B") || rateApplicable.equals("F")) {

								if(totalTxnCount>=minTxnCount && totalTxnCount<=maxTxnCount) {

									mercInvRateTypeMasterFinal = mercInvRateTypeMaster;
									break;
								}

							}

						} else {
							configErrorReportBuffer.append("Invalid Transaction Status: [" +transactionDetail.getTransactionStatus()+ "], for " +
									"Transaction Master Id: [" +transactionDetail.getRepoTxnId()+ "]..."+System.getProperty("line.separator"));
							throw new Exception("Config_error: Invalid Transaction Status: [" +transactionDetail.getTransactionStatus()+ "], for " +
									"Transaction Master Id: [" +transactionDetail.getRepoTxnId()+ "]...");
						}

					}

				} else if(slabType.equalsIgnoreCase("T")) {

					for(int i=0; i<mercInvRateTypeMasterList.size(); i++) {

						mercInvRateTypeMaster = mercInvRateTypeMasterList.get(i);
						slabTypeTemp = mercInvRateTypeMaster.getSlabType();
						rateApplicable = mercInvRateTypeMaster.getRateApplicable();
						minAmount = Long.parseLong(mercInvRateTypeMaster.getMinAmount())*100;// Modified to make in 100ps
						maxAmount = Long.parseLong(mercInvRateTypeMaster.getMaxAmount())*100;// Modified to make in 100ps

						//just comparing slab types to make sure whether config has any mistake...
						if(!slabTypeTemp.equalsIgnoreCase(slabType)) {
							configErrorReportBuffer.append("Slab Types are not same for mnemonic: [" +invInvRateTypeMnemonic+ "]..."+System.getProperty("line.separator"));
							throw new Exception("Config_error: Slab Types are not same for mnemonic: [" +invInvRateTypeMnemonic+ "]...");
						}

						if(transactionDetail.getTransactionStatus().equals(AdminConstants.SUCCESS)) {

							if(Data.isEmpty(rateApplicable) || rateApplicable.equals("B") || rateApplicable.equals("S")) {

								if(totalTxnAmount>minAmount && totalTxnAmount<=maxAmount) {

									mercInvRateTypeMasterFinal = mercInvRateTypeMaster;
									break;
								}

							}
						} else if(transactionDetail.getTransactionStatus().equals(AdminConstants.FAILED)) {

							if(rateApplicable.equals("B") || rateApplicable.equals("F")) {

								if(totalTxnAmount>minAmount && totalTxnAmount<=maxAmount) {

									mercInvRateTypeMasterFinal = mercInvRateTypeMaster;
									break;
								}

							}

						} else {
							configErrorReportBuffer.append("Invalid Transaction Status: [" +transactionDetail.getTransactionStatus()+ "], for " +
									"Transaction Master Id: [" +transactionDetail.getRepoTxnId()+ "]..."+System.getProperty("line.separator"));
							throw new Exception("Config_error: Invalid Transaction Status: [" +transactionDetail.getTransactionStatus()+ "], for " +
									"Transaction Master Id: [" +transactionDetail.getRepoTxnId()+ "]...");
						}

					}

				} else {
					configErrorReportBuffer.append("Invalid Slab Type for mnemonic: [" +invInvRateTypeMnemonic+ "]..."+System.getProperty("line.separator"));
					throw new Exception("Config_error: Invalid Slab Type for mnemonic: [" +invInvRateTypeMnemonic+ "]...");
				}
			}

			if(mercInvRateTypeMasterFinal==null) {
				configErrorReportBuffer.append("Rate Type not found for mnemonic: [" +invInvRateTypeMnemonic+ "]["+slabType+"]["+txnAmountLong+"]["+transactionDetail.getTransactionStatus()+"]..."+System.getProperty("line.separator"));
				throw new Exception("Config_error: Rate Type not found for mnemonic: [" +invInvRateTypeMnemonic+ "]["+slabType+"]["+txnAmountLong+"]["+transactionDetail.getTransactionStatus()+"]...");
			}

			if(isMercCITI){
				transactionDetail.setMercInvRateTypeMasterCITI(mercInvRateTypeMasterFinal);
				calculateFinalChargesAndTaxesCITI(transactionDetail, mercInvRateTypeMasterFinal);
			}else{
				transactionDetail.setMercInvRateTypeMaster(mercInvRateTypeMasterFinal);
				calculateFinalChargesAndTaxes(transactionDetail, mercInvRateTypeMasterFinal);
			}			

		} catch(Exception e) {

			if(mercInvoiceTotalDetails!=null) {
				mercInvoiceTotalDetails.removeTxn(transactionDetail);
			}
			e.printStackTrace();
			configErrorReportBuffer.append("MercInv Rate master not proper for Mnemonic ["+mercInvRateTypeMasterFinal.getRateTypeMnemonic()+"]"+System.getProperty("line.separator"));
			throw new Exception("Config_error: MercInv Rate master not proper for Mnemonic ["+mercInvRateTypeMasterFinal.getRateTypeMnemonic()+"]");
		}finally{
			mercInvRateTypeMasterList = null;
		}


	}	

	private void calculateFinalChargesAndTaxes(TransactionDetail transactionDetail, 
			MercInvRateTypeMaster mercInvRateTypeMaster) {


		double payAmount	    = (transactionDetail.getTransactionAmountLong()/10000.0);
		double per_charges      = 0;
		double service_tax      = 0;
		double net_amount       = payAmount;
		String net_amount_sign  = "P";

		double per_charges_rate	   = Double.parseDouble(mercInvRateTypeMaster.getPerChargesC());
		double per_minCharges	   = Double.parseDouble(AmountUtil.getAmountInRsPs(mercInvRateTypeMaster.getPerMinChargesC()));
		double per_maxCharges	   = Double.parseDouble(AmountUtil.getAmountInRsPs(mercInvRateTypeMaster.getPerMaxChargesC()));
		double fix_charge_rate     = Double.parseDouble(AmountUtil.getAmountInRsPs(mercInvRateTypeMaster.getFixChargesC()));

		boolean isChargePresent=false;

		if(per_charges_rate>0) {

			isChargePresent=true;
			per_charges=AmountUtil.roundFourDecimal(payAmount*(per_charges_rate/100.0));

			if (per_charges<per_minCharges){
				per_charges=per_minCharges; //Charge
			}
			if (per_charges>per_maxCharges){
				per_charges=per_maxCharges; //Charge
			}
		}

		if(fix_charge_rate>0) {
			isChargePresent=true;
			per_charges=AmountUtil.roundFourDecimal(per_charges+fix_charge_rate);
		}
		boolean notCardOrCardAndGreaterThan2K = true;
		if(payAmount<=AdminConstants.NO_GST_CARD_LIMIT && isCardMnemonic(mercInvRateTypeMaster.getRateTypeMnemonic())){
			notCardOrCardAndGreaterThan2K = false;
		}
		
		if(isChargePresent && notCardOrCardAndGreaterThan2K) {

			if(mercInvRateTypeMaster.getServiceTaxInclusive().equalsIgnoreCase("Y")) {
				//a+a*(rate/100)=c
				//a(1+rate/100)=c
				//a=c/(1+rate/100)
				double per_charges_total = per_charges;
				per_charges = AmountUtil.roundFourDecimal(per_charges_total/(1+(transactionDetail.getTaxMaster().getTotalServiceTaxInPercentage().doubleValue()/100.0)));
				service_tax = AmountUtil.roundFourDecimal(per_charges_total - per_charges); //Service Tax

			} else {

				service_tax = AmountUtil.roundFourDecimal(per_charges*(transactionDetail.getTaxMaster().getTotalServiceTaxInPercentage().doubleValue()/100)); //Service Tax

			}
			net_amount = AmountUtil.roundFourDecimal(payAmount-(per_charges+service_tax));

		} else {
			transactionDetail.setGstAppliedGross("N");
			net_amount = AmountUtil.roundFourDecimal(payAmount);
		}
		//logger.warn("per_charges: ["+per_charges+ "]");
		//logger.warn("service_tax: ["+service_tax+ "]");
		//logger.warn("net_amount: ["+net_amount+ "]");

		double ecsCharges = 0;
		if(transactionDetail.getBankId().startsWith(AdminConstants.ECS_BANK_STARTSWITH) || transactionDetail.getPayeeBankId().startsWith(AdminConstants.ECS_BANK_STARTSWITH)){
			if(transactionDetail.getTransactionStatus().equals(AdminConstants.SUCCESS)) {

				if(AmountUtil.hasValidAmount(mercInvRateTypeMaster.getEcsSuccessCharge())) {

					transactionDetail.setEcsSuccessCharge(mercInvRateTypeMaster.getEcsSuccessCharge()+AdminConstants.AMOUNT_DECIMAL_UPTO);
				} else {
					transactionDetail.setEcsSuccessCharge(AdminConstants.ZERO_AMOUNT+AdminConstants.AMOUNT_DECIMAL_UPTO);
				}

				transactionDetail.setEcsFailureCharge(AdminConstants.ZERO_AMOUNT+AdminConstants.AMOUNT_DECIMAL_UPTO);

				ecsCharges = Double.parseDouble(AmountUtil.getAmountInRsPs(mercInvRateTypeMaster.getEcsSuccessCharge()));

			} else if(transactionDetail.getTransactionStatus().equalsIgnoreCase(AdminConstants.FAILED)) {

				if(AmountUtil.hasValidAmount(mercInvRateTypeMaster.getEcsFailureCharge())) {

					transactionDetail.setEcsFailureCharge(mercInvRateTypeMaster.getEcsFailureCharge()+AdminConstants.AMOUNT_DECIMAL_UPTO);
				} else {
					transactionDetail.setEcsFailureCharge(AdminConstants.ZERO_AMOUNT+AdminConstants.AMOUNT_DECIMAL_UPTO);
				}

				transactionDetail.setEcsSuccessCharge(AdminConstants.ZERO_AMOUNT+AdminConstants.AMOUNT_DECIMAL_UPTO);

				ecsCharges = Double.parseDouble(AmountUtil.getAmountInRsPs(mercInvRateTypeMaster.getEcsFailureCharge()));

			}

		}else{

			transactionDetail.setEcsSuccessCharge(AdminConstants.ZERO_AMOUNT+AdminConstants.AMOUNT_DECIMAL_UPTO);
			transactionDetail.setEcsFailureCharge(AdminConstants.ZERO_AMOUNT+AdminConstants.AMOUNT_DECIMAL_UPTO);

		}

		if(notCardOrCardAndGreaterThan2K){
			double ecsServiceTax = AmountUtil.roundFourDecimal(ecsCharges*(transactionDetail.getTaxMaster().getTotalServiceTaxInPercentage().doubleValue()/100)); //ECS Service Tax	
			long ecsServiceTaxLong  = AmountUtil.getLongAmountMilliPs(ecsServiceTax);
			transactionDetail.setEcsChargeServiceTax(Data.lpad(String.valueOf(ecsServiceTaxLong), '0', 18));
		}else{
			transactionDetail.setEcsChargeServiceTax(Data.lpad(String.valueOf(0l), '0', 18));	
		}
		
		long per_charges_long      = AmountUtil.getLongAmountMilliPs(per_charges);
		long service_tax_long      = AmountUtil.getLongAmountMilliPs(service_tax);
		long net_amount_long       = AmountUtil.getLongAmountMilliPs(net_amount);

		if (net_amount_long<0) {

			net_amount_sign = "N";
			net_amount_long = Math.abs(net_amount_long);

		} else {

			net_amount_sign = "P";

		}
		transactionDetail.setInvoiceTransactionCharges(Data.lpad(String.valueOf(per_charges_long), '0', 18));

		transactionDetail.setInvoiceTransactionStax(Data.lpad(String.valueOf(service_tax_long), '0', 18));

	}

	private void calculateFinalChargesAndTaxesCITI(TransactionDetail transactionDetail, 
			MercInvRateTypeMaster mercInvRateTypeMaster) throws Exception{

		try {	

			double payAmount	    = (transactionDetail.getTransactionAmountLong()/10000.0);
			double per_charges      = 0;
			double service_tax      = 0;
			double net_amount       = payAmount;
			String net_amount_sign  = "P";
	
			double per_charges_rate	   = Double.parseDouble(mercInvRateTypeMaster.getPerChargesC());
			double per_minCharges	   = Double.parseDouble(AmountUtil.getAmountInRsPs(mercInvRateTypeMaster.getPerMinChargesC()));
			double per_maxCharges	   = Double.parseDouble(AmountUtil.getAmountInRsPs(mercInvRateTypeMaster.getPerMaxChargesC()));
			double fix_charge_rate     = Double.parseDouble(AmountUtil.getAmountInRsPs(mercInvRateTypeMaster.getFixChargesC()));
	
			boolean isChargePresent=false;
	
			if(per_charges_rate>0) {
	
				isChargePresent=true;
				per_charges=AmountUtil.roundFourDecimal(payAmount*(per_charges_rate/100.0));
	
				if (per_charges<per_minCharges){
					per_charges=per_minCharges; //Charge
				}
				if (per_charges>per_maxCharges){
					per_charges=per_maxCharges; //Charge
				}
			}
	
			if(fix_charge_rate>0) {
				isChargePresent=true;
				per_charges=AmountUtil.roundFourDecimal(per_charges+fix_charge_rate);
			}
			boolean notCardOrCardAndGreaterThan2K = true;
			if(payAmount<=AdminConstants.NO_GST_CARD_LIMIT && isCardMnemonic(mercInvRateTypeMaster.getRateTypeMnemonic())){
				notCardOrCardAndGreaterThan2K = false;
			}
	
			if(isChargePresent && notCardOrCardAndGreaterThan2K) {
	
				if(mercInvRateTypeMaster.getServiceTaxInclusive().equalsIgnoreCase("Y")) {
					//a+a*(rate/100)=c
					//a(1+rate/100)=c
					//a=c/(1+rate/100)
					double per_charges_total = per_charges;
					per_charges = AmountUtil.roundFourDecimal(per_charges_total/(1+(transactionDetail.getTaxMaster().getTotalServiceTaxInPercentage().doubleValue()/100.0)));
					service_tax = AmountUtil.roundFourDecimal(per_charges_total - per_charges); //Service Tax
	
				} else {
	
					service_tax = AmountUtil.roundFourDecimal(per_charges*(transactionDetail.getTaxMaster().getTotalServiceTaxInPercentage().doubleValue()/100)); //Service Tax
	
				}
				net_amount = AmountUtil.roundFourDecimal(payAmount-(per_charges+service_tax));
	
			} else { // GUESSSS
				net_amount = AmountUtil.roundFourDecimal(payAmount);
			}
	
	
			long per_charges_long      = AmountUtil.getLongAmountMilliPs(per_charges);
			long service_tax_long      = AmountUtil.getLongAmountMilliPs(service_tax);
			long net_amount_long       = AmountUtil.getLongAmountMilliPs(net_amount);
	
			if (net_amount_long<0) {
	
				net_amount_sign = "N";
				net_amount_long = Math.abs(net_amount_long);
	
			} else {
	
				net_amount_sign = "P";
	
			}
	
			transactionDetail.setCitiCharges1(Data.lpad(String.valueOf(per_charges_long), '0', 18));
	
			transactionDetail.setCitiCharges1STax(Data.lpad(String.valueOf(service_tax_long), '0', 18));
		} catch (Exception e) {
			logger.info("Exception: calculateFinalChargesAndTaxesCITI", e);
			e.printStackTrace();
			throw new Exception("Config_error: CalculateFinalChargesAndTaxesCITI: [" +e.getMessage()+ "]");
		}
	}


	private void getActualTransactionDetailsRefundList(ArrayList<String> transactionIdList, ArrayList<TransactionDetail> transactionDetailRefundList) {

		TransactionDetail transactionDetail;
		for(int txnCount=0; txnCount<transactionDetailRefundList.size(); txnCount++) {

			transactionDetail = (TransactionDetail) transactionDetailRefundList.get(txnCount);

			if(!transactionIdList.contains(transactionDetail.getTransactionId())){

				transactionDetailRefundList.remove(txnCount);

			}			
		}
	}

	private long getTotalRefundAmount( ArrayList<TransactionDetail> transactionDetailRefundList) throws Exception {

		long totalRefundAmount = 0;
		TransactionDetail transactionDetail = null;

		for(int txnCount=0; txnCount<transactionDetailRefundList.size(); txnCount++) {
			transactionDetail = transactionDetailRefundList.get(txnCount);
			totalRefundAmount += Long.parseLong(transactionDetail.getRefundAmount());

		}
		return totalRefundAmount;

	}

	private MercInvRateTypeMaster getMercInvRateTypeMaster(String slabTypeRate, String subMerchantFN) throws Exception{
		MercInvRateTypeMaster invRateTypeMaster = new MercInvRateTypeMaster();
		String[] slabTypeRateArray = slabTypeRate.split(AdminConstants.SPLIT_CHAR);
		if(slabTypeRateArray.length!=13){
			throw new Exception("Config_error: Slab Type decription in property file for "+subMerchantFN+" is not proper.");
		}
		int i = 0;
		invRateTypeMaster.setEcsSuccessCharge("0000000000000000");
		invRateTypeMaster.setEcsFailureCharge("0000000000000000");
		invRateTypeMaster.setSlabType(slabTypeRateArray[i++]);
		invRateTypeMaster.setRateApplicable(slabTypeRateArray[i++]);
		invRateTypeMaster.setMinAmount(slabTypeRateArray[i++]);
		invRateTypeMaster.setMaxAmount(slabTypeRateArray[i++]);
		invRateTypeMaster.setMinTxn(slabTypeRateArray[i++]);
		invRateTypeMaster.setMaxTxn(slabTypeRateArray[i++]);
		invRateTypeMaster.setPerChargesC(slabTypeRateArray[i++]);
		invRateTypeMaster.setPerMinChargesC(slabTypeRateArray[i++]);
		invRateTypeMaster.setPerMaxChargesC(slabTypeRateArray[i++]);
		invRateTypeMaster.setFixChargesC(slabTypeRateArray[i++]);
		invRateTypeMaster.setServiceTaxInclusive(slabTypeRateArray[i++]);
		invRateTypeMaster.setSlabParam(slabTypeRateArray[i++]);
		invRateTypeMaster.setStartDate(DateUtil.getUDFTimestamp(slabTypeRateArray[i++], "yyyyMMdd"));
		invRateTypeMaster.setEndDate(DateUtil.getUDFTimestamp(slabTypeRateArray[i++], "yyyyMMdd"));

		return invRateTypeMaster;
	}
	
	private boolean checkWhiteListingOfZeroCharges(String whiteListKey){
		boolean isWhiteListed = false;
		try {
			String[] arraywhiteList = AdminConstants.ZEROCHARGES_WHITELIST;
			List<String> list2to1 = Arrays.asList(arraywhiteList);
			if(list2to1.contains(whiteListKey)){
				isWhiteListed = true;
			}
		} catch (Exception e) {
			logger.error("Exception: ", e);
		}
		return isWhiteListed;
	}
	
	private TaxMaster getTransactionTax(TransactionDetail transactionDetail, String base){
		TaxMaster taxMaster = null;
		Timestamp startDate;
		Timestamp endDate;
		String datestr = transactionDetail.getTransactionTimestamp().substring(0, 8);
		if(base.equalsIgnoreCase(AdminConstants.TAX_BASE_TID)){
			datestr = DateUtil.getUDFDateString(transactionDetail.getMerchantTidDate(), "yyyyMMdd");
		}
		try {
			Timestamp timestampDateBase = DateUtil.getUDFTimestamp(datestr,"yyyyMMdd");
			for(int i=0; i<taxMasters.size(); i++) {
				taxMaster = taxMasters.get(i);
				startDate = taxMaster.getStartDate();
				endDate = taxMaster.getEndDate();
				Long milisecondsInaDay = Long.valueOf(24 * 60 * 60 * 1000);
				endDate = new Timestamp(endDate.getTime() + milisecondsInaDay);
				if((startDate.before(timestampDateBase) || startDate.equals(timestampDateBase))  && endDate.after(timestampDateBase)){
					break;
				}else{
					taxMaster = null;
				}
			}
		} catch (Exception e) {
			
		}
		return taxMaster;
	}
	
	public StringBuffer stripDuplicates(String aHunk) {
		StringBuffer result = new StringBuffer();
        SortedSet<String> uniqueLines = new TreeSet<String>();
        String[] chunks = aHunk.split(System.getProperty("line.separator"));
        uniqueLines.addAll(Arrays.asList(chunks));

        for (String chunk : uniqueLines) {
            result.append(chunk).append(System.getProperty("line.separator"));
        }
        return result;
    }
	
public void rerunMerchantInvoice(final String groupMerchantId, final String invoiceType, final String invoiceMonth, final String emailId, final String userId){
		
		new Runnable() {
		
			boolean isDone = true;
			
			public void run() {
				
				boolean isSuccess = updateDataForRerun(groupMerchantId, invoiceType, invoiceMonth, userId);
				
				if(isSuccess){
					
					CreateTemplatesAndCoverLettersInvoice coverLettersInvoice = new CreateTemplatesAndCoverLettersInvoice();
					coverLettersInvoice.startMercTemplatesCreation(invoiceMonth, groupMerchantId, invoiceType);
					
				}else{
					isDone = false;
				}
				try {
					String subject = null;
					String messageBody = null;
					if(isDone){
						subject = "Invoice rerun proccess successfully executed. You can now download the Invoice for "+groupMerchantId+".";
						messageBody = "Invoice rerun proccess successfully executed. You can now download the Invoice for GROUP_MERCHANT_ID = "+groupMerchantId+" and InvoiceType ="+invoiceType+" for Invoice month ="+invoiceMonth;
					}else{
						subject = "Invoice rerun proccess failed for "+groupMerchantId+".";
						messageBody = "Invoice rerun proccess failed. Please check for GROUP_MERCHANT_ID = "+groupMerchantId+" and InvoiceType ="+invoiceType+" for Invoice month ="+invoiceMonth;
					}
					SendMailMessage.sendMailMsg(emailId, subject, null, messageBody);	
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
		}.run();	
		
	}
	
	private boolean updateDataForRerun(String groupMerchantId, String invoiceType, String invoiceMonth, String userId){
		
		Connection dbConnection=null;
		
		boolean isDone = false;
		try {
			
			dbConnection = manager.getConnection();
			dbConnection.setAutoCommit(false);
			
			Timestamp month = DateUtil.getFirstDateOfMonth(invoiceMonth);
			isDone  = invoiceDefinition.updateAndDeleteMercInvDetails(groupMerchantId, invoiceType, month, "A", "R", userId, dbConnection);
			
			if(isDone){
				String invoiceDate = DateUtil.getLastDayOfMonthView(invoiceMonth);
				isDone  = invoiceDefinition.updateMercInvInvCancelled(groupMerchantId, invoiceDate, invoiceType, "Y", "N", dbConnection);
				if(isDone){
					dbConnection.commit();
				}
			}else{
				return isDone;
			}
			
		} catch (Exception e) {
			
		}finally{
			DBUtility.rollbackConnection(dbConnection);
			DBUtility.closeConnection(dbConnection);
		}
		
		
		return isDone;
	}
	private List<String> getCardMnemonicInitials(){
		List<String> list = new ArrayList<String>();
		list.add("AMEX_");
		list.add("CCARD_");
		list.add("CARD_SI_");
		list.add("CARDSI_");
		list.add("DCD_");
		list.add("DCARD_");
		list.add("C_");
		list.add("DDCP_C_");
		list.add("DDCR_C_");
		list.add("DMCP_C_");
		list.add("DMCR_C_");
		list.add("DRCP_C_");
		list.add("DRCR_C_");
		list.add("DVCP_C_");
		list.add("DVCR_C_");
		list.add("FMCP_C_");
		list.add("FMCR_C_");
		list.add("FVCP_C_");
		list.add("FVCR_C_");
		list.add("DMDP_C_");
		list.add("DMDR_C_");
		list.add("DRDP_C_");
		list.add("DRDR_C_");
		list.add("DVDP_C_");
		list.add("DVDR_C_");
		list.add("FMDP_C_");
		list.add("FMDR_C_");
		list.add("FVDP_C_");
		list.add("FVDR_C_");
		list.add("ATM_");
		return list;
	}

	private boolean isCardMnemonic(String mnemonic){
		boolean isCardMnemonic = false;
		String stringMnemonic = null;
		if(mnemonic!=null){
			for (Iterator iterator = mnemonicListCard.iterator(); iterator.hasNext();) {
				stringMnemonic = (String) iterator.next();
				if(mnemonic.startsWith(stringMnemonic)){
					return true;
				}
			}
		}
		return isCardMnemonic;
	}
	
	private boolean isAmexCardTransactionInclusive(TransactionDetail transactionDetail){
		boolean isCardMnemonic = false;
		String tomerchantRateTypeMnemonic = transactionDetail.getTomerchantRateTypeMnemonic();
		String amexMnemonicStart1 = "AMEX_";//For All Systems
		String amexMnemonicStart2 = "CARD_SI_A_";//For BMS
		String amexMnemonicStart3 = "CARDSI_A_";//For CardPay
		String amexMnemonicEnds1 = "_AMX";//For PG
		String amexMnemonicEnds2 = "_AMEX";//For PG
		String amexMnemonicContains = "_AMEX_";//For PG
		if(tomerchantRateTypeMnemonic != null){
			if(tomerchantRateTypeMnemonic.startsWith(amexMnemonicStart1) || tomerchantRateTypeMnemonic.startsWith(amexMnemonicStart2) || tomerchantRateTypeMnemonic.startsWith(amexMnemonicStart3) || tomerchantRateTypeMnemonic.endsWith(amexMnemonicEnds1) || tomerchantRateTypeMnemonic.endsWith(amexMnemonicEnds2) || tomerchantRateTypeMnemonic.contains(amexMnemonicContains)){
				isCardMnemonic = true;
			}
			
		}		
		boolean isInTimeStamp = false;
		if(isCardMnemonic){
			try {
				if(transactionDetail.getSystemId().equalsIgnoreCase(AdminConstants.SYSTEM_ID_PGI) && transactionDetail.getMerchantTidDate().getTime()>DateUtil.getUDFTimestamp("18-AUG-2017", "dd-MMM-yyyy").getTime()){
					isInTimeStamp = true;
				}else if(transactionDetail.getSystemId().equalsIgnoreCase(AdminConstants.SYSTEM_ID_BMS) && transactionDetail.getMerchantTidDate().getTime()>DateUtil.getUDFTimestamp("23-AUG-2017", "dd-MMM-yyyy").getTime()){
					isInTimeStamp = true;
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return isInTimeStamp;
	}

}
