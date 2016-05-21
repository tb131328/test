#include "OcsCharge.h"
#include "Table/bs_preproc.h"
#include "Table/bs_aboutcas.h"
#include "balanceFunc.h"
#include "OweDismount/OweDismount.h"
#include "OweDeal/OweDealFunc.h"
#include "OcsDataExPlain.h"
#include "PreTransact.h"
#include "Compress.h"

using namespace IBC_Compress;


//#define SECONDS_1900_1970	2208988800UL
//�����ڷ���cca֮�����
string g_strPolicyAccNbr = "";   //�������ѵĺ���

int g_iBeginTime = 700;
int g_iEndTime = 2300;


extern vector <struct ACCUMULATOR_RECORD> g_vecAccum;
extern vector <struct ACCUMULATOR_RECORD> g_vecTicketAccum;
extern int g_IncrementMode;   //ȫ��ģʽ0  ����ģʽ1 ����initcharge���ʼ����ȫ��ģʽ



//20140422 LTE
extern CLong g_lFluxCardUsed;


extern CustPricingPlan	cpplan;	


//����ProcessTicketDisctʱ���
//δ�����������ֿ۵ķ��� (Charge1+charge2+charge3) 
extern CLong g_lOriginCharge;
//������ʵ�ʵֿ��� 
extern CLong g_lActualPayoutAmount;


#define _DEBUG_OUT cout << "[" << __FILE__ << ":" << __LINE__ << "]"

extern int g_PrintLog;

          
#define DEBUG_2(I,II)  if(g_PrintLog > 5)   _DEBUG_OUT << I << II<<endl;
#define DEBUG_4(I,II,III,IV)  if(g_PrintLog > 5) _DEBUG_OUT << I << II << III <<IV <<endl;


/*
bool isTelNumber(string& strCalledNbr,CTime& tStartTime)
{
	T130AreaCode strT130AreaCode;
    strT130AreaCode.sNbrWithAreaCode = strCalledNbr.c_str();
    strT130AreaCode.tStartTime = tStartTime;
	int iRet = strT130AreaCode.search();
	if (1 == iRet)
	{
		return true;
	}

	return false;

}
*/

int FillOrgTicket(int& iSourceEventTypeID,int& iRoamFlag,Record& rec,char* pszOrgTicket,SOcsOrgFieldMap* pFiledMap,
	           int& iArryNum,COcsDataExPlain& rDataExPlain,string& strerrmsg,int& iErrCode)
{
	char szErrMsg[255] ={0};
	int   iTmp = 0;
	CLong lTmp =0;
	CReal fTmp =0; 
	string strValue ="";
	int iRecLength = 0;
    SOcsOrgFieldMap* pszFieldMap = NULL;  
	
    int iAllField = sizeof(sOcsOrgTicketMapAry)/sizeof(SOcsOrgFieldMap);                                                
                                                                                                                                                                                          
	for (int i = 0; i<iArryNum; i++)                                                                             
	{                                                                                                            
	    /*�жϱ���ʽ�Ƿ��и��ֶ� û�о�ȡ��һ��*/                                                                
	    if (!rec.hasField(pFiledMap[i].iBillFieldID))                                                        
	    {                                                                                                                                                        
	        continue;                                                                                            
	    }                                                                                                        
	                                                                                                             
	    /*iOcsFieldIDΪ0�ı�ʾ������д ���ں�������⴦���ֶ�һ����д*/                                          
	    if (pFiledMap[i].iOcsFieldID == 0)                                                                   
	    continue;                                                                                                
	                                                                                                             
	    /*��ͨ�ø�ʽ����������Ӧ��ƫ����*/                                                                       
	    pszFieldMap = find(sOcsOrgTicketMapAry,sOcsOrgTicketMapAry+iAllField,pFiledMap[i].iBillFieldID);     
	                                                                                                             
	    if (pszFieldMap == sOcsOrgTicketMapAry+iAllField)                                                        
	    {                                                                                                        
	        snprintf(szErrMsg,255,"not find iBillFieldID=%d,SourceEventTypeID=%d",                               
	        pFiledMap[i].iBillFieldID,iSourceEventTypeID);                                                        
	        strerrmsg = szErrMsg;                                                                              
	        iErrCode = RESULT_CODE_NOT_FIND_MAP_FAILED;                                                                                      
	        return -1;                                                                                           
	    }                                                                                                        
  	                                                                                                             
	    /*ȡҵ����Ϣ�ַ�����Ӧ�ֶεļ�¼*/                                                                       
	    strValue = rDataExPlain.getDataValues(pFiledMap[i].iOcsFieldID);                                     
	    if ( strValue=="" && !filterFiled(iSourceEventTypeID,iRoamFlag,pFiledMap[i].iOcsFieldID))                                                                                       
	    {                                                                                                        
	        snprintf(szErrMsg,255,"ҵ����Ϣ�ַ�����û�ж�Ӧ�ֶΣ�iOcsFieldID =%d",pFiledMap[i].iOcsFieldID); 
	        iErrCode = RESULT_CODE_NOT_MAPPING_FIELD_FAILED;                                                                                     
	        strerrmsg = szErrMsg;                                                                              
	        return -1;                                                                                           
	                                                                                                             
	    }                                                                                                        
		
	    switch(rec.field(pFiledMap[i].iBillFieldID).getType())                                               
	    {                                                                                                        
	        case TYPE_INT:                                                                                       
	        {                                                                                                    
	            iTmp = atoi(strValue.c_str());                                                                   
	            *((int*)(pszOrgTicket+pszFieldMap->iOffset)) = iTmp;                                             
	        }                                                                                                    
	        break;                                                                                               
	        case TYPE_LONG:                                                                                      
	        {                                                                                                    
	            lTmp = atol(strValue.c_str());                                                                   
	            *((CLong*)(pszOrgTicket+pszFieldMap->iOffset)) = lTmp;                                           
	        }                                                                                                    
	        break;                                                                                               
	        case TYPE_REAL:                                                                                      
	        {                                                                                                    
	            fTmp = atol(strValue.c_str());                                                                   
	            *((CReal*)(pszOrgTicket+pszFieldMap->iOffset)) = fTmp;                                           
	        }                                                                                                    
	        break;                                                                                               
	        case TYPE_STR:                                                                                       
	        {                                                                                                    
	            iRecLength = rec.field(pFiledMap[i].iBillFieldID).getLength();                               
	            strncpy(pszOrgTicket+pszFieldMap->iOffset,strValue.c_str(),iRecLength);                          
	            (pszOrgTicket+pszFieldMap->iOffset)[iRecLength] ='\0';                                           
	                                                                                                             
	        }                                                                                                    
	        break;                                                                                               
	        default:                                                                                             
	        {                                                                                                    
	            snprintf(szErrMsg,255,"unknown data type=%d",rec.field(pFiledMap[i].iBillFieldID).getType());
	            strerrmsg = szErrMsg;                                                                          
	            iErrCode = RESULT_CODE_UNKNOW_DATA_TYPE_FAILED;                                                                                
	            return -1;                                                                                       
	        }                                                                                                    
	        break;                                                                                               
	    }                                                                                                        
	                                                                                                             
	    DEBUG_4("attr_id = ",pFiledMap[i].iBillFieldID,",value  = ",strValue);         
	                                                                                                             
	} 

	return 1;
}


//��ѡ�ֶβ�һ��Ҫ�� ����ֵ����Ϊ�� 
bool filterFiled(int& iSourceEventTypeID,int& iRoamFlag,int& iOcsFiled)
{
	switch(iSourceEventTypeID)
	{
		case EVENT_TYPE_CDMA_SOURCE_VOICE:
			//140006  LAC_A   140007  CELL_A  140008  LAC_B  140009  CELL_B 
			//�����⼸������Ҫ����record_type���ж��Ƿ�Ҫ��
			if (OCS_FIELD_LAC_A == iOcsFiled || OCS_FIELD_CELL_A == iOcsFiled || OCS_FIELD_LAC_B == iOcsFiled || OCS_FIELD_CELL_B == iOcsFiled)
			{
				return true;
			}
			//�ض��򷽺���  140011   �ض�����Ϣ 140012  
			//����90338 Roam_org_type ���������п����͵��ǿ� �������ֵ
			if (OCS_FIELD_REDIRECTING_PARTY_ID == iOcsFiled || OCS_FIELD_REDIRECTION_INFO == iOcsFiled )
			{
				return true;
			}

			if ((0 == iRoamFlag) && (OCS_FIELD_CHARGE_MODE == iOcsFiled  || OCS_FIELD_ORG_PARTNER_ID == iOcsFiled))
			{
				return true;
			}
			break;
		case EVENT_TYPE_CDMA_SOURCE_GROUPING:
			//PID��Ӧ���ֶ�ֻ��CCGҵ�����3AĬ��Ϊ0
			//BSC_ID 120027���ܲ���
			if (OCS_FIELD_ORG_PRODUCT_OFFER_ID == iOcsFiled || OCS_FIELD_USE_NODE_CODE == iOcsFiled)
			{
				return true;
			}
			break;
		case EVENT_TYPE_CDMA_SOURCE_SMS:
			//SMSC_ADDRESS ��ʱ��û��
			if (OCS_FIELD_SMSC_ADDRESS == iOcsFiled )
			{
				return true;
			}

			if ((0 == iRoamFlag) && (OCS_FIELD_CHARGE_MODE == iOcsFiled))
			{
				return true;
			}
			
			break;
		case EVENT_TYPE_CDMA_SOURCE_OPERATION:
			//100004 CALLED_NBR ��ֵҵ��� ���к��� �����ǿ�ѡ�� sm_id��ѡ
			if (OCS_FIELD_CALLED_NBR == iOcsFiled || 90332 == iOcsFiled )
			{
				return true;
			}
			break;
		case EVENT_TYPE_SOURCE_BNG:
			//BNG�������ܲ��ͱ��к���
			if (OCS_FIELD_CALLED_NBR == iOcsFiled )
			{
				return true;
			}
			break;
	}

	return false;

}


int ParseTicket(int& iSourceEventTypeID,int& iRoamFlag,Record& rec,PACK_OCS_REQUEST& stOcsReq,SAbmOrgTicket& rOrgTicket,
				COcsDataExPlain& rDataExPlain,string& strerrmsg,int& iErrCode)
{
    try
    {
        
        int iRecLength = 0;
		int iArryNum = 0;
		string strValue = "";
        char szErrMsg[255] ={0};
        char* pszOrgTicket = (char *)&rOrgTicket;
		memset(pszOrgTicket,0,sizeof(rOrgTicket));
	
        switch(iSourceEventTypeID)
        {
            case EVENT_TYPE_CDMA_SOURCE_VOICE:
                {
                    iArryNum = sizeof(sOcsVoiceMapAry0001)/sizeof(SOcsOrgFieldMap);
                    if (-1 == FillOrgTicket(iSourceEventTypeID,iRoamFlag,rec,pszOrgTicket,sOcsVoiceMapAry0001,iArryNum,rDataExPlain,strerrmsg,iErrCode))
                    {
						return -1;
					}
					
                    rOrgTicket.iSwitchID = 25180 ;   

                    //FIELD_ID(SESSION_ID):
                    iRecLength = rec.field(FIELD_ID(SESSION_ID)).getLength()+1;
					snprintf(rOrgTicket.szSessionID,iRecLength,"%s",stOcsReq.sSessionId);

                    
                    //FIELD_ID(ORG_SESSION_ID):
                    iRecLength = rec.field(FIELD_ID(OCS_SESSION_ID)).getLength()+1;
					snprintf(rOrgTicket.szOcsSessionID,iRecLength,"%s",stOcsReq.sSessionId);

                                    
                    //FIELD_ID(SOURCE_EVENT_TYPE_ID):
                    rOrgTicket.iSourceEventTypeID = iSourceEventTypeID;   
                    
                    //FIELD_ID(ORG_START_TIME):            
                    iRecLength = rec.field(FIELD_ID(ORG_START_TIME)).getLength()+1; 
                    strValue = rOrgTicket.szOrgStartTime;      
                    strValue += "00";
					snprintf(rOrgTicket.szOrgStartTime,iRecLength,"%s",strValue.c_str()+2);
     
                    //FIELD_ID(ORG_END_TIME): 
                    strValue = rOrgTicket.szOrgEndTime;
                    strValue += "00";
					snprintf(rOrgTicket.szOrgEndTime,iRecLength,"%s",strValue.c_str()+2);

                    //���⴦��SRECORD_TYPE
                    switch(rOrgTicket.iRecordType)
                    {
                        case 2:
                        rOrgTicket.iRecordType = 0;
                        break;
                        case 12:
                        rOrgTicket.iRecordType = 1;
                        break;  
						default:
						snprintf(szErrMsg,255,"RecordType=%d ����ȡֵ��Χ��",rOrgTicket.iRecordType);
						strerrmsg = szErrMsg;
						iErrCode = RESULT_CODE_UNKNOW_RECORD_TYPE_FAILED;
						return -1; 
						break;
                    }

					//���������ʶ����RECORD_TYPE
					switch(rOrgTicket.iRecordType)
					{
						case 0:
							rOrgTicket.iLatnID = atoi(rOrgTicket.szCallingBelongAreaCode);
							break;
						case 1:
							rOrgTicket.iLatnID = atoi(rOrgTicket.szCalledBelongAreaCode);
							break;
					}
						
		
                    //���⴦��SUB_RECORD_TYPE
                    switch(rOrgTicket.iSubRecordType)
                    {
                    	case 0:
						rOrgTicket.iSubRecordType = 0;
						break;
                        case 1:
                        rOrgTicket.iSubRecordType = 3;
                        break;
                        case 2:
                        rOrgTicket.iSubRecordType = 4;
                        break;         
                        case 3:
                        rOrgTicket.iSubRecordType = 2 ;
                        break;     
                        case 6:
                        rOrgTicket.iSubRecordType = 5;
                        break;   
						default:
						snprintf(szErrMsg,255,"SubRecordType=%d ����ȡֵ��Χ��",rOrgTicket.iSubRecordType);
						strerrmsg = szErrMsg;
						iErrCode = RESULT_CODE_UNKNOW_SUB_RECORD_TYPE_FAILED;
						return -1;
						break;
                    }


					//����Ǻ�ת�� ���������������Ե�
                    switch(rOrgTicket.iSubRecordType)
                    {

                        case 2:
                        case 3:
                        case 4:   
                        case 5:
							{
								char szTmpNbr[34+1] = {0};
								strncpy(szTmpNbr,rOrgTicket.szCallingOrgNbr,34);					
								strncpy(rOrgTicket.szCallingOrgNbr,rOrgTicket.szThirdPartyOrgNbr,34);
								strncpy(rOrgTicket.szThirdPartyOrgNbr,szTmpNbr,34);

		
								string strMscAddress = rDataExPlain.getDataValues(OCS_FIELD_MSC_ADDRESS);
								string strBillingBelongAreaCode = rDataExPlain.getDataValues(OCS_FIELD_BILLING_BELONG_AREA_CODE);
								if (strMscAddress.empty() || strBillingBelongAreaCode.empty())
								{
									strerrmsg = "MSC_ADDRESS����BILLING_BELONG_AREA_CODEΪ��";
									iErrCode = RESULT_CODE_NOT_MAPPING_FIELD_FAILED;
									return -1;

								}
								// itnm00072743 �����������ת�Ƶ���ǿ���ж�Ϊ���ط����Σ����з���������ת�Ƶ����ͣ�ת�Ƶ����ж�Ϊ���ط�����
								//if (2 == rOrgTicket.iSubRecordType)
								//{
								    strncpy(rOrgTicket.szCallingRoamAreaCode,strBillingBelongAreaCode.c_str(),9);
								//}
								//else
								//{
								//	strncpy(rOrgTicket.szCallingRoamAreaCode,strMscAddress.c_str(),9);
								//}
												
								strncpy(rOrgTicket.szCallingBelongAreaCode,strBillingBelongAreaCode.c_str(),9);
								rOrgTicket.iLatnID = atoi(rOrgTicket.szCallingBelongAreaCode);
							}
							break;

                    }

                    //���⴦��ROAM_TYPE
                    switch(rOrgTicket.iRoamOrgType)
                    {
                        case 1:
                        rOrgTicket.iRoamOrgType = 4;
                        break;
                        case 2:
                        rOrgTicket.iRoamOrgType = 1;
						break;
                        case 0:
                        rOrgTicket.iRoamOrgType = 0;
                        break; 
						default:
						//����������������Roam_org_type ocs�Ϳջ����ֵ �Ʒ���Ҫ���ݹ�����Ϣ����������Ӫ�̱�������
						//��ʼ��Ϊ0
						rOrgTicket.iRoamOrgType = 0;
						/*
						snprintf(szErrMsg,255,"RoamOrgType=:%d ����ȡֵ��Χ��",rOrgTicket.iRoamOrgType);
						strerrmsg = szErrMsg;
						iErrCode = RESULT_CODE_UNKNOW_ROAM_ORG_TYPE_FAILED;
						return -1;
						*/
						break;
                    }

					//�����������������Roam_org_type,org_partner_id
					if ( 0  == rOrgTicket.iRecordType)
					{

						CString sCallingRoamAreaCode =  rOrgTicket.szCallingRoamAreaCode;		
						//��д���л���ORG_PARTNER_ID
						CToGCarrierFromFile   CtoGCarrier;
						CtoGCarrier.imsi_seg =  sCallingRoamAreaCode;
						CtoGCarrier.m_tTime = CTime::getCurrentTime();
						int iRet = CtoGCarrier.search();
						if (1 == iRet )
						{
							//��ѯ����дORG_PARTNER_ID ��Roam_org_type
							strncpy(rOrgTicket.szOrgPartnerID,(const char*)CtoGCarrier.vCToGCarrierInfo[0].visited_carrier_code,6);
							rOrgTicket.iRoamOrgType = 6;
						}

					}else if (1 == rOrgTicket.iRecordType)
					{
						
						CString sCalledRoamAreaCode = rOrgTicket.szCalledRoamAreaCode;		
						GetCountryInfoByCtryPrefix getCountry;
						getCountry.strCtryPrefix =	sCalledRoamAreaCode;
						getCountry.m_tTime = CTime::getCurrentTime();
						int iRet = getCountry.search();
												
						if(1 == iRet )
						{
							strncpy(rOrgTicket.szOrgPartnerID,(const char*)getCountry.v_strCountryInfo[0].strCountryCode,6);
							rOrgTicket.iRoamOrgType = 6;
						}


					}

					//����ǹ���������Ҫ��дIMSI�� ��Ҫ��д��Ӫ�̱�ʶ
					if (6 == rOrgTicket.iRoamOrgType)
					{	
						//90224 BILLING_IMSI
						strValue = rDataExPlain.getDataValues(90224); 
						string strimsi = "46003";
						strimsi += strValue;
						strncpy(rOrgTicket.szImsiNbr,strimsi.c_str(),31);	
						
						//��Ԫ������Ϣ��ֱ���ṩ���ֶΣ��Ʒѿ���Ĭ�ϸ�ֵ
						rOrgTicket.iServiceType3G = 0;
						//���ε���Ӫ����ķ����־
						strncpy(rOrgTicket.szSrcDeviceID,"D",21);

						rOrgTicket.iChargeMode  = 4;					
					}
					
					
                    //���⴦��iOrgCallAmountʱ�� ��ʼ ����ȡ����RSU_CC_TIME  
                    //��ֹ ����ȡ����DURATION
                    switch(stOcsReq.iReqType)
                    {
                        //1,2 ��ʼ�͸���ȡ����RSU_CC_TIME
                        case 1:
                        case 2: //û�;͵�����0                                        
                        strValue = rDataExPlain.getDataValues(OCS_FIELD_RSU_CC_TIME);    
                        if (strValue == "")
                        {  strValue = "0";  }
                        rOrgTicket.iOrgCallAmount = 10 * atoi(strValue.c_str());
						if (0 == rOrgTicket.iOrgCallAmount)
						{
						    strerrmsg = "�Ʒ�ʱ��Ϊ0";
	                        iErrCode = RESULT_CODE_ZERO_CHARGE_AMOUNT_FAILED;
	                        return -1;  

						}
                        break;
                        //1,2 ��ֹ������ȡ����DURATION
                        case 3:
                        case 4: 
						case 5:
						{

							CString strStartTime = rDataExPlain.getDataValues(OCS_FIELD_START_TIME).c_str();
							CString strEndTime = rDataExPlain.getDataValues(OCS_FIELD_END_TIME).c_str();
							CTimeSpan cts = strEndTime.toDate() - strStartTime.toDate();
							rOrgTicket.iOrgCallAmount = cts.getTotalSeconds()*10;

                    	}
                        break;
                        default:
                        {
                            snprintf(szErrMsg,255,"Ŀǰ��֧�ָ���������:%d",stOcsReq.iReqType);
                            strerrmsg = szErrMsg;
                            iErrCode = RESULT_CODE_UNKNOW_REQ_TYPE_FAILED;
                            return -1;  
                        }
                        break;
                    } 
                }
                break;
            case EVENT_TYPE_CDMA_SOURCE_GROUPING:
                {
                    iArryNum = sizeof(sOcsDataMapAry0001)/sizeof(SOcsOrgFieldMap);
                    if (-1 == FillOrgTicket(iSourceEventTypeID,iRoamFlag,rec,pszOrgTicket,sOcsDataMapAry0001,iArryNum,rDataExPlain,strerrmsg,iErrCode))
                    {
						return -1;
					}        

                    //20140324 4gȡCCR_PS_SGSNADDRESS 97805�ֶ� 3accg����OCS_PDSN_ADDRESS 120061			
                    //FIELD_ID(SWITCH_ID):
                    string strSrvCntxtId = stOcsReq.sSrvCntxtId;
					transform(strSrvCntxtId.begin(), strSrvCntxtId.end(), strSrvCntxtId.begin(), ::toupper);
					if (strSrvCntxtId.find("PGW") !=  strSrvCntxtId.npos)
					{
						iRecLength = rec.field(FIELD_ID(PDSN_IP)).getLength()+1;
						strValue =  rDataExPlain.getDataValues(OCS_FIELD_CCR_PS_SGSNADDRESS); 
						snprintf(rOrgTicket.szPdsnIP,iRecLength,"%s",strValue.c_str());
	                    
	                    iRecLength = rec.field(FIELD_ID(LOGIN_NAME_3G)).getLength()+1;
	                    strValue =  rDataExPlain.getDataValues(97817);  	
						snprintf(rOrgTicket.szLoginName3G,iRecLength,"%s",strValue.c_str());
						
						//lte RatingGroupID��MSCC_RATING_GROUP  ContentCode��0
	                    iRecLength = rec.field(FIELD_ID(RATING_GROUP_ID)).getLength()+1; 
	                    strValue =  rDataExPlain.getDataValues(OCS_FIELD_MSCC_RATING_GROUP);                          	                 
						snprintf(rOrgTicket.szRatingGroupID,iRecLength,"%s",strValue.c_str());
						snprintf(rOrgTicket.szContentCode,iRecLength,"0");
                        
					}else
					{
						iRecLength = rec.field(FIELD_ID(PDSN_IP)).getLength()+1;
						strValue =  rDataExPlain.getDataValues(OCS_FIELD_OCS_PDSN_ADDRESS);
						snprintf(rOrgTicket.szPdsnIP,iRecLength,"%s",strValue.c_str());

	                    
	                    iRecLength = rec.field(FIELD_ID(LOGIN_NAME_3G)).getLength()+1;
	                    strValue =  rDataExPlain.getDataValues(OCS_FIELD_NAI_USER_NAME);  	
						snprintf(rOrgTicket.szLoginName3G,iRecLength,"%s",strValue.c_str());
               
	                    iRecLength = rec.field(FIELD_ID(CONTENT_CODE)).getLength()+1;
	                    strValue =  rDataExPlain.getDataValues(96181); 				
	                    snprintf(rOrgTicket.szContentCode,iRecLength,"%s",strValue.c_str());
						snprintf(rOrgTicket.szRatingGroupID,iRecLength,"0");

					}
					
                
               		if (strSrvCntxtId.find("PGW") !=  strSrvCntxtId.npos)
                	{
						rOrgTicket.iSwitchID = 25192; 	
					}else if (strSrvCntxtId.find("CCG") !=  strSrvCntxtId.npos)
					{
						rOrgTicket.iSwitchID = 25183; 
					}else if (strSrvCntxtId.find("CDMAPS") !=  strSrvCntxtId.npos)
					{
						rOrgTicket.iSwitchID = 25182;
						iRecLength = rec.field(FIELD_ID(CONTENT_CODE)).getLength()+1;
						snprintf(rOrgTicket.szContentCode,iRecLength,"%s","0");
					}
                                                        
                    //FIELD_ID(SESSION_ID):
                    //����Ԥ�����ʱ����ж�SessionID�Ƿ�Ϊ���Ƿ񳬹�8λ ���⴦����
                    //iRecLength = rec.field(FIELD_ID(SESSION_ID)).getLength()+1;
                    snprintf(rOrgTicket.szSessionID,9,"%s",stOcsReq.sSessionId);

                    
                    //FIELD_ID(ORG_SESSION_ID):
                    iRecLength = rec.field(FIELD_ID(OCS_SESSION_ID)).getLength()+1;
					snprintf(rOrgTicket.szOcsSessionID,iRecLength,"%s",stOcsReq.sSessionId);
                    
                    //FIELD_ID(SOURCE_EVENT_TYPE_ID):
                    rOrgTicket.iSourceEventTypeID = iSourceEventTypeID;   
                    
                    //FIELD_ID(ORG_START_TIME):            
                    iRecLength = rec.field(FIELD_ID(ORG_START_TIME)).getLength()+1; 
                    strValue = rOrgTicket.szOrgStartTime;      
                    strValue += "00";
					snprintf(rOrgTicket.szOrgStartTime,iRecLength,"%s",strValue.c_str()+2);
     
                    //FIELD_ID(ORG_END_TIME): 
                    strValue = rOrgTicket.szOrgEndTime;
                    strValue += "00";
					snprintf(rOrgTicket.szOrgEndTime,iRecLength,"%s",strValue.c_str()+2);
			
                    //FIELD_ID(IMSI_NBR):                
                    strncpy(rOrgTicket.szImsiNbr,"NULLNULLNULLNUl",strlen("NULLNULLNULLNUl")+1); 
                    
                    //���⴦��ROAM_TYPE
                    switch(rOrgTicket.iRoamOrgType)
                    {
                        case 1:
                        rOrgTicket.iRoamOrgType = 2;
                        break;
                        case 2:
                        rOrgTicket.iRoamOrgType = 9;
						break;
                        case 0:
                        rOrgTicket.iRoamOrgType = 0;
                        break; 
						default:
						snprintf(szErrMsg,255,"RoamOrgType=:%d ����ȡֵ��Χ��",rOrgTicket.iRoamOrgType);
						strerrmsg = szErrMsg;
						iErrCode = RESULT_CODE_UNKNOW_ROAM_ORG_TYPE_FAILED;
						return -1;
						break;
                    }
                                      
                    switch(stOcsReq.iReqType)
                    {
                        //1,2 ��ʼ�͸���ȡ����RSU_CC_TIME  RSU_CC_TOTAL_OCTETS
                        case 1:
                        case 2: 
                            { 
                                //����ʱ��               
                                strValue = rDataExPlain.getDataValues(OCS_FIELD_RSU_CC_TIME);    
                                if (strValue == "")
                                {  strValue = "0";  }
                                rOrgTicket.iOrgCallAmount = 10 * atoi(strValue.c_str());
                                //��������
                                strValue = rDataExPlain.getDataValues(OCS_FIELD_RSU_CC_TOTAL_OCTETS);    
                                if (strValue == "")
                                {  strValue = "0";  }
                                rOrgTicket.lVolumeDownLink = atol(strValue.c_str()); 
								if (0 == rOrgTicket.iOrgCallAmount && 
									0 == rOrgTicket.lVolumeDownLink)
								{
								    strerrmsg = "�Ʒ�ʱ��������Ϊ0";
			                        iErrCode = RESULT_CODE_ZERO_CHARGE_AMOUNT_FAILED;
			                        return -1;  
								}
                                break;
                            }
                        //1,2 ��ֹ������ȡ����DURATION ����Ϊ������
                        case 3:
                        case 4:
						case 5:
                            {
								
                                strValue = rDataExPlain.getDataValues(OCS_FIELD_DURATION);        
                                if (strValue == "" || strValue == "0")
                                {  
                                	CString strStartTime = rDataExPlain.getDataValues(OCS_FIELD_START_TIME).c_str();
									CString strEndTime = rDataExPlain.getDataValues(OCS_FIELD_END_TIME).c_str();
									CTimeSpan cts = strEndTime.toDate() - strStartTime.toDate();
									rOrgTicket.iOrgCallAmount = cts.getTotalSeconds()*10;
								}
								else
								{
									rOrgTicket.iOrgCallAmount = 10 * atoi(strValue.c_str());
								}
                    
                                //��ֹ������������ȡ����BYTES
                                strValue = rDataExPlain.getDataValues(OCS_FIELD_BYTES);        
                                if (strValue == "")
                                {  strValue = "0";  }
                                rOrgTicket.lVolumeDownLink = atol(strValue.c_str());
	
                                break;
                            }
                        default:
                            {
                                snprintf(szErrMsg,255,"Ŀǰ��֧�ָ���������:%d",stOcsReq.iReqType);
                                strerrmsg = szErrMsg;
                                iErrCode = RESULT_CODE_UNKNOW_REQ_TYPE_FAILED;
                                return -1;  
                                break;
                            }
                            
                    }          
                                      
					transform(rOrgTicket.szLoginName3G, rOrgTicket.szLoginName3G+sizeof(rOrgTicket.szLoginName3G), rOrgTicket.szLoginName3G, ::toupper); 
                }
                break;
            case EVENT_TYPE_CDMA_SOURCE_SMS:
                {
                    iArryNum = sizeof(sOcsSmsMapAry0001)/sizeof(SOcsOrgFieldMap);
                    if (-1 == FillOrgTicket(iSourceEventTypeID,iRoamFlag,rec,pszOrgTicket,sOcsSmsMapAry0001,iArryNum,rDataExPlain,strerrmsg,iErrCode))
                    {
						return -1;
					} 
					
                    //FIELD_ID(SWITCH_ID):
                    rOrgTicket.iSwitchID = 25181;

                    iRecLength = rec.field(FIELD_ID(CHARGE_TYPE)).getLength()+1; 
                    snprintf(rOrgTicket.szChargeType,iRecLength,"%d",stOcsReq.iReqAct);

                    //FIELD_ID(RECORD_TYPE): 
					/*if (strlen(rOrgTicket.szCalledOrgNbr) != 11)
					{
						rOrgTicket.iRecordType = 4;
					}else
					{	

						CString strStartTime = rDataExPlain.getDataValues(OCS_FIELD_START_TIME).c_str();
						CTime tStartTime = strStartTime.toDate();
						string strCalledOrgNbr = rOrgTicket.szCalledOrgNbr;
						if (isTelNumber(strCalledOrgNbr,tStartTime))
						{
							rOrgTicket.iRecordType = 1;
						}else
						{
							rOrgTicket.iRecordType = 4;
						}
				
					}*/
                    rOrgTicket.iRecordType = 1; //itnm00071961Э�飺���ţ�D201408060591KF045673  
                    
                    //FIELD_ID(ORG_CALL_AMOUNT): 
                    rOrgTicket.iOrgCallAmount = 1;

                    //FIELD_ID(SOURCE_EVENT_TYPE_ID):
                    rOrgTicket.iSourceEventTypeID = iSourceEventTypeID;   
                    
                    //FIELD_ID(ORG_START_TIME):            
                    iRecLength = rec.field(FIELD_ID(ORG_START_TIME)).getLength()+1; 
                    strValue = rOrgTicket.szOrgStartTime;      
                    strValue += "00";
					snprintf(rOrgTicket.szOrgStartTime,iRecLength,"%s",strValue.c_str()+2);
     
                    //FIELD_ID(ORG_END_TIME): 
                    strValue = rOrgTicket.szOrgEndTime;
                    strValue += "00";
					snprintf(rOrgTicket.szOrgEndTime,iRecLength,"%s",strValue.c_str()+2);
                    
                    //FIELD_ID(OCS_SESSION_ID):
                    iRecLength = rec.field(FIELD_ID(OCS_SESSION_ID)).getLength()+1;
					snprintf(rOrgTicket.szOcsSessionID,iRecLength,"%s",stOcsReq.sSessionId);

					if (1 == iRoamFlag)
					{
						rOrgTicket.iRoamOrgType  = 1;
						//���ŵ�imsi�Ͳ���д��
					}
                    
                }
                break;
            case EVENT_TYPE_CDMA_SOURCE_OPERATION:
                {
                    iArryNum = sizeof(sOcsOpearMapAry0001)/sizeof(SOcsOrgFieldMap);
                    if (-1 == FillOrgTicket(iSourceEventTypeID,iRoamFlag,rec,pszOrgTicket,sOcsOpearMapAry0001,iArryNum,rDataExPlain,strerrmsg,iErrCode))
                    {
						return -1;
					}                     

                    //FIELD_ID(SWITCH_ID):
                    rOrgTicket.iSwitchID = 25184;  
             
                                      
                    //FIELD_ID(CHARGE_RESULT): 
                    //sOrgTicket.iChargeResult = stOcsReq.iReqAct;
                    iRecLength = rec.field(FIELD_ID(CHARGE_TYPE)).getLength()+1; 
                    snprintf(rOrgTicket.szChargeType,iRecLength,"%d",stOcsReq.iReqAct);

                    
                    //FIELD_ID(SOURCE_EVENT_TYPE_ID):
                    rOrgTicket.iSourceEventTypeID = iSourceEventTypeID;   
                    
                    //FIELD_ID(ORG_START_TIME):            
                    iRecLength = rec.field(FIELD_ID(ORG_START_TIME)).getLength()+1; 
                    strValue = rOrgTicket.szOrgStartTime;      
                    strValue += "00";
					snprintf(rOrgTicket.szOrgStartTime,iRecLength,"%s",strValue.c_str()+2);
     
                    //FIELD_ID(ORG_END_TIME): 
                    strValue = rOrgTicket.szOrgEndTime;
                    strValue += "00";
					snprintf(rOrgTicket.szOrgEndTime,iRecLength,"%s",strValue.c_str()+2);

                    
                    //FIELD_ID(ORG_SESSION_ID):
                    iRecLength = rec.field(FIELD_ID(OCS_SESSION_ID)).getLength()+1;
					snprintf(rOrgTicket.szOcsSessionID,iRecLength,"%s",stOcsReq.sSessionId);

                              
					int iRecordType = 1;
					int iFind = searchIsmpRecordByIsmpProdOfferID(rOrgTicket.szCombineServiceID,strValue,iRecordType);
					if (1 == iFind)
					{
						strncpy(rOrgTicket.szSpServiceID3G,strValue.c_str(),50);
						rOrgTicket.iRecordType = iRecordType;
					}
					else
					{
						if (rOrgTicket.iServiceTypeID != 0)
						{
							rOrgTicket.iRecordType = 1;
						}
						else
						{
							rOrgTicket.iRecordType = 999;
						}

					}
					
                }                	
                break;
			// itnm00075356lanlh:�����캽ƽ̨�� OCS ϵͳ�Խӵ�����   2015/10 
			case EVENT_TYPE_SOURCE_BNG:		
				{
                    iArryNum = sizeof(sOcsBngMapAry0001)/sizeof(SOcsOrgFieldMap);
                    if (-1 == FillOrgTicket(iSourceEventTypeID,iRoamFlag,rec,pszOrgTicket,sOcsBngMapAry0001,iArryNum,rDataExPlain,strerrmsg,iErrCode))
                    {
						return -1;
					} 
					
                    //FIELD_ID(SWITCH_ID):
                    rOrgTicket.iSwitchID = 25195;

					//FIELD_ID(SOURCE_EVENT_TYPE_ID):
                    rOrgTicket.iSourceEventTypeID = iSourceEventTypeID;   

					//FIELD_ID(CHARGE_RESULT)
					rOrgTicket.iChargeResult =stOcsReq.iReqAct;
					
					//FIELD_ID(LOGIN_NAME_3G):
          			iRecLength = rec.field(FIELD_ID(LOGIN_NAME_3G)).getLength()+1;          
					snprintf(rOrgTicket.szLoginName3G,iRecLength,"%s",stOcsReq.strAccNbr.c_str());

                    //FIELD_ID(ORG_SESSION_ID):
                    iRecLength = rec.field(FIELD_ID(OCS_SESSION_ID)).getLength()+1;
					snprintf(rOrgTicket.szOcsSessionID,iRecLength,"%s",stOcsReq.sSessionId);

					// FIELD_ID(LATN_ID)
					char temp[5];
					snprintf(temp,5,"%s",rOrgTicket.szLoginName3G);					
					rOrgTicket.iLatnID =atoi(temp);
					
					CString strStartTime = rDataExPlain.getDataValues(OCS_FIELD_START_TIME).c_str();
					CString strEndTime = rDataExPlain.getDataValues(OCS_FIELD_END_TIME).c_str();
					
					//���鿪ʼ����ʱ��һ�� �����ʱ���1��
					if (strStartTime == strEndTime  || strStartTime.toDate() > strEndTime.toDate())
					{
						//1��
						rOrgTicket.iOrgCallAmount =10;
						iRecLength = rec.field(FIELD_ID(ORG_START_TIME)).getLength()+1; 
	                    strValue = rOrgTicket.szOrgStartTime;      
	                    strValue += "00";
						snprintf(rOrgTicket.szOrgStartTime,iRecLength,"%s",strValue.c_str()+2);

						//��ʼʱ���1��
						strValue = (const char*)(strStartTime.toDate()+1).format("%Y%m%d%H%M%S");
						strValue += "00";
						snprintf(rOrgTicket.szOrgEndTime,iRecLength,"%s",strValue.c_str()+2);

					}//����ʱ��Ϊ��������ʼ
					else
					{

						CTimeSpan cts = strEndTime.toDate() - strStartTime.toDate();
						rOrgTicket.iOrgCallAmount = cts.getTotalSeconds()*10;

					}

					rOrgTicket.iRecordType = 1;				
					//����ת���� һ��Ԥ����ʱ ����ת�ɺ�
					rOrgTicket.lExtCharge =rOrgTicket.lExtCharge/10;

					if(1 == stOcsReq.iReqAct)  // �˿����� ��Ҫ�ѷ���ȡ��
					{
						rOrgTicket.lExtCharge =-rOrgTicket.lExtCharge;
					}
					
                }
                break;
            default:
                snprintf(szErrMsg,255,"unknown SourceEventTypeID%d",iSourceEventTypeID);
                strerrmsg = szErrMsg;
                iErrCode = RESULT_CODE_UNKNOW_SOURCE_EVENT_TYPE_FAILED;
                return -1;
                break;
            
        }

    }
    catch(CException& e)
    {
        iErrCode = RESULT_CODE_HANDLE_EXCEPTION_FAILED;
        strerrmsg = e.what();
        return -1;
        
    }
    return 1;    
}


int GetSourceEventType(PACK_OCS_REQUEST& stOcsReq,COcsDataExPlain& rDataExPlain,int& iRoamFlag,string& strerrmsg,int& iErrCode)
{	
	string strEventType = stOcsReq.sSrvCntxtId;	
	transform(strEventType.begin(), strEventType.end(), strEventType.begin(), ::toupper);
	unsigned int    nLoc = 0;
	nLoc = strEventType.find('@',0);
	strEventType = strEventType.substr(0,nLoc);
	DEBUG_2("strEventType=",strEventType); 

	int iSourceEventTypeID = -1;
	iRoamFlag = 0 ; //������������������ֶ������Ƿ����������
	if (strEventType.find("IN") != strEventType.npos)
	{
		iSourceEventTypeID = EVENT_TYPE_CDMA_SOURCE_VOICE;


		/*
		if (strEventType.find("ROAM") != strEventType.npos )
		{
			iRoamFlag = 1;
		}
		*/
		//90338Ϊ6�ǹ�������
		/*
		string strRoamOrgType =  rDataExPlain.getDataValues(90338);
		if (strRoamOrgType == "6")
		{
			iRoamFlag = 1;
		}
		*/
		
	}
	else if (strEventType.find("CCG") != strEventType.npos || strEventType.find("CDMAPS") != strEventType.npos || strEventType.find("PGW") != strEventType.npos)
	{
		iSourceEventTypeID =EVENT_TYPE_CDMA_SOURCE_GROUPING;
	}
	else if (strEventType.find("P2PSMS") != strEventType.npos)
	{
		iSourceEventTypeID = EVENT_TYPE_CDMA_SOURCE_SMS;
		/*
		if (strEventType.find("ROAM") != strEventType.npos )
		{
			iRoamFlag = 1;
		}
		*/
		//MSC_ADDRESS��90329�� 0�������û� ��0���û��������ε���Ϣ
		string strMscAddress =  rDataExPlain.getDataValues(90329);
		if (strMscAddress != "0" && strMscAddress != "")
		{
			iRoamFlag = 1;
		}
	}
	else if (strEventType.find("ISMP") != strEventType.npos)
	{
		string strSessionID =stOcsReq.sSessionId;
		transform(strSessionID.begin(),strSessionID.end(),strSessionID.begin(),::toupper);
		if(strncmp(strSessionID.c_str(),"BNET",4) == 0)		// itnm00075356lanlh:�����캽ƽ̨�� OCS ϵͳ�Խӵ�����   2015/10 
		{
			iSourceEventTypeID = EVENT_TYPE_SOURCE_BNG;
		}else
		{
			iSourceEventTypeID = EVENT_TYPE_CDMA_SOURCE_OPERATION;
		}
		
		
	}
	else
	{
		iSourceEventTypeID = -1;
		strerrmsg = "GetSourceEventType fail, sSrvCntxtId = ";
        strerrmsg +=stOcsReq.sSrvCntxtId;
        iErrCode = RESULT_CODE_GET_SOURCE_EVENT_FAILED;
	}
		
    return iSourceEventTypeID;                             
    
}


bool QueryBalance(Record& rec,CLong& lCurEffBalance,string& strerrmsg,int& iErrCode )
{
	DTimerCalc calc(__LINE__, "QueryBalance");
    SBAlImport tagCondition;
    vector<ACCT_ITEM_RECORD> AcctItemRecVct;
    vector<SQueryRecord> BalanceRecVct;
    SOweOutRec tagSimulWriteOffRes;
    AcctItemRecVct.clear();
    BalanceRecVct.clear();

	
	int iLatnID;
	CLong lAcctID;
	CLong nLOweCharge = 0;
	CLong nLCurEffBalance = 0;
	lCurEffBalance = 0;

	
    iLatnID = tagCondition.nLatnID = rec.field(FIELD_ID(LATN_ID)).getInteger();;
    lAcctID = tagCondition.lAcctID = rec.field(FIELD_ID(default_acct_id)).getLong();
    tagCondition.tDealTime = CTime::getCurrentTime();
    tagCondition.nStaffID = BACKGROUND_STAFF_ID;

    if (!getOweBalance(tagCondition, tagSimulWriteOffRes,BalanceRecVct, AcctItemRecVct)) 
    {
    	iErrCode = RESULT_CODE_BALANCE_SEARCH_FAIL;
    	strerrmsg += "QueryBalance fail,"; 
        strerrmsg += getQBalErrMsg();
        return false;
    
    }  

	nLOweCharge = tagSimulWriteOffRes.nLOweCharge;
	nLCurEffBalance = tagSimulWriteOffRes.nLCurEffBalance;

	//������Ƿ�ѵ�ʱ���ٸ��ݻ����еĲ�Ʒ��ȡ��Ʒ�Ĳ�Ȩ�ͻ�ID
	//���ݵȼ���ȡ�ͻ�����
	CLong lServID = rec.field(FIELD_ID(SERV_ID)).getLong();
	CTime tTime = rec.field(FIELD_ID(START_TIME)).getDate();

	CustCreditLevelConfig tagCustLvConfig;
	CustCreditLevel tagCustLv;
	Serv serv;
	ProdInstInfo strProdInstInfo;
	serv.InitByServID(lServID,tTime);
	int iRet = serv.getSingleProdInst(strProdInstInfo);
	if (1 != iRet)
	{
		iErrCode = RESULT_CODE_GET_SINGE_PRODINST_FAILED;
		strerrmsg += "QueryBalance getSingleProdInst fail"; 
		return false;
	}
	else
	{
		Cust CCust;
		CustInfo tagCustInfo;
		CCust.setLatnID( iLatnID );
		CCust.Initiate( strProdInstInfo.lOwnerCustID,tTime );
		CString sCreditLv;
		
		iRet = CCust.getCreditLevel( sCreditLv );
	    if(iRet < 0 )
	    {
	    	iErrCode = RESULT_CODE_GET_CREDIT_LEVEL_FAILED;
			strerrmsg += "QueryBalance getCreditLevel fail";
			return false;
	    }
		else if (1 == iRet)
		{
			if( !sCreditLv.isNumber() )
			{
				iErrCode = RESULT_CODE_CREDIT_LEVEL_NOT_NUMBER;
				strerrmsg += "QueryBalance Credit_Level Not Number fail"; 
				return false;
			}
				
		    // ȡ�ͻ������ŵȼ�����
		    memset( &tagCustLv, '\0', sizeof(tagCustLv) );
		    tagCustLvConfig.iLatnId = iLatnID;
		    tagCustLvConfig.sCustLevelId = sCreditLv;
			//tagCustLvConfig.info.lUndirAmount = 0;
		    iRet = tagCustLvConfig.search();
			//��ѯ��������
			if (1 != iRet )
			{
				memcpy( &tagCustLv, &tagCustLvConfig.info, sizeof(CustCreditLevel));
				tagCustLv.lUndirAmount = 0;				
			}
			else
			{
				memcpy( &tagCustLv, &tagCustLvConfig.info, sizeof(CustCreditLevel));
			}
		    			

		}
		else
		{
			tagCustLv.lUndirAmount = 0;
		}

	}

    if (nLOweCharge > 0)
	{		
		//������= (- CustCreditLevel.lUndirAmount(��ͣ����)) �C Ƿ�� 
		lCurEffBalance = (-tagCustLv.lUndirAmount) - nLOweCharge;	
	}
	else
	{
		////������= (- CustCreditLevel.lUndirAmount(��ͣ����)) + ģ�����
		lCurEffBalance = (-tagCustLv.lUndirAmount) + nLCurEffBalance; 
	}  
	
    return true;
}




COcsCharge::COcsCharge()
{
	m_pVoiceShm = NULL;
	m_pFluxShm = NULL;
	m_pVoiceMutex = NULL;
	m_pFluxMutex = NULL;
	m_pSessionSwitch = NULL;
	m_iSourceEventTypeID = -1;
	m_iRoamFlag = 0;
	//m_pLteShm = NULL;
	//m_pLteMutex = NULL;

	m_iIndbFlux = 20;  //20M
	m_iIndbDuration = 60; //60����
	m_iIndbRequestCnt = 40; //40��
	m_iIndbFlag = 0;
	m_ucSwitchFlag = 0x00;
	//m_bMVNOFlag = false;
	//m_pConnBill590 = NULL;
}


bool COcsCharge::init(int& rLatnID,bool bMVNOFlag)
{
	//m_bMVNOFlag = bMVNOFlag;
	m_iLatnID = rLatnID;
	//����һ��Ԥ����ĳ�ʼ��
	bool bRet = m_cPreProc1OL.Init();
	if (!bRet)
	{
		m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
		m_strerrmsg += "m_cPreProc1OL.Init  failed," + m_strerrmsg;
		cout << m_strerrmsg<<endl;
		return false;	 
	}

	//���߶���Ԥ����
	bRet = m_cPreProc2OL.Init();
	if (!bRet)
	{
		m_cPreProc2OL.getErrMessage(m_iErrCode,m_strerrmsg);
		m_strerrmsg += "m_cPreProc2OL.Init  failed," + m_strerrmsg;
		cout << m_strerrmsg<<endl;
		return false;	 
	}
	
		  
	//�������۳�ʼ��
	bRet = m_cRatingOL.Init();
	if (!bRet)
	{	
		m_cRatingOL.getErrMessage(m_iErrCode,m_strerrmsg);
		m_strerrmsg += "m_cRatingOL.Init  failed," + m_strerrmsg;
		cout << m_strerrmsg<<endl;
		return false;	
	}

	bRet = m_cMemBaseHandle.Init(bMVNOFlag);
    if (!bRet)
    {
        m_cMemBaseHandle.getErrMessage(m_iErrCode,m_strerrmsg);
        m_strerrmsg += "g_cMemBaseHandle.Init  failed," + m_strerrmsg;
		cout << "error :" << m_strerrmsg <<endl;
        return false;    
    }

	char szShmName[50];
    string sHostId;
    sHostId = getenv(ENV_NAME_HOST_ID_DSPCH.c_str());
    sprintf(szShmName, "MT_VOICE_SESSION_MNG_%d",atoi(sHostId.c_str())); 
	
    if (!LoadOcsSession(m_pVoiceShm,m_pVoiceMutex,m_cVoiceOcsSessionMng,szShmName))
    {
        return false;
    }
    

    sprintf(szShmName, "MT_FLUX_SESSION_MNG_%d",atoi(sHostId.c_str())); 
    if (!LoadOcsSession(m_pFluxShm,m_pFluxMutex,m_cFluxOcsSessionMng,szShmName))
    {
        return false;
    }
	/*
	sprintf(szShmName, "MT_LTE_%d",atoi(sHostId.c_str())); 
	if (!LoadOcsAccumMng(m_pLteShm,m_pLteMutex,m_cAccumMng,szShmName))
    {     
        return false;    
    }
    */
	
	
	if (!LoadErrorTypeMap())
	{
		return false;
	}

	if (!LoadIndbConfig())
	{
		return false;
	}

	if (!LoadIniCfg())
	{
		return false;
	}

	
	
	/*
	string sErr;
    m_pConnBill590 = DBConnect(sErr, "bill590");
    if (NULL == m_pConnBill590)
    {
        cout << "connect bill590 failed " << sErr << endl;
        return false;
    }
    
	if (!m_MvnoMng.onInit(m_pConnBill590,m_iLatnID))
	{
		cout <<"IBC_MvnoMng onInit failed, "<< m_MvnoMng.getErrMsg()<<endl;
		return false;
	}
	*/

	return true;
}

/*
bool COcsCharge::LoadOcsAccumMng(ShareMemory *&pShm,Mutex *&pMutex,OcsAccumMng &rOcsAccumMng,const char* pShmName)
{
	//����LTE�����ڴ�
    _IPC_RESOURCE ipcResource;
	if (!QueryShmKeyByName(pShmName, ipcResource))
	{
		//m_iErrCode = RESULT_CODE_INIT_OCSSESSION_FAILED;
		//m_strerrmsg += "QueryShmKeyByName fail, SHM_NAME = ";
		//m_strerrmsg += tmp_shm_name;
		cout << "QueryShmKeyByName fail, SHM_NAME = " << pShmName<<endl;
		return false;
	}


	cout << "LTE ipcResource.unIpcKey = " << ipcResource.unIpcKey <<endl;
	cout << "LTE ipcResource.unSize = "<<ipcResource.unSize<<endl;



    pShm  = new ShareMemory(ipcResource.unIpcKey);
    if (!pShm->open())
    {

        cout << "pShm.open fail,tmp_shm_name =" << pShmName <<","<< pShm->getLastError()<<endl;
		pShm = NULL;
        return false;

    }
    
    char *pShmAddr = (char*)pShm->addr();

    if(NULL == pShmAddr)
    {

        cout << "NULL == pShmAddr,tmp_shm_name=" << pShmName <<endl;
        return false;
    }	

    
    //��������������
    _IPC_RESOURCE ipcMutex;
    if(!QuerySemKeyByName(pShmName, ipcMutex))
    {	

		cout << "QuerySemKeyByName fail, SHM_NAME = "<<pShmName<<endl; 
		return false;
    }


	cout << "ipcMutex key = " << ipcMutex.unIpcKey <<endl;


    
    pMutex = new Mutex(ipcMutex.unIpcKey );

    if (!pMutex->open())
    {
        cout << "pMutex->open failed,tmp_shm_name="<< pShmName<<endl;
		pMutex = NULL;
        return false;
    }

    if(1 != rOcsAccumMng.load(pShmAddr,pMutex))
    {
        cout << "rOcsAccumMng.load failed,tmp_shm_name=" <<pShmName<<endl;
        return false;
    }
    
    return true;  


}*/


bool COcsCharge::LoadOcsSession(ShareMemory *&pShm,Mutex *&pMutex,OcsSessionMng &rOcsSessionMng,const char* pShmName)
{
   
    
    _IPC_RESOURCE ipcResource;
	if (!QueryShmKeyByName(pShmName, ipcResource))
	{
		cout << "QueryShmKeyByName fail,SHM_NAME = " << pShmName<<endl;
		return false;
	}
    
    cout << "ipcResource.unIpcKey = " << ipcResource.unIpcKey <<endl;
    cout << "ipcResource.unSize = "<<ipcResource.unSize <<endl;
    
    pShm  = new ShareMemory(ipcResource.unIpcKey);
    if (!pShm->open())
    {
		cout<<"InitOcsSession, pShm->open failed"<<endl;
        return false;
    }
    
    char *pShmAddr = (char*)pShm->addr();
    
    _IPC_RESOURCE ipcMutex;
    if(!QuerySemKeyByName(pShmName, ipcMutex))
    {	
		cout << "QuerySemKeyByName fail, SHM_NAME = " << pShmName<<endl;
		return false;
    }
    
    cout << "ipcMutex key = " <<ipcMutex.unIpcKey <<endl;
    
    
    pMutex = new Mutex(ipcMutex.unIpcKey );
    
    if (!pMutex->open())
    {
        cout << "pMutex->create failed" <<endl;
        return false;

    }

    if(1 != rOcsSessionMng.load(pShmAddr,pMutex))
    {
        cout << "m_cOcsSessionMng.load failed" <<endl;
        return false;
    }
    
    return true; 
    
    
}



bool COcsCharge::LoadIndbConfig()
{
	IBC_ParamCfgMng  cParamCfgMng;
    
    // ȡ�������ļ�������
    //��ʼ��������ʹ��ǰ�����ʼ��
    if(!cParamCfgMng.bOnInit())
    {
        cParamCfgMng.getError(m_strerrmsg, m_iErrCode);
        cout << "�������ýӿڳ�ʼ��ʧ�ܣ�������=" << m_iErrCode <<", ������Ϣ=" << m_strerrmsg << endl;
        return false;
    }

	string strParamName = "OCSDSPCH.OCS_INDB_FLUX";
	bool res = cParamCfgMng.getParamValueInt(strParamName, m_iIndbFlux);
    if (!res)
	{
		cParamCfgMng.getError(m_strerrmsg,m_iErrCode);
		cout << "��ѯOCS_INDB_FLUXʧ�ܣ�������="<<m_iErrCode<<", ������Ϣ="<<m_strerrmsg<<endl;
		return false;
	}
	cout << "OCSDSPCH.OCS_INDB_FLUX = "  << m_iIndbFlux <<endl;


	strParamName = "OCSDSPCH.OCS_INDB_DURATION";
	cParamCfgMng.getParamValueInt(strParamName, m_iIndbDuration);
    if (!res)
	{
		cParamCfgMng.getError(m_strerrmsg,m_iErrCode);
		cout << "��ѯOCS_INDB_DURATIONʧ�ܣ�������="<<m_iErrCode<<", ������Ϣ="<<m_strerrmsg<<endl;
		return false;
	}


	cout << "OCSDSPCH.OCS_INDB_DURATION = "  << m_iIndbDuration <<endl;


	strParamName = "OCSDSPCH.OCS_INDB_REQUESTCNT";
	cParamCfgMng.getParamValueInt(strParamName, m_iIndbRequestCnt);
    if (!res)
	{
		cParamCfgMng.getError(m_strerrmsg,m_iErrCode);
		cout << "��ѯOCS_INDB_REQUESTCNTʧ�ܣ�������="<<m_iErrCode<<", ������Ϣ="<<m_strerrmsg<<endl;
		return false;
	}

	strParamName = "OCSDSPCH.OCS_INDB_FLAG";
	cParamCfgMng.getParamValueInt(strParamName, m_iIndbFlag);
    if (!res)
	{
		cParamCfgMng.getError(m_strerrmsg,m_iErrCode);
		cout << "��ѯOCS_INDB_FLAGʧ�ܣ�������="<<m_iErrCode<<", ������Ϣ="<<m_strerrmsg<<endl;
		return false;
	}


	cout << "OCSDSPCH.OCS_INDB_FLAG = "  << m_iIndbFlag <<endl;
	
	return true;
}


bool COcsCharge::LoadIniCfg()
{
	IniReader ini;
    
    int iModule = MODULE_ID(TicketMove);
    int iFlowID = 0;
	string sHostId = getenv(ENV_NAME_HOST_ID_DSPCH.c_str());
    int iHostID = atoi(sHostId.c_str());
	int iSwitchFlag = 0;
	//cout << "iModule = " << iModule << ",iFlowID = " << iFlowID << ",iHostID = " << iHostID <<endl;

	
    if (!ini.loadFromShm( iModule,iFlowID,iHostID))
	{   
		cout << "init, loadFromShm failed" <<endl;
		return false;
	}
	
	//ȡ�����ڴ����ݿ��������
    if (!ini.queryValue("TicketMove","SwitchFlag", iSwitchFlag))
	{
	    
		cout << "get TicketMove, SwitchFlag failed " <<endl;
        return false;
	}
	
	//��ʼ��Ϊ��
	for (int i=0;iSwitchFlag;i++)
	{	
		if ((iSwitchFlag%10) ==1)
		{
			m_ucSwitchFlag = m_ucSwitchFlag | (1 <<i);
		}
		iSwitchFlag = iSwitchFlag/10;
	}
	
	printf("%x\n",m_ucSwitchFlag);
	return true;

}

bool COcsCharge::LoadErrorTypeMap()
{
	const int MAX_FETCH_NUM = 5000; 
	string sErr;
	Connection * pConnCfg = DBConnect(sErr, "billing_config_release");
	Statement* stmt = NULL;
	try{
		stmt = pConnCfg->createStatement();

		int iErrorTypeID[MAX_FETCH_NUM];
		int iOcsErrorTypeID[MAX_FETCH_NUM];

		
		string sql = "select error_type_id,nvl(ocs_error_type_id,9999) ocs_error_type_id from tp_error_type";
		
		stmt->setSQL(sql);

	    ResultSet *rs = stmt->executeQuery();

	    rs->setDataBuffer(1,iErrorTypeID,OCCIINT,sizeof(int));
	    rs->setDataBuffer(2,iOcsErrorTypeID,OCCIINT,sizeof(int));

	    CInt recCnt = 0;
		CInt getRecCnt = 0;
		CInt lastRecCnt = 0;
        int db_is_high_patch = DBisHighPatch();
        while (true)
        {
            rs->next(MAX_FETCH_NUM);
            recCnt = rs->getNumArrayRows();
            if (db_is_high_patch == 1)
            {
                getRecCnt = recCnt;
            }
            else if (db_is_high_patch == 0)
            {
                getRecCnt = recCnt - lastRecCnt;
            }
            else
            {
                cout << "ȡOCCI�汾�쳣" <<endl;
                return false;
            }

            for (int i = 0; i < getRecCnt; i++)
            {
                m_ErrorTypeMap.insert(make_pair<int,int>(iErrorTypeID[i],iOcsErrorTypeID[i]));
            }

            if (getRecCnt != MAX_FETCH_NUM)
                break;
            else
                lastRecCnt = lastRecCnt + getRecCnt;
        }

		
        if (rs)
        {
            stmt->closeResultSet(rs);
            rs = NULL;
        }
        
        if (stmt)
        {
            pConnCfg->terminateStatement(stmt);
            stmt = NULL;
        }

		if (pConnCfg)
		{
			DBDisconnect(pConnCfg);
			pConnCfg = NULL;
		}

		cout << "m_ErrorTypeMap.size() =" << m_ErrorTypeMap.size()<<endl;

	}
	catch(SQLException& e)
	{
		cout << e.getMessage() << endl;
		pConnCfg->terminateStatement(stmt);
		m_iErrCode = RESULT_CODE_INIT_ERROR_TYPE_MAP_FAILED;
		m_strerrmsg += e.getMessage();
		return false;
	}
	return true;
}


bool COcsCharge::GetAmount(SessionChargeInfo& sChargeInfo,PACK_OCS_REQUEST& stOcsReq,Record& Rec,bool bUpdate)
{
	DTimerCalc calc(__LINE__, "GetAmount");
	//Ĭ����Ȩ��ΪRSU_CC_TIME  RSU_CC_TOTAL_OCTETS
    sChargeInfo.lGrantedTime = atol(GetOcsDataValues(OCS_FIELD_RSU_CC_TIME).c_str());  
    sChargeInfo.lGrantedTotalOctets = atol(GetOcsDataValues(OCS_FIELD_RSU_CC_TOTAL_OCTETS).c_str());
	//ȡCCG��Ŀ��ʶ ������3A�������ֶ�Ϊ0
	GetPid(Rec,sChargeInfo);
	
	//���°��Ų�ѯ�Ự����
    if (bUpdate)
    {
    	
        //���ڵ�һ�����°���˵ ��ѯ�Ự����ʹ����Ҳ��û��¼�� 
        //�ڶ��β��� �Ƿ��ڳ�ʼ����ʱ��Ͳ�һ��0�Ľ�ȥ
        SessionRec sessionrec;
		m_pSessionSwitch->queryRec(stOcsReq.sSessionId,sChargeInfo.szPID,sChargeInfo.iDataOffset);
        if (sChargeInfo.iDataOffset != -1)
        {	
        	//SessionRec sessionrec;
            m_pSessionSwitch->readRec(sChargeInfo.iDataOffset,sessionrec);  
			sChargeInfo.lAmount_duration = sessionrec.lAmount_duration;
			sChargeInfo.lAmount_flux = sessionrec.lAmount_flux;

			//switchflag��λΪ1�Ĵ�����ĩ��ÿ��6�㡢23��0�����
			if (m_ucSwitchFlag&0x01)
			{
				if (EVENT_TYPE_CDMA_SOURCE_GROUPING == sChargeInfo.iSourceEventTypeID)
				{
					//ÿ��6����� ��ʼʱ������6��,��ǰʱ������6��Ķ�����
					CTime tBeginTime = sessionrec.tBeginTime;
					CTime tCurrTIme = CTime::getCurrentTime();
					CTime tNextTime = tCurrTIme + 86400;
					//��ĩ23��55�ֶ�
					CTime tBanTime = CTime(tCurrTIme.getYear(),tCurrTIme.getMonth(),tCurrTIme.getDay(),23,55);
					if (tNextTime.getMonth() != tCurrTIme.getMonth())
					{
						if (tBeginTime < tBanTime &&  tCurrTIme >= tBanTime)
						{
							sChargeInfo.iTerminationFlag = 0;
							sChargeInfo.bBanflag = true;
						}
					}
					
					//6���
					tBanTime = CTime(tCurrTIme.getYear(),tCurrTIme.getMonth(),tCurrTIme.getDay(),6);
					if (tBeginTime < tBanTime && tCurrTIme >= tBanTime)
					{
							sChargeInfo.iTerminationFlag = 0;
							sChargeInfo.bBanflag = true;
					}
					
					//23���
					tBanTime = CTime(tCurrTIme.getYear(),tCurrTIme.getMonth(),tCurrTIme.getDay(),23);
					if (tBeginTime < tBanTime && tCurrTIme >= tBanTime)
					{
							sChargeInfo.iTerminationFlag = 0;
							sChargeInfo.bBanflag = true;
					}
		
					//24���
					if (tBeginTime.getDay() !=  tCurrTIme.getDay())
					{
							sChargeInfo.iTerminationFlag = 0;
							sChargeInfo.bBanflag = true;
					}

				}

			}

			
			//switchflag����10��ǲŶ�bscid�б���Ľ����л�
			if (m_ucSwitchFlag&0x02)
			{
				if (EVENT_TYPE_CDMA_SOURCE_GROUPING == sChargeInfo.iSourceEventTypeID)
				{
					CLong lProdSpecID = Rec.field(FIELD_ID(billing_prod_spec_id)).getLong();
					//����ֻ����ֻ��û������û�
					if (800000002 == lProdSpecID)
					{
						int iRoamOrgType = Rec.field(FIELD_ID(ROAM_ORG_TYPE)).getInteger();
						//���������ͷ����仯ʱ���ϱ�ǰ��û�������
						//ֻ��ʡ�ڲŻ���bsc_id roam_org_type0���� 9ʡ��
						if (sessionrec.iRoamOrgType !=iRoamOrgType && 
							(sessionrec.iRoamOrgType == 0 || sessionrec.iRoamOrgType == 9)
							&& strcmp(sessionrec.szBscID,""))
						{
							sChargeInfo.iTerminationFlag = 0;
							sChargeInfo.bBanflag = true;
							sessionrec.ifSessionCommit = true;
						}

					}

					
				}

			}

			
        }
		else //��ѯ���� ����ʧ�� 
		{
			
			//SessionRec sessionrec;
			memset(&sessionrec,0,sizeof(SessionRec));
			strncpy(sessionrec.session_id,stOcsReq.sSessionId,130);
			sessionrec.session_id[130] = '\0';
			strncpy(sessionrec.PID,sChargeInfo.szPID,20);
			sessionrec.PID[20] = '\0';
			sessionrec.lAmount_duration = 0;
			sessionrec.serv_id= sChargeInfo.lServID;
			sessionrec.lAmount_flux = 0;
			sessionrec.sessionState = '1';	   
			sessionrec.lCharge = 0;
			sessionrec.tTariffChangeTime = 0;
			//sessionrec.tBeginTime = 0;
			sessionrec.lFluxCardUsed = 0;
			sessionrec.iRequestCnt = 0;
			sessionrec.LastDealTime = CTime::getCurrentTime().getTime();
			//��һ�����°���¼��ʼʱ��
			sessionrec.tBeginTime = CTime::getCurrentTime().getTime();
			sessionrec.ifSessionCommit = false;

			if (m_ucSwitchFlag&0x02)
			{
				if (EVENT_TYPE_CDMA_SOURCE_GROUPING == sChargeInfo.iSourceEventTypeID)
				{
					CLong lProdSpecID = Rec.field(FIELD_ID(billing_prod_spec_id)).getLong();
					//����ֻ����ֻ��û������û�
					if (800000002 == lProdSpecID)
					{
						int iRoamOrgType = Rec.field(FIELD_ID(ROAM_ORG_TYPE)).getInteger();
						CString sBSCId = Rec.field(FIELD_ID(BSC_ID)).getString();

						if (0 ==iRoamOrgType || 9 == iRoamOrgType)
						{	
							strncpy(sessionrec.szBscID,(const char*)sBSCId,21);
							sessionrec.iRoamOrgType = iRoamOrgType;
						}

					}

					
				}

			}
		
			sChargeInfo.iDataOffset = m_pSessionSwitch->addRec(sessionrec);
			if (sChargeInfo.iDataOffset == -1)
			{			 
				m_iErrCode = RESULT_CODE_SESSION_ADD_REC_FAILED;
				m_strerrmsg = "m_pSessionSwitch->addRec failed,SourceEventTypeID= ";
				m_strerrmsg += toString(m_iSourceEventTypeID);
				return false;
			} 
			
			/*
			m_iErrCode = RESULT_CODE_SESSION_NOT_EXIST;
			m_strerrmsg = "Session is not exist,sSessionId =";
			m_strerrmsg += stOcsReq.sSessionId;
			m_strerrmsg += ",PID =";
			m_strerrmsg += sChargeInfo.szPID;	
			return false;
			*/
		}

		/*
		int iRet = m_pSessionSwitch->updateSessionState(sChargeInfo.iDataOffset,'0');
		if (iRet == -1)
		{
	
			m_iErrCode = RESULT_CODE_SESSION_UPDATE_REC_FAILED;
			m_strerrmsg = "pSessionMng->updateSessionState failed,iSourceEventTypeID= ";
			m_strerrmsg += toString(m_iSourceEventTypeID);
			return false;
		}
		*/
	
	
    } 

	//ȡ�ϴ�ʵ��ʹ����
    switch(m_iSourceEventTypeID)
    {
        case EVENT_TYPE_CDMA_SOURCE_VOICE:            
            //ȡ�����ϴ�ʵ��ʱ�� ��������0����
            //ʵ��ʹ����
            sChargeInfo.iUsuCcTime = atoi(GetOcsDataValues(OCS_FIELD_USU_CC_TIME).c_str());  
            
            //ȡʱ��Ƭ           
            sChargeInfo.iRsuCcTime = atoi(GetOcsDataValues(OCS_FIELD_RSU_CC_TIME).c_str());  
            
            break;
            
        case EVENT_TYPE_CDMA_SOURCE_GROUPING:
            //ȡ������������0           
            sChargeInfo.iUsuCcTime = atoi(GetOcsDataValues(OCS_FIELD_USU_CC_TIME).c_str());
            
            //ȡʱ��Ƭ          
            sChargeInfo.iRsuCcTime = atoi(GetOcsDataValues(OCS_FIELD_RSU_CC_TIME).c_str()); 
            
            //�ϴ�ʵ��ʹ����
            sChargeInfo.lUsuCcTotalOctets = atol(GetOcsDataValues(OCS_FIELD_USU_CC_TOTAL_OCTETS).c_str());   
			
            //�������
            sChargeInfo.lRsuCcTotalOctets = atoi(GetOcsDataValues(OCS_FIELD_RSU_CC_TOTAL_OCTETS).c_str()); 
            
            break; 
            
         default:               
            m_iErrCode = RESULT_CODE_OTHER_EVENT_FAILED;
			m_strerrmsg = "�Ự���Ʋ�֧�ָ��¼�����,iSourceEventTypeID= ";
            m_strerrmsg += toString(m_iSourceEventTypeID);
            return false;
            break;   

    }

	if (stOcsReq.cRequestedMinServiceUnit.size() == 0)
	{
        m_iErrCode = RESULT_CODE_REQ_MIN_UNIT_FAILED;
        m_strerrmsg += ",cRequestedMinServiceUnit.size == 0";
        return false;	
	}


    //Requested-Min-Service-Unit. CC-Time
    int iMinCCTime = stOcsReq.cRequestedMinServiceUnit[0].lCCTime;
	sChargeInfo.iMinCCTime = iMinCCTime;
	
    //Requested-Min-Service-Unit. CC-Total-Octets
    long lMinCCTotalOctets = stOcsReq.cRequestedMinServiceUnit[0].lCCTotalOctets; 
    sChargeInfo.lMinCCTotalOctets = lMinCCTotalOctets;

	
	return true;

}


/*
���ؽ���ҵ����Ϣ��
ȡ���¼�����
�����¼�����
*/
int COcsCharge::initCharge(PACK_OCS_REQUEST& stOcsReq,OcsChargeInfo& sChargeInfo)
{

	g_IncrementMode = 0;
	bool bRet = m_cDataExPlain.loadData(stOcsReq.cServiceInformationVec[0].c_str());	   
	if (!bRet)
	{
		m_iErrCode = RESULT_CODE_LOAD_DATAEXPLAIN_FAILED;
		m_strerrmsg += "COcsDataExPlain loadData fail";   
		return -1;
	}
	m_iRoamFlag =0;
	m_iSourceEventTypeID = GetSourceEventType(stOcsReq,m_cDataExPlain,m_iRoamFlag,m_strerrmsg,m_iErrCode);
	//����sSrvCntxtIdȡԴ�¼�����
	if (-1 == m_iSourceEventTypeID)
	{
		return -1;
	}
	
	sChargeInfo.setSourceEventType(m_iSourceEventTypeID);
    
	//�����¼�����
	switch(stOcsReq.iReqType)
	{
		case 1:
		case 2:			
		case 3:
			//�Ự��ֻ���������ͷ������ҵ��
		    if (EVENT_TYPE_CDMA_SOURCE_VOICE != m_iSourceEventTypeID 
		        && EVENT_TYPE_CDMA_SOURCE_GROUPING != m_iSourceEventTypeID )
		    {      
		        m_iErrCode = RESULT_CODE_OTHER_EVENT_FAILED;
				m_strerrmsg ="�Ự���Ʋ�֧�ָ��¼�����,SourceEventTypeID= " ;
				m_strerrmsg += toString(m_iSourceEventTypeID);  
				return -1;
		    }	
			if (!SwitchSession())
			{
				return -1;
			}
			break;
		
		case 4:	
			{
				/*
				//�¼��ͳ������߻���  ֻ������ź�ismp
				string strSessionCdr = stOcsReq.sSessionId;
		        transform(strSessionCdr.begin(), strSessionCdr.end(), strSessionCdr.begin(), ::toupper);
				//��������߻��������Դ���
	            if(strncmp(strSessionCdr.c_str(),"CDR",3) ==0)
	            {
					break;
				}*/
						
				if (EVENT_TYPE_CDMA_SOURCE_SMS != m_iSourceEventTypeID 
					&& EVENT_TYPE_CDMA_SOURCE_OPERATION != m_iSourceEventTypeID
					&& EVENT_TYPE_SOURCE_BNG != m_iSourceEventTypeID   // itnm00075356lanlh:�����캽ƽ̨�� OCS ϵͳ�Խӵ�����   2015/10 
					)
				{
					m_iErrCode = RESULT_CODE_OTHER_EVENT_FAILED;
					m_strerrmsg ="�¼����Ʋ�֧�ָ��¼�����,SourceEventTypeID= " ;
					m_strerrmsg += toString(m_iSourceEventTypeID);  
					return -1;
				}
			}
			break;
		default:
			break;

	}

	return 1;

}



bool COcsCharge::deleteSessionByServID(CLong& lServID)
{	
	DTimerCalc calc(__LINE__, "deleteSessionByServID");
	if (NULL != m_pSessionSwitch)
	{

		int iRet = m_pSessionSwitch->clearOffLineSession(lServID);
		if (iRet==-1)
		{
			char szErr[512] = {0};
			m_iErrCode = RESULT_CODE_SESSION_DELETE_REC_FAILED;
			snprintf(szErr,sizeof(szErr),"m_pSessionSwitch->clearOffLineSession fail,ServID=%ld "\
			,lServID);
			m_strerrmsg = szErr;
			iRet = 0;
			return false;		

		}
	}
	return true;

}


bool COcsCharge::deleteSession(PACK_OCS_REQUEST& stOcsReq,const char* pszPID)
{	
	DTimerCalc calc(__LINE__, "deleteSession");
	if (NULL != m_pSessionSwitch)
	{

		int iRet = m_pSessionSwitch->deleteRec(stOcsReq.sSessionId,pszPID);
		if (iRet==-1)
		{
			char szErr[512] = {0};
			m_iErrCode = RESULT_CODE_SESSION_DELETE_REC_FAILED;
			snprintf(szErr,sizeof(szErr),"pSessionMng->deleteRec fail,sSessionId=%s,PID=%s,SourceEventTypeID"\
			,stOcsReq.sSessionId,pszPID,m_iSourceEventTypeID);
			m_strerrmsg = szErr;
			iRet = 0;
			return false;		

		}
	}
	return true;

}


bool COcsCharge::updateSession(SessionChargeInfo& sChargeInfo)
{	
	if (sChargeInfo.iDataOffset == -1)
	{
		m_iErrCode = RESULT_CODE_NOT_FIND_DATA_OFFSET_FAILED;
		m_strerrmsg += "second GetSessionAmount failed,iDataOffset == -1";
		return false;						
	}

	int iRet = m_pSessionSwitch->updateRec(sChargeInfo.iDataOffset,sChargeInfo.iUsuCcTime,sChargeInfo.lUsuCcTotalOctets,sChargeInfo.bBanflag);
	if (iRet == -1)
	{		  
		m_iErrCode = RESULT_CODE_SESSION_UPDATE_REC_FAILED;
		m_strerrmsg = "pSessionMng->updateRec failed,iSourceEventTypeID= ";
		m_strerrmsg += toString(m_iSourceEventTypeID);
		return false;
	}
	
	/*
	iRet = m_pSessionSwitch->updateSessionState(sChargeInfo.iDataOffset,'1');
	if (iRet == -1)
	{			

		m_iErrCode = RESULT_CODE_SESSION_UPDATE_STATE_FAILED;
		m_strerrmsg = "pSessionMng->updateSessionState failed,iSourceEventTypeID= ";
		m_strerrmsg += toString(m_iSourceEventTypeID);
		return false;
	}
	*/

	return true;

}



//��ʱ�� = ʱ��Ƭ���(RSU_CC_TIME) + �ϴ�ʵ��ʹ�õ�ʱ����USU_CC_TIME�� + �Ự�ۻ���ʱ��(SessionRec::lAmount_duration)        
//��СԤ��������ʱ�� = Requested-Min-Service-Unit. CC-Time +  �ϴ�ʵ��ʹ�õ�ʱ����USU_CC_TIME�� + �Ự�ۻ���ʱ��(SessionRec::lAmount_duration)    

//������ = �������(RSU_CC_TOTAL_OCTETS) + �ϴ�ʵ��ʹ�õ�������USU_CC_TOTAL_OCTETS�� + �Ự�ۻ�������(SessionRec::lAmount_flux) 
//��СԤ������������ = Requested-Min-Service-Unit. CC-Total-Octets + �ϴ�ʵ��ʹ�õ�������USU_CC_TOTAL_OCTETS��+ �Ự�ۻ�������(SessionRec::lAmount_flux)  
 
//�����ܼƷ��� ������СԤ����
bool COcsCharge::SetBillingAmount(SessionChargeInfo& sChargeInfo,Record& rec,bool bMinflag)
{
    //ʱ��Ƭ 
    int iCallInterval = 0;        
    long lFluxInterval = 0; 
    
    int iMinCCTime = sChargeInfo.iMinCCTime;
    long lMinCCTotalOctets = sChargeInfo.lMinCCTotalOctets; 
		
    //��ʼ��ʱ�Ự�ۻ���ʹ������0
    long lAmountDuration= sChargeInfo.lAmount_duration;
    long lAmountFlux = sChargeInfo.lAmount_flux;

	int iUsuCcTime = sChargeInfo.iUsuCcTime;
	long lUsuCcTotalOctets = sChargeInfo.lUsuCcTotalOctets;
       	
    switch(m_iSourceEventTypeID)
    {
        //����ȡ�ϴ�ʵ��ʹ�õ�ʱ��
        case EVENT_TYPE_CDMA_SOURCE_VOICE: 

			iCallInterval = bMinflag ? sChargeInfo.iMinCCTime : sChargeInfo.iRsuCcTime;
            rec.field(FIELD_ID(CALL_AMOUNT)).setValue((int)(iCallInterval+lAmountDuration+iUsuCcTime));
            break;
            
        //��������Ҫȡ���� ʱ����֪���᲻����
        case EVENT_TYPE_CDMA_SOURCE_GROUPING:                            
            iCallInterval = bMinflag ? sChargeInfo.iMinCCTime : sChargeInfo.iRsuCcTime;
            rec.field(FIELD_ID(CALL_AMOUNT)).setValue((int)(iCallInterval+lAmountDuration+iUsuCcTime));
            
            lFluxInterval = bMinflag ? sChargeInfo.lMinCCTotalOctets : sChargeInfo.lRsuCcTotalOctets;
            rec.field(FIELD_ID(volume_downlink)).setValue((CLong)(lFluxInterval+lAmountFlux+lUsuCcTotalOctets));
            break; 
            
         default:               
            m_iErrCode = RESULT_CODE_OTHER_EVENT_FAILED;     
			m_strerrmsg = "�Ự���Ʋ�֧�ָ��¼�����,SourceEventTypeID= ";
			m_strerrmsg += toString(m_iSourceEventTypeID);
            return false;
            break;   

    }
  
    return true;    
}





//������Ȩ�� Total-Octets TotalOctets
bool COcsCharge::ReverseCharge(Record& rec,SessionChargeInfo& sChargeInfo)
{
    sChargeInfo.lGrantedTime = 0;
    sChargeInfo.lGrantedTotalOctets = 0; 
   
    //�ܼƷ�ʱ��
    long lCallAmount = 0; 
    //�ܼƷ�����
    long lTotalVolume = 0;

    if (billing_amount_type_duation == sChargeInfo.iBillingAmountType)
    {
       
        lCallAmount = rec.field(FIELD_ID(CALL_AMOUNT)).getInteger();
        sChargeInfo.lGrantedTime = (lCallAmount - sChargeInfo.lCurrTicketDurationResource) * sChargeInfo.lCurEffBalance /sChargeInfo.lCharge;
        
        sChargeInfo.lGrantedTime += sChargeInfo.lCurrTicketDurationResource - (sChargeInfo.lAmount_duration+sChargeInfo.iUsuCcTime);
     
        if (sChargeInfo.lGrantedTime > sChargeInfo.iRsuCcTime)
        {
            sChargeInfo.lGrantedTime = sChargeInfo.iRsuCcTime;

            
        }else if (sChargeInfo.lGrantedTime <sChargeInfo.iMinCCTime)
        {
            sChargeInfo.lGrantedTime = sChargeInfo.iMinCCTime;    
        }
    }
    else if (billing_amount_type_flow == sChargeInfo.iBillingAmountType)
    {
        lTotalVolume = rec.field(FIELD_ID(VOLUME_DOWNLINK)).getLong() + rec.field(FIELD_ID(VOLUME_UPLINK)).getLong();
        
        CString sVolumFlug=rec.field(FIELD_ID(VOLUME_FLAG)).getString();

        //�о�����������۵���Ҫ��δ���������ĵ�ǰ��������������
        if( sVolumFlug.compare("2") == 0)  
        {            
            if (sChargeInfo.lOriginCharge == 0)
            {
                m_iErrCode = RESULT_CODE_ZERO_ORG_CHARGE_FAILED;
                m_strerrmsg += "δ���������ĵ�ǰ��������Ϊ0";
                return false;
            }

            sChargeInfo.lGrantedTotalOctets = (lTotalVolume - sChargeInfo.lCurrTicketTotalLinkResource) * sChargeInfo.lCurEffBalance /sChargeInfo.lOriginCharge; 
            
        }//�������޵ֿ�        
        else
        {
            sChargeInfo.lGrantedTotalOctets = (lTotalVolume - sChargeInfo.lCurrTicketTotalLinkResource) * sChargeInfo.lCurEffBalance /sChargeInfo.lCharge;          
        }

        sChargeInfo.lGrantedTotalOctets += sChargeInfo.lCurrTicketTotalLinkResource + sChargeInfo.lActualPayoutAmount*1024    
        - (sChargeInfo.lAmount_flux+sChargeInfo.lUsuCcTotalOctets);
        

        if (sChargeInfo.lGrantedTotalOctets > sChargeInfo.lRsuCcTotalOctets)
        {
            sChargeInfo.lGrantedTotalOctets = sChargeInfo.lRsuCcTotalOctets;          
        }
        else if (sChargeInfo.lGrantedTotalOctets<sChargeInfo.lMinCCTotalOctets)
        {
            sChargeInfo.lGrantedTotalOctets = sChargeInfo.lMinCCTotalOctets;
        }
        
    
    }
    else
    {
        m_strerrmsg = "iBillingAmountType��������ʱ��,iBillingAmountType= ";
		m_strerrmsg += toString(sChargeInfo.iBillingAmountType);
        sChargeInfo.lGrantedTotalOctets = 0;
        sChargeInfo.lGrantedTime = 0;
    }
    
    
    return true;    
}


int COcsCharge::ChargeAmount(PACK_OCS_REQUEST& stOcsReq,SessionChargeInfo& sChargeInfo,bool bUpdate)
{
    g_vecAccum.clear();
    int iRet = m_cPreProc1OL.InitTicket(m_iSourceEventTypeID);
    if (iRet<0)
    {
        m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
        m_strerrmsg += "m_cPreProc1OL.InitTicket fail," + m_strerrmsg;
        return -1;
    }

    Record& Rec = m_cPreProc1OL.getRecord();

    iRet = ParseTicket(m_iSourceEventTypeID,m_iRoamFlag,Rec,stOcsReq,m_sOrgTicket,m_cDataExPlain,m_strerrmsg,m_iErrCode);
    if (iRet<0)
    {               
        return -1;         
    }

    iRet = m_cPreProc1OL.convertHandle(Rec,m_iSourceEventTypeID,m_sOrgTicket);
    if (iRet<0)
    {
        m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
        m_strerrmsg += "g_cPreProc1OL.convertHandle fail," + m_strerrmsg;
        return -1;
    }

    //DEBUG_2("CPreProc1OL  rec = ",Rec);
    
    if (IsAbnormal(Rec))
    {
        return -1;    
    }       

    iRet = m_cPreProc2OL.PreProcTicket(Rec);
    if (iRet<0)
    {
        m_cPreProc2OL.getErrMessage(m_iErrCode,m_strerrmsg);
        m_strerrmsg += "m_cPreProc2OL.PreProcTicket fail," + m_strerrmsg;
        return -1;
    }


    //DEBUG_2("CPreProc2OL  rec = ",Rec);
	
    if (IsAbnormal(Rec))
    {            
        return -1;    
    }

	//ȡ��������Ԥ�����SERVID
	sChargeInfo.lServID =Rec.field(FIELD_ID(SERV_ID)).getLong();
    //ȡ�ƷѺ���
    //sChargeInfo.strAccNbr = Rec.field(FIELD_ID(BILLING_NBR)).getString();
    //g_strAccNbr = Rec.field(FIELD_ID(BILLING_NBR)).getString();
    //ȡ��������Ԥ������rec���� ���ڼ�����СԤ����
	m_cPreProc1OL.SetMinRecord();

    
    //��ʼ����ʱ��������Ϊ0
	if (!GetAmount(sChargeInfo,stOcsReq,Rec,bUpdate))
	{
		return -1; 
	}

	//�����������־Ϊtrue ���û�������
	if (EVENT_TYPE_CDMA_SOURCE_GROUPING == sChargeInfo.iSourceEventTypeID && sChargeInfo.bBanflag)
	{
		sChargeInfo.iBillingAmountType = 2;
		sChargeInfo.lGrantedTime = 0;
		sChargeInfo.lGrantedTotalOctets = 0;
		sChargeInfo.lCharge = 0;
		return 1;
	}
      
    //�����ܵļƷ��� �ϴ�ʵ��ʹ����+����ʱ��Ƭ+�Ự����ʹ����    
    if (!SetBillingAmount(sChargeInfo,Rec,false))
    {
        return -1;
    }                   

    iRet = m_cRatingOL.ProcessTicket(Rec);
    if ((iRet<0))
    {

        m_cRatingOL.getErrMessage(m_iErrCode,m_strerrmsg);
		IsAbnormal(Rec);
        m_strerrmsg += "g_cRatingOL.ProcessTicket fail," + m_strerrmsg;
        return -1;
    }
 
    DEBUG_2("RatingOL rec = ",Rec);
    
    if (IsAbnormal(Rec))
    {            
        return -1;    
    }
    
    
    //ԭ���۽������ļƷ������� �� �ײ������͵�ʹ����  δ���������ĵ�ǰ�������� ������ʵ�ʵֿ���
    sChargeInfo.lCurrTicketDurationResource = cpplan.lCurrTicketDurationResource;
    sChargeInfo.lCurrTicketTotalLinkResource = cpplan.lCurrTicketTotalLinkResource;
    
    sChargeInfo.lOriginCharge = g_lOriginCharge;   
    sChargeInfo.lActualPayoutAmount = g_lActualPayoutAmount;



    sChargeInfo.iBillingAmountType = Rec.field(FIELD_ID(BILLING_AMOUNT_TYPE)).getInteger();  

	if (0 == sChargeInfo.iBillingAmountType)
	{
		if ( EVENT_TYPE_CDMA_SOURCE_VOICE == sChargeInfo.iSourceEventTypeID)
    	{	
			sChargeInfo.iBillingAmountType = 1;
		}
		else if (EVENT_TYPE_CDMA_SOURCE_GROUPING == sChargeInfo.iSourceEventTypeID)
		{
			sChargeInfo.iBillingAmountType = 2;
		}

	}


	//����charge + charge2	
	if(Rec.hasField(FIELD_ID(CHARGE)))
	{
		sChargeInfo.lCharge += Rec.field(FIELD_ID(CHARGE)).getLong();
	}
	if(Rec.hasField(FIELD_ID(CHARGE_2)))
	{
		sChargeInfo.lCharge += Rec.field(FIELD_ID(CHARGE_2)).getLong();
	}

	//����100������
	//���ô���0 ˵��ʹ�����Ѿ��������͵���
	if (m_ucSwitchFlag&0x01 && sChargeInfo.lCharge > 0  && EVENT_TYPE_CDMA_SOURCE_GROUPING == sChargeInfo.iSourceEventTypeID)
	{
		//�������̴� �Ƿ�����ٽ�״̬
		vector<string> v_sUnit;
		v_sUnit.clear();

		unsigned char compressed[6000];
		char unCompressed[6000];
		CRefString sRefStamp(unCompressed, sizeof(unCompressed));

		memset(compressed, 0x00, sizeof(compressed));
		memset(unCompressed, 0x00, sizeof(unCompressed));

		strncpy((char*)compressed, Rec.field(FIELD_ID(process_stamp)).getString(), sizeof(compressed));
		if (!unCompress(compressed, strlen((char*)compressed), unCompressed))
		{
			//����ʧ�� ��������
			return 1;
		}

		int iLimitCount = 0;
		
		parseWords(unCompressed, ';', v_sUnit);
		for (int iLoop1=0; iLoop1<v_sUnit.size(); iLoop1++)
		{
			if(strncmp(v_sUnit[iLoop1].c_str(), PROCESS_STAMP_CHARGE, 2) == 0)
			{
				continue;
			}
			if (strncmp(v_sUnit[iLoop1].c_str(), PROCESS_STAMP_ACCUMULATOR, 2) == 0)
			{
				vector<string> v_sElement;
				v_sElement.clear();
				// ���ڰ����Ų�ֳ�����
				parseWords((const string)v_sUnit[iLoop1], ',', v_sElement);
				if (strncmp(v_sElement[6].c_str(), "999999999", 9) == 0 )
				{
					continue;
				}
				else if (CRefString(v_sElement[4].c_str()).toLong() > 0 )
				{
					iLimitCount++;
				}
			}

		}

		if (iLimitCount >= 2)
		{
			sChargeInfo.iBillingAmountType = 2;
			sChargeInfo.lGrantedTime = atol(GetOcsDataValues(OCS_FIELD_RSU_CC_TIME).c_str());  
			sChargeInfo.lGrantedTotalOctets = atol(GetOcsDataValues(OCS_FIELD_RSU_CC_TOTAL_OCTETS).c_str());
			sChargeInfo.lCharge = 0;
			sChargeInfo.iTerminationFlag = 0;
			sChargeInfo.bBanflag = true;
			return 1;
		}

		
	}

	
	
	
    return 1;
}


int COcsCharge::ChargeMinAmount(SessionChargeInfo& sChargeInfo,bool bUpdate)
{	
	g_vecAccum.clear();
	//ȡ��������Ԥ������rec���� ���ڼ�����СԤ����
    Record& Rec = m_cPreProc1OL.getMinRecord();

	//��ѯ���		 
	if (!QueryBalance(Rec,sChargeInfo.lCurEffBalance,m_strerrmsg,m_iErrCode))
	{  
		return -1;
	}

	DEBUG_2("��� =",sChargeInfo.lCurEffBalance); 

	if (sChargeInfo.lCurEffBalance<=0)
	{
		DEBUG_2("���С����0 ,CurEffBalance =",sChargeInfo.lCurEffBalance);
		if (bUpdate)
		{
			sChargeInfo.lGrantedTime = sChargeInfo.iMinCCTime;
			sChargeInfo.lGrantedTotalOctets = sChargeInfo.lMinCCTotalOctets;
			sChargeInfo.iTerminationFlag = 0;
			return 1;
		}
		//����ǻỰ��ʼ ���С����0 ����4501 action����-1
		else
		{
			sChargeInfo.iTerminationFlag = -1;
			m_iErrCode = RESULT_CODE_BAL_LOW_ZRO;
			m_strerrmsg = "�Ự��ʼ,���С����0";
			sChargeInfo.lGrantedTime = 0;
			sChargeInfo.lGrantedTotalOctets = 0;
			return -1;
		}
	}



	if (sChargeInfo.lCharge<=sChargeInfo.lCurEffBalance)
	{ 
		//DEBUG_4("����㹻,lCharge =",sChargeInfo.lCharge,",lCurEffBalance=",sChargeInfo.lCurEffBalance); 
		//DEBUG_4("lGrantedTime=",sChargeInfo.lGrantedTime,",lGrantedTotalOctet=",sChargeInfo.lGrantedTotalOctets); 
					 
	}//����
	else
	{
		
	    if (!SetBillingAmount(sChargeInfo,Rec,true))
	    {
	        return -1;
	    } 
		//����СԤ������������
		int iRet = m_cRatingOL.ProcessTicket(Rec);
		if (iRet<0)
		{
			m_cRatingOL.getErrMessage(m_iErrCode,m_strerrmsg);
			IsAbnormal(Rec);
			m_strerrmsg += "g_cRatingOL.ProcessTicket fail," + m_strerrmsg;
			return -1;
		}


		DEBUG_2("��СԤ�������۽�� =",Rec); 
		
		if (IsAbnormal(Rec))
		{	
			return -1;	 
		}
		
		//��СԤ�����ķ��� 
		CLong lMinCharge =0;
		if(Rec.hasField(FIELD_ID(CHARGE)))
		{
			lMinCharge += Rec.field(FIELD_ID(CHARGE)).getLong();
		}
		if(Rec.hasField(FIELD_ID(CHARGE_2)))
		{
			lMinCharge += Rec.field(FIELD_ID(CHARGE_2)).getLong();
		}
		
		if (lMinCharge<=sChargeInfo.lCurEffBalance)
		{
			//	���㣬������СԤ����������0, FUC action_id = 0�������Ԫ������ֹ����
			sChargeInfo.iTerminationFlag = 0;

			//���ﷴ���Ӧ���Ƕ�������۵�REC
			Record& OriRec = m_cPreProc1OL.getRecord();
			if (!ReverseCharge(OriRec,sChargeInfo))
			{
				return -1;
			}			  
		}
		else
		{
			//���㣬��������СԤ����������0, FUC action_id =0 ������СԤ����
			sChargeInfo.iTerminationFlag = 0;		
			sChargeInfo.lGrantedTime = sChargeInfo.iMinCCTime;
			sChargeInfo.lGrantedTotalOctets = sChargeInfo.lMinCCTotalOctets;

		}
		DEBUG_4("��СԤ��������,lMinCharge =",lMinCharge,",lCurEffBalance=",sChargeInfo.lCurEffBalance); 
	}
	return 1;
}


int COcsCharge::InitialCharge(PACK_OCS_REQUEST& stOcsReq,SessionChargeInfo& sChargeInfo)
{

	try
    {  
		int iRet = initCharge(stOcsReq,sChargeInfo);
		if (iRet<0)
		{
			return -1;
		}

        iRet = ChargeAmount(stOcsReq,sChargeInfo,false);
        if (iRet<0)
        {
            return -1;
        }

		/*
		string strEventType = stOcsReq.sSrvCntxtId;	
        transform(strEventType.begin(), strEventType.end(), strEventType.begin(), ::toupper);
        unsigned int    nLoc = 0;
        nLoc = strEventType.find('@',0);
        strEventType = strEventType.substr(0,nLoc);

		if (sChargeInfo.lServID != 0  && strEventType.find("PGW") != strEventType.npos )
		{
			deleteSessionByServID(sChargeInfo.lServID);
		}
		*/

        if (sChargeInfo.lCharge!=0 && !checkNoStopUrge(sChargeInfo.lServID))
        {
		
        	//���ò�����0������СԤ���� ���ҷ���
           iRet = ChargeMinAmount(sChargeInfo,false);
           if (iRet<0)
           {
			  return -1;
		   }
       
        }

		/*
		if (strEventType.find("PGW") != strEventType.npos  
			|| strEventType.find("CCG") != strEventType.npos)
		{
	
			//���LTE�ڴ��ύ ɾ��LTE�ڴ�
			m_cAccumMng.deleteRec(sChargeInfo.lServID);
					
			SessionRec sessionrec;
			memset(&sessionrec,0,sizeof(sessionrec));
			strncpy(sessionrec.session_id,stOcsReq.sSessionId,130);
			sessionrec.session_id[130] = '\0';
			strncpy(sessionrec.PID,sChargeInfo.szPID,20);
			sessionrec.PID[20] = '\0';
			sessionrec.lAmount_duration = 0;
			sessionrec.serv_id= sChargeInfo.lServID;
			sessionrec.lAmount_flux = 0;
			sessionrec.sessionState = '1';	   
			sessionrec.lCharge = 0;
			
			// EVENT_BEGIN_TIME		
			CString strBeginTime = GetOcsDataValues(OCS_FIELD_EVENT_BEGIN_TIME).c_str();
			DEBUG_2("EVENT_BEGIN_TIME = ",strBeginTime);
			CTime tTime = strBeginTime.toDate();
			sessionrec.tBeginTime = tTime.getTime();
			sessionrec.lFluxCardUsed = 0;
			
			//LastDealTime ȡEVENT_BEGIN_TIME 90003		
			sessionrec.LastDealTime = tTime.getTime();	
			sessionrec.ifSessionCommit = false;

			//SERVICE_TYPE_ID
			snprintf(sessionrec.sServiceTypeID,10,"%d",m_sOrgTicket.iServiceTypeID);

			int iTariffTimeChange = 0;
			//�����ʷ��л���
			if (TariffTimeChange(iTariffTimeChange))
			{
				sChargeInfo.lTariffTimeChange = iTariffTimeChange;
				sChargeInfo.bTariffTimeChange = true;

			}

			sessionrec.tTariffChangeTime = iTariffTimeChange;
			m_pSessionSwitch->addRec(sessionrec);

		}
		*/

    }
    catch(CException& e)   
    {
        m_iErrCode = RESULT_CODE_HANDLE_EXCEPTION_FAILED;
        m_strerrmsg += e.what();
        return -1;    
    }

    return 1;

}


int COcsCharge::UpdateCharge(PACK_OCS_REQUEST& stOcsReq,SessionChargeInfo& sChargeInfo)
{
	
	try
	{
        //����Ǹ��°� 
		int iRet = initCharge(stOcsReq,sChargeInfo);
		if (iRet<0)
		{
			return -1;
		}

		iRet = ChargeAmount(stOcsReq,sChargeInfo,true);
		if (iRet<0)
		{
			return -1;
		}	

		if (sChargeInfo.lCharge!=0 && !checkNoStopUrge(sChargeInfo.lServID))
		{
			iRet = ChargeMinAmount(sChargeInfo,true);
			if (iRet<0)
		    {
				return -1;
		    }
		}
		

		if (!updateSession(sChargeInfo))
		{
			return -1;
		}
		


	}
	catch(CException& e)   
    {
        m_iErrCode = RESULT_CODE_HANDLE_EXCEPTION_FAILED;
        m_strerrmsg += e.what();
        return -1;    
    }


	return 1;

}


int COcsCharge::EventCharge(PACK_OCS_REQUEST& stOcsReq,OcsChargeInfo& sChargeInfo)
{

	try
	{	
	    //����Ǹ��°� 
        g_vecAccum.clear();
		int iRet = initCharge(stOcsReq,sChargeInfo);

		if (iRet<0)
		{
			return -1;
		}
	
		//����һ��Ԥ�����ʽ
		iRet = m_cPreProc1OL.InitTicket(m_iSourceEventTypeID);
		if (iRet<0)
		{
			m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
			m_strerrmsg += "m_cPreProc1OL.InitTicket fail," + m_strerrmsg;
			return -1;
		}

		Record& rec = m_cPreProc1OL.getRecord();   

				
		//���������ַ�������ԭʼ�����ṹ����
		iRet = ParseTicket(m_iSourceEventTypeID,m_iRoamFlag,rec,stOcsReq,m_sOrgTicket,m_cDataExPlain,m_strerrmsg,m_iErrCode);
		if (iRet<0)
		{		 
			return -1;		
		}

		bool bEventCharge = true;		
		switch(stOcsReq.iReqAct)
		{
			case 0:
				bEventCharge = true;			
				break;
			case 1:
				bEventCharge = false;
				break;
			default:
				bEventCharge = false;
				break;

		}
		
		//�۷Ѳ���Ҫ�������� ���ߺͲ����Ҫ
		if (bEventCharge)
		{

			//��дRecord�ֶ�
			iRet = m_cPreProc1OL.convertHandle(rec,m_iSourceEventTypeID,m_sOrgTicket);
			if (iRet<0)
			{
				m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
				m_strerrmsg += "g_cPreProc1OL.convertHandle fail," + m_strerrmsg;
				return -1;
			}
		
			//�Ƿ����쳣�� ��ֱ�ӷ��� ������
			if (IsAbnormal(rec))
			{
				return -1;	 
			}
			
			//DEBUG_2("CPreProc1OL  rec = " ,rec);	
			iRet = m_cPreProc2OL.PreProcTicket(rec);
			if (iRet<0)
			{
				m_cPreProc2OL.getErrMessage(m_iErrCode,m_strerrmsg);
				m_strerrmsg += "g_cPreProc2OL.PreProcTicket fail," + m_strerrmsg;
				return -1;
			}

	
			if (IsAbnormal(rec))
			{		
				return -1;	 
			}
			
			//DEBUG_2("CPreProc2OL  rec=",rec);	   
			
			iRet = m_cRatingOL.ProcessTicket(rec);
			if (iRet<0)
			{
				m_cRatingOL.getErrMessage(m_iErrCode,m_strerrmsg);
				IsAbnormal(rec);
				m_strerrmsg += "g_cRatingOL.ProcessTicket fail," + m_strerrmsg;
				return -1;
			}
	
			if (IsAbnormal(rec))
			{
				return -1;	 
			}
			
			DEBUG_2("RatingOL rec =",rec); 
			 
			if(rec.hasField(FIELD_ID(CHARGE)))
			{
				sChargeInfo.lCharge += rec.field(FIELD_ID(CHARGE)).getLong();
			}
			if(rec.hasField(FIELD_ID(CHARGE_2)))
			{
				sChargeInfo.lCharge += rec.field(FIELD_ID(CHARGE_2)).getLong();
			}
			
			sChargeInfo.lServID = rec.field(FIELD_ID(SERV_ID)).getLong();
			//���ò�Ϊ0�Ų�ѯ��� 
			if  (sChargeInfo.lCharge != 0   && !checkNoStopUrge(sChargeInfo.lServID) )
			{
				//��ѯ���
				if (!QueryBalance(rec,sChargeInfo.lCurEffBalance,m_strerrmsg,m_iErrCode))
				{
					DEBUG_2("QueryBalance fail, g_strOcsChargeErrMsg =",m_strerrmsg);	   
					return -1;
				}
 
				if (sChargeInfo.lCurEffBalance<=0)
				{
					m_iErrCode = RESULT_CODE_BAL_LOW_ZRO;
					m_strerrmsg = "���С����0,��� = ";
			        m_strerrmsg += toString(sChargeInfo.lCurEffBalance);
					return -1;
				}
		
				//���㲻�� �� ����ȫ������
				if (stOcsReq.iChargeType ==0 || stOcsReq.iChargeType ==2)
				{
					if (sChargeInfo.lCharge<=sChargeInfo.lCurEffBalance)
					{
						DEBUG_2("����㹻",sChargeInfo.lCurEffBalance);
					}
					else
					{
						m_iErrCode = RESULT_CODE_BAL_LOW_ZRO;
						m_strerrmsg += "����";				
						return -1;
					}
					
				}//���ֵֿ�
				else if (stOcsReq.iChargeType == 1)
				{
					if (sChargeInfo.lCurEffBalance<=0)
					{
						m_iErrCode = RESULT_CODE_BAL_LOW_ZRO;
						m_strerrmsg += "�������С����0";			  
						return -1;
					}
				 
				}
				else
				{
					m_iErrCode = RESULT_CODE_UNKNOW_CHARGE_TYPE_FAILED;
					m_strerrmsg += "Ŀǰ��֧�ָ�ChargeType= ";
					m_strerrmsg+=toString(stOcsReq.iChargeType);
					return -1;
					
				}
					 			
			} 

		} 
			   
		if (!m_cMemBaseHandle.Insert(m_sOrgTicket)) 
		{
			m_cMemBaseHandle.getErrMessage(m_iErrCode,m_strerrmsg);
			m_strerrmsg += "m_cMemBaseHandle.Insert fail," + m_strerrmsg;
			m_cMemBaseHandle.rollbackMemDb(m_cMemBaseHandle.getMemConn());
			return -1;
		}  

		if (!m_cMemBaseHandle.commitMemDb(m_cMemBaseHandle.getMemConn())) 
		{
			m_cMemBaseHandle.getErrMessage(m_iErrCode,m_strerrmsg);
			m_strerrmsg += "m_cMemBaseHandle.commitMemDb fail," + m_strerrmsg;
			return -1;
		}  

	}
	catch(CException& e)   
	{
		m_cMemBaseHandle.rollbackMemDb(m_cMemBaseHandle.getMemConn());
		m_iErrCode = RESULT_CODE_HANDLE_EXCEPTION_FAILED;
		m_strerrmsg += e.what();
		return -1;	 
	}
	
	return 1;

}



//��ֹ������ ��Ҫ���
int COcsCharge::TerminationCharge(PACK_OCS_REQUEST& stOcsReq,SessionChargeInfo& sChargeInfo)
{

    try
    {
        bool bSuccess = true; 
		int iRet = -1;
		
		for(int i = 0; i < stOcsReq.cServiceInformationVec.size();i++)
        {   
            
        	iRet = initCharge(stOcsReq,sChargeInfo);
			if (iRet<0)
			{
				bSuccess = false; 
                break;
			}
               
            //����һ��Ԥ�����ʽ
            iRet = m_cPreProc1OL.InitTicket(m_iSourceEventTypeID);
            if (iRet<0)
            {
                m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
                m_strerrmsg += "g_cPreProc1OL.InitTicket fail," + m_strerrmsg;
                bSuccess = false; 
                break;
            }

    
            Record& rec = m_cPreProc1OL.getRecord();   
                            
            //���������ַ�������ԭʼ�����ṹ����
            int iRet = ParseTicket(m_iSourceEventTypeID,m_iRoamFlag,rec,stOcsReq,m_sOrgTicket,m_cDataExPlain,m_strerrmsg,m_iErrCode);
            if (iRet<0)
            {        
                bSuccess = false; 
                break;            
            }



			string strPID = GetOcsDataValues(OCS_FIELD_ORG_PRODUCT_OFFER_ID);

			if (m_ucSwitchFlag&0x01)
			{
				if (EVENT_TYPE_CDMA_SOURCE_GROUPING == m_iSourceEventTypeID)
				{
					SessionRec sessionrec;
					CInt iDataOffset = -1;
					m_pSessionSwitch->queryRec(stOcsReq.sSessionId,strPID.c_str(),iDataOffset);
					if (-1 != iDataOffset)
					{
						m_pSessionSwitch->readRec(iDataOffset,sessionrec);  
						if (sessionrec.ifSessionCommit)
						{
							strncpy(m_sOrgTicket.szImsiNbr,"NULLNULLNULLNUJ",strlen("NULLNULLNULLNUJ")+1); 
						}
						
					}
					
				}
			}

			
			if (m_ucSwitchFlag&0x02)
			{
				if (EVENT_TYPE_CDMA_SOURCE_GROUPING == m_iSourceEventTypeID)
				{
	        		SessionRec sessionrec;
					CInt iDataOffset = -1;
					m_pSessionSwitch->queryRec(stOcsReq.sSessionId,strPID.c_str(),iDataOffset);

					if (-1 != iDataOffset)
					{
					    //SessionRec sessionrec;
            			m_pSessionSwitch->readRec(iDataOffset,sessionrec);  
						if (sessionrec.ifSessionCommit)
						{
							m_sOrgTicket.iRoamOrgType = sessionrec.iRoamOrgType;
							strncpy(m_sOrgTicket.szBscID,sessionrec.szBscID,21);
							CString sBSCId = m_sOrgTicket.szBscID;
							CString sBSCIdLeft4 = sBSCId.left(4);//ȡǰ4λ
							sBSCIdLeft4.makeUpper(); //ת��Ϊ��д
							if (sBSCIdLeft4.compare("3738")==0)
							{
								strncpy(m_sOrgTicket.szCallingRoamAreaCode,"0591",10);
							}else if (sBSCIdLeft4.compare("3739")==0)
							{
								strncpy(m_sOrgTicket.szCallingRoamAreaCode,"0592",10);
							}else if (sBSCIdLeft4.compare("373A")==0)
							{
								strncpy(m_sOrgTicket.szCallingRoamAreaCode,"0593",10);
							}else if (sBSCIdLeft4.compare("373B")==0)
							{
								strncpy(m_sOrgTicket.szCallingRoamAreaCode,"0594",10);
							}else if (sBSCIdLeft4.compare("373C")==0)
							{
								strncpy(m_sOrgTicket.szCallingRoamAreaCode,"0595",10);
							}else if (sBSCIdLeft4.compare("373D")==0)
							{
								strncpy(m_sOrgTicket.szCallingRoamAreaCode,"0596",10);
							}else if (sBSCIdLeft4.compare("373E")==0)
							{
								strncpy(m_sOrgTicket.szCallingRoamAreaCode,"0597",10);
							}else if (sBSCIdLeft4.compare("373F")==0)
							{
								strncpy(m_sOrgTicket.szCallingRoamAreaCode,"0598",10);
							}else if (sBSCIdLeft4.compare("3740")==0) 
							{
								strncpy(m_sOrgTicket.szCallingRoamAreaCode,"0599",10);
							}
							strncpy(m_sOrgTicket.szImsiNbr,"NULLNULLNULLNUK",strlen("NULLNULLNULLNUK")+1); 
						}

						
					}
	
				}

			}

			if (!deleteSession(stOcsReq,strPID.c_str()))
			{
				bSuccess = false;
				break;
			}

			DEBUG_2("USU_CC_TIME=",m_cDataExPlain.getDataValues(OCS_FIELD_USU_CC_TIME));
			DEBUG_2("USU_CC_TOTAL_OCTETS=",m_cDataExPlain.getDataValues(OCS_FIELD_USU_CC_TOTAL_OCTETS));
			DEBUG_2("BYTES=",m_cDataExPlain.getDataValues(OCS_FIELD_BYTES));

			//ʹ����Ϊ0ʱ m_iIndbFlagΪ1�仰�� 0���仰��
			if ( 0 == m_iIndbFlag)
			{	
				//����ʱ��ͬʱΪ0���仰��
				if (0 == m_sOrgTicket.lVolumeDownLink && 0 == m_sOrgTicket.iOrgCallAmount)
				{
					continue;
				}
			}
					
            if (!m_cMemBaseHandle.Insert(m_sOrgTicket)) 
            {
                m_cMemBaseHandle.getErrMessage(m_iErrCode,m_strerrmsg);
                m_strerrmsg += "m_cMemBaseHandle.Insert fail," + m_strerrmsg;
                bSuccess = false; 
                break;  
                  
            }   

        }
        
        if (!bSuccess)
        {
            m_cMemBaseHandle.rollbackMemDb(m_cMemBaseHandle.getMemConn());
    
        }
     	else
     	{
	     	//���ж��ɹ��˲��ύ
	        if (!m_cMemBaseHandle.commitMemDb(m_cMemBaseHandle.getMemConn())) 
	        {
	            m_cMemBaseHandle.getErrMessage(m_iErrCode,m_strerrmsg);
	            m_strerrmsg += "m_cMemBaseHandle.commitMemDb fail," + m_strerrmsg;
				bSuccess = false;
	        } 
           
		}

		if (!bSuccess)
		{
			m_strerrmsg += ",TerminationHandle fail";
			return -1; 
		}

        
    }
    catch(CException& e)   
    {
        m_iErrCode = RESULT_CODE_HANDLE_EXCEPTION_FAILED;
        m_strerrmsg += e.what();
        return -1;    
    }

    return 1;  
}

int COcsCharge::OcscheckNoStopUrge( CLong lProdInstId, int nOweBusiType, bool bSpecialUser )
{
    int nRet;
    OweStopUrge tagOweStopUrge;
    tagOweStopUrge.latn_id = m_iLatnID;
    tagOweStopUrge.owe_business_type_id = nOweBusiType;
    tagOweStopUrge.serv_id = lProdInstId;
    tagOweStopUrge.currTime = CTime::getCurrentTime();
    //return -1:�쳣; 0:������; 1:������IF_SPECIAL_USER=F; 2:������IF_SPECIAL_USER=T
    nRet = tagOweStopUrge.search();
    if( nRet<0 )
    {
        m_strerrmsg = "��ѯ�⸴ͣ��(OWE_STOP_URGE)�쳣!LATN_ID=";
        m_strerrmsg += toString( m_iLatnID );
        m_strerrmsg += " OWE_BUSINESS_TYPE_ID=";
        m_strerrmsg += toString( nOweBusiType );
        m_strerrmsg += " PROD_INST_ID=";
        m_strerrmsg += toString( lProdInstId );
        return -1;
    }
    if( nRet>0 )
    {
        if( !bSpecialUser )
            return 1;
        else
        {
            if( nRet==2 )
                return 1;
        }
    }
    return 0;
}


bool COcsCharge::checkNoStopUrge(CLong& lProdInstId)
{
	DTimerCalc calc(__LINE__, "checkNoStopUrge");
	int  iNoStopUrge  = OcscheckNoStopUrge(lProdInstId,1);
	if (0 == iNoStopUrge)
	{
		
		iNoStopUrge = OcscheckNoStopUrge(lProdInstId,2);
		if (0 == iNoStopUrge)
		{
			iNoStopUrge = OcscheckNoStopUrge(lProdInstId,8);
		}
	}

	if (1 == iNoStopUrge)
	{
		DEBUG_2("checkNoStopUrge reutn true, ServID= ",lProdInstId)
		return true;
	}
	return false;
}






bool COcsCharge::TariffTimeChange(int& iTariffTimeChange)
{
	iTariffTimeChange = 0;
    int validityTime = 3600;    //500��  ���潨�����Ϊ1��Сʱ
    
    
    int beginHour = g_iBeginTime/100;
    int beginMin = g_iBeginTime%100;
    
    int endHour = g_iEndTime/100;
    int endMin = g_iEndTime%100;
	
    CTime tTime = CTime::getCurrentTime();
    CTime tTime7 = CTime(tTime.getYear(), tTime.getMonth(), tTime.getDay(), beginHour,beginMin);
    CTime tTime24 = CTime(tTime.getYear(), tTime.getMonth(), tTime.getDay()) + 3600 * endHour + 60*endMin;
    
    if(tTime < tTime7)   //�Ƿ�С��7��
    {
        if(tTime + validityTime  >= tTime7)
        {
            
           iTariffTimeChange = tTime7.getTime() ;
		   DEBUG_2("tTime7 =",tTime7);
           return true;          
        } 
		else
        {
            return false;
        }
        
    } else if (tTime <= tTime24)
    {
        
        if(tTime + validityTime  >= tTime24)
        {
            iTariffTimeChange = tTime24.getTime();
			DEBUG_2("tTime24 = " ,tTime24);
			return true;

        } 
		else
        {
            return false;
        }
    }
    
    return false;
}




int COcsCharge::LteUpdateChargeMinAmount(PACK_OCS_REQUEST& stOcsReq,SessionChargeInfo& sChargeInfo)
{

	g_IncrementMode = 1;
	g_vecAccum.clear();
	//ȡ��������Ԥ������rec���� ���ڼ�����СԤ����
	
	//�뽫LTE�ڴ����µ��� g_vecAccum	
	//m_cAccumMng.queryRec(sChargeInfo.lServID,g_vecAccum);

	
	//SessionRec sessionrec;
	//m_pSessionSwitch->readRec(sChargeInfo.iDataOffset,sessionrec);
	
    Record& Rec = m_cPreProc1OL.getMinRecord();
	//��ѯ���		 
	if (!QueryBalance(Rec,sChargeInfo.lCurEffBalance,m_strerrmsg,m_iErrCode))
	{  
		return -1;
	}

	DEBUG_2("��� =",sChargeInfo.lCurEffBalance); 

	if (sChargeInfo.lCurEffBalance<=0)
	{
		DEBUG_2("���С����0",sChargeInfo.lCurEffBalance);
		sChargeInfo.lGrantedTime = sChargeInfo.iMinCCTime;
		sChargeInfo.lGrantedTotalOctets = sChargeInfo.lMinCCTotalOctets;
		sChargeInfo.iTerminationFlag = 0;
		return 1;
	}



	if (sChargeInfo.lCharge<=sChargeInfo.lCurEffBalance)
	{ 
		DEBUG_4("����㹻,lCharge =",sChargeInfo.lCharge,",lCurEffBalance=",sChargeInfo.lCurEffBalance); 
		DEBUG_4("lGrantedTime=",sChargeInfo.lGrantedTime,",lGrantedTotalOctet=",sChargeInfo.lGrantedTotalOctets); 
					 
	}//����
	else
	{
		

		//ȡEVENT_BEGIN_TIME
		CString strEventBeginTime = GetOcsDataValues(OCS_FIELD_EVENT_BEGIN_TIME).c_str();
		CTime tEventBeginTime = strEventBeginTime.toDate();
		DEBUG_2("MIN EVENT_BEGIN_TIME = ",tEventBeginTime);

		
		Rec.field(FIELD_ID(start_time)).setValue(tEventBeginTime);	
		Rec.field(FIELD_ID(end_time)).setValue(tEventBeginTime+sChargeInfo.iMinCCTime);

		Rec.field(FIELD_ID(CALL_AMOUNT)).setValue(sChargeInfo.iMinCCTime);
		Rec.field(FIELD_ID(volume_downlink)).setValue((CLong)sChargeInfo.lMinCCTotalOctets);


		
		//����СԤ������������
		int iRet = m_cRatingOL.ProcessTicket(Rec);
		if (iRet<0)
		{
			m_cRatingOL.getErrMessage(m_iErrCode,m_strerrmsg);
			IsAbnormal(Rec);
			m_strerrmsg += "g_cRatingOL.ProcessTicket fail," + m_strerrmsg;
			return -1;
		}


		DEBUG_2("��СԤ�������۽�� =",Rec); 
		
		if (IsAbnormal(Rec))
		{	
			return -1;	 
		}
			
		//��СԤ�����ķ��� 
		CLong lMinCharge =0;
		if(Rec.hasField(FIELD_ID(CHARGE)))
		{
			lMinCharge += Rec.field(FIELD_ID(CHARGE)).getLong();
		}
		if(Rec.hasField(FIELD_ID(CHARGE_2)))
		{
			lMinCharge += Rec.field(FIELD_ID(CHARGE_2)).getLong();
		}

		//�ۼ���֮ǰ��ķ���
		lMinCharge += sChargeInfo.lCharge;
		if (lMinCharge<=sChargeInfo.lCurEffBalance)
		{
			//	���㣬������СԤ����������0, FUC action_id = 0�������Ԫ������ֹ����
			sChargeInfo.iTerminationFlag = 0;
			
			//���ﷴ���Ӧ���Ƕ�������۵�REC
			Record& OriRec = m_cPreProc1OL.getRecord();
			if (!LteReverseCharge(OriRec,sChargeInfo))
			{
				return -1;
			}			  
		}
		else
		{
			//���㣬��������СԤ����������0, FUC action_id =0 ������СԤ����
			sChargeInfo.iTerminationFlag = 0;		
			sChargeInfo.lGrantedTime = sChargeInfo.iMinCCTime;
			sChargeInfo.lGrantedTotalOctets = sChargeInfo.lMinCCTotalOctets;

		}
		DEBUG_4("��СԤ��������,lMinCharge =",lMinCharge,",lCurEffBalance=",sChargeInfo.lCurEffBalance); 
	}
	
	return 1;
}

int COcsCharge::TicketInDB(Record& rec,SessionRec& sessionrec,bool bIndbType)
{
	DTimerCalc calc(__LINE__, "TicketInDB");
	string strValue = (const char*)rec.field(FIELD_ID(START_TIME)).getDate().format("%Y%m%d%H%M%S");
	int iRecLength = rec.field(FIELD_ID(ORG_START_TIME)).getLength(); 
	strValue += "00";
	strncpy(m_sOrgTicket.szOrgStartTime,strValue.c_str()+2,iRecLength);  
	m_sOrgTicket.szOrgStartTime[iRecLength] ='\0'; 

	
	strValue = (const char*)rec.field(FIELD_ID(END_TIME)).getDate().format("%Y%m%d%H%M%S");
	strValue += "00";
	strncpy(m_sOrgTicket.szOrgEndTime,strValue.c_str()+2,iRecLength);  
	m_sOrgTicket.szOrgEndTime[iRecLength] ='\0'; 

		
	CTimeSpan tSpanTime= rec.field(FIELD_ID(END_TIME)).getDate() - rec.field(FIELD_ID(START_TIME)).getDate();

	long lDownLink = rec.field(FIELD_ID(VOLUME_DOWNLINK)).getLong();
	long lVolumeDownLink = sessionrec.lAmount_flux + lDownLink;
	
	m_sOrgTicket.iOrgCallAmount = tSpanTime.getTotalSeconds()*10;
	m_sOrgTicket.lVolumeDownLink = lVolumeDownLink;

	//Ϊ�������м䵥��ĵ��ӻ�����ֹ�������IMSI_NBR����¼
	if (!bIndbType)
	{
		strncpy(m_sOrgTicket.szImsiNbr,"NULLNULLNULLNU2",strlen("NULLNULLNULLNU2")+1);
	}
	else
	{
		strncpy(m_sOrgTicket.szImsiNbr,"NULLNULLNULLNU3",strlen("NULLNULLNULLNU3")+1);
	}
	
	
	if (!m_cMemBaseHandle.Insert(m_sOrgTicket)) 
	{
		m_cMemBaseHandle.getErrMessage(m_iErrCode,m_strerrmsg);
		m_strerrmsg += "m_cMemBaseHandle.Insert fail," + m_strerrmsg;
		m_cMemBaseHandle.rollbackMemDb(m_cMemBaseHandle.getMemConn());
		return -1;
	}  

	if (!m_cMemBaseHandle.commitMemDb(m_cMemBaseHandle.getMemConn())) 
    {
        m_cMemBaseHandle.getErrMessage(m_iErrCode,m_strerrmsg);
        m_strerrmsg += "m_cMemBaseHandle.commitMemDb fail," + m_strerrmsg;
		return -1;
    } 
	return 1;
}



int COcsCharge::LteUpdateChargeAmount(PACK_OCS_REQUEST& stOcsReq,SessionChargeInfo& sChargeInfo)
{

	//CCG �� LTE �������Ʒ�ģʽ
	g_IncrementMode = 1;
	g_vecAccum.clear();
	//����service_context�ж���CCG����LTE
	bool bLteFlag = true;
	string strEventType = stOcsReq.sSrvCntxtId; 
	transform(strEventType.begin(), strEventType.end(), strEventType.begin(), ::toupper);
	unsigned int	nLoc = 0;
	nLoc = strEventType.find('@',0);
	strEventType = strEventType.substr(0,nLoc);
	if (strEventType.find("CCG") != strEventType.npos)
	{
		bLteFlag = false;
	}
	

    //һ��Ԥ�����ʼ��
    int iRet = m_cPreProc1OL.InitTicket(m_iSourceEventTypeID);
    if (iRet<0)
    {
        m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
        m_strerrmsg += "m_cPreProc1OL.InitTicket fail," + m_strerrmsg;
        return -1;
    }
	
	//�������ʹ��
    Record& Rec = m_cPreProc1OL.getRecord();
    //ҵ����Ϣ�����뻰��
    iRet = ParseTicket(m_iSourceEventTypeID,m_iRoamFlag,Rec,stOcsReq,m_sOrgTicket,m_cDataExPlain,m_strerrmsg,m_iErrCode);
    if (iRet<0)
    {               
        return -1;         
    }

    
    //һ��Ԥ����
    iRet = m_cPreProc1OL.convertHandle(Rec,m_iSourceEventTypeID,m_sOrgTicket);
    if (iRet<0)
    {
        m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
        m_strerrmsg += "g_cPreProc1OL.convertHandle fail," + m_strerrmsg;
        return -1;
    }
    
    if (IsAbnormal(Rec))
    {
        return -1;    
    }       
    
    //����Ԥ����
    iRet = m_cPreProc2OL.PreProcTicket(Rec);
    if (iRet<0)
    {
        m_cPreProc2OL.getErrMessage(m_iErrCode,m_strerrmsg);
        m_strerrmsg += "m_cPreProc2OL.PreProcTicket fail," + m_strerrmsg;
        return -1;
    }
	
    if (IsAbnormal(Rec))
    {
        return -1;    
    }

	//��Ҫ�ڶ���Ԥ����֮���ڱ���
	m_cPreProc1OL.SetMinRecord();
    m_cPreProc1OL.SetLteRecord();


	//�������Ѻ���
	/*
	if (bLteFlag)
	{
		g_strPolicyAccNbr = Rec.field(FIELD_ID(BILLING_NBR)).getString();
	}
	else
	{
		g_strPolicyAccNbr = "";
	}
	*/
    
 
	//ȡ��������Ԥ�����SERVID
	CLong lServID = Rec.field(FIELD_ID(SERV_ID)).getLong();
	sChargeInfo.setServID(lServID);

	//����ǰɾ��commit���Ϊtrue�ļ�¼
	//m_cAccumMng.deleteRec(lServID);
	
	//ȡLTE��RG �ֶ�
	string strPid = "";
	if (bLteFlag)
	{
		strPid = GetOcsDataValues(OCS_FIELD_MSCC_RATING_GROUP);
	}
	else
	{
		strPid = GetOcsDataValues(96181);
	}
	
	sChargeInfo.setPID(strPid.c_str());

	//ȡ�ʷ��л���־
    string strTariffChargeUsage = GetOcsDataValues(OCS_FIELD_TARIFF_CHANGE_USAGE);
    string strTariffChargeUsage2 = GetOcsDataValues(96121);
	DEBUG_2("RG or PID = ",strPid);
	DEBUG_2("TARIFF_CHANGE_USAGE = ",strTariffChargeUsage);
    DEBUG_2("TARIFF_CHANGE_USAGE2 = ",strTariffChargeUsage2);

	bool bTariffChangeUsage = false;
	if ("1" == strTariffChargeUsage  || "1" == strTariffChargeUsage2)
	{
		bTariffChangeUsage = true;
	}
	
	//ȡEVENT_BEGIN_TIME
	CString strEventBeginTime = GetOcsDataValues(OCS_FIELD_EVENT_BEGIN_TIME).c_str();
	CTime tEventBeginTime = strEventBeginTime.toDate();
	
	//����4g�ĵ�һ������ʹ��
	Record& RecLte1 = m_cPreProc1OL.getLte1Record();

	int iDataOffset = -1;
	m_pSessionSwitch->queryRec(stOcsReq.sSessionId,strPid.c_str(),iDataOffset);
	
	SessionRec sessionrec;
	memset(&sessionrec,0,sizeof(SessionRec));
	if (-1 == iDataOffset)
	{
		//�ڴ��в�ѯ������¼�����ʷ��л����־���ó�false
		//�������µĻỰ������
		bTariffChangeUsage = false;
		strncpy(sessionrec.session_id,stOcsReq.sSessionId,130);
		sessionrec.session_id[130] = '\0';
		strncpy(sessionrec.PID,sChargeInfo.szPID,20);
		sessionrec.PID[20] = '\0';
		sessionrec.lAmount_duration = 0;
		sessionrec.serv_id= lServID;
		sessionrec.lAmount_flux = 0;
		sessionrec.sessionState = '1';	   
		sessionrec.lCharge = 0;
		sessionrec.tTariffChangeTime = 0;
		sessionrec.iRequestCnt = 1;			
		sessionrec.tBeginTime = tEventBeginTime.getTime();	
		sessionrec.lFluxCardUsed = 0;
		//��LastDealTime��¼��һ�����°���EventBeginTime
		sessionrec.LastDealTime = tEventBeginTime.getTime();//CTime::getCurrentTime().getTime();	
		sessionrec.ifSessionCommit = false;
		//SERVICE_TYPE_ID
		snprintf(sessionrec.sServiceTypeID,10,"%d",m_sOrgTicket.iServiceTypeID);
		iDataOffset = m_pSessionSwitch->addRec(sessionrec);
		if (-1 == iDataOffset)
		{
			m_iErrCode = RESULT_CODE_SESSION_ADD_REC_FAILED;
			m_strerrmsg = "m_pSessionSwitch->addRec failed,SourceEventTypeID= ";
			m_strerrmsg += toString(m_iSourceEventTypeID);
			return -1;
		}
		DEBUG_4("��RG�ĵ�һ�����°�,session_id =",stOcsReq.sSessionId,",PID=",sChargeInfo.szPID);
	}
	else
	{
		m_pSessionSwitch->readRec(iDataOffset,sessionrec);
	}

	sChargeInfo.setDataOffset(iDataOffset);

	
	
	int iCallAmount1 = tEventBeginTime.getTime() - sessionrec.LastDealTime;
	
	//�ϱ�ʵ��ʹ����
	CLong lMsccUsuCcOctets = (CLong)atol(GetOcsDataValues(OCS_FIELD_MSCC_USU_CC_TOTAL_OCTETS).c_str());
	

	CLong lMsccUsuCcTime = (CLong)atol(GetOcsDataValues(OCS_FIELD_MSCC_USU_CC_TIME).c_str());
	DEBUG_2("sessionID = " , stOcsReq.sSessionId);	
	DEBUG_2("iCallAmount1 = ",iCallAmount1);
	DEBUG_2("MSCC_USU_CC_TOTAL_OCTETS = ",lMsccUsuCcOctets);
	DEBUG_2("MSCC_USU_CC_TIME = ",lMsccUsuCcTime);
	DEBUG_2("EVENT_BEGIN_TIME = ",strEventBeginTime);
	
	RecLte1.field(FIELD_ID(start_time)).setValue(CTime(sessionrec.tBeginTime));
	RecLte1.field(FIELD_ID(end_time)).setValue(tEventBeginTime);
	RecLte1.field(FIELD_ID(CALL_AMOUNT)).setValue(iCallAmount1);
	RecLte1.field(FIELD_ID(volume_downlink)).setValue(lMsccUsuCcOctets);

	if (bTariffChangeUsage)
	{
		DEBUG_2("���ʷ��л����ʶ �ʷ��л��� = ",CTime(sessionrec.tTariffChangeTime));
		RecLte1.field(FIELD_ID(end_time)).setValue(CTime(sessionrec.tTariffChangeTime));
	}

	//��ѯ��ʹ�õ������� ��λKB
	CLong    lFluxCardUsed = 0;
	long lCharge = 0;
	//m_pSessionSwitch->queryAllFluxCardUsed(lServID,lFluxCardUsed);
	//g_lFluxCardUsed = lFluxCardUsed;
	//lte�ڴ浼��g_vecAccum
	//m_cAccumMng.queryRec(lServID,g_vecAccum);

	/*
	iRet = m_cRatingOL.ProcessTicket(RecLte1);
    if ((iRet<0))
    {
        m_cRatingOL.getErrMessage(m_iErrCode,m_strerrmsg);
		IsAbnormal(RecLte1);
        m_strerrmsg += "g_cRatingOL.ProcessTicket fail," + m_strerrmsg;
        return -1;
    }
	DEBUG_2("RecLte1 = ",RecLte1);
	
	if (IsAbnormal(RecLte1))
	{
		return -1;
	}
	
	if(RecLte1.hasField(FIELD_ID(CHARGE)))
	{
		lCharge += RecLte1.field(FIELD_ID(CHARGE)).getLong();
	}
	if(RecLte1.hasField(FIELD_ID(CHARGE_2)))
	{
		lCharge += RecLte1.field(FIELD_ID(CHARGE_2)).getLong();
	}
	*/

	//����ʱ������
	m_pSessionSwitch->updateRec(iDataOffset,tEventBeginTime.getTime() - sessionrec.LastDealTime ,lMsccUsuCcOctets);
	//���·���
	m_pSessionSwitch->updateRecCharge(iDataOffset,lCharge);
	//����������
	//m_pSessionSwitch->updateFluxCardUsed(iDataOffset,g_lActualPayoutAmount);
	//���´���
	m_pSessionSwitch->addRecRequestCnt(iDataOffset);
	//����LastDealTime
	m_pSessionSwitch->updateLastDealTime(iDataOffset,tEventBeginTime.getTime());
	//�����ۺ�Ļ�������lte�ڴ���
    //m_cAccumMng.updateRec(lServID,strPid.c_str(),sessionrec.tBeginTime,g_vecTicketAccum);
	//������0ǰ�ĻỰ����
	long lBeforeCharge = 0;
	//��������ʷ��л�����Ҫ���ʷ��л���ǰ���»���


	if (bTariffChangeUsage)
	{
		DEBUG_2("�ʷ��л����ύ",sessionrec);

		TicketInDB(RecLte1,sessionrec,true);
		sessionrec.lAmount_duration = 0;
		sessionrec.lAmount_flux = 0;
		sessionrec.tBeginTime = sessionrec.tTariffChangeTime;
		sessionrec.LastDealTime = tEventBeginTime.getTime();
		sessionrec.iRequestCnt = 0;
		lBeforeCharge = sessionrec.lCharge;
		sessionrec.lCharge= 0;
		sessionrec.lFluxCardUsed = 0;
		sessionrec.ifSessionCommit = false;
		m_pSessionSwitch->updateRec(iDataOffset,sessionrec);

		
	} //����Ǵﵽ�仰������Щ���� ʲô��ʱ ����Ҳ�仰��
	else
	{	
		if (bLteFlag)
		{
			//��λΪ�ֽ�
			if (sessionrec.lAmount_flux >= m_iIndbFlux*1024*1024 
				|| sessionrec.lAmount_duration >= m_iIndbDuration*60
				|| sessionrec.iRequestCnt >= m_iIndbRequestCnt
				)
			{
				
				DEBUG_2("�м䵥���",sessionrec);
				TicketInDB(RecLte1,sessionrec);
				sessionrec.lAmount_duration = 0;
				sessionrec.lAmount_flux = 0;
				sessionrec.tBeginTime = tEventBeginTime.getTime();
				sessionrec.LastDealTime =  tEventBeginTime.getTime();
				sessionrec.iRequestCnt = 0;
				lBeforeCharge = sessionrec.lCharge;
				sessionrec.lCharge= 0;
				sessionrec.lFluxCardUsed = 0;
				sessionrec.ifSessionCommit = false;		
				m_pSessionSwitch->updateRec(iDataOffset,sessionrec);

			}
		}
		
	}
	
	///////////////////////�ڶ��ζ��ʷ��л���������
	if (bTariffChangeUsage)
	{
		 //ʵ���ϱ�ʵ�ʵ�ʹ��ʱ�� ���ʷ��л���
		int iCallAmount2 = tEventBeginTime.getTime()-sessionrec.tTariffChangeTime;
		DEBUG_2("iCallAmount2 = ",iCallAmount2);
		
		//ʵ���ϱ�ʵ�ʵ�����(�ʷ��л���) 
		CLong lMsccUsuCcOctets2 = (CLong)atol(GetOcsDataValues(OCS_FIELD_MSCC_USU_CC_TOTAL_OCTETS2).c_str());
		DEBUG_2("MSCC_USU_CC_TOTAL_OCTETS2 = ",lMsccUsuCcOctets2);

		Record& RecLte2 = m_cPreProc1OL.getLte2Record();


		DEBUG_2("�ڶ��������ʷ��л��� = ",CTime(sessionrec.tTariffChangeTime));
		
		RecLte2.field(FIELD_ID(start_time)).setValue(CTime(sessionrec.tTariffChangeTime));	
		RecLte2.field(FIELD_ID(end_time)).setValue(tEventBeginTime);
		RecLte2.field(FIELD_ID(CALL_AMOUNT)).setValue(iCallAmount2);
		RecLte2.field(FIELD_ID(volume_downlink)).setValue(lMsccUsuCcOctets2);
		
		m_pSessionSwitch->queryAllFluxCardUsed(lServID,lFluxCardUsed);
		g_lFluxCardUsed = lFluxCardUsed;

        iRet = m_cRatingOL.ProcessTicket(RecLte2);
        if ((iRet<0))
        {  
            m_cRatingOL.getErrMessage(m_iErrCode,m_strerrmsg);
			IsAbnormal(RecLte2);
            m_strerrmsg += "g_cRatingOL.ProcessTicket fail," + m_strerrmsg;
            return -1;
        }
		
		DEBUG_2("RecLte2 = ",RecLte2);


		//����charge + charge2	
		lCharge = 0;
		if(RecLte2.hasField(FIELD_ID(CHARGE)))
		{
			lCharge += RecLte2.field(FIELD_ID(CHARGE)).getLong();
		}
		if(RecLte2.hasField(FIELD_ID(CHARGE_2)))
		{
			lCharge += RecLte2.field(FIELD_ID(CHARGE_2)).getLong();
		}
		
		m_pSessionSwitch->updateRec(iDataOffset,iCallAmount2,lMsccUsuCcOctets2);
		m_pSessionSwitch->updateRecCharge(iDataOffset,lCharge);
		//m_pSessionSwitch->updateFluxCardUsed(iDataOffset,g_lActualPayoutAmount);
		m_pSessionSwitch->addRecRequestCnt(iDataOffset);	
		//����LastDealTime
		m_pSessionSwitch->updateLastDealTime(iDataOffset,tEventBeginTime.getTime());
		//���ڶ������۲����Ļ�������LTE�ڴ���
        //m_cAccumMng.updateRec(lServID,strPid.c_str(),sessionrec.tTariffChangeTime,g_vecTicketAccum);
	}


	//���������
	int  iRsuCCTime = atol(GetOcsDataValues(OCS_FIELD_RSU_CC_TIME).c_str());
	DEBUG_2("RSU_CC_TIME = ",iRsuCCTime);
	CLong lRsuCCTotalOctets =  (CLong)atol(GetOcsDataValues(OCS_FIELD_RSU_CC_TOTAL_OCTETS).c_str());
	DEBUG_2("RSU_CC_TOTAL_OCTETS = ",lRsuCCTotalOctets);
	
	sChargeInfo.lGrantedTime = iRsuCCTime;  
    sChargeInfo.lGrantedTotalOctets = lRsuCCTotalOctets;


	if (stOcsReq.cRequestedMinServiceUnit.size() == 0)
	{
        m_iErrCode = RESULT_CODE_REQ_MIN_UNIT_FAILED;
        m_strerrmsg += ",cRequestedMinServiceUnit.size == 0";
        return -1;	
	}

	sChargeInfo.iMinCCTime = stOcsReq.cRequestedMinServiceUnit[0].lCCTime;
    sChargeInfo.lMinCCTotalOctets = stOcsReq.cRequestedMinServiceUnit[0].lCCTotalOctets;
	
	Rec.field(FIELD_ID(start_time)).setValue(tEventBeginTime);	
	Rec.field(FIELD_ID(end_time)).setValue(tEventBeginTime+iRsuCCTime);
		
	Rec.field(FIELD_ID(CALL_AMOUNT)).setValue(iRsuCCTime);
	Rec.field(FIELD_ID(volume_downlink)).setValue(lRsuCCTotalOctets);

	g_lFluxCardUsed = sessionrec.lFluxCardUsed;
    iRet = m_cRatingOL.ProcessTicket(Rec);
    if ((iRet<0))
    {
        m_cRatingOL.getErrMessage(m_iErrCode,m_strerrmsg);
		IsAbnormal(Rec);
        m_strerrmsg += "g_cRatingOL.ProcessTicket fail," + m_strerrmsg;
        return -1;
    }
    
    
    DEBUG_2("RatingOL rec = ",Rec);
    
    if (IsAbnormal(Rec))
    {            
        return -1;    
    }


	lCharge = 0;
	if(Rec.hasField(FIELD_ID(CHARGE)))
	{
		lCharge += Rec.field(FIELD_ID(CHARGE)).getLong();
	}
	if(Rec.hasField(FIELD_ID(CHARGE_2)))
	{
		lCharge += Rec.field(FIELD_ID(CHARGE_2)).getLong();
	}
    
    //ԭ���۽������ļƷ������� �� �ײ������͵�ʹ����  δ���������ĵ�ǰ�������� ������ʵ�ʵֿ���
    sChargeInfo.iBillingAmountType = Rec.field(FIELD_ID(BILLING_AMOUNT_TYPE)).getInteger();  

	//��ѯ������rg�ķ��� ���� ���ķ��� 
	CLong lSessionCharge = 0;
	m_pSessionSwitch->queryProdInstAllCharge(lServID,lSessionCharge);

	DEBUG_2("�Ự�ۻ��ķ��� = ",lSessionCharge);
	DEBUG_2("��������ķ��� = ",lCharge);
	DEBUG_2("��RG�䵥ǰ���� = ",lBeforeCharge);

	sChargeInfo.lCharge = lSessionCharge + lCharge + lBeforeCharge;

	//���°������ʷ��л������·�
	int iTariffTimeChange = 0;
	if (TariffTimeChange(iTariffTimeChange))
	{
		//�ڴ��е��ʷ��л��㲻�����µ��ʷ��л��������
		if (sessionrec.tTariffChangeTime != iTariffTimeChange)
		{
			m_pSessionSwitch->updateRecTariffChangeTime(iDataOffset,iTariffTimeChange);
		}
		sChargeInfo.lTariffTimeChange = iTariffTimeChange;
		sChargeInfo.bTariffTimeChange = true;

	}
	
    return 1;    
}


int COcsCharge::LteUpdateCharge(PACK_OCS_REQUEST& stOcsReq,SessionChargeInfo& sChargeInfo)
{
    
    try
    {  
        g_vecAccum.clear();
		int iRet = initCharge(stOcsReq,sChargeInfo);
		if (iRet<0)
		{
			return -1;
		}
		
        iRet = LteUpdateChargeAmount(stOcsReq,sChargeInfo);
        if (iRet<0)
        {
            return -1;
        }
		
		DEBUG_2("ԭ�� =",sChargeInfo.lCharge); 
        
        if (sChargeInfo.lCharge!=0   && !checkNoStopUrge(sChargeInfo.lServID))
        {

        	//���ò�����0������СԤ���� ���ҷ���
           iRet = LteUpdateChargeMinAmount(stOcsReq,sChargeInfo);
           if (iRet<0)
           {
			  return -1;
		   }
		       
        }  


    }
    catch(CException& e)   
    {
        m_iErrCode = RESULT_CODE_HANDLE_EXCEPTION_FAILED;
        m_strerrmsg += e.what();
        return -1;    
    }

    return 1;
    
  
}


int COcsCharge::LteTerminationCharge(PACK_OCS_REQUEST& stOcsReq,SessionChargeInfo& sChargeInfo)
{
    try
    {
    	//OCS_FIELD_CCR_MSCC_CUR_INDEX �±��1��ʼ
		int iPIDCCG = 96181 -1 ;
		int iRGLTE = OCS_FIELD_MSCC_RATING_GROUP -1;
        int iUsuCcTotalOctets = OCS_FIELD_MSCC_USU_CC_TOTAL_OCTETS -1;
        bool bSuccess = true; 
		int iRet = -1;


		bool bLteFlag = true;
		string strEventType = stOcsReq.sSrvCntxtId; 
		transform(strEventType.begin(), strEventType.end(), strEventType.begin(), ::toupper);
		unsigned int	nLoc = 0;
		nLoc = strEventType.find('@',0);
		strEventType = strEventType.substr(0,nLoc);
		if (strEventType.find("CCG") != strEventType.npos)
		{
			bLteFlag = false;
		}


		string strSession = stOcsReq.sSessionId;
		strSession = strSession.substr(0,strSession.rfind(";")+1);
		DEBUG_2("session = ", strSession);

		DEBUG_2(" stOcsReq.cServiceInformationVec.size() = " ,stOcsReq.cServiceInformationVec.size());
        for(int i = 0; i < stOcsReq.cServiceInformationVec.size();i++)
        {   
        
			bool bRet = m_cDataExPlain.loadData(stOcsReq.cServiceInformationVec[i].c_str());	   
			if (!bRet)
			{
				m_iErrCode = RESULT_CODE_LOAD_DATAEXPLAIN_FAILED;
				m_strerrmsg += "COcsDataExPlain loadData fail";   
				return -1;
			}

			m_iSourceEventTypeID = GetSourceEventType(stOcsReq,m_cDataExPlain,m_iRoamFlag,m_strerrmsg,m_iErrCode);
			//����sSrvCntxtIdȡԴ�¼�����
			if (-1 == m_iSourceEventTypeID)
			{
				return -1;
			}
			
          	SwitchSession();
			
            //����һ��Ԥ�����ʽ
            iRet = m_cPreProc1OL.InitTicket(m_iSourceEventTypeID);
            if (iRet<0)
            {
                m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
                m_strerrmsg += "g_cPreProc1OL.InitTicket fail," + m_strerrmsg;
                bSuccess = false; 
                break;
            }

    
            Record& rec = m_cPreProc1OL.getRecord();   
                            
            //���������ַ�������ԭʼ�����ṹ����
            iRet = ParseTicket(m_iSourceEventTypeID,m_iRoamFlag,rec,stOcsReq,m_sOrgTicket,m_cDataExPlain,m_strerrmsg,m_iErrCode);
            if (iRet<0)
            {        
                bSuccess = false; 
                break;            
            }		

			int iCurIndex = atoi(GetOcsDataValues(OCS_FIELD_CCR_MSCC_CUR_INDEX).c_str());
			DEBUG_2("iCurIndex = ",iCurIndex);

			CString strEventBeginTime = GetOcsDataValues(OCS_FIELD_EVENT_BEGIN_TIME).c_str();
			DEBUG_2("EVENT_BEGIN_TIME = ",strEventBeginTime);
			CTime tEventBeginTime = strEventBeginTime.toDate();

			//��дPID
	        int iRecLength = rec.field(FIELD_ID(CONTENT_CODE)).getLength();

			string strPID = "";
			if (bLteFlag)
			{
				strPID = GetOcsDataValues(iRGLTE+iCurIndex);
				strncpy(m_sOrgTicket.szRatingGroupID,strPID.c_str(),iRecLength); 
            	m_sOrgTicket.szRatingGroupID[iRecLength] = '\0'; 
				snprintf(m_sOrgTicket.szContentCode,iRecLength,"0");
			}
			else
			{
				strPID = GetOcsDataValues(iPIDCCG+iCurIndex);
				strncpy(m_sOrgTicket.szContentCode,strPID.c_str(),iRecLength); 
            	m_sOrgTicket.szContentCode[iRecLength] = '\0'; 
				snprintf(m_sOrgTicket.szRatingGroupID,iRecLength,"0");
			}
           		

			DEBUG_2("CONTENT_CODE = ",strPID);

			int iDataOffset = -1;

			string strNewSession = strSession + strPID;
			DEBUG_2("strNewSession = ", strNewSession);
			SessionRec sessionrec;
			m_pSessionSwitch->queryRec(strNewSession.c_str(),strPID.c_str(),iDataOffset);
			
			if (-1 == iDataOffset)
			{
            	// itnm00080626 20160517 ��ʹ������û��ʱ��(�û����°�������ֹ��ʱ����̫���Ự��¼����)  
				string strUsuCcTotalOctets = m_cDataExPlain.getDataValues(iUsuCcTotalOctets + iCurIndex);
				if(0 != atol(strUsuCcTotalOctets.c_str()))
            	{
            		// ��ֹ��������Ϊ0 ���ǻỰ��¼��ѯ���� ��ʱ begintime Ҫ���ƻ���
            		string strUsuCcTime = m_cDataExPlain.getDataValues(OCS_FIELD_MSCC_USU_CC_TIME);
					sessionrec.tBeginTime= tEventBeginTime.getTime() - atoi(strUsuCcTime.c_str());
					DEBUG_2("sessionrec.tBeginTime =",sessionrec.tBeginTime);
				}else
				{
	                DEBUG_4("�Ự�������ڸü�¼sSessionId =", strNewSession, ",RG=", strPID.c_str());
	                sessionrec.tBeginTime  =  tEventBeginTime.getTime();
	                sessionrec.lAmount_flux = 0;
	                strncpy(m_sOrgTicket.szImsiNbr, "NULLNULLNULLNU4", strlen("NULLNULLNULLNU4") + 1);
				}
            }
			else
			{
				m_pSessionSwitch->readRec(iDataOffset,sessionrec);
			}

			//��дORG_START_TIME
			iRecLength = rec.field(FIELD_ID(ORG_START_TIME)).getLength();
			string strDate = (const char*)CTime(sessionrec.tBeginTime).format("%Y%m%d%H%M%S");
			strDate += "00";
			strncpy(m_sOrgTicket.szOrgStartTime,strDate.c_str()+2,iRecLength);  
			m_sOrgTicket.szOrgStartTime[iRecLength] ='\0'; 




			//��дORG_END_TIME
			strDate = (const char*)tEventBeginTime.format("%Y%m%d%H%M%S");
			strDate += "00";
			strncpy(m_sOrgTicket.szOrgEndTime,strDate.c_str()+2,iRecLength);  
			m_sOrgTicket.szOrgEndTime[iRecLength] ='\0'; 

			//��дORG_CALL_AMOUNT
			m_sOrgTicket.iOrgCallAmount = 10*(tEventBeginTime.getTime() - sessionrec.tBeginTime);
			
			
            //��д����
            string strUsuCcTotalOctets = m_cDataExPlain.getDataValues(iUsuCcTotalOctets+iCurIndex);
			DEBUG_2("MSCC_USU_CC_TOTAL_OCTETS = ",strUsuCcTotalOctets);
			m_sOrgTicket.lVolumeDownLink = sessionrec.lAmount_flux + atol(strUsuCcTotalOctets.c_str());

			//����SessionID
			snprintf(m_sOrgTicket.szOcsSessionID,130,"%s",strNewSession.c_str());
			

			iRet = m_pSessionSwitch->deleteRec(strNewSession.c_str(),strPID.c_str());
			if (iRet==-1)
			{
				char szErr[512] = {0};
				m_iErrCode = RESULT_CODE_SESSION_DELETE_REC_FAILED;
				snprintf(szErr,sizeof(szErr),"Lte pSessionMng->deleteRec fail,sSessionId=%s,PID=%s,SourceEventTypeID"\
				,strNewSession.c_str(),strPID.c_str(),m_iSourceEventTypeID);
				m_strerrmsg = szErr;
				iRet = 0;
				bSuccess = false;
				break; 	
			}

			//ʹ����Ϊ0ʱ m_iIndbFlagΪ1�仰�� 0���仰��
			if ( 0 == m_iIndbFlag)
			{	
				if ( 0 == m_sOrgTicket.iOrgCallAmount && 0 == m_sOrgTicket.lVolumeDownLink)
				{
					continue;
				}
			}
		
            if (!m_cMemBaseHandle.Insert(m_sOrgTicket)) 
            {
                m_cMemBaseHandle.getErrMessage(m_iErrCode,m_strerrmsg);
                m_strerrmsg += "m_cMemBaseHandle.Insert fail," + m_strerrmsg;
                bSuccess = false; 
                break;  
                  
            }   	

        }
        
        if (!bSuccess)
        {
            m_cMemBaseHandle.rollbackMemDb(m_cMemBaseHandle.getMemConn());
    
        }
     	else
     	{
	     	//���ж��ɹ��˲��ύ
	        if (!m_cMemBaseHandle.commitMemDb(m_cMemBaseHandle.getMemConn())) 
	        {
	            m_cMemBaseHandle.getErrMessage(m_iErrCode,m_strerrmsg);
	            m_strerrmsg += "m_cMemBaseHandle.commitMemDb fail," + m_strerrmsg;
				bSuccess = false;
	        } 
           
		}

		if (!bSuccess)
		{
			m_strerrmsg += ",TerminationHandle fail";
			return -1; 
		}

        
    }
    catch(CException& e)   
    {
        m_iErrCode = RESULT_CODE_HANDLE_EXCEPTION_FAILED;
        m_strerrmsg += e.what();
        return -1;    
    }

    return 1;  

}



bool COcsCharge::LteReverseCharge(Record& rec,SessionChargeInfo& sChargeInfo)
{
    sChargeInfo.lGrantedTime = 0;
    sChargeInfo.lGrantedTotalOctets = 0; 
   
    //�ܼƷ�ʱ��
    long lCallAmount = 0; 
    //�ܼƷ�����
    long lTotalVolume = 0;

	//ȡ������۵ķ���
	CLong lCharge = 0;
	if(rec.hasField(FIELD_ID(CHARGE)))
	{
		lCharge += rec.field(FIELD_ID(CHARGE)).getLong();
	}
	if(rec.hasField(FIELD_ID(CHARGE_2)))
	{
		lCharge += rec.field(FIELD_ID(CHARGE_2)).getLong();
	}

	
	
    if (billing_amount_type_duation == sChargeInfo.iBillingAmountType)
    {
		if (0 == lCharge)
		{
			sChargeInfo.lGrantedTime = sChargeInfo.iMinCCTime;  
		}
		else
		{
			lCallAmount = rec.field(FIELD_ID(CALL_AMOUNT)).getInteger();
			//lCallAmount ���ʱ��  lCharge���ʱ�����������
			//sChargeInfo.lCurEffBalance - (sChargeInfo.lCharge - lCharge) ���
	        sChargeInfo.lGrantedTime = lCallAmount* (sChargeInfo.lCurEffBalance - sChargeInfo.lCharge + lCharge) /lCharge;
	        
	        if (sChargeInfo.lGrantedTime > sChargeInfo.iRsuCcTime)
	        {
	            sChargeInfo.lGrantedTime = sChargeInfo.iRsuCcTime;
	        }
			else if (sChargeInfo.lGrantedTime <sChargeInfo.iMinCCTime)
	        {
	            sChargeInfo.lGrantedTime = sChargeInfo.iMinCCTime;    
	        }
		}
	   
        
    }
    if (billing_amount_type_flow == sChargeInfo.iBillingAmountType)
    {
    	if (0 == lCharge)
    	{
			sChargeInfo.lGrantedTotalOctets = sChargeInfo.lMinCCTotalOctets;
		}
		else
		{
			lTotalVolume = rec.field(FIELD_ID(VOLUME_DOWNLINK)).getLong() + rec.field(FIELD_ID(VOLUME_UPLINK)).getLong();

	        sChargeInfo.lGrantedTotalOctets = (lTotalVolume* (sChargeInfo.lCurEffBalance - sChargeInfo.lCharge + lCharge) /lCharge)*1024; 
	        
	        if (sChargeInfo.lGrantedTotalOctets > sChargeInfo.lRsuCcTotalOctets)
	        {
	            sChargeInfo.lGrantedTotalOctets = sChargeInfo.lRsuCcTotalOctets;          
	        }
	        else if (sChargeInfo.lGrantedTotalOctets<sChargeInfo.lMinCCTotalOctets)
	        {
	            sChargeInfo.lGrantedTotalOctets = sChargeInfo.lMinCCTotalOctets;
	        }
		}
        
    }
    else
    {
        m_strerrmsg = "iBillingAmountType��������ʱ��,iBillingAmountType= ";
		m_strerrmsg += toString(sChargeInfo.iBillingAmountType);
        sChargeInfo.lGrantedTotalOctets = 0;
        sChargeInfo.lGrantedTime = 0;
    }
    
    
    return true;    
}



CMemBaseHandle::CMemBaseHandle()
{
      m_pMemConn = NULL;  
	  m_pPublicConn = NULL;
      m_strLoginName = "";
}


CMemBaseHandle::~CMemBaseHandle()
{
    if (m_pMemConn!=NULL)
    {
        rollbackMemDb(m_pMemConn);
        MemDbDisconnect(m_pMemConn,m_strerrmsg);
    }
    if (m_pMemConn!=NULL)
    {
        delete m_pMemConn;
        m_pMemConn = NULL;
    }

	/*
	if (m_pPublicConn!=NULL)
    {
        rollbackMemDb(m_pPublicConn);
        MemDbDisconnect(m_pPublicConn,m_strerrmsg);
    }

	if (m_pPublicConn!=NULL)
    {
        delete m_pPublicConn;
        m_pPublicConn = NULL;
    }
    */

	
}


bool CMemBaseHandle::Init(bool bMVNOFlag)
{
    IniReader ini;
    
    int iModule = MODULE_ID(TicketMove);
    int iFlowID = 0;
	string sHostId = getenv(ENV_NAME_HOST_ID_DSPCH.c_str());
    int iHostID = atoi(sHostId.c_str());

	if (bMVNOFlag)
	{
		iHostID = 20;
	}
	cout << "iModule = " << iModule << ",iFlowID = " << iFlowID << ",iHostID = " << iHostID <<endl;

	
    if (!ini.loadFromShm( iModule,iFlowID,iHostID))
	{   
	    m_iErrCode = RESULT_CODE_INIT_MEMBASE_FAILED; 
		m_strerrmsg = "init, loadFromShm failed";
		return false;
	}
	
	//ȡ�����ڴ����ݿ��������
    if (!ini.queryValue("TicketMove","AbmLoginName", m_strLoginName))
	{
	    m_iErrCode = RESULT_CODE_INIT_MEMBASE_FAILED; 
		m_strerrmsg = "get TicketMove, AbmLoginName  failed";
        return false;
	}
    
    cout << "MemBase name =" << m_strLoginName <<endl;
    	
    //�ڴ����ݿ������
    m_pMemConn = new AltibaseConnection;
    int iRet = MemDbConnect2(m_strLoginName, m_strerrmsg, *m_pMemConn);
    if (iRet != 1)
    {
        m_iErrCode = RESULT_CODE_INIT_MEMBASE_FAILED; 
        m_strerrmsg += " MemDbConnect2 fail,�����ڴ����ݿ�ʧ�ܣ�strLoginName:";
        m_strerrmsg += m_strLoginName;
        return false;
    }

	/*
	m_pPublicConn = new AltibaseConnection;
	iRet = MemDbConnect2("public", m_strerrmsg, *m_pPublicConn);
    if (iRet != 1)
    {
        m_iErrCode = RESULT_CODE_INIT_MEMBASE_FAILED; 
        m_strerrmsg += " MemDbConnect2 fail,�����ڴ����ݿ�ʧ��,public";
 
        return false;
    }
    */
	
    //���ò��ƶ��ύ
    bool bRet = MemDbAutoCommitOff();
    if (!bRet)
    {
        m_iErrCode = RESULT_CODE_INIT_MEMBASE_FAILED; 
        m_strerrmsg = "MemDbAutoCommitOff fail " + m_strerrmsg;
        return false;
    }
    bRet = prepareSQL(bMVNOFlag);
    if (!bRet)
    {
        m_iErrCode = RESULT_CODE_INIT_MEMBASE_FAILED; 
        m_strerrmsg = "prepareSQL fail," + m_strerrmsg;
        return false;    
    }
    
    return true;    
}

bool CMemBaseHandle::MemDbAutoCommitOff()
{

    try
    {
        m_pMemConn->SetAutoCommitOff(m_stat);
		//m_pPublicConn->SetAutoCommitOff(m_stat);
    }
	catch (AltibaseStatus st)
	{
	    m_iErrCode = RESULT_CODE_HANDLE_EXCEPTION_FAILED; 
        m_strerrmsg = "MemDb����: ";
        m_strerrmsg += st.err_msg;
        return false;
	}
	return true;
}




bool CMemBaseHandle::prepareSQL(bool bMVNOFlag)
{
    char szSql[4000];
    
    if (NULL == m_pMemConn)
    {
        m_iErrCode = RESULT_CODE_INIT_MEMBASE_FAILED; 
        m_strerrmsg  = "Prepare TICKET_ONLINE_ORG��ʱ���ݿ�ConnectionΪ��!";
        return false;
    }

    try { m_insertcmd.Drop(m_stat); } catch (...) { }
	//try { m_insertOpnLog.Drop(m_stat); } catch (...) { }
    try
    {
       
        //abmȡ�����ÿ��500����¼
        //ת������ȡת�۵ı�
		if (bMVNOFlag)
		{
			sprintf(szSql, "insert into MVNO_TICKET_ONLINE_ORG ( TICKET_ONLINE_ORG_ID, SWITCH_ID,  CALLING_ORG_NBR,\
                       CALLED_ORG_NBR, LATN_ID, ORG_START_TIME, ORG_END_TIME, \
                       ORG_CALL_AMOUNT, SESSION_ID, SMS_CENTER_ID,\
                       CALLING_BELONG_AREA_CODE,CALLED_BELONG_AREA_CODE,MSG_ID, \
                       CHARGE_TYPE,SOURCE_EVENT_TYPE_ID,CREATE_DATE,RECORD_TYPE,\
                       CALLING_ROAM_AREA_CODE,CALLED_ROAM_AREA_CODE,THIRD_PARTY_ORG_NBR,ROAM_ORG_TYPE,\
                       SUB_RECORD_TYPE,CALLING_CELL_ID,CALLED_CELL_ID,CALLING_LAC_ID,CALLED_LAC_ID,\
                       VOLUME_DOWNLINK,CONTENT_CODE,SERVICE_TYPE_ID,\
                       SP_ID,TIME_STAMP,BILLING_ORG_NBR,EXT_CHARGE,COMBINE_SERVICE_ID,BSC_ID,PDSN_IP,\
                       IMSI_NBR,OCS_SESSION_ID,LOGIN_NAME_3G,SP_SERVICE_ID_3G,CHTEL_OTHER_PARTNER,CHARGE_MODE,RATING_GROUP_ID,SECTOR_ID,\
                       SERVICE_TYPE_3G,ORG_PARTNER_ID,SRC_DEVICE_ID)\
                       Values(TICKET_ONLINE_ORG_ID_SEQ.nextval,?,?,?,?,?,?,?,?,?,?,?,?,?,?,sysdate,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
         

		}
		else
		{
			sprintf(szSql,"insert into TICKET_ONLINE_ORG ( TICKET_ONLINE_ORG_ID, SWITCH_ID,  CALLING_ORG_NBR,\
                       CALLED_ORG_NBR, LATN_ID, ORG_START_TIME, ORG_END_TIME, \
                       ORG_CALL_AMOUNT, SESSION_ID, SMS_CENTER_ID,\
                       CALLING_BELONG_AREA_CODE,CALLED_BELONG_AREA_CODE,MSG_ID, \
                       CHARGE_TYPE,SOURCE_EVENT_TYPE_ID,CREATE_DATE,RECORD_TYPE,\
                       CALLING_ROAM_AREA_CODE,CALLED_ROAM_AREA_CODE,THIRD_PARTY_ORG_NBR,ROAM_ORG_TYPE,\
                       SUB_RECORD_TYPE,CALLING_CELL_ID,CALLED_CELL_ID,CALLING_LAC_ID,CALLED_LAC_ID,\
                       VOLUME_DOWNLINK,CONTENT_CODE,SERVICE_TYPE_ID,\
                       SP_ID,TIME_STAMP,BILLING_ORG_NBR,EXT_CHARGE,COMBINE_SERVICE_ID,BSC_ID,PDSN_IP,\
                       IMSI_NBR,OCS_SESSION_ID,LOGIN_NAME_3G,SP_SERVICE_ID_3G,CHTEL_OTHER_PARTNER,CHARGE_MODE,RATING_GROUP_ID,SECTOR_ID,\
                       SERVICE_TYPE_3G,ORG_PARTNER_ID,SRC_DEVICE_ID,CHARGE_RESULT,ORG_CHARGE_ID,ORG_SESSION_ID,OFFER_NBR)\
                       Values(TICKET_ONLINE_ORG_ID_SEQ.nextval,?,?,?,?,?,?,?,?,?,?,?,?,?,?,sysdate,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
         

		}
           
        cout << "m_insertcmd szSql = " << szSql <<endl;
        m_insertcmd.Prepare(m_pMemConn, szSql, m_stat);

    }
    catch (AltibaseStatus st)
    {
        m_iErrCode = RESULT_CODE_INIT_MEMBASE_FAILED; 
        m_strerrmsg = "m_insertcmd alitibase prepare for TICKET_ONLINE_ORG fail, Msg:";
        m_strerrmsg += st.err_msg;
        return false;
    }
    
    return true;
}

bool CMemBaseHandle::Insert(SAbmOrgTicket& sOrgTicket)
{
	DTimerCalc calc(__LINE__, "Insert");
    bool bRet = false;
    try
    {
      
        m_insertcmd.setParam(1,sOrgTicket.iSwitchID);
        m_insertcmd.setParam(2,sOrgTicket.szCallingOrgNbr);
        m_insertcmd.setParam(3,sOrgTicket.szCalledOrgNbr);
        m_insertcmd.setParam(4,sOrgTicket.iLatnID);
        m_insertcmd.setParam(5,sOrgTicket.szOrgStartTime);
        m_insertcmd.setParam(6,sOrgTicket.szOrgEndTime);
        m_insertcmd.setParam(7,sOrgTicket.iOrgCallAmount);
		if (strcmp(sOrgTicket.szSessionID,"")==0)
		{
			strncpy(sOrgTicket.szSessionID,"NULL",9);
		}
        m_insertcmd.setParam(8,sOrgTicket.szSessionID);
        m_insertcmd.setParam(9,sOrgTicket.szSmsCenterID);
        m_insertcmd.setParam(10,sOrgTicket.szCallingBelongAreaCode);
        m_insertcmd.setParam(11,sOrgTicket.szCalledBelongAreaCode);
        m_insertcmd.setParam(12,sOrgTicket.szMsgID);
        m_insertcmd.setParam(13,sOrgTicket.szChargeType);
        m_insertcmd.setParam(14,sOrgTicket.iSourceEventTypeID);
        m_insertcmd.setParam(15,sOrgTicket.iRecordType);
        m_insertcmd.setParam(16,sOrgTicket.szCallingRoamAreaCode);
        m_insertcmd.setParam(17,sOrgTicket.szCalledRoamAreaCode);
        m_insertcmd.setParam(18,sOrgTicket.szThirdPartyOrgNbr);
        m_insertcmd.setParam(19,sOrgTicket.iRoamOrgType);
        m_insertcmd.setParam(20,sOrgTicket.iSubRecordType);
        m_insertcmd.setParam(21,sOrgTicket.szCallingCellID);
        m_insertcmd.setParam(22,sOrgTicket.szCalledCellID);
        m_insertcmd.setParam(23,sOrgTicket.szCallingLacID);
        m_insertcmd.setParam(24,sOrgTicket.szCalledLacID);
        m_insertcmd.setParam(25,sOrgTicket.lVolumeDownLink);
        m_insertcmd.setParam(26,sOrgTicket.szContentCode);
        m_insertcmd.setParam(27,sOrgTicket.iServiceTypeID);
        m_insertcmd.setParam(28,sOrgTicket.szSpID);
        m_insertcmd.setParam(29,sOrgTicket.szTimeStamp);
        m_insertcmd.setParam(30,sOrgTicket.szBillingOrgNbr);
        m_insertcmd.setParam(31,sOrgTicket.lExtCharge);
        m_insertcmd.setParam(32,sOrgTicket.szCombineServiceID);   
        m_insertcmd.setParam(33,sOrgTicket.szBscID);
        m_insertcmd.setParam(34,sOrgTicket.szPdsnIP);        
        m_insertcmd.setParam(35,sOrgTicket.szImsiNbr);
        m_insertcmd.setParam(36,sOrgTicket.szOcsSessionID);  
		m_insertcmd.setParam(37,sOrgTicket.szLoginName3G);  
		m_insertcmd.setParam(38,sOrgTicket.szSpServiceID3G); 
		m_insertcmd.setParam(39,sOrgTicket.szChtelOtherPartner); 
		m_insertcmd.setParam(40,sOrgTicket.iChargeMode); 
		m_insertcmd.setParam(41,sOrgTicket.szRatingGroupID); 
		//20141020
		m_insertcmd.setParam(42,sOrgTicket.szSectorID); 
		m_insertcmd.setParam(43,sOrgTicket.iServiceType3G); 
		m_insertcmd.setParam(44,sOrgTicket.szOrgPartnerID); 
		m_insertcmd.setParam(45,sOrgTicket.szSrcDeviceID); 
		//20151105
		m_insertcmd.setParam(46,sOrgTicket.iChargeResult); 
		m_insertcmd.setParam(47,sOrgTicket.iOrgChargeID); 
		m_insertcmd.setParam(48,sOrgTicket.szOrgSessionID); 
		m_insertcmd.setParam(49,sOrgTicket.szOfferNbr); 
		
        m_insertcmd.Execute(m_stat);               
  
  
        if (!MemDbStatementClose(m_insertcmd))
        {
            bRet = rollbackMemDb(m_pMemConn);
            if (!bRet)
            {
                m_iErrCode = RESULT_CODE_INSERT_MEMBASE_FAILED; 
                m_strerrmsg += "Insert fail," + m_strerrmsg;
                return false;    
            }
            m_iErrCode = RESULT_CODE_INSERT_MEMBASE_FAILED; 
            m_strerrmsg = "Insert fail,����TICKET_ONLINE_ORG��ر��α�ʧ��! MemDb����: " + m_strerrmsg;
            return false;
        }

    }
    catch (AltibaseStatus st)
    {
        m_iErrCode = RESULT_CODE_INSERT_MEMBASE_FAILED; 
        m_strerrmsg = "Insert fail,";
        m_strerrmsg += st.err_msg;
        return false;
    }
    return true;    
}

bool CMemBaseHandle::MemDbStatementClose(AltibaseStatement& ttcmd)
{
    //AltibaseStatus stat;
    try
    {
        ttcmd.Close(m_stat);
    }
	catch (AltibaseStatus st)
	{
	    m_iErrCode = RESULT_CODE_INSERT_MEMBASE_FAILED; 
        m_strerrmsg = "MemDb����: ";
        m_strerrmsg += st.err_msg;
        return false;
	}
    return true;
}


bool CMemBaseHandle::rollbackMemDb(AltibaseConnection* pMemConn)
{
    try
    {
        pMemConn->Rollback(m_stat);
    }
	catch (AltibaseStatus st)
	{
	    m_iErrCode = RESULT_CODE_INSERT_MEMBASE_FAILED; 
        m_strerrmsg = "MemDb�ع�ʧ��! MemDb����: ";
        m_strerrmsg += st.err_msg;
        return false;
	}
	return true;
}


bool CMemBaseHandle::commitMemDb(AltibaseConnection* pMemConn)
{
    try
    {
        pMemConn->Commit(m_stat);
    }
	catch (AltibaseStatus st)
	{
	    m_iErrCode = RESULT_CODE_INSERT_MEMBASE_FAILED; 
        m_strerrmsg = "MemDb�ύʧ��! MemDb����: ";
        m_strerrmsg += st.err_msg;
        return false;
	}
	return true;
}


