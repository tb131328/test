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
//达量在返回cca之后才做
string g_strPolicyAccNbr = "";   //达量提醒的号码

int g_iBeginTime = 700;
int g_iEndTime = 2300;


extern vector <struct ACCUMULATOR_RECORD> g_vecAccum;
extern vector <struct ACCUMULATOR_RECORD> g_vecTicketAccum;
extern int g_IncrementMode;   //全量模式0  增量模式1 丢在initcharge里初始化成全量模式



//20140422 LTE
extern CLong g_lFluxCardUsed;


extern CustPricingPlan	cpplan;	


//进入ProcessTicketDisct时清空
//未经过流量卡抵扣的费用 (Charge1+charge2+charge3) 
extern CLong g_lOriginCharge;
//流量卡实际抵扣量 
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
	    /*判断本格式是否有该字段 没有就取下一个*/                                                                
	    if (!rec.hasField(pFiledMap[i].iBillFieldID))                                                        
	    {                                                                                                                                                        
	        continue;                                                                                            
	    }                                                                                                        
	                                                                                                             
	    /*iOcsFieldID为0的表示福富填写 放在后面和特殊处理字段一起填写*/                                          
	    if (pFiledMap[i].iOcsFieldID == 0)                                                                   
	    continue;                                                                                                
	                                                                                                             
	    /*到通用格式里面找他对应的偏移量*/                                                                       
	    pszFieldMap = find(sOcsOrgTicketMapAry,sOcsOrgTicketMapAry+iAllField,pFiledMap[i].iBillFieldID);     
	                                                                                                             
	    if (pszFieldMap == sOcsOrgTicketMapAry+iAllField)                                                        
	    {                                                                                                        
	        snprintf(szErrMsg,255,"not find iBillFieldID=%d,SourceEventTypeID=%d",                               
	        pFiledMap[i].iBillFieldID,iSourceEventTypeID);                                                        
	        strerrmsg = szErrMsg;                                                                              
	        iErrCode = RESULT_CODE_NOT_FIND_MAP_FAILED;                                                                                      
	        return -1;                                                                                           
	    }                                                                                                        
  	                                                                                                             
	    /*取业务信息字符串对应字段的记录*/                                                                       
	    strValue = rDataExPlain.getDataValues(pFiledMap[i].iOcsFieldID);                                     
	    if ( strValue=="" && !filterFiled(iSourceEventTypeID,iRoamFlag,pFiledMap[i].iOcsFieldID))                                                                                       
	    {                                                                                                        
	        snprintf(szErrMsg,255,"业务信息字符串中没有对应字段，iOcsFieldID =%d",pFiledMap[i].iOcsFieldID); 
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


//可选字段不一定要送 所以值可能为空 
bool filterFiled(int& iSourceEventTypeID,int& iRoamFlag,int& iOcsFiled)
{
	switch(iSourceEventTypeID)
	{
		case EVENT_TYPE_CDMA_SOURCE_VOICE:
			//140006  LAC_A   140007  CELL_A  140008  LAC_B  140009  CELL_B 
			//上面这几个好像要根据record_type来判断是否要送
			if (OCS_FIELD_LAC_A == iOcsFiled || OCS_FIELD_CELL_A == iOcsFiled || OCS_FIELD_LAC_B == iOcsFiled || OCS_FIELD_CELL_B == iOcsFiled)
			{
				return true;
			}
			//重定向方号码  140011   重定向信息 140012  
			//语音90338 Roam_org_type 国际漫游有可能送的是空 或者随机值
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
			//PID对应的字段只有CCG业务会送3A默认为0
			//BSC_ID 120027可能不送
			if (OCS_FIELD_ORG_PRODUCT_OFFER_ID == iOcsFiled || OCS_FIELD_USE_NODE_CODE == iOcsFiled)
			{
				return true;
			}
			break;
		case EVENT_TYPE_CDMA_SOURCE_SMS:
			//SMSC_ADDRESS 有时候没有
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
			//100004 CALLED_NBR 增值业务的 被叫号码 好像是可选的 sm_id可选
			if (OCS_FIELD_CALLED_NBR == iOcsFiled || 90332 == iOcsFiled )
			{
				return true;
			}
			break;
		case EVENT_TYPE_SOURCE_BNG:
			//BNG话单可能不送被叫号码
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

                    //特殊处理SRECORD_TYPE
                    switch(rOrgTicket.iRecordType)
                    {
                        case 2:
                        rOrgTicket.iRecordType = 0;
                        break;
                        case 12:
                        rOrgTicket.iRecordType = 1;
                        break;  
						default:
						snprintf(szErrMsg,255,"RecordType=%d 不在取值范围内",rOrgTicket.iRecordType);
						strerrmsg = szErrMsg;
						iErrCode = RESULT_CODE_UNKNOW_RECORD_TYPE_FAILED;
						return -1; 
						break;
                    }

					//重填本地网标识根据RECORD_TYPE
					switch(rOrgTicket.iRecordType)
					{
						case 0:
							rOrgTicket.iLatnID = atoi(rOrgTicket.szCallingBelongAreaCode);
							break;
						case 1:
							rOrgTicket.iLatnID = atoi(rOrgTicket.szCalledBelongAreaCode);
							break;
					}
						
		
                    //特殊处理SUB_RECORD_TYPE
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
						snprintf(szErrMsg,255,"SubRecordType=%d 不在取值范围内",rOrgTicket.iSubRecordType);
						strerrmsg = szErrMsg;
						iErrCode = RESULT_CODE_UNKNOW_SUB_RECORD_TYPE_FAILED;
						return -1;
						break;
                    }


					//如果是呼转单 主叫与第三方号码对调
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
									strerrmsg = "MSC_ADDRESS或者BILLING_BELONG_AREA_CODE为空";
									iErrCode = RESULT_CODE_NOT_MAPPING_FIELD_FAILED;
									return -1;

								}
								// itnm00072743 【处理意见】转移单请强制判断为本地非漫游＝》研发无需区分转移的类型，转移单都判断为本地非漫游
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

                    //特殊处理ROAM_TYPE
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
						//国际漫游语音话单Roam_org_type ocs送空或随机值 计费需要根据国家信息表还有漫游运营商表来重填
						//初始化为0
						rOrgTicket.iRoamOrgType = 0;
						/*
						snprintf(szErrMsg,255,"RoamOrgType=:%d 不在取值范围内",rOrgTicket.iRoamOrgType);
						strerrmsg = szErrMsg;
						iErrCode = RESULT_CODE_UNKNOW_ROAM_ORG_TYPE_FAILED;
						return -1;
						*/
						break;
                    }

					//重填国际漫游语音的Roam_org_type,org_partner_id
					if ( 0  == rOrgTicket.iRecordType)
					{

						CString sCallingRoamAreaCode =  rOrgTicket.szCallingRoamAreaCode;		
						//填写主叫话单ORG_PARTNER_ID
						CToGCarrierFromFile   CtoGCarrier;
						CtoGCarrier.imsi_seg =  sCallingRoamAreaCode;
						CtoGCarrier.m_tTime = CTime::getCurrentTime();
						int iRet = CtoGCarrier.search();
						if (1 == iRet )
						{
							//查询到填写ORG_PARTNER_ID 和Roam_org_type
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

					//如果是国际漫游需要填写IMSI码 需要填写运营商标识
					if (6 == rOrgTicket.iRoamOrgType)
					{	
						//90224 BILLING_IMSI
						strValue = rDataExPlain.getDataValues(90224); 
						string strimsi = "46003";
						strimsi += strValue;
						strncpy(rOrgTicket.szImsiNbr,strimsi.c_str(),31);	
						
						//网元在线消息不直接提供此字段，计费可以默认赋值
						rOrgTicket.iServiceType3G = 0;
						//漫游地运营商填的服务标志
						strncpy(rOrgTicket.szSrcDeviceID,"D",21);

						rOrgTicket.iChargeMode  = 4;					
					}
					
					
                    //特殊处理iOrgCallAmount时长 初始 更新取的是RSU_CC_TIME  
                    //终止 离线取的是DURATION
                    switch(stOcsReq.iReqType)
                    {
                        //1,2 初始和更新取的是RSU_CC_TIME
                        case 1:
                        case 2: //没送就当作是0                                        
                        strValue = rDataExPlain.getDataValues(OCS_FIELD_RSU_CC_TIME);    
                        if (strValue == "")
                        {  strValue = "0";  }
                        rOrgTicket.iOrgCallAmount = 10 * atoi(strValue.c_str());
						if (0 == rOrgTicket.iOrgCallAmount)
						{
						    strerrmsg = "计费时长为0";
	                        iErrCode = RESULT_CODE_ZERO_CHARGE_AMOUNT_FAILED;
	                        return -1;  

						}
                        break;
                        //1,2 终止和离线取的是DURATION
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
                            snprintf(szErrMsg,255,"目前不支持该请求类型:%d",stOcsReq.iReqType);
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

                    //20140324 4g取CCR_PS_SGSNADDRESS 97805字段 3accg还是OCS_PDSN_ADDRESS 120061			
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
						
						//lte RatingGroupID填MSCC_RATING_GROUP  ContentCode填0
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
                    //二次预处理的时候会判断SessionID是否为空是否超过8位 特殊处理下
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
                    
                    //特殊处理ROAM_TYPE
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
						snprintf(szErrMsg,255,"RoamOrgType=:%d 不在取值范围内",rOrgTicket.iRoamOrgType);
						strerrmsg = szErrMsg;
						iErrCode = RESULT_CODE_UNKNOW_ROAM_ORG_TYPE_FAILED;
						return -1;
						break;
                    }
                                      
                    switch(stOcsReq.iReqType)
                    {
                        //1,2 初始和更新取的是RSU_CC_TIME  RSU_CC_TOTAL_OCTETS
                        case 1:
                        case 2: 
                            { 
                                //在线时长               
                                strValue = rDataExPlain.getDataValues(OCS_FIELD_RSU_CC_TIME);    
                                if (strValue == "")
                                {  strValue = "0";  }
                                rOrgTicket.iOrgCallAmount = 10 * atoi(strValue.c_str());
                                //在线流量
                                strValue = rDataExPlain.getDataValues(OCS_FIELD_RSU_CC_TOTAL_OCTETS);    
                                if (strValue == "")
                                {  strValue = "0";  }
                                rOrgTicket.lVolumeDownLink = atol(strValue.c_str()); 
								if (0 == rOrgTicket.iOrgCallAmount && 
									0 == rOrgTicket.lVolumeDownLink)
								{
								    strerrmsg = "计费时长流量均为0";
			                        iErrCode = RESULT_CODE_ZERO_CHARGE_AMOUNT_FAILED;
			                        return -1;  
								}
                                break;
                            }
                        //1,2 终止和离线取的是DURATION 流量为上下行
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
                    
                                //终止包和离线流量取的是BYTES
                                strValue = rDataExPlain.getDataValues(OCS_FIELD_BYTES);        
                                if (strValue == "")
                                {  strValue = "0";  }
                                rOrgTicket.lVolumeDownLink = atol(strValue.c_str());
	
                                break;
                            }
                        default:
                            {
                                snprintf(szErrMsg,255,"目前不支持该请求类型:%d",stOcsReq.iReqType);
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
                    rOrgTicket.iRecordType = 1; //itnm00071961协查：单号：D201408060591KF045673  
                    
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
						//短信的imsi就不填写了
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
			// itnm00075356lanlh:商务领航平台与 OCS 系统对接的需求   2015/10 
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
					
					//检验开始结束时间一样 则结束时间加1秒
					if (strStartTime == strEndTime  || strStartTime.toDate() > strEndTime.toDate())
					{
						//1秒
						rOrgTicket.iOrgCallAmount =10;
						iRecLength = rec.field(FIELD_ID(ORG_START_TIME)).getLength()+1; 
	                    strValue = rOrgTicket.szOrgStartTime;      
	                    strValue += "00";
						snprintf(rOrgTicket.szOrgStartTime,iRecLength,"%s",strValue.c_str()+2);

						//开始时间加1秒
						strValue = (const char*)(strStartTime.toDate()+1).format("%Y%m%d%H%M%S");
						strValue += "00";
						snprintf(rOrgTicket.szOrgEndTime,iRecLength,"%s",strValue.c_str()+2);

					}//否则时间为结束减开始
					else
					{

						CTimeSpan cts = strEndTime.toDate() - strStartTime.toDate();
						rOrgTicket.iOrgCallAmount = cts.getTotalSeconds()*10;

					}

					rOrgTicket.iRecordType = 1;				
					//费用转成厘 一次预处理时 还会转成毫
					rOrgTicket.lExtCharge =rOrgTicket.lExtCharge/10;

					if(1 == stOcsReq.iReqAct)  // 退款类型 需要把费用取反
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
	iRoamFlag = 0 ; //现在语音不根据这个字段来判是否国际漫游了
	if (strEventType.find("IN") != strEventType.npos)
	{
		iSourceEventTypeID = EVENT_TYPE_CDMA_SOURCE_VOICE;


		/*
		if (strEventType.find("ROAM") != strEventType.npos )
		{
			iRoamFlag = 1;
		}
		*/
		//90338为6是国际漫游
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
		//MSC_ADDRESS（90329） 0：国内用户 非0：用户国际漫游地信息
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
		if(strncmp(strSessionID.c_str(),"BNET",4) == 0)		// itnm00075356lanlh:商务领航平台与 OCS 系统对接的需求   2015/10 
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

	//改造有欠费的时候再根据话单中的产品提取产品的产权客户ID
	//根据等级提取客户门限
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
				
		    // 取客户化授信等级门限
		    memset( &tagCustLv, '\0', sizeof(tagCustLv) );
		    tagCustLvConfig.iLatnId = iLatnID;
		    tagCustLvConfig.sCustLevelId = sCreditLv;
			//tagCustLvConfig.info.lUndirAmount = 0;
		    iRet = tagCustLvConfig.search();
			//查询不到门限
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
		//结果余额= (- CustCreditLevel.lUndirAmount(单停门限)) – 欠费 
		lCurEffBalance = (-tagCustLv.lUndirAmount) - nLOweCharge;	
	}
	else
	{
		////结果余额= (- CustCreditLevel.lUndirAmount(单停门限)) + 模销余额
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
	m_iIndbDuration = 60; //60分钟
	m_iIndbRequestCnt = 40; //40次
	m_iIndbFlag = 0;
	m_ucSwitchFlag = 0x00;
	//m_bMVNOFlag = false;
	//m_pConnBill590 = NULL;
}


bool COcsCharge::init(int& rLatnID,bool bMVNOFlag)
{
	//m_bMVNOFlag = bMVNOFlag;
	m_iLatnID = rLatnID;
	//在线一次预处理的初始化
	bool bRet = m_cPreProc1OL.Init();
	if (!bRet)
	{
		m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
		m_strerrmsg += "m_cPreProc1OL.Init  failed," + m_strerrmsg;
		cout << m_strerrmsg<<endl;
		return false;	 
	}

	//在线二次预处理
	bRet = m_cPreProc2OL.Init();
	if (!bRet)
	{
		m_cPreProc2OL.getErrMessage(m_iErrCode,m_strerrmsg);
		m_strerrmsg += "m_cPreProc2OL.Init  failed," + m_strerrmsg;
		cout << m_strerrmsg<<endl;
		return false;	 
	}
	
		  
	//在线批价初始化
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
	//加载LTE共享内存
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

    
    //加载语音互斥锁
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
    
    // 取出参数文件的配置
    //初始化参数，使用前必须初始化
    if(!cParamCfgMng.bOnInit())
    {
        cParamCfgMng.getError(m_strerrmsg, m_iErrCode);
        cout << "参数配置接口初始化失败！错误码=" << m_iErrCode <<", 错误信息=" << m_strerrmsg << endl;
        return false;
    }

	string strParamName = "OCSDSPCH.OCS_INDB_FLUX";
	bool res = cParamCfgMng.getParamValueInt(strParamName, m_iIndbFlux);
    if (!res)
	{
		cParamCfgMng.getError(m_strerrmsg,m_iErrCode);
		cout << "查询OCS_INDB_FLUX失败！错误码="<<m_iErrCode<<", 错误信息="<<m_strerrmsg<<endl;
		return false;
	}
	cout << "OCSDSPCH.OCS_INDB_FLUX = "  << m_iIndbFlux <<endl;


	strParamName = "OCSDSPCH.OCS_INDB_DURATION";
	cParamCfgMng.getParamValueInt(strParamName, m_iIndbDuration);
    if (!res)
	{
		cParamCfgMng.getError(m_strerrmsg,m_iErrCode);
		cout << "查询OCS_INDB_DURATION失败！错误码="<<m_iErrCode<<", 错误信息="<<m_strerrmsg<<endl;
		return false;
	}


	cout << "OCSDSPCH.OCS_INDB_DURATION = "  << m_iIndbDuration <<endl;


	strParamName = "OCSDSPCH.OCS_INDB_REQUESTCNT";
	cParamCfgMng.getParamValueInt(strParamName, m_iIndbRequestCnt);
    if (!res)
	{
		cParamCfgMng.getError(m_strerrmsg,m_iErrCode);
		cout << "查询OCS_INDB_REQUESTCNT失败！错误码="<<m_iErrCode<<", 错误信息="<<m_strerrmsg<<endl;
		return false;
	}

	strParamName = "OCSDSPCH.OCS_INDB_FLAG";
	cParamCfgMng.getParamValueInt(strParamName, m_iIndbFlag);
    if (!res)
	{
		cParamCfgMng.getError(m_strerrmsg,m_iErrCode);
		cout << "查询OCS_INDB_FLAG失败！错误码="<<m_iErrCode<<", 错误信息="<<m_strerrmsg<<endl;
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
	
	//取连接内存数据库的主机名
    if (!ini.queryValue("TicketMove","SwitchFlag", iSwitchFlag))
	{
	    
		cout << "get TicketMove, SwitchFlag failed " <<endl;
        return false;
	}
	
	//初始化为空
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
                cout << "取OCCI版本异常" <<endl;
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
	//默认授权量为RSU_CC_TIME  RSU_CC_TOTAL_OCTETS
    sChargeInfo.lGrantedTime = atol(GetOcsDataValues(OCS_FIELD_RSU_CC_TIME).c_str());  
    sChargeInfo.lGrantedTotalOctets = atol(GetOcsDataValues(OCS_FIELD_RSU_CC_TOTAL_OCTETS).c_str());
	//取CCG节目标识 语音和3A话单该字段为0
	GetPid(Rec,sChargeInfo);
	
	//更新包才查询会话积量
    if (bUpdate)
    {
    	
        //对于第一个更新包来说 查询会话管理使用量也是没记录的 
        //第二次才有 是否在初始化的时候就插一条0的进去
        SessionRec sessionrec;
		m_pSessionSwitch->queryRec(stOcsReq.sSessionId,sChargeInfo.szPID,sChargeInfo.iDataOffset);
        if (sChargeInfo.iDataOffset != -1)
        {	
        	//SessionRec sessionrec;
            m_pSessionSwitch->readRec(sChargeInfo.iDataOffset,sessionrec);  
			sChargeInfo.lAmount_duration = sessionrec.lAmount_duration;
			sChargeInfo.lAmount_flux = sessionrec.lAmount_flux;

			//switchflag个位为1的代表月末，每日6点、23点0点断网
			if (m_ucSwitchFlag&0x01)
			{
				if (EVENT_TYPE_CDMA_SOURCE_GROUPING == sChargeInfo.iSourceEventTypeID)
				{
					//每日6点断网 开始时间早于6点,当前时间大等于6点的断下网
					CTime tBeginTime = sessionrec.tBeginTime;
					CTime tCurrTIme = CTime::getCurrentTime();
					CTime tNextTime = tCurrTIme + 86400;
					//月末23点55分断
					CTime tBanTime = CTime(tCurrTIme.getYear(),tCurrTIme.getMonth(),tCurrTIme.getDay(),23,55);
					if (tNextTime.getMonth() != tCurrTIme.getMonth())
					{
						if (tBeginTime < tBanTime &&  tCurrTIme >= tBanTime)
						{
							sChargeInfo.iTerminationFlag = 0;
							sChargeInfo.bBanflag = true;
						}
					}
					
					//6点断
					tBanTime = CTime(tCurrTIme.getYear(),tCurrTIme.getMonth(),tCurrTIme.getDay(),6);
					if (tBeginTime < tBanTime && tCurrTIme >= tBanTime)
					{
							sChargeInfo.iTerminationFlag = 0;
							sChargeInfo.bBanflag = true;
					}
					
					//23点断
					tBanTime = CTime(tCurrTIme.getYear(),tCurrTIme.getMonth(),tCurrTIme.getDay(),23);
					if (tBeginTime < tBanTime && tCurrTIme >= tBanTime)
					{
							sChargeInfo.iTerminationFlag = 0;
							sChargeInfo.bBanflag = true;
					}
		
					//24点断
					if (tBeginTime.getDay() !=  tCurrTIme.getDay())
					{
							sChargeInfo.iTerminationFlag = 0;
							sChargeInfo.bBanflag = true;
					}

				}

			}

			
			//switchflag打上10标记才对bscid有变更的进行切换
			if (m_ucSwitchFlag&0x02)
			{
				if (EVENT_TYPE_CDMA_SOURCE_GROUPING == sChargeInfo.iSourceEventTypeID)
				{
					CLong lProdSpecID = Rec.field(FIELD_ID(billing_prod_spec_id)).getLong();
					//并且只针对手机用户上网用户
					if (800000002 == lProdSpecID)
					{
						int iRoamOrgType = Rec.field(FIELD_ID(ROAM_ORG_TYPE)).getInteger();
						//当漫游类型发生变化时打上标记把用户踢下线
						//只有省内才会送bsc_id roam_org_type0本地 9省内
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
		else //查询不到 返回失败 
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
			//第一个更新包记录开始时间
			sessionrec.tBeginTime = CTime::getCurrentTime().getTime();
			sessionrec.ifSessionCommit = false;

			if (m_ucSwitchFlag&0x02)
			{
				if (EVENT_TYPE_CDMA_SOURCE_GROUPING == sChargeInfo.iSourceEventTypeID)
				{
					CLong lProdSpecID = Rec.field(FIELD_ID(billing_prod_spec_id)).getLong();
					//并且只针对手机用户上网用户
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

	//取上次实际使用量
    switch(m_iSourceEventTypeID)
    {
        case EVENT_TYPE_CDMA_SOURCE_VOICE:            
            //取不到上次实际时长 当作是送0处理
            //实际使用量
            sChargeInfo.iUsuCcTime = atoi(GetOcsDataValues(OCS_FIELD_USU_CC_TIME).c_str());  
            
            //取时间片           
            sChargeInfo.iRsuCcTime = atoi(GetOcsDataValues(OCS_FIELD_RSU_CC_TIME).c_str());  
            
            break;
            
        case EVENT_TYPE_CDMA_SOURCE_GROUPING:
            //取不到都当作送0           
            sChargeInfo.iUsuCcTime = atoi(GetOcsDataValues(OCS_FIELD_USU_CC_TIME).c_str());
            
            //取时间片          
            sChargeInfo.iRsuCcTime = atoi(GetOcsDataValues(OCS_FIELD_RSU_CC_TIME).c_str()); 
            
            //上次实际使用量
            sChargeInfo.lUsuCcTotalOctets = atol(GetOcsDataValues(OCS_FIELD_USU_CC_TOTAL_OCTETS).c_str());   
			
            //间隔流量
            sChargeInfo.lRsuCcTotalOctets = atoi(GetOcsDataValues(OCS_FIELD_RSU_CC_TOTAL_OCTETS).c_str()); 
            
            break; 
            
         default:               
            m_iErrCode = RESULT_CODE_OTHER_EVENT_FAILED;
			m_strerrmsg = "会话控制不支持该事件类型,iSourceEventTypeID= ";
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
加载解析业务信息串
取得事件类型
检验事件类型
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
	//根据sSrvCntxtId取源事件类型
	if (-1 == m_iSourceEventTypeID)
	{
		return -1;
	}
	
	sChargeInfo.setSourceEventType(m_iSourceEventTypeID);
    
	//检验事件类型
	switch(stOcsReq.iReqType)
	{
		case 1:
		case 2:			
		case 3:
			//会话型只处理语音和分组域的业务
		    if (EVENT_TYPE_CDMA_SOURCE_VOICE != m_iSourceEventTypeID 
		        && EVENT_TYPE_CDMA_SOURCE_GROUPING != m_iSourceEventTypeID )
		    {      
		        m_iErrCode = RESULT_CODE_OTHER_EVENT_FAILED;
				m_strerrmsg ="会话控制不支持该事件类型,SourceEventTypeID= " ;
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
				//事件型除了离线话单  只处理短信和ismp
				string strSessionCdr = stOcsReq.sSessionId;
		        transform(strSessionCdr.begin(), strSessionCdr.end(), strSessionCdr.begin(), ::toupper);
				//如果是离线话单都可以处理
	            if(strncmp(strSessionCdr.c_str(),"CDR",3) ==0)
	            {
					break;
				}*/
						
				if (EVENT_TYPE_CDMA_SOURCE_SMS != m_iSourceEventTypeID 
					&& EVENT_TYPE_CDMA_SOURCE_OPERATION != m_iSourceEventTypeID
					&& EVENT_TYPE_SOURCE_BNG != m_iSourceEventTypeID   // itnm00075356lanlh:商务领航平台与 OCS 系统对接的需求   2015/10 
					)
				{
					m_iErrCode = RESULT_CODE_OTHER_EVENT_FAILED;
					m_strerrmsg ="事件控制不支持该事件类型,SourceEventTypeID= " ;
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



//总时长 = 时间片间隔(RSU_CC_TIME) + 上次实际使用的时长（USU_CC_TIME） + 会话累积的时长(SessionRec::lAmount_duration)        
//最小预留量的总时长 = Requested-Min-Service-Unit. CC-Time +  上次实际使用的时长（USU_CC_TIME） + 会话累积的时长(SessionRec::lAmount_duration)    

//总流量 = 间隔流量(RSU_CC_TOTAL_OCTETS) + 上次实际使用的流量（USU_CC_TOTAL_OCTETS） + 会话累积的流量(SessionRec::lAmount_flux) 
//最小预留量的总流量 = Requested-Min-Service-Unit. CC-Total-Octets + 上次实际使用的流量（USU_CC_TOTAL_OCTETS）+ 会话累积的流量(SessionRec::lAmount_flux)  
 
//设置总计费量 还有最小预留量
bool COcsCharge::SetBillingAmount(SessionChargeInfo& sChargeInfo,Record& rec,bool bMinflag)
{
    //时间片 
    int iCallInterval = 0;        
    long lFluxInterval = 0; 
    
    int iMinCCTime = sChargeInfo.iMinCCTime;
    long lMinCCTotalOctets = sChargeInfo.lMinCCTotalOctets; 
		
    //初始包时会话累积的使用量是0
    long lAmountDuration= sChargeInfo.lAmount_duration;
    long lAmountFlux = sChargeInfo.lAmount_flux;

	int iUsuCcTime = sChargeInfo.iUsuCcTime;
	long lUsuCcTotalOctets = sChargeInfo.lUsuCcTotalOctets;
       	
    switch(m_iSourceEventTypeID)
    {
        //语音取上次实际使用的时长
        case EVENT_TYPE_CDMA_SOURCE_VOICE: 

			iCallInterval = bMinflag ? sChargeInfo.iMinCCTime : sChargeInfo.iRsuCcTime;
            rec.field(FIELD_ID(CALL_AMOUNT)).setValue((int)(iCallInterval+lAmountDuration+iUsuCcTime));
            break;
            
        //分组域需要取流量 时长不知道会不会送
        case EVENT_TYPE_CDMA_SOURCE_GROUPING:                            
            iCallInterval = bMinflag ? sChargeInfo.iMinCCTime : sChargeInfo.iRsuCcTime;
            rec.field(FIELD_ID(CALL_AMOUNT)).setValue((int)(iCallInterval+lAmountDuration+iUsuCcTime));
            
            lFluxInterval = bMinflag ? sChargeInfo.lMinCCTotalOctets : sChargeInfo.lRsuCcTotalOctets;
            rec.field(FIELD_ID(volume_downlink)).setValue((CLong)(lFluxInterval+lAmountFlux+lUsuCcTotalOctets));
            break; 
            
         default:               
            m_iErrCode = RESULT_CODE_OTHER_EVENT_FAILED;     
			m_strerrmsg = "会话控制不支持该事件类型,SourceEventTypeID= ";
			m_strerrmsg += toString(m_iSourceEventTypeID);
            return false;
            break;   

    }
  
    return true;    
}





//反算授权量 Total-Octets TotalOctets
bool COcsCharge::ReverseCharge(Record& rec,SessionChargeInfo& sChargeInfo)
{
    sChargeInfo.lGrantedTime = 0;
    sChargeInfo.lGrantedTotalOctets = 0; 
   
    //总计费时长
    long lCallAmount = 0; 
    //总计费流量
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

        //有经过流量卡虚扣的需要按未经流量卡的当前话单费用来打折
        if( sVolumFlug.compare("2") == 0)  
        {            
            if (sChargeInfo.lOriginCharge == 0)
            {
                m_iErrCode = RESULT_CODE_ZERO_ORG_CHARGE_FAILED;
                m_strerrmsg += "未经流量卡的当前话单费用为0";
                return false;
            }

            sChargeInfo.lGrantedTotalOctets = (lTotalVolume - sChargeInfo.lCurrTicketTotalLinkResource) * sChargeInfo.lCurEffBalance /sChargeInfo.lOriginCharge; 
            
        }//流量卡无抵扣        
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
        m_strerrmsg = "iBillingAmountType非流量非时长,iBillingAmountType= ";
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

	//取经过二次预处理的SERVID
	sChargeInfo.lServID =Rec.field(FIELD_ID(SERV_ID)).getLong();
    //取计费号码
    //sChargeInfo.strAccNbr = Rec.field(FIELD_ID(BILLING_NBR)).getString();
    //g_strAccNbr = Rec.field(FIELD_ID(BILLING_NBR)).getString();
    //取经过二次预处理后的rec备份 用于计算最小预留量
	m_cPreProc1OL.SetMinRecord();

    
    //初始包的时长流量都为0
	if (!GetAmount(sChargeInfo,stOcsReq,Rec,bUpdate))
	{
		return -1; 
	}

	//分组域断网标志为true 则将用户踢下线
	if (EVENT_TYPE_CDMA_SOURCE_GROUPING == sChargeInfo.iSourceEventTypeID && sChargeInfo.bBanflag)
	{
		sChargeInfo.iBillingAmountType = 2;
		sChargeInfo.lGrantedTime = 0;
		sChargeInfo.lGrantedTotalOctets = 0;
		sChargeInfo.lCharge = 0;
		return 1;
	}
      
    //设置总的计费量 上次实际使用量+本次时间片+会话管理使用量    
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
    
    
    //原批价结果算出的计费量类型 和 套餐已赠送的使用量  未经流量卡的当前话单费用 流量卡实际抵扣量
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


	//计算charge + charge2	
	if(Rec.hasField(FIELD_ID(CHARGE)))
	{
		sChargeInfo.lCharge += Rec.field(FIELD_ID(CHARGE)).getLong();
	}
	if(Rec.hasField(FIELD_ID(CHARGE_2)))
	{
		sChargeInfo.lCharge += Rec.field(FIELD_ID(CHARGE_2)).getLong();
	}

	//超过100％档次
	//费用大于0 说明使用量已经超出赠送的量
	if (m_ucSwitchFlag&0x01 && sChargeInfo.lCharge > 0  && EVENT_TYPE_CDMA_SOURCE_GROUPING == sChargeInfo.iSourceEventTypeID)
	{
		//解析过程戳 是否出现临界状态
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
			//解析失败 正常返回
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
				// 组内按逗号拆分成向量
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
	//取经过二次预处理后的rec备份 用于计算最小预留量
    Record& Rec = m_cPreProc1OL.getMinRecord();

	//查询余额		 
	if (!QueryBalance(Rec,sChargeInfo.lCurEffBalance,m_strerrmsg,m_iErrCode))
	{  
		return -1;
	}

	DEBUG_2("余额 =",sChargeInfo.lCurEffBalance); 

	if (sChargeInfo.lCurEffBalance<=0)
	{
		DEBUG_2("余额小等于0 ,CurEffBalance =",sChargeInfo.lCurEffBalance);
		if (bUpdate)
		{
			sChargeInfo.lGrantedTime = sChargeInfo.iMinCCTime;
			sChargeInfo.lGrantedTotalOctets = sChargeInfo.lMinCCTotalOctets;
			sChargeInfo.iTerminationFlag = 0;
			return 1;
		}
		//如果是会话初始 余额小等于0 返回4501 action返回-1
		else
		{
			sChargeInfo.iTerminationFlag = -1;
			m_iErrCode = RESULT_CODE_BAL_LOW_ZRO;
			m_strerrmsg = "会话初始,余额小等于0";
			sChargeInfo.lGrantedTime = 0;
			sChargeInfo.lGrantedTotalOctets = 0;
			return -1;
		}
	}



	if (sChargeInfo.lCharge<=sChargeInfo.lCurEffBalance)
	{ 
		//DEBUG_4("余额足够,lCharge =",sChargeInfo.lCharge,",lCurEffBalance=",sChargeInfo.lCurEffBalance); 
		//DEBUG_4("lGrantedTime=",sChargeInfo.lGrantedTime,",lGrantedTotalOctet=",sChargeInfo.lGrantedTotalOctets); 
					 
	}//余额不足
	else
	{
		
	    if (!SetBillingAmount(sChargeInfo,Rec,true))
	    {
	        return -1;
	    } 
		//对最小预留量进行批价
		int iRet = m_cRatingOL.ProcessTicket(Rec);
		if (iRet<0)
		{
			m_cRatingOL.getErrMessage(m_iErrCode,m_strerrmsg);
			IsAbnormal(Rec);
			m_strerrmsg += "g_cRatingOL.ProcessTicket fail," + m_strerrmsg;
			return -1;
		}


		DEBUG_2("最小预留量批价结果 =",Rec); 
		
		if (IsAbnormal(Rec))
		{	
			return -1;	 
		}
		
		//最小预留量的费用 
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
			//	余额不足，够打最小预留量，返回0, FUC action_id = 0，后继网元会送终止包。
			sChargeInfo.iTerminationFlag = 0;

			//这里反算的应该是对配额批价的REC
			Record& OriRec = m_cPreProc1OL.getRecord();
			if (!ReverseCharge(OriRec,sChargeInfo))
			{
				return -1;
			}			  
		}
		else
		{
			//余额不足，不够打最小预留量，返回0, FUC action_id =0 返回最小预留量
			sChargeInfo.iTerminationFlag = 0;		
			sChargeInfo.lGrantedTime = sChargeInfo.iMinCCTime;
			sChargeInfo.lGrantedTotalOctets = sChargeInfo.lMinCCTotalOctets;

		}
		DEBUG_4("最小预留量费用,lMinCharge =",lMinCharge,",lCurEffBalance=",sChargeInfo.lCurEffBalance); 
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
		
        	//费用不等于0计算最小预留量 并且反算
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
	
			//如果LTE内存提交 删除LTE内存
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
			
			//LastDealTime 取EVENT_BEGIN_TIME 90003		
			sessionrec.LastDealTime = tTime.getTime();	
			sessionrec.ifSessionCommit = false;

			//SERVICE_TYPE_ID
			snprintf(sessionrec.sServiceTypeID,10,"%d",m_sOrgTicket.iServiceTypeID);

			int iTariffTimeChange = 0;
			//到达资费切换点
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
        //如果是更新包 
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
	    //如果是更新包 
        g_vecAccum.clear();
		int iRet = initCharge(stOcsReq,sChargeInfo);

		if (iRet<0)
		{
			return -1;
		}
	
		//构造一次预处理格式
		iRet = m_cPreProc1OL.InitTicket(m_iSourceEventTypeID);
		if (iRet<0)
		{
			m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
			m_strerrmsg += "m_cPreProc1OL.InitTicket fail," + m_strerrmsg;
			return -1;
		}

		Record& rec = m_cPreProc1OL.getRecord();   

				
		//将解析的字符串填入原始话单结构体中
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
		
		//扣费才需要进行批价 离线和补款不需要
		if (bEventCharge)
		{

			//填写Record字段
			iRet = m_cPreProc1OL.convertHandle(rec,m_iSourceEventTypeID,m_sOrgTicket);
			if (iRet<0)
			{
				m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
				m_strerrmsg += "g_cPreProc1OL.convertHandle fail," + m_strerrmsg;
				return -1;
			}
		
			//是否是异常单 是直接返回 出离线
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
			//费用不为0才查询余额 
			if  (sChargeInfo.lCharge != 0   && !checkNoStopUrge(sChargeInfo.lServID) )
			{
				//查询余额
				if (!QueryBalance(rec,sChargeInfo.lCurEffBalance,m_strerrmsg,m_iErrCode))
				{
					DEBUG_2("QueryBalance fail, g_strOcsChargeErrMsg =",m_strerrmsg);	   
					return -1;
				}
 
				if (sChargeInfo.lCurEffBalance<=0)
				{
					m_iErrCode = RESULT_CODE_BAL_LOW_ZRO;
					m_strerrmsg = "余额小等于0,余额 = ";
			        m_strerrmsg += toString(sChargeInfo.lCurEffBalance);
					return -1;
				}
		
				//不足不扣 和 必须全部扣完
				if (stOcsReq.iChargeType ==0 || stOcsReq.iChargeType ==2)
				{
					if (sChargeInfo.lCharge<=sChargeInfo.lCurEffBalance)
					{
						DEBUG_2("余额足够",sChargeInfo.lCurEffBalance);
					}
					else
					{
						m_iErrCode = RESULT_CODE_BAL_LOW_ZRO;
						m_strerrmsg += "余额不足";				
						return -1;
					}
					
				}//部分抵扣
				else if (stOcsReq.iChargeType == 1)
				{
					if (sChargeInfo.lCurEffBalance<=0)
					{
						m_iErrCode = RESULT_CODE_BAL_LOW_ZRO;
						m_strerrmsg += "可用余额小等于0";			  
						return -1;
					}
				 
				}
				else
				{
					m_iErrCode = RESULT_CODE_UNKNOW_CHARGE_TYPE_FAILED;
					m_strerrmsg += "目前不支持该ChargeType= ";
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



//终止包处理 需要入库
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
               
            //构造一次预处理格式
            iRet = m_cPreProc1OL.InitTicket(m_iSourceEventTypeID);
            if (iRet<0)
            {
                m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
                m_strerrmsg += "g_cPreProc1OL.InitTicket fail," + m_strerrmsg;
                bSuccess = false; 
                break;
            }

    
            Record& rec = m_cPreProc1OL.getRecord();   
                            
            //将解析的字符串填入原始话单结构体中
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
							CString sBSCIdLeft4 = sBSCId.left(4);//取前4位
							sBSCIdLeft4.makeUpper(); //转化为大写
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

			//使用量为0时 m_iIndbFlag为1落话单 0不落话单
			if ( 0 == m_iIndbFlag)
			{	
				//流量时长同时为0则不落话单
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
	     	//所有都成功了才提交
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
    //return -1:异常; 0:无配置; 1:有配置IF_SPECIAL_USER=F; 2:有配置IF_SPECIAL_USER=T
    nRet = tagOweStopUrge.search();
    if( nRet<0 )
    {
        m_strerrmsg = "查询免复停催(OWE_STOP_URGE)异常!LATN_ID=";
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
    int validityTime = 3600;    //500秒  后面建议调整为1个小时
    
    
    int beginHour = g_iBeginTime/100;
    int beginMin = g_iBeginTime%100;
    
    int endHour = g_iEndTime/100;
    int endMin = g_iEndTime%100;
	
    CTime tTime = CTime::getCurrentTime();
    CTime tTime7 = CTime(tTime.getYear(), tTime.getMonth(), tTime.getDay(), beginHour,beginMin);
    CTime tTime24 = CTime(tTime.getYear(), tTime.getMonth(), tTime.getDay()) + 3600 * endHour + 60*endMin;
    
    if(tTime < tTime7)   //是否小于7点
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
	//取经过二次预处理后的rec备份 用于计算最小预留量
	
	//想将LTE内存重新导入 g_vecAccum	
	//m_cAccumMng.queryRec(sChargeInfo.lServID,g_vecAccum);

	
	//SessionRec sessionrec;
	//m_pSessionSwitch->readRec(sChargeInfo.iDataOffset,sessionrec);
	
    Record& Rec = m_cPreProc1OL.getMinRecord();
	//查询余额		 
	if (!QueryBalance(Rec,sChargeInfo.lCurEffBalance,m_strerrmsg,m_iErrCode))
	{  
		return -1;
	}

	DEBUG_2("余额 =",sChargeInfo.lCurEffBalance); 

	if (sChargeInfo.lCurEffBalance<=0)
	{
		DEBUG_2("余额小等于0",sChargeInfo.lCurEffBalance);
		sChargeInfo.lGrantedTime = sChargeInfo.iMinCCTime;
		sChargeInfo.lGrantedTotalOctets = sChargeInfo.lMinCCTotalOctets;
		sChargeInfo.iTerminationFlag = 0;
		return 1;
	}



	if (sChargeInfo.lCharge<=sChargeInfo.lCurEffBalance)
	{ 
		DEBUG_4("余额足够,lCharge =",sChargeInfo.lCharge,",lCurEffBalance=",sChargeInfo.lCurEffBalance); 
		DEBUG_4("lGrantedTime=",sChargeInfo.lGrantedTime,",lGrantedTotalOctet=",sChargeInfo.lGrantedTotalOctets); 
					 
	}//余额不足
	else
	{
		

		//取EVENT_BEGIN_TIME
		CString strEventBeginTime = GetOcsDataValues(OCS_FIELD_EVENT_BEGIN_TIME).c_str();
		CTime tEventBeginTime = strEventBeginTime.toDate();
		DEBUG_2("MIN EVENT_BEGIN_TIME = ",tEventBeginTime);

		
		Rec.field(FIELD_ID(start_time)).setValue(tEventBeginTime);	
		Rec.field(FIELD_ID(end_time)).setValue(tEventBeginTime+sChargeInfo.iMinCCTime);

		Rec.field(FIELD_ID(CALL_AMOUNT)).setValue(sChargeInfo.iMinCCTime);
		Rec.field(FIELD_ID(volume_downlink)).setValue((CLong)sChargeInfo.lMinCCTotalOctets);


		
		//对最小预留量进行批价
		int iRet = m_cRatingOL.ProcessTicket(Rec);
		if (iRet<0)
		{
			m_cRatingOL.getErrMessage(m_iErrCode,m_strerrmsg);
			IsAbnormal(Rec);
			m_strerrmsg += "g_cRatingOL.ProcessTicket fail," + m_strerrmsg;
			return -1;
		}


		DEBUG_2("最小预留量批价结果 =",Rec); 
		
		if (IsAbnormal(Rec))
		{	
			return -1;	 
		}
			
		//最小预留量的费用 
		CLong lMinCharge =0;
		if(Rec.hasField(FIELD_ID(CHARGE)))
		{
			lMinCharge += Rec.field(FIELD_ID(CHARGE)).getLong();
		}
		if(Rec.hasField(FIELD_ID(CHARGE_2)))
		{
			lMinCharge += Rec.field(FIELD_ID(CHARGE_2)).getLong();
		}

		//累加上之前算的费用
		lMinCharge += sChargeInfo.lCharge;
		if (lMinCharge<=sChargeInfo.lCurEffBalance)
		{
			//	余额不足，够打最小预留量，返回0, FUC action_id = 0，后继网元会送终止包。
			sChargeInfo.iTerminationFlag = 0;
			
			//这里反算的应该是对配额批价的REC
			Record& OriRec = m_cPreProc1OL.getRecord();
			if (!LteReverseCharge(OriRec,sChargeInfo))
			{
				return -1;
			}			  
		}
		else
		{
			//余额不足，不够打最小预留量，返回0, FUC action_id =0 返回最小预留量
			sChargeInfo.iTerminationFlag = 0;		
			sChargeInfo.lGrantedTime = sChargeInfo.iMinCCTime;
			sChargeInfo.lGrantedTotalOctets = sChargeInfo.lMinCCTotalOctets;

		}
		DEBUG_4("最小预留量费用,lMinCharge =",lMinCharge,",lCurEffBalance=",sChargeInfo.lCurEffBalance); 
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

	//为区分是中间单落的单子还是终止包落的用IMSI_NBR来记录
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

	//CCG 和 LTE 是增量计费模式
	g_IncrementMode = 1;
	g_vecAccum.clear();
	//根据service_context判断是CCG还是LTE
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
	

    //一次预处理初始化
    int iRet = m_cPreProc1OL.InitTicket(m_iSourceEventTypeID);
    if (iRet<0)
    {
        m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
        m_strerrmsg += "m_cPreProc1OL.InitTicket fail," + m_strerrmsg;
        return -1;
    }
	
	//配额批价使用
    Record& Rec = m_cPreProc1OL.getRecord();
    //业务信息串导入话单
    iRet = ParseTicket(m_iSourceEventTypeID,m_iRoamFlag,Rec,stOcsReq,m_sOrgTicket,m_cDataExPlain,m_strerrmsg,m_iErrCode);
    if (iRet<0)
    {               
        return -1;         
    }

    
    //一次预处理
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
    
    //二次预处理
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

	//需要在二次预处理之后在保存
	m_cPreProc1OL.SetMinRecord();
    m_cPreProc1OL.SetLteRecord();


	//达量提醒号码
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
    
 
	//取经过二次预处理的SERVID
	CLong lServID = Rec.field(FIELD_ID(SERV_ID)).getLong();
	sChargeInfo.setServID(lServID);

	//批价前删除commit标记为true的记录
	//m_cAccumMng.deleteRec(lServID);
	
	//取LTE的RG 字段
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

	//取资费切换标志
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
	
	//取EVENT_BEGIN_TIME
	CString strEventBeginTime = GetOcsDataValues(OCS_FIELD_EVENT_BEGIN_TIME).c_str();
	CTime tEventBeginTime = strEventBeginTime.toDate();
	
	//用于4g的第一次批价使用
	Record& RecLte1 = m_cPreProc1OL.getLte1Record();

	int iDataOffset = -1;
	m_pSessionSwitch->queryRec(stOcsReq.sSessionId,strPid.c_str(),iDataOffset);
	
	SessionRec sessionrec;
	memset(&sessionrec,0,sizeof(SessionRec));
	if (-1 == iDataOffset)
	{
		//内存中查询不到记录，则资费切换点标志设置成false
		//当作是新的会话来处理
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
		//用LastDealTime记录上一个更新包的EventBeginTime
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
		DEBUG_4("该RG的第一个更新包,session_id =",stOcsReq.sSessionId,",PID=",sChargeInfo.szPID);
	}
	else
	{
		m_pSessionSwitch->readRec(iDataOffset,sessionrec);
	}

	sChargeInfo.setDataOffset(iDataOffset);

	
	
	int iCallAmount1 = tEventBeginTime.getTime() - sessionrec.LastDealTime;
	
	//上报实际使用量
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
		DEBUG_2("带资费切换点标识 资费切换点 = ",CTime(sessionrec.tTariffChangeTime));
		RecLte1.field(FIELD_ID(end_time)).setValue(CTime(sessionrec.tTariffChangeTime));
	}

	//查询已使用的流量卡 单位KB
	CLong    lFluxCardUsed = 0;
	long lCharge = 0;
	//m_pSessionSwitch->queryAllFluxCardUsed(lServID,lFluxCardUsed);
	//g_lFluxCardUsed = lFluxCardUsed;
	//lte内存导入g_vecAccum
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

	//更新时长流量
	m_pSessionSwitch->updateRec(iDataOffset,tEventBeginTime.getTime() - sessionrec.LastDealTime ,lMsccUsuCcOctets);
	//更新费用
	m_pSessionSwitch->updateRecCharge(iDataOffset,lCharge);
	//更新流量卡
	//m_pSessionSwitch->updateFluxCardUsed(iDataOffset,g_lActualPayoutAmount);
	//更新次数
	m_pSessionSwitch->addRecRequestCnt(iDataOffset);
	//更新LastDealTime
	m_pSessionSwitch->updateLastDealTime(iDataOffset,tEventBeginTime.getTime());
	//将批价后的积量导入lte内存中
    //m_cAccumMng.updateRec(lServID,strPid.c_str(),sessionrec.tBeginTime,g_vecTicketAccum);
	//保留清0前的会话费用
	long lBeforeCharge = 0;
	//如果到达资费切换点需要对资费切换点前的下话单


	if (bTariffChangeUsage)
	{
		DEBUG_2("资费切换点提交",sessionrec);

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

		
	} //如果是达到落话单的那些条件 什么按时 按量也落话单
	else
	{	
		if (bLteFlag)
		{
			//单位为字节
			if (sessionrec.lAmount_flux >= m_iIndbFlux*1024*1024 
				|| sessionrec.lAmount_duration >= m_iIndbDuration*60
				|| sessionrec.iRequestCnt >= m_iIndbRequestCnt
				)
			{
				
				DEBUG_2("中间单入库",sessionrec);
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
	
	///////////////////////第二次对资费切换点后的批价
	if (bTariffChangeUsage)
	{
		 //实际上报实际的使用时长 （资费切换后）
		int iCallAmount2 = tEventBeginTime.getTime()-sessionrec.tTariffChangeTime;
		DEBUG_2("iCallAmount2 = ",iCallAmount2);
		
		//实际上报实际的流量(资费切换后) 
		CLong lMsccUsuCcOctets2 = (CLong)atol(GetOcsDataValues(OCS_FIELD_MSCC_USU_CC_TOTAL_OCTETS2).c_str());
		DEBUG_2("MSCC_USU_CC_TOTAL_OCTETS2 = ",lMsccUsuCcOctets2);

		Record& RecLte2 = m_cPreProc1OL.getLte2Record();


		DEBUG_2("第二次批价资费切换点 = ",CTime(sessionrec.tTariffChangeTime));
		
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


		//计算charge + charge2	
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
		//更新LastDealTime
		m_pSessionSwitch->updateLastDealTime(iDataOffset,tEventBeginTime.getTime());
		//将第二次批价产生的积量导入LTE内存中
        //m_cAccumMng.updateRec(lServID,strPid.c_str(),sessionrec.tTariffChangeTime,g_vecTicketAccum);
	}


	//对配额批价
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
    
    //原批价结果算出的计费量类型 和 套餐已赠送的使用量  未经流量卡的当前话单费用 流量卡实际抵扣量
    sChargeInfo.iBillingAmountType = Rec.field(FIELD_ID(BILLING_AMOUNT_TYPE)).getInteger();  

	//查询下所有rg的费用 加上 配额的费用 
	CLong lSessionCharge = 0;
	m_pSessionSwitch->queryProdInstAllCharge(lServID,lSessionCharge);

	DEBUG_2("会话累积的费用 = ",lSessionCharge);
	DEBUG_2("配额批出的费用 = ",lCharge);
	DEBUG_2("该RG落单前费用 = ",lBeforeCharge);

	sChargeInfo.lCharge = lSessionCharge + lCharge + lBeforeCharge;

	//更新包到达资费切换点则下发
	int iTariffTimeChange = 0;
	if (TariffTimeChange(iTariffTimeChange))
	{
		//内存中的资费切换点不等于新的资费切换点则更新
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
		
		DEBUG_2("原价 =",sChargeInfo.lCharge); 
        
        if (sChargeInfo.lCharge!=0   && !checkNoStopUrge(sChargeInfo.lServID))
        {

        	//费用不等于0计算最小预留量 并且反算
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
    	//OCS_FIELD_CCR_MSCC_CUR_INDEX 下标从1开始
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
			//根据sSrvCntxtId取源事件类型
			if (-1 == m_iSourceEventTypeID)
			{
				return -1;
			}
			
          	SwitchSession();
			
            //构造一次预处理格式
            iRet = m_cPreProc1OL.InitTicket(m_iSourceEventTypeID);
            if (iRet<0)
            {
                m_cPreProc1OL.getErrMessage(m_iErrCode,m_strerrmsg);
                m_strerrmsg += "g_cPreProc1OL.InitTicket fail," + m_strerrmsg;
                bSuccess = false; 
                break;
            }

    
            Record& rec = m_cPreProc1OL.getRecord();   
                            
            //将解析的字符串填入原始话单结构体中
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

			//填写PID
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
            	// itnm00080626 20160517 有使用流量没有时长(用户更新包距离终止包时间间隔太长会话记录被清)  
				string strUsuCcTotalOctets = m_cDataExPlain.getDataValues(iUsuCcTotalOctets + iCurIndex);
				if(0 != atol(strUsuCcTotalOctets.c_str()))
            	{
            		// 终止包流量不为0 可是会话记录查询不到 此时 begintime 要倒推回填
            		string strUsuCcTime = m_cDataExPlain.getDataValues(OCS_FIELD_MSCC_USU_CC_TIME);
					sessionrec.tBeginTime= tEventBeginTime.getTime() - atoi(strUsuCcTime.c_str());
					DEBUG_2("sessionrec.tBeginTime =",sessionrec.tBeginTime);
				}else
				{
	                DEBUG_4("会话管理不存在该记录sSessionId =", strNewSession, ",RG=", strPID.c_str());
	                sessionrec.tBeginTime  =  tEventBeginTime.getTime();
	                sessionrec.lAmount_flux = 0;
	                strncpy(m_sOrgTicket.szImsiNbr, "NULLNULLNULLNU4", strlen("NULLNULLNULLNU4") + 1);
				}
            }
			else
			{
				m_pSessionSwitch->readRec(iDataOffset,sessionrec);
			}

			//填写ORG_START_TIME
			iRecLength = rec.field(FIELD_ID(ORG_START_TIME)).getLength();
			string strDate = (const char*)CTime(sessionrec.tBeginTime).format("%Y%m%d%H%M%S");
			strDate += "00";
			strncpy(m_sOrgTicket.szOrgStartTime,strDate.c_str()+2,iRecLength);  
			m_sOrgTicket.szOrgStartTime[iRecLength] ='\0'; 




			//填写ORG_END_TIME
			strDate = (const char*)tEventBeginTime.format("%Y%m%d%H%M%S");
			strDate += "00";
			strncpy(m_sOrgTicket.szOrgEndTime,strDate.c_str()+2,iRecLength);  
			m_sOrgTicket.szOrgEndTime[iRecLength] ='\0'; 

			//填写ORG_CALL_AMOUNT
			m_sOrgTicket.iOrgCallAmount = 10*(tEventBeginTime.getTime() - sessionrec.tBeginTime);
			
			
            //填写流量
            string strUsuCcTotalOctets = m_cDataExPlain.getDataValues(iUsuCcTotalOctets+iCurIndex);
			DEBUG_2("MSCC_USU_CC_TOTAL_OCTETS = ",strUsuCcTotalOctets);
			m_sOrgTicket.lVolumeDownLink = sessionrec.lAmount_flux + atol(strUsuCcTotalOctets.c_str());

			//重填SessionID
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

			//使用量为0时 m_iIndbFlag为1落话单 0不落话单
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
	     	//所有都成功了才提交
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
   
    //总计费时长
    long lCallAmount = 0; 
    //总计费流量
    long lTotalVolume = 0;

	//取配额批价的费用
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
			//lCallAmount 配额时长  lCharge配额时长所算出费用
			//sChargeInfo.lCurEffBalance - (sChargeInfo.lCharge - lCharge) 余额
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
        m_strerrmsg = "iBillingAmountType非流量非时长,iBillingAmountType= ";
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
	
	//取连接内存数据库的主机名
    if (!ini.queryValue("TicketMove","AbmLoginName", m_strLoginName))
	{
	    m_iErrCode = RESULT_CODE_INIT_MEMBASE_FAILED; 
		m_strerrmsg = "get TicketMove, AbmLoginName  failed";
        return false;
	}
    
    cout << "MemBase name =" << m_strLoginName <<endl;
    	
    //内存数据库的连接
    m_pMemConn = new AltibaseConnection;
    int iRet = MemDbConnect2(m_strLoginName, m_strerrmsg, *m_pMemConn);
    if (iRet != 1)
    {
        m_iErrCode = RESULT_CODE_INIT_MEMBASE_FAILED; 
        m_strerrmsg += " MemDbConnect2 fail,连接内存数据库失败，strLoginName:";
        m_strerrmsg += m_strLoginName;
        return false;
    }

	/*
	m_pPublicConn = new AltibaseConnection;
	iRet = MemDbConnect2("public", m_strerrmsg, *m_pPublicConn);
    if (iRet != 1)
    {
        m_iErrCode = RESULT_CODE_INIT_MEMBASE_FAILED; 
        m_strerrmsg += " MemDbConnect2 fail,连接内存数据库失败,public";
 
        return false;
    }
    */
	
    //设置不制动提交
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
        m_strerrmsg = "MemDb错误: ";
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
        m_strerrmsg  = "Prepare TICKET_ONLINE_ORG表时数据库Connection为空!";
        return false;
    }

    try { m_insertcmd.Drop(m_stat); } catch (...) { }
	//try { m_insertOpnLog.Drop(m_stat); } catch (...) { }
    try
    {
       
        //abm取数语句每次500条记录
        //转售主机取转售的表
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
            m_strerrmsg = "Insert fail,插入TICKET_ONLINE_ORG表关闭游标失败! MemDb错误: " + m_strerrmsg;
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
        m_strerrmsg = "MemDb错误: ";
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
        m_strerrmsg = "MemDb回滚失败! MemDb错误: ";
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
        m_strerrmsg = "MemDb提交失败! MemDb错误: ";
        m_strerrmsg += st.err_msg;
        return false;
	}
	return true;
}


