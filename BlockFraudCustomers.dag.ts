import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getMultiBody, getOut, getValueByKey } = dao;

@Dag({ method: "POST", url: "v1/block-fraud-customers" })
class BlockFraudCustomers {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: 251, y: -17 } })
  @Relation(r => dao.isSuccess(), 'StaticConfiguration')
  async AppConfigurations() {
    const script = {

        execute: () => {

            const appVersion = "2.1";
            logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`)

            return {
                body:
                {
                    APP_VERSION: appVersion
                }
            };

        }
    }
  }

  @Script({ pos: { x: 485, y: -30 } })
  @Relation(r => dao.isSuccess(), 'CreateHeadersForCustomerLookup')
  @Relation(r => dao.hasError(), 'error')
  async StaticConfiguration() {
    const script = {

        execute: async () => {
            const body = getApiRequest("Trigger")?.body
            const eventName = body?.attributes?.eventName
            const customeraddEmailDetails = await JSON.parse(await getValueByKey("FRAUD_CUSTOMER_EMAIL_CONFIG"))
             const transactionAddEmailDetails = await JSON.parse(await getValueByKey("FRAUD_TRANSACTION_ADDED_EMAIL_"))
            const pnrAlert = (await getValueByKey("PNR_NOTIFICATION_THRESHOLD"))
            const updateStatusPayload = await JSON.parse(await getValueByKey("FRAUD_CUSTOMER_STATUS_UPDATE"))
            const emailDetails= eventName === "transactionAdded" ? transactionAddEmailDetails : customeraddEmailDetails
            const literals = {
                "customerAddedEvent": "customerAdded",
                "pointsRedeemedEvent": "pointsRedeemed",
                "transactionAddedEvent": "transactionAdded",
                "source": "INSTORE",
                "identifierName": "externalId",
                "emailIdentifierType": "email",
                "format": "json",
                "updateCustomerStatusApiBody": {
                    "label": updateStatusPayload.label,
                    "reason": updateStatusPayload.reason
                },
                "to":emailDetails.to,
                "cc":emailDetails.cc,
                "from": emailDetails.from,
                "pnrAlert": Number(pnrAlert),
                "customerAddedSubject": "Fraudulent Alert on Customer Add",
                "pointsRedeemedSubject": "Fraudulent Alert on Points Redemption",
                "transcationAddedSubject": "Multiple PNRs claimed for same departure date",
                "pointRedeemedSubject": "Customer Redemption"

            }

            return {
                body: literals
            };

        }
    }
  }

  @Script({ pos: { x: 730, y: -21 } })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.eventName !== dao.getBody("StaticConfiguration")?.body?.transactionAddedEvent), 'GetCustomerDetails')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.eventName === dao.getBody("StaticConfiguration")?.body?.transactionAddedEvent), 'checkMongo')
  async CreateHeadersForCustomerLookup() {
    const script = {

        execute: () => {

            const body=getApiRequest("Trigger")?.body
            const literals=getBody("StaticConfiguration")?.body
            const customerId=body?.attributes?.data?.customerIdentifiers?.customerId
            const eventName=body?.attributes?.eventName
            const headers=getEffectiveHeaders()
            const queryParams={
                "source":literals.source
            }
            const pathParams={
                "customerId":customerId
            }
            return {
                headers,
                queryParams,
                pathParams,
                eventName
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 1009, y: 35 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().errors?.length), 'GetCustomerErrorHandler')
  @Relation(r => dao.isSuccess() && !(dao.getBody().errors?.length) && (dao.getBody("CreateHeadersForCustomerLookup")?.eventName === dao.getBody("StaticConfiguration")?.body?.customerAddedEvent), 'GetMongo')
  @Relation(r => dao.isSuccess() && !(dao.getBody().errors?.length) && (dao.getBody("CreateHeadersForCustomerLookup")?.eventName === dao.getBody("StaticConfiguration")?.body?.pointsRedeemedEvent), 'ValidateCustomerRegisteredDate')
  @Relation(r => dao.hasError(), 'HasErrorHandler')
  async GetCustomerDetails() {
  return {
        url: `https://apac.api.capillarytech.com/v2/customers/{customerId}`,
        method: `GET`,
      };
  }

  @Script({ pos: { x: 834.4323749557386, y: -208.74902937761237 } })
  async HasErrorHandler() {
    const script = {

        execute: () => {

            const errors = getBody("GetCustomerDetails")
            if (errors.code >= 500 && errors.code <= 599) {
                return {
                    http: {
                        res: {
                            status: 500,
                            json: errors,
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            return {
                http: {
                    res: {
                        "json": errors,
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 1295.091281944766, y: -158.63568096983266 } })
  async GetCustomerErrorHandler() {
    const script = {

        execute: () => {
            const response=getBody("GetCustomerDetails")

            //Write your code here.
            return {
                http:{
                    res:{
                        "json":response,
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }

            };

        }
    }
  }

  @Script({ pos: { x: 1544.579413239015, y: -38.703602857250246 } })
  @Relation(r => dao.isSuccess(), 'CreateHeadersForUpdateCustomerStatusApi')
  async ValidateEmail() {
    const script = {

        execute: () => {
            let fraudFlag=false
            let literals=getBody("StaticConfiguration")?.body
            const getMongoResponse = getMultiBody("GetMongo")[0]?.emailDomains
            logger.info(`Email Domains: ${JSON.stringify(getMongoResponse)}`)

            const identifiers=getBody("GetCustomerDetails")?.profiles[0]?.identifiers
            const email= identifiers.filter((data)=>{
                if(data?.type === literals.emailIdentifierType){
                    return data
                }
            })[0]?.value
            logger.info(`Customer Email: ${JSON.stringify(email)}`)
            const emailDomain=email?.split("@")[1]
            if (getMongoResponse && getMongoResponse.length > 0) {
                if (getMongoResponse.includes(emailDomain)) {
                    fraudFlag=true
                    return{
                        fraudFlag
                    }
                }
            }

            return {
                http: {
                    res: {
                        "json": getBody("GetCustomerDetails"),
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            }


        }
    }
  }

  @Script({ pos: { x: 1403.734894455423, y: 102.11847586944947 } })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.fraudFlag=== true), 'CreateHeadersForUpdateCustomerStatusApi')
  @Relation(r => dao.isSuccess(), 'CreatePayloadForEmailCommunicationApi')
  async ValidateCustomerRegisteredDate() {
    const script = {

        execute: () => {
            const body = getApiRequest("Trigger")?.body
            const epochTime = body?.attributes?.createdAt
            let fraudFlag = false
            const pointsRedeemedEventDate = new Date(epochTime).toISOString().split("T")[0]
            logger.info(`pointsRedeemedDate: ${JSON.stringify(pointsRedeemedEventDate)}`)
            const getCustomerResponse = getBody("GetCustomerDetails")?.profiles[0]?.attribution?.createDate
            logger.info(`customerCreatedDate: ${JSON.stringify(getCustomerResponse)}`)

            const customerCreatedDate = getCustomerResponse.split("T")[0]

            const customerRegisteredDate = new Date(customerCreatedDate + "T00:00:00Z");
            const pointsRedeemedDate = new Date(pointsRedeemedEventDate + "T00:00:00Z");
            const diffDays = (pointsRedeemedDate - customerRegisteredDate) / (1000 * 60 * 60 * 24);

            if (diffDays >= 0 && diffDays <= 2) {
                fraudFlag = true
                return {
                    customerRegisteredDate,
                    pointsRedeemedDate,
                    fraudFlag
                };
            }
            return{
                fraudFlag
            }






        }
    }
  }

  @Script({ pos: { x: 1724.2266825710506, y: 74.4566412473468 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'UpdateCustomerStatusApi')
  async CreateHeadersForUpdateCustomerStatusApi() {
    const script = {

        execute: () => {

            const getCustomerResponse=getBody("GetCustomerDetails")
            const literals = getBody("StaticConfiguration")?.body
            const identifiers=getCustomerResponse?.profiles[0]?.identifiers
            const externalId=identifiers.filter((data)=>{
                if(data?.type === literals.identifierName){
                    return data
                }
            })[0]?.value
            const headers = getEffectiveHeaders()
            const queryParams = {
                "source": literals.source,
                "identifierName": literals.identifierName,
                "identifierValue": externalId
            }
            const body = {
                "label": literals["updateCustomerStatusApiBody"]?.label,
                "reason": literals["updateCustomerStatusApiBody"]?.reason
            }
            return {
                headers,
                queryParams,
                body:JSON.stringify(body)

            }

        }
    }
  }

  @ApiRequest({ pos: { x: 2037.208028627891, y: -56.38713414477343 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().errors?.length), 'CreatePayloadForCommunicationApi')
  @Relation(r => dao.isSuccess() && !(dao.getBody().errors?.length), 'CreatePayloadForCommunicationApi')
  @Relation(r => dao.hasError(), 'UpdateStatusHasErrorHandler')
  async UpdateCustomerStatusApi() {
  return {
        url: `https://apac.api.capillarytech.com/v2/customers/lookup/status`,
        method: `PUT`,
      };
  }

  @Script({ pos: { x: 2395.2844760934017, y: 164.52099270158493 } })
  async UpdateStatusHasErrorHandler() {
    const script = {

        execute: () => {

            const errors = getBody("UpdateCustomerStatusApi")
            if (errors.code >= 500 && errors.code <= 599) {
                return {
                    http: {
                        res: {
                            status: 500,
                            json: errors,
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            return {
                http: {
                    res: {
                        "json": errors,
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 2411.7689172273117, y: 24.660863250018622 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'EmailCommunicationApi')
  async CreatePayloadForCommunicationApi() {
    const script = {

        execute: () => {
            const requestBody = getApiRequest("Trigger")?.body
            const literals = getBody("StaticConfiguration")?.body
            const getCustomerResponse = getBody("GetCustomerDetails")?.profiles[0]
            const identifiers = getCustomerResponse?.identifiers
            const externalId = identifiers.filter((data) => {
                if (data?.type === literals.identifierName) {
                    return data
                }
            })[0]?.value
            const email = identifiers.filter((data) => {
                if (data?.type === literals.emailIdentifierType) {
                    return data
                }
            })[0]?.value

            let payload = {}
            let subject=""
            const queryParams = {
                "format": literals.format
            }
            const headers = getEffectiveHeaders()
            const eventName = getBody("CreateHeadersForCustomerLookup")?.eventName
            if (eventName === literals.customerAddedEvent) {
                payload = {
                    "FFN": externalId,
                    "firstName": getCustomerResponse?.firstName,
                    "lastName": getCustomerResponse?.lastName,
                    "registrationDate": getCustomerResponse?.attribution?.createDate,
                    "customerEmailId":email
                }
                subject=literals.customerAddedSubject

            }
            else if (eventName === literals.pointsRedeemedEvent) {
                payload = {
                    "FFN": externalId,
                    "firstName": getCustomerResponse?.firstName,
                    "lastName": getCustomerResponse?.lastName,
                    "registrationDate": getCustomerResponse?.attribution?.createDate,
                    "pointsRedeemed": requestBody?.attributes?.data?.totalPointsRedeemed,
                    "billNumber": requestBody?.attributes?.data?.redemptionBillNumber
                }
                subject=literals.pointsRedeemedSubject
            }

            const body = {
                "root": {
                    "email": [
                        {
                            "to": literals.to,
                            "cc": literals.cc,
                            "from": literals.from,
                            "subject": subject,
                            "body": payload
                        }
                    ]

                }

            }
            return {
                headers,
                queryParams,
                body: JSON.stringify(body)
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 2728.1956238901325, y: -82.63198578744971 } })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.response?.status?.success === true) &&  (dao.getBody("CreateHeadersForCustomerLookup")?.eventName === dao.getBody("StaticConfiguration")?.body?.customerAddedEvent), 'FinalSuccessResponse')
  @Relation(r => dao.hasError(), 'EmailCommunicationHasErrorHandler')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.response?.status?.success === true) && (dao.getBody("CreateHeadersForCustomerLookup")?.eventName === dao.getBody("StaticConfiguration")?.body?.pointsRedeemedEvent), 'MergedResponseForPoinstRedemption')
  async EmailCommunicationApi() {
  return {
        url: `https://apac.api.capillarytech.com/v1.1/communications/email`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 3511.9272600169315, y: 92.49044003388855 } })
  async EmailCommunicationHasErrorHandler() {
    const script = {

        execute: () => {
            const errors = getBody("EmailCommunicationApi")
            if (errors.code >= 500 && errors.code <= 599) {
                return {
                    http: {
                        res: {
                            status: 500,
                            json: errors,
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            return {
                http: {
                    res: {
                        "json": errors,
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };
        }
    }
  }

  @Script({ pos: { x: 3514.683381307999, y: -80.81578667508086 } })
  async FinalSuccessResponse() {
    const script = {

        execute: () => {

            //Write your code here.
            return {
                http: {
                    res: {
                        status: 200,
                        "json": getBody("EmailCommunicationApi"),
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }

                    }
                }
            };

        }
    }
  }

  @GetMongo({ pos: { x: 1284, y: -17 } })
  @Relation(r => dao.isSuccess(), 'ValidateEmail')
  async GetMongo() {
  return {
        collectionName: `emailDomainBlacklist`,
        query: `{}`,
        sort: `{}`,
      };
  }

  @Script({ pos: { x: 2355.7348944554233, y: 346.11847586944947 } })
  @Relation(r => dao.isSuccess(), 'CommunicationEmailApi')
  async CreatePayloadForEmailCommunicationApi() {
    const script = {

        execute: () => {

            const requestBody = getApiRequest("Trigger")?.body
            const literals = getBody("StaticConfiguration")?.body
            const getCustomerResponse = getBody("GetCustomerDetails")?.profiles[0]
            const identifiers = getCustomerResponse?.identifiers
            const externalId = identifiers.filter((data) => {
                if (data?.type === literals.identifierName) {
                    return data
                }
            })[0]?.value

            const queryParams = {
                "format": literals.format
            }
            const headers = getEffectiveHeaders()

            const body = {
                "root": {
                    "email": [
                        {
                            "to": literals.to,
                            "cc": literals.cc,
                            "from": literals.from,
                            "subject": literals.pointRedeemedSubject,
                            "body": {
                                "FFN": externalId,
                                "firstName": getCustomerResponse?.firstName,
                                "lastName": getCustomerResponse?.lastName,
                                "registrationDate": getCustomerResponse?.attribution?.createDate,
                                "pointsRedeemed": requestBody?.attributes?.data?.totalPointsRedeemed,
                                "billNumber": requestBody?.attributes?.data?.redemptionBillNumber
                            }
                        }
                    ]

                }

            }
            return {
                headers,
                queryParams,
                body: JSON.stringify(body)
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 2701.7348944554233, y: 346.11847586944947 } })
  @Relation(r => dao.hasError(), 'ErrorHandler')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.response?.status?.success === true) && (dao.getBody("ValidateCustomerRegisteredDate")?.fraudFlag === true), 'MergedResponseForPoinstRedemption')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.response?.status?.success === true) && (dao.getBody("ValidateCustomerRegisteredDate")?.fraudFlag === false), 'EmailCommunicationSuccessResponse')
  async CommunicationEmailApi() {
  return {
        url: `https://apac.api.capillarytech.com/v1.1/communications/email`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 3153.7348944554233, y: 398.1184758694494 } })
  async ErrorHandler() {
    const script = {

        execute: () => {

            const errors = getBody("CommunicationEmailApi")
            if (errors.code >= 500 && errors.code <= 599) {
                return {
                    http: {
                        res: {
                            status: 500,
                            json: errors,
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            return {
                http: {
                    res: {
                        "json": errors,
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 3180.1956238901325, y: 271.3680142125503 } })
  async MergedResponseForPoinstRedemption() {
    const script = {

        execute: () => {

            //Write your code here.
            return {
                http:{
                    res:{
                        "json":"Successfully sent both the Emails",
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }

            };

        }
    }
  }

  @Script({ pos: { x: 3176.7348944554233, y: 639.1184758694494 } })
  @ExecutionStrategy('or')
  async EmailCommunicationSuccessResponse() {
    const script = {

        execute: () => {
            return {
                http:{
                    res:{
                        "json":getBody("CommunicationEmailApi"),
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }

            };

        }
    }
  }

  @Script({ pos: { x: 629.5737779309233, y: 645.6092545613338 } })
  @Relation(r => dao.isSuccess( ), 'checkDataInMongo')
  async checkMongo() {
    const script = {

        execute: () => {
            const body = getApiRequest("Trigger")?.body?.attributes?.data
            const literals = getBody("StaticConfiguration")?.body
            const customerId = body?.customerIdentifiers?.instore?.externalId
            const departureDate = body?.extendedFields?.find(item => item.key === "departure_date")?.value;
            return {
                body: {
                    query:  { "departureDate": departureDate.trim(),
                        "FFN": customerId.trim()}

                }
            }

        }
    }
  }

  @GetMongo({ pos: { x: 886.6635466886847, y: 691.1033417895989 } })
  @Relation(r => dao.isSuccess(), 'preparePNRSummary')
  async checkDataInMongo() {
  return {
        collectionName: `UtilisedPNR`,
        query: r => getBody().body.query,
        sort: `{"_id" : -1}`,
      };
  }

  @Script({ pos: { x: 1211.3851917275094, y: 695.8576866032556 } })
  @Relation(r => dao.isSuccess()   && dao.getBody()?.uniquePNRs.length >= Number( dao.getBody()?.pnrAlert), 'createPayload')
  async preparePNRSummary() {
    const script = {
      execute: () => {
        const res = getOut() || [];
        const pnrAlert = getBody("StaticConfiguration")?.body?.pnrAlert;

        const uniquePNRsSet = new Set();
        const pnrOriginList = [];

        let firstRecord = null;

        for (const record of res) {
          // Track first record for emailBody
          if (!firstRecord) firstRecord = record;

          // Track unique PNRs
          uniquePNRsSet.add(record.PNR);

          // Build pnrOriginList in the same pass
          pnrOriginList.push({
            PNR: record.PNR,
            origin: record.origin,
            FFN: record.FFN,
            destination: record.destination,
            departureDate: record.departureDate,
            arrivalDate: record.arrivalDate,
            departureTime: record.departureTime || 'N/A',
            arrivalTime: record.arrivalTime || 'N/A'
          });
        }

        const uniquePNRs = Array.from(uniquePNRsSet);
        if ((uniquePNRs.length <= Number(1))) {
          return {
            http: {
              res: {
                "json": {
                  status: 200,
                  data: "No duplicate PNR Transcation found",
                },
                "headers": {
                  "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                }
              }
            }
          };
        }
        const emailBody = firstRecord
          ? {
            FFN: firstRecord.FFN,
            Fname: firstRecord.firstName,
            Lname: firstRecord.lastName,
            PNR: firstRecord.PNR,
            DepartureDate: firstRecord.departureDate,
            ArrivalDate: firstRecord.arrivalDate
          }
          : {};

        return {
          emailBody,
          uniquePNRs,
          pnrAlert,
          pnrOriginList
        };
      }
    };
  }

  @Script({ pos: { x: 1509.479484829216, y: 770.0372241187781 } })
  @Relation(r => dao.isSuccess(), 'CommunicationEmail')
  async createPayload() {
    const script = {

        execute: () => {
            const literals = getBody("StaticConfiguration")?.body
            const getPNRResponse = getBody("preparePNRSummary")
            const queryParams = {
                "format": literals.format
            }
            const headers = getEffectiveHeaders()
            const body = {
                "root": {
                    "email": [
                        {
                            "to": literals.to,
                            "cc":  literals.cc,
                            "from": literals.from,
                            "subject": literals?.transcationAddedSubject,
                            "body":getPNRResponse?.emailBody
                        }
                    ]

                }

            }
            return {
                headers,
                queryParams,
                body: JSON.stringify(body)
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 1790, y: 741 } })
  @Relation(r => dao.isSuccess()&& (dao.getBody()?.response?.status?.success == true), 'CreateHeadersForUpdateCustomerStatusApiForEmail')
  @Relation(r => dao.hasError(), 'CreateHeadersForUpdateCustomerStatusApiForEmail')
  async CommunicationEmail() {
  return {
        url: `https://apac.api.capillarytech.com/v1.1/communications/email`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 2620, y: 945 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'emailtosendOriginDetails')
  async EmailSuccessResponse() {
    const script = {
      execute: () => {
        const literals = getBody("StaticConfiguration")?.body;
        const getPNRResponse = getBody("preparePNRSummary");
        const uniquePNRs = getPNRResponse?.uniquePNRs || [];
        const pnrAlert = getBody("StaticConfiguration")?.body?.pnrAlert;
        const pnrDetails = getPNRResponse?.pnrOriginList || [];
        const headers = getEffectiveHeaders()
        // Group by origin + departureDate, check for same origin with different destinations
        const originGroups = {};

        const result_origin = [];

        // Group by destination + arrivalDate, check for same destination with different origins
        const destinationGroups = {};
        const result_destination = [];

        // Step 1: Group by destination + arrivalDate
        pnrDetails.forEach(item => {
          const key = `${item.destination}_${item.arrivalDate}`;

          if (!destinationGroups[key]) {
            destinationGroups[key] = {
              origins: new Set(),
              originMap: {} // To keep distinct entry per origin
            };
          }

          destinationGroups[key].origins.add(item.origin);

          // Store only one entry per unique origin
          if (!destinationGroups[key].originMap[item.origin]) {
            destinationGroups[key].originMap[item.origin] = item;
          }
        });

        // Step 2: For each group, if origins > 1, collect unique entries
        for (const key in destinationGroups) {
          const group = destinationGroups[key];

          if (group.origins.size > 1) {
            result_destination.push(...Object.values(group.originMap));
          }
        }

        // Step 1: Group by origin + departureDate
        pnrDetails.forEach(item => {
          const key = `${item.origin}_${item.departureDate}`;

          if (!originGroups[key]) {
            originGroups[key] = {
              destinations: new Set(),
              destinationMap: {}, // To keep distinct entry per destination
            };
          }

          originGroups[key].destinations.add(item.destination);

          // Store only one entry per unique destination
          if (!originGroups[key].destinationMap[item.destination]) {
            originGroups[key].destinationMap[item.destination] = item;
          }
        });

        // Step 2: For each group, if destinations > 1, collect unique entries
        for (const key in originGroups) {
          const group = originGroups[key];

          if (group.destinations.size > 1) {
            result_origin.push(...Object.values(group.destinationMap));
          }
        }

        function filterOverlappingPNRs(pnrDetails) {
          const result = [];
          const grouped = {};

          // Group by FFN
          for (const item of pnrDetails) {
            if (item.departureDate && item.arrivalDate && item.FFN) {
              if (!grouped[item.FFN]) grouped[item.FFN] = [];
              grouped[item.FFN].push(item);
            }
          }

          // Process each FFN group
          for (const group of Object.values(grouped)) {
            const flights = group
              .map(f => ({
                ...f,
                dep: new Date(`${f.departureDate}T${f.departureTime}`),
                arr: new Date(`${f.arrivalDate}T${f.arrivalTime}`)
              }))
              .sort((a, b) => a.dep - b.dep);

            for (let i = 0; i < flights.length; i++) {
              for (let j = i + 1; j < flights.length; j++) {
                const f1 = flights[i];
                const f2 = flights[j];
                if (f1.PNR === f2.PNR) continue;
                if (f1.dep <= f2.arr && f2.dep <= f1.arr) {
                  if (!result.find(r => r.PNR === f1.PNR)) result.push(f1);
                  if (!result.find(r => r.PNR === f2.PNR)) result.push(f2);
                }
              }
            }
          }

          return result;
        }

        const result_departure_time = filterOverlappingPNRs(pnrDetails);
        // let result_departure_time = filterOverlappingPNRs(pnrDetails)
        const emailBody = getPNRResponse?.emailBody || {};
        const queryParams = {
          format: literals.format
        };

        const emails = [];
        const generateEmailTable = (rows, type) => {
          const tableRows = rows.map(row => `
              <tr>
                <td>${emailBody.FFN || ''}</td>
                <td>${row.PNR}</td>
                <td>${row.origin}</td>
                <td>${row.destination}</td>
                <td>${row.departureDate || ''}</td>
                <td>${row.arrivalDate || ''}</td>
                <td>${row.departureTime || ''}</td>
                <td>${row.arrivalTime || ''}</td>
              </tr>
            `).join('');

          // Final HTML email content
          const htmlEmail = `
              <html>
                <head>
                  <style>
                    table {
                      width: 100%;
                      border-collapse: collapse;
                      font-family: Arial, sans-serif;
                    }
                    th, td {
                      border: 1px solid #dddddd;
                      text-align: left;
                      padding: 8px;
                    }
                    th {
                      background-color: #f2f2f2;
                    }
                    tr:nth-child(even) {
                      background-color: #f9f9f9;
                    }
                  </style>
                </head>
                <body>
                  <p>Dear Team,</p>
                  <p>Please find below the PNR alert details for <strong>${type}</strong>:</p>
                  <table>
                    <thead>
                      <tr>
                        <th>FFN</th>
                        <th>PNR</th>
                        <th>Origin</th>
                        <th>Destination</th>
                        <th>Departure Date</th>
                        <th>Arrival Date</th>
                        <th>Departure Time</th>
                        <th>Arrival Time</th>
                      </tr>
                    </thead>
                    <tbody>
                      ${tableRows}
                    </tbody>
                  </table>
                  <p>Regards,<br/>System</p>
                </body>
              </html>
            `;
          return htmlEmail;
        }
        // Helper to get unique entries by PNR
        const uniqueByPNR = (list) => {
          const map = new Map();
          for (const item of list) {
            if (!map.has(item.PNR)) map.set(item.PNR, item);
          }
          return Array.from(map.values());
        };

        // Deduplicate before sending email
        const unique_origin = uniqueByPNR(result_origin);
        const unique_destination = uniqueByPNR(result_destination);
        const unique_departure_time = uniqueByPNR(result_departure_time);
        if ((unique_origin.length < Number(pnrAlert)) && (unique_departure_time.length < Number(pnrAlert)) && (unique_departure_time.length < Number(1))) {
          return {
            http: {
              res: {
                "json": {
                  status: 200,
                  data: "No duplicate PNR Transcation found",
                },
                "headers": {
                  "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                }
              }
            }
          };
        }

        // Add origin-based email
        if (unique_origin.length >= Number(pnrAlert)) {
          emails.push({
            to: literals.to,
            cc: literals.cc,
            from: literals.from,
            subject: "Report: Unique PNRs with Same Origin & Different Destinations on Same Departure Date",
            body: generateEmailTable(unique_origin, "Same Origin & Different Destinations on Same Departure Date"),
          });
        }

        // Add destination-based email
        if (unique_destination.length >= Number(pnrAlert)) {
          emails.push({
            to: literals.to,
            cc: literals.cc,
            from: literals.from,
            subject: "Report: Unique PNRs with Same Destination & Different Origins on Same Arrival Date",
            body: generateEmailTable(unique_destination, "Same Destination & Different Origins on Same Arrival Date"),
          });
        }

        // Add overlapping departure/arrival times email
        if (unique_departure_time.length >= 1) {
          emails.push({
            to: literals.to,
            cc: literals.cc,
            from: literals.from,
            subject: "Report: Unique PNRs with overlapping Departure and Arrival Time",
            body: generateEmailTable(unique_departure_time, "Overlapping Departure Times"),
          });
        }

        const body = {
          root: {
            email: emails
          }
        };

        return {
          headers,
          queryParams,
          body: JSON.stringify(body),
          timeOverlapping: result_departure_time.length
        };
      }
    };
  }

  @Script({ pos: { x: 3135, y: 966 } })
  @ExecutionStrategy('or')
  async EmailErrorHanlding() {
    const script = {

        execute: () => {

            const errors = getBody("CommunicationEmailApi")
            if (errors.code >= 500 && errors.code <= 599) {
                return {
                    http: {
                        res: {
                            status: 500,
                            json: errors,
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            return {
                http: {
                    res: {
                        "json": errors,
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 440, y: 242 } })
  async error() {
    const script = {

        execute: () => {

            //Write your code here.
            return {
                res : getOut()
            };

        }
    }
  }

  @Script({ pos: { x: 3624.3634765624984, y: 957 } })
  @ExecutionStrategy('or')
  async success() {
    const script = {

        execute: () => {
            return {
                http:{
                    res:{
                        "json":getBody("emailtosendOriginDetails"),
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }

            };

        }
    }
  }

  @ApiRequest({ pos: { x: 2918, y: 861 } })
  @Relation(r => dao.isSuccess() && dao.getBody('CreateHeadersForUpdateCustomerStatusApiForEmail').uniquePNRsLength >= 3, 'success')
  @Relation(r => dao.hasError(), 'EmailErrorHanlding')
  @Relation(r => dao.isSuccess() && dao.getBody('CreateHeadersForUpdateCustomerStatusApiForEmail').uniquePNRsLength < 3 && dao.getBody('EmailSuccessResponse').timeOverlapping >=2, 'updateCustomerStatus')
  async emailtosendOriginDetails() {
  return {
        url: `https://apac.api.capillarytech.com/v1.1/communications/email`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 2130, y: 807 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess() && dao.getBody()?.uniquePNRsLength >= 3, 'UpdateCustomerStatusApiForEmail')
  @Relation(r => dao.isSuccess() && dao.getBody()?.uniquePNRsLength < 3, 'EmailSuccessResponse')
  async CreateHeadersForUpdateCustomerStatusApiForEmail() {
    const script = {

        execute: () => {
            const req = getApiRequest("Trigger")?.body?.attributes?.data
            const literals = getBody("StaticConfiguration")?.body
            const externalId = req?.customerIdentifiers?.instore?.externalId
            const getPNRResponse = getBody("preparePNRSummary")
            const uniquePNRs = getPNRResponse?.uniquePNRs || [];
            let uniquePNRsLength = uniquePNRs.length;
            const pnrAlert = getBody("StaticConfiguration")?.body?.pnrAlert
            const headers = getEffectiveHeaders()
            if (uniquePNRsLength < Number(1)) {
                return {
                    http: {
                        res: {
                            "json": getBody("CommunicationEmail"),
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }

                };
            }
            const queryParams = {
                "source": literals.source,
                "identifierName": literals.identifierName,
                "identifierValue": externalId
            }
            const body = {
                "label": literals["updateCustomerStatusApiBody"]?.label,
                "reason": literals["updateCustomerStatusApiBody"]?.reason
            }
            return {
                headers,
                queryParams,
                body: JSON.stringify(body),
                uniquePNRsLength
            }

        }
    }
  }

  @ApiRequest({ pos: { x: 2382.430250824344, y: 836.5224474295605 } })
  @Relation(r => dao.isSuccess(), 'EmailSuccessResponse')
  async UpdateCustomerStatusApiForEmail() {
  return {
        url: `https://apac.api.capillarytech.com/v2/customers/lookup/status`,
        method: `PUT`,
      };
  }

  @ApiRequest({ pos: { x: 3410.8416906539996, y: 1108.4401703090552 } })
  @Relation(r => dao.isSuccess(), 'success')
  async updateCustomerStatusLookup() {
  return {
        url: `https://apac.api.capillarytech.com/v2/customers/lookup/status`,
        method: `PUT`,
      };
  }

  @Script({ pos: { x: 3174.8416906539996, y: 1092.4401703090552 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'updateCustomerStatusLookup')
  async updateCustomerStatus() {
    const script = {

        execute: () => {
            const req = getApiRequest("Trigger")?.body?.attributes?.data
            const literals = getBody("StaticConfiguration")?.body
            const externalId = req?.customerIdentifiers?.instore?.externalId
            const headers = getEffectiveHeaders()
            const queryParams = {
                "source": literals.source,
                "identifierName": literals.identifierName,
                "identifierValue": externalId
            }
            const body = {
                "label": literals["updateCustomerStatusApiBody"]?.label,
                "reason": literals["updateCustomerStatusApiBody"]?.reason
            }
            return {
                headers,
                queryParams,
                body: JSON.stringify(body),
            }

        }
    }
  }
}
