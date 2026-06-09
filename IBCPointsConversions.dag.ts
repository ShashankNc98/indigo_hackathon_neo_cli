import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getValueByKey } = dao;

@Dag({ method: "POST", url: "v1/points-conversion" })
class IBCPointsConversions {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: -1209, y: 93 } })
  @Relation(r => dao.isSuccess(), 'StaticConfiguration')
  async AppConfigurations() {
    const script = {

        execute: () => {
            const appVersion = "1.3";
            logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`)
            const developer="Adarsh"
            const branch="PSV-22248"   
            const trigger = "v1/points-conversion" 
            const requestBody = getApiRequest()?.body;
            const ffn = requestBody?.FFN
            const isgRequestId = `${trigger}_${ffn}`;
            logger.info(`IsgRequestId : ${JSON.stringify(isgRequestId)}`);

            return {
                body:
                {
                    APP_VERSION: appVersion
                }
            };

        }
    }
  }

  @Script({ pos: { x: 16, y: 141 } })
  @Relation(r => dao.isSuccess(), 'GetOrgEntitiesApi')
  async ValidateBodyAndHeaders() {
    const script = {

        execute: () => {
            const requestHeaders = getApiRequest("Trigger")?.headers
            const response = getBody()
            if (response?.code != 200 || response?.code != "200") {
                return {
                    http: {
                        res: {
                            status: 200,
                            "json": {
                                "response": {
                                    "partner_response": {},
                                    "responses": {},
                                    "status": {
                                        "item_status": {
                                            "code": response?.code,
                                            "message": response?.message,
                                            "success": false
                                        }
                                    }
                                }
                            },
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }
            const body = getApiRequest("Trigger")?.body
            const literals = getBody("StaticConfiguration")?.body

            if (!requestHeaders["x-cap-api-attribution-entity-type"]) {
                return responseFunction(literals?.errorCodeAndMessage["xCapApiAttributionEntityType"].code, literals?.errorCodeAndMessage["xCapApiAttributionEntityType"].message, literals?.status)
            }

            if (!requestHeaders["x-cap-api-attribution-entity-code"]) {
                return responseFunction(literals?.errorCodeAndMessage["xCapApiAttributionEntityCode"].code, literals?.errorCodeAndMessage["xCapApiAttributionEntityCode"].message, literals?.status)
            }

            if (!body["PARTNER_CODE"]) {
                return responseFunction(literals?.errorCodeAndMessage["partnerCode"].code, literals?.errorCodeAndMessage["partnerCode"].message, literals?.status)
            }

            if (!body["POINTS_CONVERSION"]) {
                return responseFunction(literals?.errorCodeAndMessage["pointsConversion"].code, literals?.errorCodeAndMessage["pointsConversion"].message, literals?.status)
            }

            if (!body["PARTNER_CUSTOMER_ID"]) {
                return responseFunction(literals?.errorCodeAndMessage["partnerCustomerId"].code, literals?.errorCodeAndMessage["partnerCustomerId"].message, literals?.status)
            }

            if (!body["FFN"]) {
                return responseFunction(literals?.errorCodeAndMessage["ffn"].code, literals?.errorCodeAndMessage["ffn"].message, literals?.status)
            }

            const headers = {
                "X-CAP-API-OAUTH-TOKEN": requestHeaders["X-CAP-API-OAUTH-TOKEN"] || requestHeaders["x-cap-api-oauth-token"],
                "Content-Type": requestHeaders["Content-Type"] || requestHeaders["content-type"],
                "X-CAP-CLIENT-SIGNATURE": literals.xCapClientSignature,
                "Accept": literals.accept
            }
            const queryParams = {
                "code": body["PARTNER_CODE"]
            }
            const key = body["PARTNER_CODE"]
            //Write your code here.
            return {
                headers,
                queryParams,
                payload: body,
                key
            };

        }


    }

    function responseFunction(code, message, success) {
        return {
            http: {
                res: {
                    status: 200,
                    "json": {
                        "response": {
                            "partner_response": {},
                            "responses": {},
                            "status": {
                                "item_status": {
                                    "code": code,
                                    "message": message,
                                    "success": false
                                }
                            }
                        }
                    },
                    "headers": {
                        "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                    }
                }
            }
        }
    }
  }

  @ApiRequest({ pos: { x: 343, y: 26 } })
  @Cachable({ cachable: true, key: r => dao.getBody().key, ttl: 1800 })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.response?.status?.success === "false"), 'GetOrgEntitiesFailureResponse')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.response?.status?.success === "true"), 'ParsingRequiredFields')
  @Relation(r => dao.hasError(), 'GetOrgEntitiesInternalServerError')
  async GetOrgEntitiesApi() {
  return {
        url: `https://apac.api.capillarytech.com/v1.1/store/get`,
        method: `GET`,
      };
  }

  @Script({ pos: { x: 603, y: 25 } })
  async GetOrgEntitiesFailureResponse() {
    const script = {

        execute: () => {
            const item_status = getBody("GetOrgEntitiesApi")?.response?.stores?.store[0]?.item_status
            return {
                http: {
                    res: {
                        status: 200,
                        "json": {
                            "response": {
                                "partner_response": {},
                                "responses": {},
                                "status": {
                                    "item_status": {
                                        "success": item_status?.success === "false"? false : Boolean(item_status?.success),
                                        "code": item_status?.code,
                                        "message": item_status?.message
                                    }
                                }
                            }
                        },
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }

            };

        }
    }
  }

  @Script({ pos: { x: -892, y: 73 } })
  @Relation(r => dao.isSuccess(), 'createPayloadForPartnerNameValidation')
  async StaticConfiguration() {
    const script = {

        execute: async() => {
            const apiKey=await getValueByKey("ACCOR_POINTS_CREDIT_API_KEY")
            const authKey=await getValueByKey("ACCOR_POINTS_CREDIT_AUTH_KEY")
            const accorCustomResponse= await JSON.parse(await getValueByKey("ACCOR_API_CUSTOM_RESPONSE"))
            const accorBaseURL = await getValueByKey("ACCOR_BASE_URL") 

            const literals = {
                "status": false,
                "xCapClientSignature": true,
                "accept": "application/json",
                "isActive": true,
                "skipValidation": true,
                "fieldNames": ["min_ibc_redemption", "conversion_ratio", "red_multiplier"],
                "conversionRatio": "conversion_ratio",
                "xOrigin": "INDIGO",
                "apiKey": apiKey,
                "authorization": authKey,
                "accorURL": accorBaseURL,
                "transactionType": "EAR",
                "minimumIbcRedemptionArray":["min_ibc_redemption"],
                "redMultiplier":["red_multiplier"],
                "customAccorResponse": accorCustomResponse,
                "errorCodeAndMessage": {
                    "xCapApiAttributionEntityType": {
                        "code": 9506,
                        "message": "x-cap-api-attribution-entity-type is missing"
                    },
                    "xCapApiAttributionEntityCode": {
                        "code": 9507,
                        "message": "x-cap-api-attribution-entity-code is missing"
                    },
                    "partnerCode": {
                        "code": 9500,
                        "message": "PARTNER_CODE is missing"
                    },
                    "pointsConversion": {
                        "code": 9501,
                        "message": "POINTS_CONVERSION is missing"
                    },
                    "partnerCustomerId": {
                        "code": 9502,
                        "message": "PARTNER_CUSTOMER_ID is missing"
                    },
                    "ffn": {
                        "code": 9508,
                        "message": "FFN is missing"
                    },
                    "minimumIBCRedemption": {
                        "code": 9509,
                        "message": "POINTS_CONVERSION is less than min_ibc_redemption"
                    },
                    "redMultiplier": {
                        "code": 9510,
                        "message": "POINTS_CONVERSION input should be a multiplier of redemption multiplier"
                    },
                    "pointsReverseSuccess": {
                        "code": 1300,
                        "message": "Redeemed Points Reversed",
                    },
                    "pointsReverseGenericResponse": {
                        "code": 9505,
                        "message": "Redemption Reversal Failed"
                    }
                }

            }

            return {
                body: literals
            };


        }
    }
  }

  @Script({ pos: { x: 612, y: 153 } })
  @Relation(r => dao.isSuccess(), 'PointsRedeem')
  async ParsingRequiredFields() {
    const script = {

        execute: () => {
            const payload = getBody("ValidateBodyAndHeaders")?.payload
            const literals = getBody("StaticConfiguration")?.body
            const fieldNames = literals?.fieldNames
            const fields = getBody("GetOrgEntitiesApi")?.response?.stores?.store[0]?.custom_fields?.field
            const requiredFields = fields.filter((field) => {
                if (fieldNames.includes(field.name)) {
                    return field
                }
            })

            const minimum_ibc_redemption = requiredFields.filter((field) => {
                if (literals.minimumIbcRedemptionArray.includes(field.name)) {
                    return field
                }
            })[0]?.value
            const red_multiplier = requiredFields.filter((field) => {
                if (literals.redMultiplier.includes(field.name)) {
                    return field
                }
            })[0]?.value
            const result = (parseFloat(payload?.POINTS_CONVERSION)) / parseFloat(red_multiplier);
            if (parseFloat(payload?.POINTS_CONVERSION) < parseFloat(minimum_ibc_redemption)) {
                return {
                    http: {
                        res: {
                            status: 200,
                            "json": {
                                "response": {
                                    "partner_response": {},
                                    "responses": {},
                                    "status": {
                                        "item_status": {
                                            "code": literals?.errorCodeAndMessage?.
                                                minimumIBCRedemption?.code,
                                            "message": literals?.errorCodeAndMessage?.minimumIBCRedemption?.message,
                                            "success": literals.status === "false"? false : Boolean(literals.status)
                                        }
                                    }
                                }
                            },
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }
            if (result % 1 !== 0) {
                return {
                    http: {
                        res: {
                            status: 200,
                            "json": {
                                "response": {
                                    "partner_response": {},
                                    "responses": {},
                                    "status": {
                                        "item_status": {
                                            "code": literals?.errorCodeAndMessage?.redMultiplier?.code,
                                            "message": literals?.errorCodeAndMessage?.redMultiplier?.message,
                                            "success": literals.status === "false"? false : Boolean(literals.status)
                                        }
                                    }
                                }
                            },
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            const headers = getEffectiveHeaders()
            const body = {
                "root": {
                    "redeem": [
                        {
                            "points_redeemed": parseFloat(payload["POINTS_CONVERSION"]),
                            "customer": {
                                "external_id": payload["FFN"]
                            },
                            "notes": payload["NOTES"],
                            "custom_fields": payload["custom_fields"]
                        }
                    ]
                }
            }
            const queryParams = {
                "skip_validation": literals.skipValidation
            }

            //Write your code here.
            return {
                headers,
                body: JSON.stringify(body),
                queryParams,
                requiredFields
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 932, y: 153 } })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.response?.status?.success === "false"), 'PointsRedeemApiFailureResponse')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.response?.status?.success === "true"), 'CreatePayloadForAccorPointsCreditAPI')
  @Relation(r => dao.hasError(), 'ErrorHandler')
  async PointsRedeem() {
  return {
        url: `https://apac.api.capillarytech.com/v1.1/points/redeem`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 1237, y: 162 } })
  async PointsRedeemApiFailureResponse() {
    const script = {

        execute: () => {
            const response = getBody("PointsRedeem")?.response?.responses?.points
            const item_status = response?.item_status
            delete response["item_status"]

            //Write your code here.
            return {
                http: {
                    res: {
                        status: 200,
                        "json": {
                            "response": {
                                "partner_response": {},
                                "responses": response,
                                "status": {
                                    "item_status": {
                                        "success": false,
                                        "code": item_status?.code,
                                        "message": item_status?.message
                                    }
                                }
                            }
                        },
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 1204.0796061884669, y: 280.58326300984527 } })
  @Relation(r => dao.isSuccess(), 'AccorPointsCreditAPI')
  async CreatePayloadForAccorPointsCreditAPI() {
    const script = {

        execute: () => {
            const requestHeaders = getApiRequest("Trigger")?.headers
            const payload = getBody("ValidateBodyAndHeaders")?.payload
            const requiredFields = getBody("ParsingRequiredFields")?.requiredFields
            const literals = getBody("StaticConfiguration")?.body
            const [numerator, denominator] = requiredFields.filter((field) => {
                if (field.name === literals.conversionRatio) {
                    return field
                }
            })[0]?.value.split(":")

            const headers = {
                "X-TRACEID": requestHeaders["x-cap-request-id"],
                "X-ORIGIN": literals.xOrigin,
                "apikey": literals.apiKey,
                "Authorization": literals.authorization
            }
            const accorURL = `${literals?.accorURL}/loyalty/v1/earnPartner`

            const body = {
                "amountAllPoints": Math.floor(parseFloat(payload["POINTS_CONVERSION"]) / parseFloat(numerator) * parseFloat(denominator)).toString(),
                "transactionDate": new Date().toISOString().split("T")[0],
                "transactionId": getBody("PointsRedeem")?.response?.responses?.points?.redemption_id,
                "transactionType": literals.transactionType,
                "pmid": payload["PARTNER_CUSTOMER_ID"]
            }

            return {
                pathParams: {
                    url: accorURL,
                },
                headers,
                body: JSON.stringify(body)
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 1524.0796061884669, y: 280.58326300984527 } })
  @Relation(r => dao.isSuccess() && dao.getBody()?.responseCodes?.length === 0 && dao.getBody()?.status === "OK", 'FinalSuccessResponse')
  @Relation(r => dao.isSuccess() && dao.getBody()?.responseCodes?.length > 0 &&  dao.getBody()?.status === "KO", 'CreatePayloadForReverseRedeemApi')
  @Relation(r => dao.hasError(), 'CreatePayloadForReverseRedeemApi')
  async AccorPointsCreditAPI() {
  return {
        url: `{url}`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 1844.0796061884669, y: 280.58326300984527 } })
  async FinalSuccessResponse() {
    const script = {

        execute: () => {
            const accorApiResponse=getBody("AccorPointsCreditAPI")
            const response = getBody("PointsRedeem")?.response?.responses?.points
            const item_status = response?.item_status
            delete response["item_status"]

            //Write your code here.
            return {
                http: {
                    res: {
                        status: 200,
                        "json": {
                            "response": {
                                "status": {
                                    "item_status": {
                                        "success": item_status?.success === "false"? false : Boolean(item_status?.success),
                                        "code": item_status?.code,
                                        "message": item_status?.message
                                    }
                                },
                                "responses": response,
                                "partner_response": accorApiResponse,

                            }
                        },
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };



        }
    }
  }

  @Script({ pos: { x: 1844.0796061884669, y: 440.58326300984527 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'RedeemReverseApi')
  async CreatePayloadForReverseRedeemApi() {
    const script = {

        execute: () => {
            const response = getBody("PointsRedeem")?.response?.responses?.points
            const {redemption_id,points_redeemed,external_id}=response


            const headers=getEffectiveHeaders()
            const body = {
                "redemptionId":redemption_id,
                "pointsToBeReversed": parseFloat(points_redeemed),
                "identifier": {
                    "type": "externalId",
                    "value": external_id
                }
            }

            //Write your code here.
            return {
                headers,
                body:JSON.stringify(body)

            };

        }
    }
  }

  @ApiRequest({ pos: { x: 2101.6464921932024, y: 372.41180463323974 } })
  @Relation(r => dao.isSuccess() && dao.getBody()?.orgId, 'FinalRedeemReverseSuccessResponse')
  @Relation(r => dao.isSuccess() && dao.getBody()?.errors?.length >0 &&  (dao.getBody()?.errors[0].code === 3802), 'ReverseRedeemFailureResponse')
  @Relation(r => dao.isSuccess() && dao.getBody()?.errors?.length >0 &&  (dao.getBody()?.errors[0].code !== 3802), 'CreateMongoQuery')
  @Relation(r => dao.hasError(), 'CreateMongoQuery')
  async RedeemReverseApi() {
  return {
        url: `https://apac.api.capillarytech.com/v2/points/reverse`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 2371.285381082091, y: 340.07847129990637 } })
  async FinalRedeemReverseSuccessResponse() {
    const script = {

        execute: () => {
            const redeemReverseResponse = getBody("RedeemReverseApi")
            const { pointsReversed, pointsReversedDetails, pointsToBeReversed, redemptionId, reversalId } = redeemReverseResponse
            const accorApiResponse = getBody("AccorPointsCreditAPI")
            let description
            let responseCode
            let accorResponseCodes
            let customAccorResponse
            let code
            const literals = getBody("StaticConfiguration")?.body
            if (accorApiResponse?.responseCodes && accorApiResponse?.responseCodes?.length > 0) {
                accorResponseCodes = accorApiResponse?.responseCodes?.[0]?.split(':')
                responseCode = accorResponseCodes[0]
                description = accorResponseCodes[1]
                customAccorResponse = literals?.customAccorResponse?.responseCodeTransformation
                code = customAccorResponse.find((data) => {
                    if (data?.APIresponseCode.toLowerCase() === responseCode.toLowerCase()) {
                        return data
                    }
                })?.code
            }
            else {
                description = "Unknown Error"
                code = "7000"
            }

            const response = getBody("PointsRedeem")?.response?.responses?.points
            const { external_id, points_redeemed, redeemed_value, redemption_purpose, user_id } = response


            //Write your code here.
            return {
                http: {
                    res: {
                        status: 200,
                        "json": {
                            "response": {
                                "partner_response": accorApiResponse,
                                "responses": {
                                    "external_id": external_id,
                                    "points_redeemed": points_redeemed,
                                    "redeemed_value": redeemed_value,
                                    "redemption_purpose": redemption_purpose,
                                    "user_id": user_id,
                                    "pointsReversed": pointsReversed,
                                    "pointsReversedDetails": pointsReversedDetails,
                                    "pointsToBeReversed": pointsToBeReversed,
                                    "redemptionId": redemptionId,
                                    "reversalId": reversalId
                                },
                                "status": {
                                    "item_status": {
                                        "code": parseInt(code),
                                        "message": description?.trim(),
                                        "success": literals.status === "false"? false : Boolean(literals.status)
                                    }
                                }
                            }
                        },
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }

            };

        }
    }
  }

  @Script({ pos: { x: 2903.072418119128, y: 507.75439722583235 } })
  @ExecutionStrategy('or')
  async ReverseRedeemFailureResponse() {
    const script = {

        execute: () => {

            const responses = getBody("RedeemReverseApi")
            const literals = getBody("StaticConfiguration")?.body
            let errorMessage;

            let response
            const accorApiResponse = getBody("AccorPointsCreditAPI")
            const pointsRedeemResponse = getBody("PointsRedeem")?.response?.responses?.points
            const { external_id, points_redeemed, redeemed_value, redemption_purpose, user_id, redemption_id } = pointsRedeemResponse

            let redeemReverseResponse;
            if (responses?.errors && responses?.errors.length > 0) {
                redeemReverse = responses?.errors[0]
                redeemReverseResponse = {
                    "code": redeemReverse?.code,
                    "message": redeemReverse?.message,
                    "success": false
                }
            }
            else {
                errorMessage = responses?.message
                if (responses.code >= 500 && responses.code <= 599) {
                    let message = responses.code === 502 ? "Timeout" : "Internal Server Error"
                    response = {
                        "response": {
                            "partner_response": accorApiResponse,
                            "responses": {
                                "external_id": external_id,
                                "points_redeemed": points_redeemed,
                                "redeemed_value": redeemed_value,
                                "redemption_purpose": redemption_purpose,
                                "user_id": user_id,
                                "pointsReversed": 0,
                                "pointsReversedDetails": {
                                    "available": 0,
                                    "expired": 0
                                },
                                "pointsToBeReversed": 0,
                                "redemptionId": redemption_id,
                                "reversalId": ""
                            },
                            "status": {
                                "item_status": {
                                    "code": responses?.code,
                                    "message": message,
                                    "success": false
                                }
                            }
                        }
                    }
                    return {
                        http: {
                            res: {
                                status: 500,
                                json: response,
                                "headers": {
                                    "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                                }
                            }
                        }
                    }
                }

                redeemReverseResponse = {
                    "code": responses?.code,
                    "message": errorMessage,
                    "success": false
                }
            }

            return {
                http: {
                    res: {
                        status: 200,
                        "json": {
                            "response": {
                                "partner_response": accorApiResponse,
                                "responses": {
                                    "external_id": external_id,
                                    "points_redeemed": points_redeemed,
                                    "redeemed_value": redeemed_value,
                                    "redemption_purpose": redemption_purpose,
                                    "user_id": user_id,
                                    "pointsReversed": 0,
                                    "pointsReversedDetails": {
                                        "available": 0,
                                        "expired": 0
                                    },
                                    "pointsToBeReversed": 0,
                                    "redemptionId": redemption_id,
                                    "reversalId": ""
                                },
                                "status": {
                                    "item_status": redeemReverseResponse
                                }
                            }
                        },
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }

            };

        }
    }
  }

  @Script({ pos: { x: 2382.5724181191285, y: 587.2451379665735 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'PutMongo')
  async CreateMongoQuery() {
    const script = {

        execute: () => {
            const payload = getBody("ValidateBodyAndHeaders")?.payload
            const response = getBody("PointsRedeem")?.response?.responses?.points
            const { points_redeemed, redemption_id } = response
            const requestBody = JSON.parse(getBody("CreatePayloadForReverseRedeemApi")?.body)
            const literals = getBody("StaticConfiguration")?.body

            const reverseRedeemResponse = getBody("RedeemReverseApi")

            const mongoKey = {
                "redemption_id": redemption_id,
            }

            const query = {
                $set: {
                    "FFN": payload?.FFN,
                    "redemption_id":redemption_id,
                    "partner": payload?.PARTNER_CODE,
                    "points_redeemed": points_redeemed,
                    "creation_date": new Date(),
                    "modified_date": new Date(),
                    "is_active": literals.isActive,
                    "reversalAPI_request": requestBody,
                    "reversalAPI_response": reverseRedeemResponse
                }
            }
            //Write your code here.
            return {
                mongoKey,
                query
            };

        }
    }
  }

  @PutMongo({ pos: { x: 2626.5724181191285, y: 586.2451379665735 } })
  @Relation(r => dao.isSuccess(), 'ReverseRedeemFailureResponse')
  async PutMongo() {
  return {
        collectionName: `Partner_Redemption_Reversal_Fail`,
        mode: `upsert`,
        query: r => getBody().query,
        queryKey: r => getBody().mongoKey,
      };
  }

  @Script({ pos: { x: 1210, y: 416 } })
  async ErrorHandler() {
    const script = {

        execute: () => {
            const errors = getBody("PointsRedeem")
            let errorMessage;
            try{
                errorMessage = JSON.parse(errors?.message)?.response?.status?.message
            }
            catch(error){
                logger.info(`Error : ${error}`)
            }

            let response;
            if (errors.code >= 500 && errors.code <= 599) {
                let message = errors.code === 502 ? "Timeout" : "Internal Server Error"
                response = {
                    "response": {
                        "partner_response": {},
                        "responses": {},
                        "status": {
                            "item_status": {
                                "code": errors.code,
                                "message": message,
                                "success": false
                            }
                        }
                    }
                }
                return {
                    http: {
                        res: {
                            status: 500,
                            json: response,
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            response = {
                "response": {
                    "partner_response": {},
                    "responses": {},
                    "status": {
                        "item_status": {
                            "code": errors.code,
                            "message": errorMessage,
                            "success": false
                        }
                    }
                }
            }

            //Write your code here.
            return {
                http: {
                    res: {
                        json: response,
                        status: 200,
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };
        }
    }
  }

  @Script({ pos: { x: 604, y: 263 } })
  async GetOrgEntitiesInternalServerError() {
    const script = {

        execute: () => {
            const errors = getBody("GetOrgEntitiesApi")
            let errorMessage;
            try {
                errorMessage = JSON.parse(errors?.message)?.response?.status?.message
            }
            catch (error) {
                logger.info(`Error Occurred : ${error}`)
            }

            let response;
            if (errors.code >= 500 && errors.code <= 599) {
                let message = errors.code === 502 ? "Timeout" : "Internal Server Error"
                response = {
                    "response": {
                        "partner_response": {},
                        "responses": {},
                        "status": {
                            "item_status": {
                                "code": errors.code,
                                "message": message,
                                "success": false
                            }
                        }
                    }
                }
                return {
                    http: {
                        res: {
                            status: 500,
                            json: response,
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            response = {
                "response": {
                    "partner_response": {},
                    "responses": {},
                    "status": {
                        "item_status": {
                            "code": errors.code,
                            "message": errorMessage,
                            "success": false
                        }
                    }
                }
            }

            //Write your code here.
            return {
                http: {
                    res: {
                        json: response,
                        status: 200,
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };



        }
    }
  }

  @Script({ pos: { x: -618, y: 77 } })
  @Relation(r => dao.isSuccess(), 'PartnerNameValidationApiCall')
  async createPayloadForPartnerNameValidation() {
    const script = {
      execute: () => {
        const headers = getApiRequest().headers || {};

        const literals = getBody("StaticConfiguration")?.body
        const requestBody = getApiRequest().body
        let header = {}
        delete headers["x-cap-neo-test-variant-id"]

        header["apikey"] = literals.apiKey
        return {
          headers: header,
          queryParams: ({
            FFN: requestBody.FFN,
            pmid: requestBody.PARTNER_CUSTOMER_ID
          })
        };
      }
    };
  }

  @ApiRequest({ pos: { x: -384, y: 85 } })
  @Relation(r => dao.isSuccess(), 'ValidateBodyAndHeaders')
  @Relation(r => dao.hasError(), 'handleErrorPartnerName')
  async PartnerNameValidationApiCall() {
  return {
        url: `https://apac.api.capillarytech.com/api_gateway/neo/api/v1/xto6x/execute/partnerNameValidate`,
        method: `GET`,
      };
  }

  @Script({ pos: { x: -77, y: 380 } })
  async handleErrorPartnerName() {
    const script = {

        execute: () => {

            const response = getBody()
            if(response.code == 502){
                return {
                http: {
                    res: {
                        json: {
                            response: {
                                partner_response: {},
                                responses: {},
                                status: {
                                    "item_status": {
                                        success: false,
                                        code: 502,
                                        message: "TIMEOUT"
                                    }
                                }
                            }
                        },
                        status: 500,
                        headers: {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            }
            }
            //Write your code here.
            return {
                http: {
                    res: {
                        json: {
                            response: {
                                partner_response: {},
                                responses: {},
                                status: {
                                    "item_status": {
                                        success: false,
                                        code: response?.code,
                                        message:response?.message
                                    }
                                }
                            }
                        },
                        status: 200,
                        headers: {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            }
        }
    }
  }
}
