import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn, getOut } = dao;

@Dag({ method: "POST", url: "milestoneBenefitsIssual" })
class MilestoneBenefitsIssual {
  constructor() {
    this.schemaValidationBlock();
  }

  @Schema({ pos: { x: 254, y: -97 } })
  @Relation(r => dao.hasError(), 'handleValidationFailures')
  @Relation(r => dao.isSuccess(), 'MappingBlock')
  async schemaValidationBlock() {
    return {
        definitions: [],
        spec: {
            type: "object",
            "properties": {
                "body": {
                    type: 'object',
                    properties: {
                        ffn: {
                            minLength: 1,
                            "errorMessage": {
                                minLength: "ffn must not be empty"
                            }
                        },
                        productName: {
                            minLength: 1,
                            "errorMessage": {
                                minLength: "productName must not be empty"
                            }
                        },
                        typeOfBenefit: {
                            minLength: 1,
                            "errorMessage": {
                                minLength: "typeOfBenefit must not be empty"
                            }
                        }
                    }, required: ['ffn', 'productName', 'typeOfBenefit'],
                    errorMessage: {
                        required: {
                            ffn: "ffn is missing",
                            productName: "productName is missing",
                            typeOfBenefit: "typeOfBenefit is missing"
                        }
                    }
                }
            }
        }
    }
  }

  @Script({ pos: { x: 567, y: -331 } })
  async handleValidationFailures() {
    const script = {
      execute: () => {
        let input = getApiRequest().body;

        const errors = [];
        const errorMessages = getIn()?.err || [];
        errorMessages.forEach(error => {
          errors.push({
            status: false,
            code: 400,
            message: error.message,
            path: error.instancePath
          });
        });
        return {
          http: {
            "res": {
              "json": {
                errors
              },
              status: 400
            }
          }
        }
      }
    }
  }

  @Script({ pos: { x: 629, y: -45 } })
  @Relation(r => dao.isSuccess(), 'CustomerLookupApiCall')
  async PrepareCustomerLookupApiCall() {
    const script = {
        execute: () => {
            const requestBody = getApiRequest().body;

            let requestHeaders = getEffectiveHeaders();

            delete requestHeaders["x-cap-neo-test-variant-id"];

            let queryParameters = {
                "identifierName" : "externalId",
                "identifierValue" : requestBody.ffn,
                "source" : requestBody.source ? requestBody.source.toUpperCase() : "INSTORE"
            }
            return {
                headers : requestHeaders,
                queryParams : queryParameters
            };
        }
    }
  }

  @ApiRequest({ pos: { x: 921, y: -45 } })
  @Cachable({ cachable: false, key: "" })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.errors?.length), 'CustomerGetError')
  @Relation(r => dao.isSuccess(), 'ExtractActiveCardsFromCustomerProfile')
  @Relation(r => dao.hasError(), 'handleCustomerGetApiCallFailures')
  async CustomerLookupApiCall() {
  return {
        url: `https://apac.api.capillarytech.com/v2/customers/lookup/customerDetails`,
        method: `GET`,
        queryParams: {
    "embed": "cardLoyaltyDetails"
  },
      };
  }

  @Script({ pos: { x: 1281.9775474449514, y: 546.5158475330018 } })
  async handleCustomerGetApiCallFailures() {
    const script = {

        execute: () => {
            if (getBody().code === 401) {
                let errors = [];
                const error = parseXml(getBody().err?.message)
                errors.push(error);
                return {
                    http: {
                        res: {
                            status : getBody().code,
                            json : {
                                "errors": errors
                            }
                        }
                    }
                }
            } else {
                return {
                    http: {
                        res: {
                            status : getBody().code,
                            json : getBody().err?.message
                        }
                    }
                }
            }
        }

    }

    function parseXml(xml) {
      const successMatch = xml?.match(/<success>(.*?)<\/success>/);
      const codeMatch = xml?.match(/<code>(.*?)<\/code>/);
      const messageMatch = xml?.match(/<message>(.*?)<\/message>/);

      const success = successMatch ? successMatch[1] === "true" : null;
      const code = codeMatch ? parseInt(codeMatch[1], 10) : null;
      const message = messageMatch ? messageMatch[1] : null;


      return { success, code, message };
    }
  }

  @Script({ pos: { x: 1266.876253422613, y: -95.8364668329519 } })
  async CustomerGetError() {
    const script = {

        execute: () => {
            let customerGetResponse = getIn();
            let errors = customerGetResponse.errors;
            return {
                http: {
                    "res": {
                        status : 200,
                        "json" : {
                            "errors" : errors
                        }
                    }
                }
            }
        }

    }
  }

  @Script({ pos: { x: 1268.5195447348283, y: 41.47813722101557 } })
  @Relation(r => dao.hasError(), 'HandleError')
  @Relation(r => dao.isSuccess() && (dao.getBody().body?.isCardPresent && dao.getBody().body?.areMultipleCardsPresent), 'MultipleActiveCardsError')
  @Relation(r => dao.isSuccess() && (dao.getBody().body?.isCardPresent == false), 'ActiveCardMissing')
  @Relation(r => dao.isSuccess() && (dao.getBody().body?.isCardPresent == true && dao.getBody().body?.areMultipleCardsPresent == false), 'CheckMilestoneCycle')
  async ExtractActiveCardsFromCustomerProfile() {
    const script = {

        execute: () => {

            let requestHeaders = getApiRequest().headers;
            let bankCardSeriesId = requestHeaders['card-series'];

            let customerDetailsResponse = getIn("CustomerLookupApiCall");

            let customerCardDetails = customerDetailsResponse.cardDetails;
            let matchingCardFromCustomerProfile;
            let isCardPresent = false;
            let areMultipleCardsPresent = false;
            for (let customerCardDetail of customerCardDetails) {
              let cardSeriesId = customerCardDetail.seriesId;

              if (cardSeriesId == bankCardSeriesId) {
                if (customerCardDetail.statusInfo.label.trim().toUpperCase() == 'ACTIVE') {
                  matchingCardFromCustomerProfile = customerCardDetail;
                  if (isCardPresent) {
                    logger.info("multiple active cards are present in customer's profile");
                    areMultipleCardsPresent = true;
                  } else {
                    logger.info("active card is present in customer's profile - " + customerCardDetail.cardNumber);
                    isCardPresent = true;
                  }
                }
              }
            }

            return {
                body : {
                    "isCardPresent" : isCardPresent,
                    "areMultipleCardsPresent" : areMultipleCardsPresent,
                    "matchingCardFromCustomerProfile" : matchingCardFromCustomerProfile,
                    requestHeaders
                }
            }
        }

    }
  }

  @Script({ pos: { x: 1684.2754705487914, y: -93.29414820291038 } })
  async HandleError() {
    const script = {
      execute: () => {

        const errors = [];
        errors.push({
            status: false,
            code: 400,
            message: "Error occured while extracting active cards from customer's profile",
        });
        return {
          http: {
            "res": {
              "json": {
                errors
              },
              status: 500
            }
          }
        }
      }
    }
  }

  @Script({ pos: { x: 1677.3778408743767, y: 122.78087221207551 } })
  async MultipleActiveCardsError() {
    const script = {
      execute: () => {

        const errors = [];
        errors.push({
            status: false,
            code: 400,
            message: "Multiple active cards are present in customer's profile",
        });
        return {
          http: {
            "res": {
              "json": {
                errors
              },
              status: 400
            }
          }
        }
      }
    }
  }

  @Script({ pos: { x: 1703.2270203770586, y: 293.129140051071 } })
  async ActiveCardMissing() {
    const script = {
      execute: () => {

        const errors = [];
        errors.push({
            status: false,
            code: 400,
            message: "Active card is not present in customer's profile",
        });
        return {
          http: {
            "res": {
              "json": {
                errors
              },
              status: 400
            }
          }
        }
      }
    }
  }

  @Script({ pos: { x: 1694.1490805570597, y: 429.9936214183849 } })
  @Relation(r => dao.isSuccess(), 'CheckIfCardExists')
  async CheckMilestoneCycle() {
    const script = {

        execute: () => {
            let matchingCardFromCustomerProfile = getBody().body.matchingCardFromCustomerProfile;

            let cardNumber = matchingCardFromCustomerProfile.cardNumber;
            let cardSeriesId = matchingCardFromCustomerProfile.seriesId;

            let milestoneCycle = matchingCardFromCustomerProfile?.customFields?.milestone_cycle;

            if (milestoneCycle == null || milestoneCycle == undefined || milestoneCycle.length == 0) {
                return buildErrorAndReturn(400, "milestone_cycle is missing from active card in customer's profile", 400);
            }

            // milestoneCycle = "2023-08-31 00:18:59"
            // milestoneCycle is in IST
            const milestoneCycleDate = new Date(`${milestoneCycle} GMT+0530`);

            const currentDate = new Date();

            let isMilestoneCycleInFuture = currentDate.getTime() < milestoneCycleDate.getTime();
            if (!(isMilestoneCycleInFuture)) {
                return buildErrorAndReturn(400, "milestone_cycle date crossed for customer's card", 400);
            }
            let requestHeaders = getApiRequest().headers;
            let bankName = requestHeaders['bank-name'];

            let apiRequestBody = getApiRequest().body;
            let ffn = apiRequestBody.ffn;
            let productName = apiRequestBody.productName;
            let typeOfBenefit = apiRequestBody.typeOfBenefit;

            let promotionConfigMapping = getBody("MappingBlock").body.promotionConfig;

            let promotionConfig = promotionConfigMapping?.[bankName]?.[productName]?.[typeOfBenefit]

            if (promotionConfig == undefined || promotionConfig == null) {
                return buildErrorAndReturn(400, "invalid productName/typeOfBenefit passed in the request", 400);
            }
            let mongoGetQuery = {
                "card_number" : cardNumber
            }
            //Write your code here.
            return {
                body : {
                    "matchingCardFromCustomerProfile" : matchingCardFromCustomerProfile,
                    "promotionConfig" : promotionConfig,
                    "mongoGetQuery" : mongoGetQuery,
                    "milestoneCycle" : milestoneCycle,
                    "milestoneFlag" : currentDate,
                    "ffn" : ffn,
                    "cardNumber": cardNumber,
                    "cardSeriesId": cardSeriesId,
                }
            };

        }
    }

    function buildErrorAndReturn(code, message, statusCode) {
        const errors = [];
        errors.push({
            status: false,
            code: code,
            message: message,
        });
        return {
            http: {
                "res": {
                    "json": {
                        errors
                    },
                    status: statusCode
                }
            }
        }
    }
  }

  @Script({ pos: { x: 400.73586275603327, y: 122.71224017694175 } })
  @Relation(r => dao.isSuccess(), 'PrepareCustomerLookupApiCall')
  async MappingBlock() {
    const script = {
        execute: () => {
            let promotionConfig = {
                "HDFC": {
                    "IndiGo BluChip": {
                        "Milestone benefit 1 - 1.25 Lacs": {
                            "milestone_flag": "IndiGoBluChip_Mile1",
                            "couponSeries": {
                                "145179": 1,
                                "147279": 1
                            }
                        },
                        "Milestone benefit 2 - 2.5 Lacs": {
                            "milestone_flag": "IndiGoBluChip_Mile2",
                            "couponSeries": {
                                "145179": 1,
                                "147279": 1
                            }
                        },
                        "Milestone benefit 3 - 6 Lacs": {
                            "milestone_flag": "IndiGoBluChip_Mile3",
                            "couponSeries": {
                                "145179": 1,
                                "147279": 1
                            }
                        }
                    },
                    "IndiGo BluChip XL": {
                        "Milestone benefit 1 - 1.5 Lacs": {
                            "milestone_flag": "IndiGoBluChipXL_Mile1",
                            "couponSeries": {
                                "147270": 1,
                                "147279": 1
                            }
                        },
                        "Milestone benefit 2 - 4 Lacs": {
                            "milestone_flag": "IndiGoBluChipXL_Mile2",
                            "couponSeries": {
                                "147270": 1,
                                "147279": 1
                            }
                        },
                        "Milestone benefit 3 - 7.5 Lacs": {
                            "milestone_flag": "IndiGoBluChipXL_Mile3",
                            "couponSeries": {
                                "147270": 1,
                                "147279": 1
                            }
                        },
                        "Milestone benefit 4 - 12 Lacs": {
                            "milestone_flag": "IndiGoBluChipXL_Mile4",
                            "couponSeries": {
                                "147270": 1,
                                "147279": 1
                            }
                        }
                    },
                    "IndiGo BluChip XXL": {
                        "Milestone benefit 1 - 1.5 Lacs": {
                            "milestone_flag": "IndiGoBluChipXXL_Mile1",
                            "couponSeries": {
                                "148879": 1,
                                "147281": 1
                            }
                        },
                        "Milestone benefit 2 - 4 Lacs": {
                            "milestone_flag": "IndiGoBluChipXXL_Mile2",
                            "couponSeries": {
                                "148879": 1,
                                "147281": 1
                            }
                        },
                        "Milestone benefit 3 - 7.5 Lacs": {
                            "milestone_flag": "IndiGoBluChipXXL_Mile3",
                            "couponSeries": {
                                "148879": 1,
                                "147281": 1
                            }
                        },
                        "Milestone benefit 4 - 12 Lacs": {
                            "milestone_flag": "IndiGoBluChipXXL_Mile4",
                            "couponSeries": {
                                "148879": 1,
                                "147281": 1
                            }
                        }
                    }
                },
                "KOTAK": {
                    "IndiGo BluChip": {
                        "Milestone benefit 1 - 1.25 Lacs": {
                            "milestone_flag": "IndiGoBluChip_Mile1",
                            "couponSeries": {
                                "145180": 1,
                                "147271": 1
                            }
                        },
                        "Milestone benefit 2 - 2.5 Lacs": {
                            "milestone_flag": "IndiGoBluChip_Mile2",
                            "couponSeries": {
                                "145180": 1,
                                "147271": 1
                            }
                        },
                        "Milestone benefit 3 - 6 Lacs": {
                            "milestone_flag": "IndiGoBluChip_Mile3",
                            "couponSeries": {
                                "145180": 1,
                                "147271": 1
                            }
                        }
                    },
                    "IndiGo BluChip XL": {
                        "Milestone benefit 1 - 1.5 Lacs": {
                            "milestone_flag": "IndiGoBluChipXL_Mile1",
                            "couponSeries": {
                                "147269": 1,
                                "147271": 1
                            }
                        },
                        "Milestone benefit 2 - 4 Lacs": {
                            "milestone_flag": "IndiGoBluChipXL_Mile2",
                            "couponSeries": {
                                "147269": 1,
                                "147271": 1
                            }
                        },
                        "Milestone benefit 3 - 7.5 Lacs": {
                            "milestone_flag": "IndiGoBluChipXL_Mile3",
                            "couponSeries": {
                                "147269": 1,
                                "147271": 1
                            }
                        },
                        "Milestone benefit 4 - 12 Lacs": {
                            "milestone_flag": "IndiGoBluChipXL_Mile4",
                            "couponSeries": {
                                "147269": 1,
                                "147271": 1
                            }
                        }
                    },
                    "IndiGo BluChip XXL": {
                        "Milestone benefit 1 - 1.5 Lacs": {
                            "milestone_flag": "IndiGoBluChipXXL_Mile1",
                            "couponSeries": {
                                "148877": 1,
                                "147280": 1
                            }
                        },
                        "Milestone benefit 2 - 4 Lacs": {
                            "milestone_flag": "IndiGoBluChipXXL_Mile2",
                            "couponSeries": {
                                "148877": 1,
                                "147280": 1
                            }
                        },
                        "Milestone benefit 3 - 7.5 Lacs": {
                            "milestone_flag": "IndiGoBluChipXXL_Mile3",
                            "couponSeries": {
                                "148877": 1,
                                "147280": 1
                            }
                        },
                        "Milestone benefit 4 - 12 Lacs": {
                            "milestone_flag": "IndiGoBluChipXXL_Mile4",
                            "couponSeries": {
                                "148877": 1,
                                "147280": 1
                            }
                        }
                    }
                }
            };



            return {
                body: {
                    promotionConfig
                }
            };
        }
    }
  }

  @GetMongo({ pos: { x: 2010.4266317952201, y: 589.9936214183849 } })
  @Cachable({ cachable: false, key: "" })
  @Relation(r => dao.isSuccess(), 'ValidateMilestoneRevaluationCycle')
  async CheckIfCardExists() {
  return {
        collectionName: `Card_Milestone_Benefits`,
        query: r => getBody().body.mongoGetQuery,
        sort: `{}`,
      };
  }

  @Script({ pos: { x: 2333.778616379291, y: 591.9936214183849 } })
  @Relation(r => dao.isSuccess(), 'CouponIssualBlock')
  async ValidateMilestoneRevaluationCycle() {
    const script = {

        execute: () => {
            let responseFromCheckMilestoneCycleBlock = getBody("CheckMilestoneCycle").body;

            let promotionConfig = responseFromCheckMilestoneCycleBlock.promotionConfig;
            let milestoneCycle = responseFromCheckMilestoneCycleBlock.milestoneCycle;

            let cardMilestoneBenefitsFromMongo = getOut();

            if (cardMilestoneBenefitsFromMongo.length > 0) {
                let existingCardMilestoneBenefit = cardMilestoneBenefitsFromMongo[0];
                let milestoneFlagFieldName = promotionConfig.milestone_flag;

                let milestoneRevaluationCycleFieldName = milestoneFlagFieldName + "_Revaluation_Cycle";

                let milestoneFlagFromDb = existingCardMilestoneBenefit[milestoneFlagFieldName];
                let milestoneRevaluationCycleFromDb = existingCardMilestoneBenefit[milestoneRevaluationCycleFieldName];

                let milestoneRevaluationDate = new Date(`${milestoneCycle} GMT+0530`);

                let milestoneRevaluationDateFromDb = new Date(`${milestoneRevaluationCycleFromDb} GMT+0530`);

                if (milestoneRevaluationDate.getTime() == milestoneRevaluationDateFromDb.getTime()) {
                    return buildErrorAndReturn(400, "milestone_cycle is already processed", 400)
                } else if (milestoneRevaluationDate.getTime() < milestoneRevaluationDateFromDb.getTime()) {
                    return buildErrorAndReturn(400, "future milestone_cycle was already processed", 400)
                }
            }

            let apiRequestBody = getApiRequest().body;
            let externalId = apiRequestBody.ffn;

            let couponSeries = promotionConfig.couponSeries;

            let returnArray = [];

            let requestHeaders = {
                "Content-Type": "application/json",
                ...(getEffectiveHeaders())
            }

            Object.keys(couponSeries).forEach(key => {
                let seriesId = key;
                let couponCount = couponSeries[key];
                let couponIssualPayload = {
                    "seriesId": seriesId,
                    "count": couponCount,
                    "customer": {
                        "externalId": externalId
                    }
                }

                let returnObject = {
                    headers: requestHeaders,
                    body: JSON.stringify(couponIssualPayload)
                };
                returnArray.push(returnObject)
            });

            return returnArray

        }
    }


    function buildErrorAndReturn(code, message, statusCode) {
        const errors = [];
        errors.push({
            status: false,
            code: code,
            message: message,
        });
        return {
            http: {
                "res": {
                    "json": {
                        errors
                    },
                    status: statusCode
                }
            }
        }
    }
  }

  @ApiRequest({ pos: { x: 2653.778616379291, y: 589.9936214183849 } })
  @Cachable({ cachable: false, key: "" })
  @Relation(r => dao.hasError(), 'handleCouponIssualApiCallFailure')
  @Relation(r => dao.isSuccess(), 'PrepareQueryForInsertionInCardMilestoneBenefitsCollection')
  async CouponIssualBlock() {
  return {
        url: `https://apac.api.capillarytech.com/v2/coupon/issue/multiple`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 3058.1786057094328, y: 546.9324023723943 } })
  async handleCouponIssualApiCallFailure() {
    const script = {

        execute: () => {
            if (getBody().code === 401) {
                let errors = [];
                const error = parseXml(getBody().err?.message)
                errors.push(error);
                return {
                    http: {
                        res: {
                            status : getBody().code,
                            json : {
                                "errors": errors
                            }
                        }
                    }
                }
            } else {
                return {
                    http: {
                        res: {
                            status : getBody().code,
                            json : getBody().err?.message
                        }
                    }
                }
            }
        }

    }

    function parseXml(xml) {
      const successMatch = xml?.match(/<success>(.*?)<\/success>/);
      const codeMatch = xml?.match(/<code>(.*?)<\/code>/);
      const messageMatch = xml?.match(/<message>(.*?)<\/message>/);

      const success = successMatch ? successMatch[1] === "true" : null;
      const code = codeMatch ? parseInt(codeMatch[1], 10) : null;
      const message = messageMatch ? messageMatch[1] : null;


      return { success, code, message };
    }
  }

  @Script({ pos: { x: 2979.259675130474, y: 886.7306161295032 } })
  @Cachable({ cachable: false, key: "" })
  @Relation(r => dao.isSuccess() && (dao.getBody().body.failedCouponIssual.length > 0), 'CheckFailedCouponIssuals')
  @Relation(r => dao.isSuccess() && (!(dao.getBody().body.failedCouponIssual.length > 0) )&& (dao.getBody().body.queryType == 'UPDATION'), 'UpdateCardMilestonBenefits')
  @Relation(r => dao.isSuccess() && (!(dao.getBody().body.failedCouponIssual.length > 0)) && (dao.getBody().body.queryType == 'INSERTION'), 'AddCardMilestonBenefits')
  async PrepareQueryForInsertionInCardMilestoneBenefitsCollection() {
    const script = {

        execute: () => {
            let currentDate = new Date();

            let successfulCouponIssual = [];
            let failedCouponIssual = [];

            let couponIssualResponse = getOut();
            let couponIssualRequestBody = getOut("ValidateMilestoneRevaluationCycle");

            for (let index = 0; index < couponIssualResponse.length; index++) {
                let issualResponse = couponIssualResponse[index];
                let issualRequestBody = JSON.parse(couponIssualRequestBody[index].body);

                let errors = issualResponse?.errors;
                if (errors && errors.length > 0) {
                    issualResponse.seriesId = issualRequestBody.seriesId;
                    failedCouponIssual.push(issualResponse)
                } else {
                    issualResponse.seriesId = issualRequestBody.seriesId;
                    successfulCouponIssual.push(issualResponse)
                }
            }

            if (failedCouponIssual.length > 0) {
                return {
                    body: {
                        "successfulCouponIssual": successfulCouponIssual,
                        "failedCouponIssual": failedCouponIssual
                    }
                };
            }

            let cardMilestoneBenefitsFromMongo = getOut("CheckIfCardExists");

            let outputFromCheckMilestoneCycleBlock = getBody("CheckMilestoneCycle").body;
            let promotionConfig = outputFromCheckMilestoneCycleBlock.promotionConfig
            let milestoneCycleDate = outputFromCheckMilestoneCycleBlock.milestoneCycle
            let milestoneFlag = outputFromCheckMilestoneCycleBlock.milestoneFlag
            let ffn = outputFromCheckMilestoneCycleBlock.ffn
            let cardNumber = outputFromCheckMilestoneCycleBlock.cardNumber
            let cardSeriesId = outputFromCheckMilestoneCycleBlock.cardSeriesId

            let milestoneFlagFieldName = promotionConfig.milestone_flag;

            if (cardMilestoneBenefitsFromMongo.length > 0) {
                let updateMongoSet = {};

                let reValuationCycleFieldName = milestoneFlagFieldName + "_Revaluation_Cycle";
                let couponIssualResponseFieldName = milestoneFlagFieldName + "_CouponIssualResponse";

                updateMongoSet[milestoneFlagFieldName] = milestoneFlag
                updateMongoSet[reValuationCycleFieldName] = milestoneCycleDate
                updateMongoSet[couponIssualResponseFieldName] = couponIssualResponse

                let updationQuery = JSON.stringify({
                    $set: {
                        ...(updateMongoSet),
                        "date_updated": currentDate
                    }
                });
                let updationQueryKey = { "card_number": cardNumber }
                let auditLogQuery = {
                    "ffn": ffn,
                    "card_number": cardNumber,
                    "card_series_id": cardSeriesId,
                    "date_created": currentDate,
                    "type_of_benefit": milestoneFlagFieldName
                }
                return {
                    body: {
                        "queryType": "UPDATION",
                        "query": updationQuery,
                        "queryKey": updationQueryKey,
                        "successfulCouponIssual": successfulCouponIssual,
                        "failedCouponIssual": failedCouponIssual,
                        "auditLogQuery" : auditLogQuery
                    }
                }
            } else {
                let insertMongoQuery = {
                    "ffn": ffn,
                    "card_number": cardNumber,
                    "card_series_id": cardSeriesId,
                    "date_created": currentDate,
                    "date_updated": currentDate
                };

                let auditLogQuery = {
                    "ffn": ffn,
                    "card_number": cardNumber,
                    "card_series_id": cardSeriesId,
                    "date_created": currentDate,
                    "type_of_benefit": milestoneFlagFieldName
                }

                let reValuationCycleFieldName = milestoneFlagFieldName + "_Revaluation_Cycle";
                let couponIssualResponseFieldName = milestoneFlagFieldName + "_CouponIssualResponse";

                insertMongoQuery[milestoneFlagFieldName] = milestoneFlag
                insertMongoQuery[reValuationCycleFieldName] = milestoneCycleDate
                insertMongoQuery[couponIssualResponseFieldName] = couponIssualResponse

                return {
                    body: {
                        "queryType": "INSERTION",
                        "query": insertMongoQuery,
                        "successfulCouponIssual": successfulCouponIssual,
                        "failedCouponIssual": failedCouponIssual,
                        "auditLogQuery": auditLogQuery
                    }
                };
            }
        }
    }
  }

  @Script({ pos: { x: 3312.725552759527, y: 880.7119924671117 } })
  @Relation(r => dao.isSuccess(), 'CouponRevokalApi')
  async CheckFailedCouponIssuals() {
    const script = {

        execute: () => {
            let responseAfterCouponSegregation = getBody().body;

            let successfulCouponIssual = responseAfterCouponSegregation.successfulCouponIssual;

            let failedCouponIssual = responseAfterCouponSegregation.failedCouponIssual;

            let errors;

            let revokalArray = [];
            if (successfulCouponIssual.length > 0) {
                for (let index = 0; index < successfulCouponIssual.length; index++) {

                    let couponsObj = successfulCouponIssual[index];
                    let couponsIssued = couponsObj.coupons;

                    let couponsToRevoke = [];

                    for (let issuedCoupon of couponsIssued) {
                        couponsToRevoke.push(issuedCoupon.code);
                    }

                    let seriesId = couponsObj.seriesId;

                    let couponRevokalRequestBody = {
                        "couponSeriesId": seriesId,
                        "couponCodes": couponsToRevoke
                    }

                    let requestHeaders = {
                        "Content-Type": "application/json",
                        ...(getEffectiveHeaders())
                    }
                    let revokalObject = {
                        headers: requestHeaders,
                        body: JSON.stringify(couponRevokalRequestBody)
                    };

                    revokalArray.push(revokalObject);
                }
                return revokalArray;
            } else {
                let failedCoupon = failedCouponIssual[0];
                errors = failedCoupon.errors;
                return buildErrorAndReturn(errors, 500);
            }
        }
    }

    function buildErrorAndReturn(errors, statusCode) {
        return {
            http: {
                "res": {
                    "json": {
                        errors
                    },
                    status: statusCode
                }
            }
        }
    }
  }

  @ApiRequest({ pos: { x: 3632.725552759527, y: 880.7119924671117 } })
  @Relation(r => dao.isSuccess(), 'BuildResponseAfterCouponRevokal')
  async CouponRevokalApi() {
  return {
        url: `https://apac.api.capillarytech.com/v2/coupon/revoke`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 3952.725552759527, y: 880.7119924671117 } })
  async BuildResponseAfterCouponRevokal() {
    const script = {

        execute: () => {
            return buildErrorAndReturn(500, "error occurred while issuing coupons", 500);
        }
    }

    function buildErrorAndReturn(code, message, statusCode) {
        const errors = [];
        errors.push({
            status: false,
            code: code,
            message: message,
        });
        return {
            http: {
                "res": {
                    "json": {
                        errors
                    },
                    status: statusCode
                }
            }
        }
    }
  }

  @PutMongo({ pos: { x: 3458.5053677374644, y: 1135.9739382742807 } })
  @Relation(r => dao.isSuccess(), 'PrepareQueryForAuditLogInsertion')
  async UpdateCardMilestonBenefits() {
  return {
        collectionName: `Card_Milestone_Benefits`,
        mode: `update`,
        query: r => getBody().body.query,
        queryKey: r => getBody().body.queryKey,
      };
  }

  @PutMongo({ pos: { x: 3450.6264361943, y: 1402.791150357921 } })
  @Relation(r => dao.isSuccess(), 'PrepareQueryForAuditLogInsertion')
  async AddCardMilestonBenefits() {
  return {
        collectionName: `Card_Milestone_Benefits`,
        mode: `insert`,
        query: r => getBody().body.query,
      };
  }

  @Script({ pos: { x: 4258.174799943379, y: 1215.6712671321916 } })
  async finalResponseAfterMongoUpdation() {
    const script = {

       execute: () => {

           return {
               http: {
                   "res": {
                       "json": {
                        "message" : "Records added/updated in mongoDb successfully"
                       }
                   }
               }
           };

       }
    }
  }

  @PutMongo({ pos: { x: 3978.5053677374644, y: 1164.9739382742807 } })
  @Relation(r => dao.isSuccess(), 'finalResponseAfterMongoUpdation')
  async InsertDataForAuditLog() {
  return {
        collectionName: `Card_Milestone_Benefits_Audit_Log`,
        mode: `insert`,
        query: r => getBody().body.query,
      };
  }

  @Script({ pos: { x: 3699.7047176401156, y: 1224.9739382742807 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'InsertDataForAuditLog')
  async PrepareQueryForAuditLogInsertion() {
    const script = {

        execute: () => {
            let query = getBody("PrepareQueryForInsertionInCardMilestoneBenefitsCollection").body.auditLogQuery;
            return {
                body : {
                    "query" : query
                }
            };

        }
    }
  }
}
