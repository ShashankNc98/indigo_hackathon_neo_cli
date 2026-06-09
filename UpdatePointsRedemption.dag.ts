import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn, getOut, getStatus } = dao;

@Dag({ method: "PUT", url: "updatePointsRedemption" })
class UpdatePointsRedemption {
  constructor() {
    this.AppConfigurations();
  }

  @Schema({ pos: { x: 300, y: -115 } })
  @Relation(r => dao.hasError(), 'handleValidationFailures')
  @Relation(r => dao.isSuccess(), 'PrepareUpdateRedemptionApiRequestBody')
  async ValidationSchema() {
    return {
      definitions: [],
      spec: {
        type: "object",
        "properties": {
          "body": {
            type: 'object',
            properties: {
              redemptionId: {
                minLength: 1,
                "errorMessage": {
                  minLength: "redemptionId must not be empty"
                }
              },
              billNumber: {
                minLength: 1,
                "errorMessage": {
                  minLength: "billNumber must not be empty"
                }
              },
              entity: {
                type: 'object',
                properties: {
                  identifierType: {
                    type: 'string',
                    transform: ['toLowerCase'],
                    enum: ['externalid'],
                    errorMessage: {
                      enum: "The identifierType property must be 'externalId'"
                    }
                  },
                  identifierValue: {
                    minLength: 1,
                    "errorMessage": {
                      minLength: "identifierValue must not be empty"
                    }
                  },
                }, required: ['identifierType', 'identifierValue'],
                errorMessage: {
                  required: {
                    identifierType: "identifierType is missing",
                    identifierValue: "identifierValue is missing"
                  }
                }
              }
            }, required: ['redemptionId', 'billNumber', 'entity'],
            errorMessage: {
              required: {
                redemptionId: "redemptionId is missing",
                billNumber: "billNumber is missing",
                entity: "entity is missing"
              }
            }
          }
        }
      }
    }
  }

  @Script({ pos: { x: 590, y: -212 } })
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

  @Script({ pos: { x: 588.670217669244, y: 0 } })
  @Relation(r => dao.isSuccess(), 'UpdatePointsRedemptionApiCall')
  async PrepareUpdateRedemptionApiRequestBody() {
    const script = {
        execute: () => {
            const requestHeaders = {
                "Content-Type": "application/json",
                ...getEffectiveHeaders()
            };

            const apiRequestBody = { ...getApiRequest().body };

            const fieldsToConvert = new Set([
                "airline_code", "base_cash", "base_yq", "booking_amount", "booking_date",
                "cash_collect", "convenience_fee", "departure_date", "destination_name",
                "dom_or_int", "eticket_no", "fare_amount", "flight_number", "flown_date",
                "interaction_purpose", "mkt_air_code", "mkt_flt_nbr", "origin_name", "pnr",
                "premium_amt", "prod_type", "red_fee", "red_type", "retro_or_auto"
            ]);

            const customFields = apiRequestBody.customFields || {};

            // Single-pass extraction
            const [apiCustomFields, dbCustomFields] = Object.entries(customFields).reduce(
                ([api, db], [key, value]) => {
                    if (fieldsToConvert.has(key)) {
                        const ptsKey = `${key}_pts`;
                        api[ptsKey] = value;
                        db[ptsKey] = value;
                    } else {
                        db[key] = value;
                    }
                    return [api, db];
                },
                [{}, {}] // [apiCustomFields, dbCustomFields]
            );

            return {
                headers: requestHeaders,
                body: JSON.stringify({ ...apiRequestBody, customFields: apiCustomFields }),
                dbPayload: { ...apiRequestBody, customFields: dbCustomFields }
            };
        }
    };
  }

  @ApiRequest({ pos: { x: 943.1570716276026, y: -3.5444241927430085 } })
  @Relation(r => dao.hasError(), 'HandleApiCallFailure')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.errors?.length), 'RedemptionFailure')
  @Relation(r => dao.isSuccess() && !(dao.getBody()?.errors?.length), 'RedemptionSuccess')
  async UpdatePointsRedemptionApiCall() {
  return {
        url: `https://apac.api.capillarytech.com/v2/points/updateRedemption`,
        method: `PUT`,
      };
  }

  @Script({ pos: { x: 1281.115140468769, y: -114.32978233075599 } })
  async RedemptionFailure() {
    const script = {

       execute: () => {

           return {
               http: {
                   "res": {
                       "json": getBody("UpdatePointsRedemptionApiCall"),
                       "status": getStatus("UpdatePointsRedemptionApiCall")
                   }
               }
           };

       }
    }
  }

  @Script({ pos: { x: 1281.329782330756, y: 86.29850417965451 } })
  @Relation(r => dao.isSuccess(), 'BuildQueryToCheckPNR')
  async RedemptionSuccess() {
    const script = {

        execute: () => {
            let requestBody = getBody("PrepareUpdateRedemptionApiRequestBody").dbPayload;
            let apiResponse = getBody("UpdatePointsRedemptionApiCall");

            let finalResponse = {
                ...(apiResponse),
                "customFields": requestBody.customFields
            };

            return {
                body: finalResponse
            };
        }
    }
  }

  @Script({ pos: { x: 1260.1570716276026, y: -307.34542140630674 } })
  async HandleApiCallFailure() {
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

  @Script({ pos: { x: 1601.329782330756, y: 86.29850417965451 } })
  @Relation(r => dao.isSuccess(), 'CheckIfPointRedemptionExists')
  async BuildQueryToCheckPNR() {
    const script = {

        execute: () => {
            let requestBody = getBody("PrepareUpdateRedemptionApiRequestBody").dbPayload;

            let query = {
                redemption_id : requestBody.redemptionId,
                // bill_number : requestBody.billNumber,
                // pnr_number : requestBody.billNumber,
                // ffn : requestBody.entity.identifierValue
            }

            return {
                body : JSON.stringify(query)
            };

        }
    }
  }

  @GetMongo({ pos: { x: 1921.329782330756, y: 86.29850417965451 } })
  @Relation(r => dao.isSuccess(), 'prepareQueryForPointRedemptionInsertion')
  async CheckIfPointRedemptionExists() {
  return {
        collectionName: `Points_Redemption`,
        query: r => getBody().body,
        sort: `{}`,
      };
  }

  @Script({ pos: { x: 2233.958068841166, y: 86.78037207019452 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().body.queryType == 'INSERTION'), 'InsertPointsRedemption')
  @Relation(r => dao.isSuccess() && (dao.getBody().body.queryType == 'UPDATION'), 'UpdatePointsRedemption')
  async prepareQueryForPointRedemptionInsertion() {
    const script = {

        execute: () => {
            let currentDate = new Date();
            let requestBody = getBody("PrepareUpdateRedemptionApiRequestBody").dbPayload;

            let queryResponse = getOut("CheckIfPointRedemptionExists");

            if (queryResponse.length > 0) {
                let updationQueryKey = {
                    redemption_id: requestBody.redemptionId,
                    // bill_number: requestBody.billNumber,
                    // pnr_number: requestBody.billNumber,
                    // ffn: requestBody.entity.identifierValue,
                }
                let updationQuery = JSON.stringify({
                    $set: {
                        custom_fields : requestBody.customFields ? requestBody.customFields : {},
                        "date_updated": currentDate
                    }
                })
                return {
                    body: {
                        "queryType": "UPDATION",
                        "query": updationQuery,
                        "queryKey": updationQueryKey
                    }
                };

            } else {
                let insertionQuery = {
                    redemption_id: requestBody.redemptionId,
                    // bill_number: requestBody.billNumber,
                    pnr_number: requestBody.billNumber,
                    ffn: requestBody.entity.identifierValue,
                    custom_fields : requestBody.customFields ? requestBody.customFields : {},
                    "date_created": currentDate,
                    "date_updated": currentDate
                }
                return {
                    body: {
                        "queryType": "INSERTION",
                        "query": insertionQuery
                    }
                };

            }

        }
    }
  }

  @PutMongo({ pos: { x: 2553.958068841166, y: 84.93744369779711 } })
  @Relation(r => dao.isSuccess(), 'FinalResponseAfterInsertingIntoPointRedemptionCollection')
  async InsertPointsRedemption() {
  return {
        collectionName: `Points_Redemption`,
        mode: `insert`,
        query: r => getBody().body.query,
        queryKey: ``,
      };
  }

  @PutMongo({ pos: { x: 2553.958068841166, y: 264.1833637108857 } })
  @Relation(r => dao.isSuccess(), 'FinalResponseAfterUpdatingPointRedemptionCollection')
  async UpdatePointsRedemption() {
  return {
        collectionName: `Points_Redemption`,
        mode: `update`,
        query: r => getBody().body.query,
        queryKey: r => getBody().body.queryKey,
      };
  }

  @Script({ pos: { x: 2873.958068841166, y: 84.93744369779711 } })
  async FinalResponseAfterInsertingIntoPointRedemptionCollection() {
    const script = {

       execute: () => {

           return {
               http: {
                   "res": {
                       "json": getBody("RedemptionSuccess").body
                   }
               }
           };

       }
    }
  }

  @Script({ pos: { x: 2873.958068841166, y: 264.1833637108857 } })
  async FinalResponseAfterUpdatingPointRedemptionCollection() {
    const script = {

       execute: () => {

           return {
               http: {
                   "res": {
                       "json": getBody("RedemptionSuccess").body
                   }
               }
           };

       }
    }
  }

  @Script({ pos: { x: 320, y: 160 } })
  @Relation(r => dao.isSuccess(), 'ValidationSchema')
  async AppConfigurations() {
    const script = {

        execute: () => {

            const appVersion = "1.0";
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
}
