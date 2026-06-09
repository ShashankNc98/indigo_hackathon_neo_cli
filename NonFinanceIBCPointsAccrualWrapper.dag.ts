import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders } = dao;

@Dag({ method: "POST", url: "v1/external/non-finance/points" })
class NonFinanceIBCPointsAccrualWrapper {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: 407, y: 9 } })
  @Relation(r => dao.isSuccess(), 'NonFinanceApi')
  async CreatePayloadForNonFinanceApi() {
    const script = {

        execute: () => {
            const requestHeaders = getEffectiveHeaders()
            const headersRequest = getApiRequest("Trigger")?.headers
            const body = [getApiRequest("Trigger")?.body]
            const headers = {
                "X-CAP-API-OAUTH-TOKEN":requestHeaders["X-CAP-API-OAUTH-TOKEN"] || requestHeaders["x-cap-api-oauth-token"],
                //"X-CAP-API-ATTRIBUTION-entity-TYPE":requestHeaders["X-CAP-API-ATTRIBUTION-entity-TYPE"] || requestHeaders["x-cap-api-attribution-entity-type"],
                //"X-CAP-API-ATTRIBUTION-entity-CODE":requestHeaders["X-CAP-API-ATTRIBUTION-entity-CODE"] || requestHeaders["x-cap-api-attribution-entity-code"],
                // "Content-Type": requestHeaders["Content-Type"] || requestHeaders["content-type"],
                "partner": headersRequest["partner"],
                "connectPlusDataflowId": headersRequest["partnerId"] || headersRequest["partnerid"],
                "till_code": requestHeaders["X-CAP-API-ATTRIBUTION-entity-CODE"] || requestHeaders["x-cap-api-attribution-entity-code"],
            }

            //Write your code here.
            return {
                headers,
                body: JSON.stringify(body)
            };

        }
    }
  }

  @Script({ pos: { x: -69, y: 28 } })
  @Relation(r => dao.isSuccess(), 'validateRequestBody')
  async AppConfigurations() {
    const script = {

        execute: () => {

            const appVersion = "1.2";
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

  @ApiRequest({ pos: { x: 648, y: 17 } })
  @Relation(r => dao.isSuccess(), 'FinalResponse')
  @Relation(r => dao.hasError(), 'hasErrorAPI')
  async NonFinanceApi() {
  return {
        url: `https://apac.api.capillarytech.com/api_gateway/neo/api/v1/xto6x/execute/v1/non-finance/points`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 968, y: 17 } })
  async FinalResponse() {
    const script = {

        execute: () => {

            //Write your code here.
            return {
                http:{
                    res:{
                        status:200,
                        "json":getBody("NonFinanceApi"),
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }

            };

        }
    }
  }

  @Script({ pos: { x: 968, y: 177 } })
  async hasErrorAPI() {
    const script = {

        execute: () => {
            let data =  getBody()
            //Write your code here.
            return data

        }
    }
  }

  @Script({ pos: { x: 167, y: 17 } })
  @Relation(r => dao.isSuccess(), 'CreatePayloadForNonFinanceApi')
  async validateRequestBody() {
    const script = {
      execute: () => {
        // Validation rules with lowercase keys
        const FIELD_VALIDATIONS = [
          { key: "benefit_type", errorCode: 8012, type: "string" },
          { key: "bonus_ibc", errorCode: 8013, type: "integer" },
          { key: "ibc", errorCode: 8014, type: "integer" },
          { key: "city", errorCode: 8015, type: "string" },
          { key: "ffn", errorCode: 8016, type: "string" },
          { key: "currency", errorCode: 8017, type: "string" },
          { key: "partner_transaction_id", errorCode: 8018, type: "string" },
          { key: "transaction_amount", errorCode: 8019, type: "number" },
          { key: "sub_category", errorCode: 8020, type: "string" },
          { key: "business_category", errorCode: 8021, type: "string" },
          { key: "activity_date", errorCode: 8022, type: "string" }
        ];


        const reqBody = getApiRequest()?.body || {};

        for (const { key, errorCode, type } of FIELD_VALIDATIONS) {
          const value =   reqBody[key] || reqBody[key.toUpperCase()];
          if (value !== undefined) {
            if (type === "string" && typeof value !== "string") {
              return createErrorResponse(key, errorCode, `${key} must be a string`);
            }
            if (type === "integer" && (!Number.isInteger(value))) {
              return createErrorResponse(key, errorCode, `${key} must be an integer`);
            }
            if (type === "number" && (typeof value !== "number" || isNaN(value))) {
              return createErrorResponse(key, errorCode, `${key} must be a valid number`);
            }
          }
        }

        return {
          response: {
            status: {
              status: true,
              code: 200,
              message: "Validation passed"
            }
          }
        };
      }
    };

    // Helper to build the error response
    function createErrorResponse(key, errorCode, message) {
      return {
        http: {
          res: {
            status: 200,
            json: {
              response: {
                status: {
                  status: false,
                  code: errorCode,
                  message: message
                }
              }
            },
            headers: {
              "App-Version": getBody("AppConfigurations")?.body?.APP_VERSION
            }
          }
        }
      };
    }
  }
}
