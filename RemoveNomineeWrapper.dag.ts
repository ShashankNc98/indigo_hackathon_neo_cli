import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders } = dao;

@Dag({ method: "POST", url: "integrations/internal/deactivate-nominee" })
class RemoveNomineeWrapper {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: -40.5, y: 66.5 } })
  @Relation(r => dao.isSuccess(), 'CreatePayloadForNonFinanceApi')
  async AppConfigurations() {
    const script = {

        execute: () => {

            const appVersion = "1.0";
            logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`)
            const developer = "Sahana"
            const branch = "PSV-30447"
            const trigger = "integrations/internal/deactivate-nominee"
            const requestBody = getApiRequest()?.body[0];
            const identifierType = requestBody?.identifierType;
            const identifierValue = requestBody?.identifierValue;
            const nomineeId = requestBody?.nomineeId;


            return {
                body:
                {
                    APP_VERSION: appVersion,

                }
            };

        }
    }
  }

  @Script({ pos: { x: 279.5, y: 66.5 } })
  @Relation(r => dao.isSuccess(), 'NonFinanceApi')
  async CreatePayloadForNonFinanceApi() {
    const script = {

        execute: () => {
            const requestHeaders = getEffectiveHeaders();
            const headersRequest = getApiRequest("Trigger")?.headers;
            const rawBody = getApiRequest()?.body;
            const item = Array.isArray(rawBody) ? rawBody[0] : rawBody;
            delete headersRequest["x-cap-neo-test-variant-id"]
            logger.info(`headerRequest - body: ${JSON.stringify(headersRequest)}`);

            const body = {
                identifierType: item.identifierType,
                identifierValue: item.identifierValue,
                nomineeId: item.nomineeId
            };

            logger.info(`CreatePayload - body: ${JSON.stringify(body)}`);


            return {
                headers: headersRequest,
                body: JSON.stringify(body)
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 597.5, y: 63.5 } })
  @Relation(r => dao.isSuccess(), 'FinalResponse')
  @Relation(r => dao.hasError(), 'hasErrorAPI')
  async NonFinanceApi() {
  return {
        url: `https://apac.api.capillarytech.com/api_gateway/neo/api/v1/xto6x/execute/deactivate-nominee`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 855.5, y: -35.5 } })
  async FinalResponse() {
    const script = {

        execute: () => {
            const response = getBody("NonFinanceApi");

            logger.info(`response - body: ${JSON.stringify(response)}`);
            return {
                http:{
                    res:{
                        status:200,
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

  @Script({ pos: { x: 915.5, y: 158.5 } })
  async hasErrorAPI() {
    const script = {
        execute: () => {
            const errors = getBody();
            logger.info(`errors - body: ${JSON.stringify(errors)}`);

            const code = errors?.code;
            // Handle 5xx server errors
            if (code >= 500 && code <= 599) {
                return {
                    http: {
                        res: {
                            status: 500,
                            json: errors,
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        },
                    },
                };
            }

            // Handle 4xx client errors
            if (code >= 400 && code <= 499) {
                return {
                    http: {
                        res: {
                            status: code,
                            json: errors,
                            headers: {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                            },
                        },
                    },
                };
            }
        },
    };
  }
}
