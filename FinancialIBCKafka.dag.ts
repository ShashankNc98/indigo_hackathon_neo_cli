import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getError } = dao;

@Dag({ method: "POST", url: "v1/finance/behavioural-reconciliation" })
class FinancialIBCKafka {
  constructor() {
    this.appConfigurations();
  }

  @Script({ pos: { x: 25, y: -284 } })
  @Relation(r => dao.isSuccess() && (dao.getBody("validateHttpStatusCode")?.httpStatusCode === 200), 'getHeadersForEventById')
  @Relation(r => dao.isSuccess() && (dao.getBody("validateHttpStatusCode")?.httpStatusCode !== 200), 'httpStatusFail')
  async validateHttpStatusCode() {
    const script = {

        execute: () => {

            //Write your code here.
            const payload = getApiRequest()?.body;
            const httpStatusCode = payload?.httpStatusCode;

            if (!httpStatusCode) {
                return {
                    http: {
                        res: {
                            status: 200,
                            "json": {
                                "message": "httpStatusCode Not Found"
                            },
                            "headers": {
                                "App-Version": getBody("appConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            return {
                httpStatusCode,
                payload
            }

        }
    }
  }

  @Script({ pos: { x: -323, y: -212 } })
  @Relation(r => dao.isSuccess(), 'validateHttpStatusCode')
  async appConfigurations() {
    const script = {

        execute: () => {

            //Write your code here.
            const appVersion = "1.1";
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

  @Script({ pos: { x: 275.2807690243235, y: -350.3190628629819 } })
  @Relation(r => dao.isSuccess(), 'getEventApi')
  async getHeadersForEventById() {
    const script = {

        execute: () => {

            //Write your code here.
            const requestPayload = getApiRequest().body;
            const requestId = requestPayload.payload?.response?.requestId;
            if (!requestId) {
                return {
                    http: {
                        res: {
                            status: 200,
                            "json": {
                                "message": "requestId Not Found"
                            },
                            "headers": {
                                "App-Version": getBody("appConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            const headers = getEffectiveHeaders();
            const queryParams = {
                requestId
            }

            return {
                headers,
                queryParams
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 558, y: -414 } })
  @Relation(r => dao.isSuccess() && (dao.getBody("getEventApi").status), 'handleEventStatus')
  @Relation(r => dao.isSuccess() && !(dao.getBody("getEventApi").status), 'handlingMissingEventStatus')
  @Relation(r => dao.hasError(), 'errorHandler')
  async getEventApi() {
  return {
        url: `https://incrm.cc.capillarytech.com/v2/events/log`,
        method: `GET`,
      };
  }

  @Script({ pos: { x: 895.5836613702173, y: -294.362029126212 } })
  async handlingMissingEventStatus() {
    const script = {

        execute: () => {

            //Write your code here.

            return {
                http: {
                    res: {
                        status: 500,
                        json: {
                            message: "Event Not Processed"
                        },
                        headers: {
                            "App-Version": getBody("appConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 874.4265602770452, y: -455.5687178343239 } })
  @Relation(r => dao.isSuccess(), 'prepareMongoQuery')
  async handleEventStatus() {
    const script = {

        execute: () => {

            const requestPayload = getApiRequest().body;

            const apiResponse = getBody("getEventApi");
            const status = apiResponse?.status;
            const message = apiResponse?.message;

            if (apiResponse?.status === "EVENT_SUCCESS") {
                return {
                    http: {
                        res: {
                            status: 200,
                            "json": {
                                "message": "Event Executed Successfully"
                            },
                            "headers": {
                                "App-Version": getBody("appConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            requestPayload["payload"]["response"]["eventStatus"] = status;
            requestPayload["payload"]["response"]["eventMessage"] = message;
            requestPayload["httpStatusCode"] = 400;

            const creationDate = new Date().toISOString();
            requestPayload["creationDate"] = creationDate;
            requestPayload["modifiedDate"] = creationDate;
            requestPayload["isActive"] = true;

            return {
                putQuery: requestPayload
            };

        }
    }
  }

  @Script({ pos: { x: 266.30074405312865, y: -23.41288951736226 } })
  @Relation(r => dao.isSuccess(), 'prepareMongoQuery')
  async httpStatusFail() {
    const script = {

        execute: () => {

            const currentDate = new Date().toISOString();
            //Write your code here.
            const payload = getApiRequest("Trigger")?.body;
            payload["creationDate"]= currentDate;
            payload["modifiedDate"]= currentDate;
            payload["isActive"]= true;

            return {
                putQuery: payload
            };

        }
    }
  }

  @Script({ pos: { x: 1430.5522633358519, y: 11.394411844233048 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'putMongo')
  async prepareMongoQuery() {
    const script = {

        execute: () => {

            //Write your code here.
            const queryData = getBody().putQuery;
            return {
                queryData
            };

        }
    }
  }

  @PutMongo({ pos: { x: 1750.5522633358519, y: -10.605588155766952 } })
  @Relation(r => dao.isSuccess(), 'mongoAddSuccess')
  @Relation(r => dao.hasError(), 'mongoAddFail')
  async putMongo() {
  return {
        collectionName: `Partner_Transaction`,
        mode: `insert`,
        query: r => getBody().queryData,
      };
  }

  @Script({ pos: { x: 2070.5723920089195, y: -39.79993779410995 } })
  async mongoAddSuccess() {
    const script = {

        execute: () => {

            //Write your code here.
            return {
                http: {
                    res: {
                        status: 200,
                        json: {
                            message: "Data pushed to Mongo successfully"
                        },
                        headers: {
                            "App-Version": getBody("appConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 2074.5493878111283, y: 82.71816009515328 } })
  async mongoAddFail() {
    const script = {

        execute: () => {

            //Write your code here.
            return {
                http: {
                    res: {
                        status: 500,
                        json: getError(),
                        headers: {
                            "App-Version": getBody("appConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 864.6934371051805, y: -96.91772762780892 } })
  @Relation(r => dao.isSuccess(), 'prepareMongoQuery')
  async errorHandler() {
    const script = {

        execute: () => {

            const payload = getApiRequest("Trigger")?.body;
            const errorResponse=getError("getEventApi");

            payload["payload"]["response"]["eventStatus"]="Api Failed"
            payload["payload"]["response"]["eventMessage"]=errorResponse?.err?.message
            payload["httpStatusCode"]=errorResponse?.code

            payload["creationDate"]= new Date().toISOString()
            payload["modifiedDate"]= new Date().toISOString()
            payload["isActive"]= true


            return {
                putQuery:payload
            };

        }
    }
  }
}
