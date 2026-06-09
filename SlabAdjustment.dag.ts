import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getValueByKey } = dao;

@Dag({ method: "POST", url: "slabAdjustment" })
class SlabAdjustment {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: 33.55436197916666, y: -0.1865234375 } })
  @Relation(r => dao.isSuccess(), 'StaticConfiguration')
  async AppConfigurations() {
    const script = {

        execute: () => {

            const appVersion = "1.0.1";
            logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`)
            const developer = "Adarsh"
            const branch = "PSV-27203"
            const trigger = "/slabAdjustment" 
            const requestBody = getApiRequest()?.body?.[0];
            const externalId = requestBody?.externalId
            const currentTierNumber = requestBody?.currentTierNumber
            const manualSlabActionValidityUpto = requestBody?.manualSlabActionValidityUpto
            const isgRequestId = `${trigger}_${externalId}_${currentTierNumber}_${manualSlabActionValidityUpto}`;
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

  @Script({ pos: { x: 614.5, y: -31.5 } })
  @Relation(r => dao.isSuccess(), 'slabAdjustmentAPICall')
  async prepareManualSlabAdjustmentAPICall() {
    const script = {
        execute: () => {
            const returnError = (code, message, appVersion) => {
                return {
                    http: {
                        res: {
                            status: 200,
                            json: {
                                status: "false",
                                code,
                                message,
                            },
                            headers: {
                                "App-Version": appVersion,
                            },
                        },
                    },
                };
            };
            let req = getApiRequest();
            const literals = getBody("StaticConfiguration")?.body;
            const appVersion = getBody("AppConfigurations")?.body?.APP_VERSION || "N/A";

            let reqHeaders = { ...req?.headers };
            delete reqHeaders["x-cap-neo-test-variant-id"];
            // Extract incoming body
            let reqBody = req?.body?.[0];

            let eventName = reqBody?.eventName;
            if (!eventName) {
                return returnError(400, "eventName field is missing", appVersion);
            }


            let externalId = reqBody?.externalId;
            if (!externalId || externalId.toString().trim() === "") {
                return returnError(401, "externalId field is missing or invalid", appVersion);
            }


            let source = literals?.source || "INSTORE";
            let identifierName = literals?.identifierName || "externalId";

            // Query Params
            let queryParams = {
                identifierName: identifierName,
                identifierValue: externalId,
                source: source,
            };

            // Body payload
            let apiPayload = {
                programId: literals?.programId,
                slabAction: literals?.slabAction,
                manualSlabActionValidity: literals?.manualSlabActionValidity,
                manualSlabActionValidityUpto: reqBody?.manualSlabActionValidityUpto,
                reassessTierOnNextActivity: literals?.reassessTierOnNextActivity,
                slabChangeReason: reqBody?.slabChangeReason,
            };

            return {
                queryParams,
                headers: reqHeaders,
                body:JSON.stringify(apiPayload)
            };
        }
    };
  }

  @ApiRequest({ pos: { x: 934.5, y: -31.5 } })
  @Relation(r => dao.isSuccess(), 'SuccessAPIResponse')
  @Relation(r => dao.hasError(), 'errorResponse')
  async slabAdjustmentAPICall() {
  return {
        url: `https://apac.api.capillarytech.com/v2/slab/manualSlabAdjustment`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 1254.5, y: -31.5 } })
  @Relation(r => dao.isSuccess(), 'tierAPI')
  async SuccessAPIResponse() {
    const script = {

        execute: () => {
            let response = getBody()

            let req = getApiRequest();
            const literals = getBody("StaticConfiguration")?.body;
            let auth = req?.headers?.authorization
            let reqHeaders = {};
            reqHeaders['x-api-key'] = literals?.tierAPIKey
            reqHeaders['Authorization'] = auth
            delete reqHeaders["x-cap-neo-test-variant-id"];
            delete reqHeaders["x-cap-api-oauth-token"];
            let reqBody = req?.body?.[0];
            let eventName = reqBody?.eventName
            let externalId = reqBody?.externalId;
            let currentTierNumber = reqBody?.currentTierNumber
            let orgId = literals?.orgId
            if (Number(orgId) !== 2248) {
                return {
                    http: {
                        res: {
                            status: 200,
                            json: {
                                data : response,
                                status  :"true"
                            },
                        },
                    },
                };
            }
            // errors is an array
            let errors = response?.errors;

            if (Array.isArray(errors) && errors.length > 0) {
                const err = errors[0];  // take first error object

                return {
                    http: {
                        res: {
                            status: 200,
                            json: {
                                status: "false",
                                code: err?.code,
                                message: err?.message,
                            },
                        },
                    },
                };
            }
            let url = literals?.tierEventUrl
            let tierApiPayload = {
                createdAt: Date.now(),   // OR response.createdAt if coming from upstream
                data: {
                    customerIdentifiers: {
                        instore: {
                            externalId: externalId
                        }
                    },
                    currentTierNumber: currentTierNumber
                },
                eventName: eventName,
                orgId: orgId,
                eventId: crypto.randomUUID() // generates UUID like your sample
            };

            return {
                url,
                headers: reqHeaders,
                body: JSON.stringify(tierApiPayload)
            }
        }
    }
  }

  @Script({ pos: { x: 1254.5, y: 128.5 } })
  async errorResponse() {
    const script = {

        execute: () => {
            return getBody()
            const errors = getBody();
            const code = errors?.code;

            const appVersion = getBody("AppConfigurations")?.body?.APP_VERSION || "N/A";

            // 5xx server errors
            if (code >= 500 && code <= 599) {
                return {
                    http: {
                        res: {
                            status: 500,
                            json: {
                                status: "false",
                                code,
                                message: errors?.message,
                            },
                            headers: {
                                "App-Version": appVersion,
                            },
                        },
                    },
                };
            }

            // 4xx client errors
            if (code >= 400 && code <= 499) {
                return {
                    http: {
                        res: {
                            status: code,
                            json: {
                                status: "false",
                                code,
                                message: errors?.message,
                            },
                            headers: {
                                "App-Version": appVersion,
                            },
                        },
                    },
                };
            }
        }
    }
  }

  @Script({ pos: { x: 321.6748046875, y: -9.378255208333314 } })
  @Relation(r => dao.isSuccess(), 'prepareManualSlabAdjustmentAPICall')
  async StaticConfiguration() {
    const script = {

        execute: async () => {
            const body = getApiRequest("Trigger")?.body
            const slabAdjustmentDetails = await JSON.parse(await getValueByKey("SLAB_ADJUSTMENT_DETAILS"))
            const slabTierApiKey = await getValueByKey("SLABTIERAPIKEY")
            const orgId = await getValueByKey("ORG_ID")
            const tierEventUrl = await getValueByKey("SLAB_TIER_EVENT_URL")
            const literals = {
                "programId" : slabAdjustmentDetails.programId,
                "slabAction" : slabAdjustmentDetails.slabAction,
                "manualSlabActionValidity" : slabAdjustmentDetails.manualSlabActionValidity,
                "reassessTierOnNextActivity" : slabAdjustmentDetails.reassessTierOnNextActivity,
                "tierEventUrl" : tierEventUrl,
                "orgId" : orgId,
                "tierAPIKey" : slabTierApiKey,
            }
            return {
                body: literals
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 1574.5, y: -31.5 } })
  @Relation(r => dao.isSuccess(), 'successResponseAPI')
  @Relation(r => dao.hasError(), 'errorResponseApi')
  async tierAPI() {
  return {
        url: r => dao.getBody().url,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 1894.5, y: -31.5 } })
  async successResponseAPI() {
    const script = {

        execute: () => {
            let res = getBody()
            //Write your code here.
            return res;

        }
    }
  }

  @Script({ pos: { x: 1894.5, y: 128.5 } })
  async errorResponseApi() {
    const cleanMessage = (msg) => {
        if (!msg) return "";
        return String(msg)
            .replace(/<\/?[^>]+(>|$)/g, "")      // strip HTML
            .replace(/&nbsp;/g, " ")
            .replace(/\s+/g, " ")
            .trim();
    };

    const script = {
        execute: () => {
            const error = getBody();   
            const code = error?.code;
            const message = cleanMessage(error?.message);

            if (!code) {
                return;
            }

            // 5xx server errors
            if (code >= 500 && code <= 599) {
                return {
                    http: {
                        res: {
                            status: 500,
                            json: {
                                status: "false",
                                code,
                                message,
                            }
                        }
                    }
                };
            }

            // 4xx client errors
            if (code >= 400 && code <= 499) {
                return {
                    http: {
                        res: {
                            status: code,
                            json: {
                                status: "false",
                                code,
                                message,
                            }
                        }
                    }
                };
            }
        }
    };
  }
}
