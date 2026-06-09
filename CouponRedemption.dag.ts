import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders } = dao;

@Dag({ method: "POST", url: "couponRedemption" })
class CouponRedemption {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: 272.5, y: -83.5 } })
  @Relation(r => dao.isSuccess(), 'preparePayload')
  async AppConfigurations() {
    const script = {

        execute: () => {

           const appVersion = "1.0.1";        
            logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`)
            const developer="Adarsh"
                const branch="PSV-22248"   
                const trigger = "/couponRedemption" 
                const requestBody = this.dao.getApiRequest()?.body;
                const externalId = requestBody?.user?.externalId
                const transactionNumber = requestBody?.transactionNumber
                const isgRequestId = `${trigger}_${externalId}_${transactionNumber}`;
                logger.info(`IsgRequestId : ${JSON.stringify(isgRequestId)}`);

            return {                       
                body:
                {
                    APP_VERSION : appVersion
                }
            };

        }
    }
  }

  @Script({ pos: { x: 592.5, y: -83.5 } })
  @Relation(r => dao.isSuccess(), 'couponRedemptionApiCall')
  async preparePayload() {
    const script = {

        execute: () => {
            const reqBody = getApiRequest().body
            const reqHeaders = getEffectiveHeaders()
            delete reqHeaders["x-cap-neo-test-variant-id"];

            let headers = reqHeaders

            // Array to store metadata with code as key
            let metadataArray = [];

            if (Array.isArray(reqBody?.redemptionRequestList)) {
                reqBody.redemptionRequestList = reqBody.redemptionRequestList.map(
                    (item) => {
                        const { metadata, ...rest } = item;

                        // If metadata exists, store it with its corresponding code
                        if (metadata && item.code) {
                            metadataArray.push({
                                code: item.code,
                                metadata: metadata,
                            });
                        }

                        return rest;
                    }
                );
            }

            // Return both modified body and stored metadata
            return {
                body: JSON.stringify(reqBody),
                headers,
                metadataArray, 
            };
        }
    };
  }

  @ApiRequest({ pos: { x: 912.5, y: -83.5 } })
  @Relation(r => dao.isSuccess(), 'prepareResponse')
  @Relation(r => dao.hasError(), 'errorRedemption')
  async couponRedemptionApiCall() {
  return {
        url: `https://apac.api.capillarytech.com/v2/coupon/bulk/redeem`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 1232.5, y: -83.5 } })
  async prepareResponse() {
    const script = {
      execute: () => {
        const res = getBody();
        const metadataArray = getBody('preparePayload')?.metadataArray || [];
        const metadataMap = {};
        metadataArray.forEach(item => {
          if (item?.code && item.metadata) {
            metadataMap[item.code] = item.metadata;
          }
        });
        logger.info("Metadata map", JSON.stringify(metadataMap));

        // Ensure response exists
        const responseList = res?.response || [];
        responseList.forEach(item => {
          const code = item?.result?.code;
          if (code && metadataMap[code]) {
            // Add metadata inside result
            item.result.metadata = metadataMap[code];
          }
        });

        // Return the updated res directly
        return {
          http: {
            res: {
              status: 200,
              json: res,
              headers: {
                "App-Version": getBody("AppConfigurations")?.body?.APP_VERSION
              }
            }
          }
        };
      }
    };
  }

  @Script({ pos: { x: 1232.5, y: 166.5 } })
  async errorRedemption() {
    const script = {
        execute: () => {
            const errors = getBody();
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
