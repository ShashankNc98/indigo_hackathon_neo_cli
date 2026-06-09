import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders } = dao;

@Dag({ method: "POST", url: "integrations/negativePointsAdjustment" })
class NegativePointsAdjustment {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: 329.5, y: 27.5 } })
  @Relation(r => dao.isSuccess(), 'NegativeAdjustmentPayload')
  async AppConfigurations() {
    const script = {

        execute: () => {

            const appVersion = "1.0.0";
            logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`)
            const developer = "Divya"
            const branch = "PSV-30655"

            return {
                body:
                {
                    APP_VERSION: appVersion
                }
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 807.5, y: -183.5 } })
  @Relation(r => dao.isSuccess(), 'negativePointsAdjustmentResponse')
  @Relation(r => dao.hasError(), 'apiError')
  async negativePointsAdjustment() {
  return {
        url: `{url}`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 548.5, y: -109.5 } })
  @Relation(r => dao.isSuccess(), 'negativePointsAdjustment')
  async NegativeAdjustmentPayload() {
    const script = {
        execute: () => {
            let body = getApiRequest()?.body[0]
            logger.info(`APi Request body, ${body}`)
            let requestHeaders = getEffectiveHeaders();
            let userid = body?.userid
            delete body?.userid
            delete body?.LINE_NO

            return {
                headers : requestHeaders,
                body : JSON.stringify(body),
                pathParams : {
                    url : `https://apac.api.capillarytech.com/v2/customers/${userid}/negativePointsAdjustment?source=INSTORE`
                }
            };
        }
    }
  }

  @Script({ pos: { x: 1109.5, y: -234.5 } })
  async negativePointsAdjustmentResponse() {
    const script = {

        execute: () => {
            logger.info(getBody())
            // return getBody()
            return {
                http: {
                    "res": {
                        status : 200,
                        "json" : getBody()
                    }
                }
            }

        }

    }
  }

  @Script({ pos: { x: 1140.5, y: -28.5 } })
  async apiError() {
    const script = {

        execute: () => {
            logger.info(getBody())
            // return getBody()
            return {
                http: {
                    "res": {
                        status : getBody().code,
                        "json" : getBody()
                    }
                }
            }

        }

    }
  }
}
