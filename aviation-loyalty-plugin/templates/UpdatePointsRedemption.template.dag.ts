// PLACEHOLDERS (5) — replace with values from your <airline>.config.json
// {{DAG_URL}}                            — e.g. "updatePointsRedemption"
// {{UPDATE_REDEMPTION_URL}}              — e.g. "https://apac.api.capillarytech.com/v2/points/redeem"
// {{CUSTOMER_LOOKUP_URL}}                — e.g. "https://apac.api.capillarytech.com/v2/customers/lookup"
// {{MONGO_POINTS_REDEMPTION_COLLECTION}} — e.g. "PointsRedemptions"
// {{APP_VERSION}}                        — e.g. "1.0.0"

import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn, getOut, getStatus } = dao;

@Dag({ method: "PUT", url: "{{DAG_URL}}" })
class UpdatePointsRedemption {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: -300, y: 0 } })
  @Relation(r => dao.isSuccess(), 'ValidationSchema')
  async AppConfigurations() {
    const script = {
      execute: () => {
        const appVersion = "{{APP_VERSION}}";
        logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`);
        return { body: { APP_VERSION: appVersion } };
      }
    }
  }

  @Schema({ pos: { x: 0, y: 0 } })
  @Relation(r => dao.hasError(), 'handleValidationFailures')
  @Relation(r => dao.isSuccess(), 'PrepareUpdateRedemptionApiRequestBody')
  async ValidationSchema() {
    return {
      definitions: [],
      spec: {
        type: "object",
        properties: {
          body: {
            type: 'object',
            properties: {
              redemptionId: { minLength: 1, errorMessage: { minLength: "redemptionId must not be empty" } },
              billNumber:   { minLength: 1, errorMessage: { minLength: "billNumber must not be empty" } },
              entity: {
                type: 'object',
                properties: {
                  identifierType:  { type: 'string', transform: ['toLowerCase'], enum: ['externalid'],
                    errorMessage: { enum: "identifierType must be 'externalId'" } },
                  identifierValue: { minLength: 1, errorMessage: { minLength: "identifierValue must not be empty" } }
                },
                required: ['identifierType', 'identifierValue'],
                errorMessage: { required: { identifierType: "identifierType is missing", identifierValue: "identifierValue is missing" } }
              },
              points: { type: 'number', minimum: 0, errorMessage: { minimum: "points must be non-negative" } }
            },
            required: ['redemptionId', 'billNumber', 'entity'],
            errorMessage: { required: { redemptionId: "redemptionId is missing", billNumber: "billNumber is missing", entity: "entity is missing" } }
          }
        }
      }
    }
  }

  @Script({ pos: { x: 300, y: 0 } })
  @Relation(r => dao.isSuccess(), 'UpdateRedemptionApiCall')
  async PrepareUpdateRedemptionApiRequestBody() {
    const script = {
      execute: () => {
        const requestPayload = getApiRequest().body;
        logger.info(`Updating points redemption: redemptionId=${requestPayload?.redemptionId}, billNumber=${requestPayload?.billNumber}`);
        return {
          headers: { "Content-Type": "application/json", ...getEffectiveHeaders() },
          body: JSON.stringify({
            redemptionId: requestPayload?.redemptionId,
            billNumber: requestPayload?.billNumber,
            entity: requestPayload?.entity,
            points: requestPayload?.points
          })
        };
      }
    }
  }

  @ApiRequest({ pos: { x: 600, y: 0 } })
  @Relation(r => dao.isSuccess(), 'PrepareCustomerLookupRequest')
  @Relation(r => dao.hasError(), 'UpdateRedemptionErrorBlock')
  async UpdateRedemptionApiCall() {
    return { url: `{{UPDATE_REDEMPTION_URL}}`, method: `PUT` };
  }

  @Script({ pos: { x: 900, y: 0 } })
  @Relation(r => dao.isSuccess(), 'GetCustomerForRedemptionUpdate')
  async PrepareCustomerLookupRequest() {
    const script = {
      execute: () => {
        const requestPayload = getApiRequest().body;
        return {
          headers: getEffectiveHeaders(),
          queryParams: {
            identifierName: requestPayload?.entity?.identifierType,
            identifierValue: requestPayload?.entity?.identifierValue,
            source: "INSTORE"
          }
        };
      }
    }
  }

  @ApiRequest({ pos: { x: 1200, y: 0 } })
  @Relation(r => dao.isSuccess(), 'UpsertPointsRedemptionRecord')
  @Relation(r => dao.hasError(), 'CustomerLookupErrorBlock')
  async GetCustomerForRedemptionUpdate() {
    return { url: `{{CUSTOMER_LOOKUP_URL}}`, method: `GET` };
  }

  @Script({ pos: { x: 1500, y: 0 } })
  @Relation(r => dao.isSuccess(), 'PersistRedemptionUpdate')
  async UpsertPointsRedemptionRecord() {
    const script = {
      execute: () => {
        const requestPayload = getApiRequest().body;
        const updateResponse = getBody("UpdateRedemptionApiCall");
        const customerResponse = getBody();
        const currentDate = new Date();
        const redemptionRecord = {
          redemptionId: requestPayload?.redemptionId,
          billNumber: requestPayload?.billNumber,
          identifierValue: requestPayload?.entity?.identifierValue,
          pointsRedeemed: requestPayload?.points,
          redemptionResponse: updateResponse,
          customerId: customerResponse?.profiles?.[0]?.userId,
          updatedAt: currentDate,
          status: "UPDATED"
        };
        logger.info(`Upserting points redemption for billNumber=${requestPayload?.billNumber}`);
        return { status: 200, body: { redemptionRecord,
          query: JSON.stringify({ $set: redemptionRecord }),
          queryKey: JSON.stringify({ billNumber: requestPayload?.billNumber }) } };
      }
    }
  }

  @PutMongo({ pos: { x: 1800, y: 0 } })
  @Relation(r => dao.isSuccess(), 'SuccessRedemptionUpdateResponse')
  @Relation(r => dao.hasError(), 'MongoUpdateErrorBlock')
  async PersistRedemptionUpdate() {
    return {
      collectionName: `{{MONGO_POINTS_REDEMPTION_COLLECTION}}`,
      mode: `upsert`,
      query: r => getBody().body.query,
      queryKey: r => getBody().body.queryKey,
    };
  }

  @Script({ pos: { x: 2100, y: 0 } })
  async SuccessRedemptionUpdateResponse() {
    const script = {
      execute: () => ({
        http: { res: { status: 200, json: { status: true, message: "Points redemption updated successfully",
          billNumber: getApiRequest().body?.billNumber, redemptionId: getApiRequest().body?.redemptionId },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } }
      })
    }
  }

  @Script({ pos: { x: 0, y: -200 } })
  async handleValidationFailures() {
    const script = {
      execute: () => {
        const errors = getIn()?.err || [];
        return { http: { res: { status: 400, json: { status: false, errors },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 600, y: 200 } })
  async UpdateRedemptionErrorBlock() {
    const script = {
      execute: () => {
        const code = getBody()?.code || 500;
        const message = getBody()?.err?.message || "Points redemption update failed";
        return { http: { res: { status: code, json: { status: false, code, message },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 1200, y: 200 } })
  async CustomerLookupErrorBlock() {
    const script = {
      execute: () => {
        const code = getBody()?.code || 500;
        const message = getBody()?.err?.message || "Customer lookup failed";
        return { http: { res: { status: code, json: { status: false, code, message },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 1800, y: 200 } })
  async MongoUpdateErrorBlock() {
    const script = {
      execute: () => {
        const code = getBody()?.code || 500;
        const message = getBody()?.err?.message || "MongoDB update failed";
        return { http: { res: { status: code, json: { status: false, code, message },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }
}
