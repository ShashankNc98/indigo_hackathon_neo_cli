// PLACEHOLDERS (6) — replace with values from your <airline>.config.json
// {{DAG_URL}}                            — e.g. "cancelBookings"
// {{MONGO_UTILISED_PNR_COLLECTION}}      — e.g. "UtilisedPNR"
// {{MONGO_PNR_TRANSACTIONS_COLLECTION}}  — e.g. "PNR_Transactions"
// {{TRANSACTION_RETURN_URL}}             — e.g. "https://apac.api.capillarytech.com/v2/transactions/return"
// {{APP_VERSION}}                        — e.g. "1.0.0"
// {{CANCEL_COUPON_REVERSAL_DAG_URL}}     — internal Neo URL for CancelPNR-CouponReversal DAG

import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn, getOut } = dao;

@Dag({ method: "POST", url: "{{DAG_URL}}" })
class CancelPNROrchestrator {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: -600, y: 0 } })
  @Relation(r => dao.isSuccess(), 'validatePayload')
  async AppConfigurations() {
    const script = {
      execute: () => {
        const appVersion = "{{APP_VERSION}}";
        logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`);
        return { body: { APP_VERSION: appVersion } };
      }
    }
  }

  @Schema({ pos: { x: -300, y: 0 } })
  @Relation(r => dao.hasError(), 'validationFailureBlock')
  @Relation(r => dao.isSuccess(), 'CheckPNRInMongo')
  async validatePayload() {
    return {
      definitions: [],
      spec: {
        type: 'object',
        properties: {
          body: {
            type: 'array', minItems: 1, maxItems: 1,
            errorMessage: { type: 'The payload must be an array', minItems: 'At least one item required', maxItems: 'At most one item allowed' },
            items: {
              type: 'object',
              properties: {
                returnType: { type: 'string', enum: ['FULL'], errorMessage: { enum: 'returnType must be FULL' } },
                type:       { type: 'string', enum: ['RETURN'], errorMessage: { enum: 'type must be RETURN' } },
                extendedFields: {
                  type: 'object',
                  properties: { pnrnumber: { type: 'string', minLength: 1 } },
                  required: ['pnrnumber'], errorMessage: { required: { pnrnumber: 'pnrnumber is missing' } }
                }
              },
              required: ['returnType', 'type', 'extendedFields'],
              errorMessage: { required: { returnType: 'returnType is missing', type: 'type is missing', extendedFields: 'extendedFields are missing' } }
            }
          }
        }
      }
    }
  }

  @Script({ pos: { x: 0, y: 0 } })
  async validationFailureBlock() {
    const script = {
      execute: () => {
        const errors = getIn()?.err || [];
        return { http: { res: { status: 400, json: { errors },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @GetMongo({ pos: { x: 0, y: 200 } })
  @Relation(r => dao.isSuccess(), 'CheckIfPNRExists')
  async CheckPNRInMongo() {
    return {
      collectionName: `{{MONGO_UTILISED_PNR_COLLECTION}}`,
      query: r => JSON.stringify({ PNR: getApiRequest().body[0]?.extendedFields?.pnrnumber }),
      sort: `{"_id":-1}`,
    };
  }

  @Script({ pos: { x: 300, y: 200 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().body.pnrRecords.length === 0), 'PNRNotFoundResponse')
  @Relation(r => dao.isSuccess() && (dao.getBody().body.pnrRecords.length > 0), 'PrepareCancellationRequest')
  async CheckIfPNRExists() {
    const script = {
      execute: () => {
        const pnrRecords = getOut() || [];
        logger.info(`PNR records found: ${pnrRecords.length}`);
        return { status: 200, body: { pnrRecords } };
      }
    }
  }

  @Script({ pos: { x: 600, y: 0 } })
  async PNRNotFoundResponse() {
    const script = {
      execute: () => ({
        http: { res: { status: 404, json: { status: false, code: 404, message: "PNR not found in utilised records" },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } }
      })
    }
  }

  @Script({ pos: { x: 600, y: 200 } })
  @Relation(r => dao.isSuccess(), 'TransactionReturnApiCall')
  async PrepareCancellationRequest() {
    const script = {
      execute: () => {
        const payload = getApiRequest().body[0];
        const pnrRecords = getBody("CheckIfPNRExists").body.pnrRecords;
        const billNumbers = pnrRecords.map(r => r.billNumber);
        logger.info(`Cancelling ${billNumbers.length} transaction(s) for PNR: ${payload.extendedFields.pnrnumber}`);
        return {
          headers: { "Content-Type": "application/json", ...getEffectiveHeaders() },
          body: JSON.stringify(billNumbers.map(bn => ({
            returnType: payload.returnType,
            type: payload.type,
            billNumber: bn,
            extendedFields: payload.extendedFields
          })))
        };
      }
    }
  }

  @ApiRequest({ pos: { x: 900, y: 200 } })
  @Relation(r => dao.isSuccess(), 'RouteCouponReversal')
  @Relation(r => dao.hasError(), 'TransactionReturnErrorBlock')
  async TransactionReturnApiCall() {
    return { url: `{{TRANSACTION_RETURN_URL}}`, method: `POST` };
  }

  @Script({ pos: { x: 1200, y: 200 } })
  @Relation(r => dao.isSuccess(), 'CouponReversalApiCall')
  async RouteCouponReversal() {
    const script = {
      execute: () => {
        const pnrRecords = getBody("CheckIfPNRExists").body.pnrRecords;
        const hasCouponRedemption = pnrRecords.some(r => r.couponCode || r.redemptionId);
        logger.info(`Has coupon redemption: ${hasCouponRedemption}`);
        return { status: 200, body: { hasCouponRedemption, pnrRecords } };
      }
    }
  }

  @ApiRequest({ pos: { x: 1500, y: 200 } })
  @Relation(r => dao.isSuccess(), 'PersistCancellationInMongo')
  @Relation(r => dao.hasError(), 'CouponReversalErrorBlock')
  async CouponReversalApiCall() {
    return { url: `{{CANCEL_COUPON_REVERSAL_DAG_URL}}`, method: `POST` };
  }

  @Script({ pos: { x: 1800, y: 200 } })
  @Relation(r => dao.isSuccess(), 'FinalCancellationResponse')
  async PersistCancellationInMongo() {
    const script = {
      execute: () => {
        const pnrRecords = getBody("CheckIfPNRExists").body.pnrRecords;
        const billNumbers = pnrRecords.map(r => r.billNumber);
        return billNumbers.map(bn => ({
          body: {
            query: JSON.stringify({ $set: { is_active: false, cancellation_date: new Date(), flight_status: "CANCELLED" } }),
            queryKey: JSON.stringify({ billNumber: bn })
          }
        }));
      }
    }
  }

  @Script({ pos: { x: 2100, y: 200 } })
  async FinalCancellationResponse() {
    const script = {
      execute: () => ({
        http: { res: { status: 200, json: { status: true, message: "PNR cancellation processed successfully",
          transactionReturn: getBody("TransactionReturnApiCall") },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } }
      })
    }
  }

  @Script({ pos: { x: 900, y: 0 } })
  async TransactionReturnErrorBlock() {
    const script = {
      execute: () => {
        const code = getBody()?.code || 500;
        const message = getBody()?.err?.message || "Transaction return failed";
        return { http: { res: { status: code, json: { status: false, code, message },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 1500, y: 0 } })
  async CouponReversalErrorBlock() {
    const script = {
      execute: () => {
        const code = getBody()?.code || 500;
        const message = getBody()?.err?.message || "Coupon reversal failed";
        return { http: { res: { status: code, json: { status: false, code, message },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }
}
