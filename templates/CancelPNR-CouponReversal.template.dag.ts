// PLACEHOLDERS (5) — replace with values from your <airline>.config.json
// {{DAG_URL}}                   — e.g. "cancelPNRCouponReversal" (internal, not a public endpoint)
// {{COUPON_REDEEM_URL}}         — e.g. "https://apac.api.capillarytech.com/v2/coupon/redeem"
// {{MONGO_UTILISED_PNR_COLLECTION}} — e.g. "UtilisedPNR"
// {{MONGO_COUPON_REDEMPTION_COLLECTION}} — e.g. "CouponRedemptions"
// {{APP_VERSION}}               — e.g. "1.0.0"

import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn, getOut } = dao;

@Dag({ method: "POST", url: "{{DAG_URL}}" })
class CancelPNRCouponReversal {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: -400, y: 0 } })
  @Relation(r => dao.isSuccess(), 'LoadCouponRedemptionRecords')
  async AppConfigurations() {
    const script = {
      execute: () => {
        const appVersion = "{{APP_VERSION}}";
        logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`);
        return { body: { APP_VERSION: appVersion } };
      }
    }
  }

  @GetMongo({ pos: { x: 0, y: 0 } })
  @Relation(r => dao.isSuccess(), 'CheckForRedemptions')
  async LoadCouponRedemptionRecords() {
    return {
      collectionName: `{{MONGO_COUPON_REDEMPTION_COLLECTION}}`,
      query: r => JSON.stringify({ pnr: getApiRequest().body?.pnr, is_active: true }),
      sort: `{"_id":-1}`,
    };
  }

  @Script({ pos: { x: 300, y: 0 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().body.redemptions.length === 0), 'NoRedemptionsResponse')
  @Relation(r => dao.isSuccess() && (dao.getBody().body.redemptions.length > 0), 'PrepareReversalRequests')
  async CheckForRedemptions() {
    const script = {
      execute: () => {
        const redemptions = getOut() || [];
        logger.info(`Coupon redemptions found: ${redemptions.length}`);
        return { status: 200, body: { redemptions } };
      }
    }
  }

  @Script({ pos: { x: 300, y: -200 } })
  async NoRedemptionsResponse() {
    const script = {
      execute: () => ({
        http: { res: { status: 200, json: { status: true, message: "No active coupon redemptions found for this PNR" },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } }
      })
    }
  }

  @Script({ pos: { x: 600, y: 0 } })
  @Relation(r => dao.isSuccess(), 'ReverseCouponApiCall')
  async PrepareReversalRequests() {
    const script = {
      execute: () => {
        const redemptions = getBody("CheckForRedemptions").body.redemptions;
        return redemptions.map(r => ({
          headers: { "Content-Type": "application/json", ...getEffectiveHeaders() },
          body: JSON.stringify({
            redemptionId: r.redemptionId,
            billNumber: r.billNumber,
            type: "RETURN"
          })
        }));
      }
    }
  }

  @ApiRequest({ pos: { x: 900, y: 0 } })
  @Relation(r => dao.isSuccess(), 'MarkRedemptionsInactive')
  @Relation(r => dao.hasError(), 'ReversalErrorBlock')
  async ReverseCouponApiCall() {
    return { url: `{{COUPON_REDEEM_URL}}`, method: `PUT` };
  }

  @Script({ pos: { x: 1200, y: 0 } })
  @Relation(r => dao.isSuccess(), 'UpdateUtilisedPNRStatus')
  async MarkRedemptionsInactive() {
    const script = {
      execute: () => {
        const redemptions = getBody("CheckForRedemptions").body.redemptions;
        return redemptions.map(r => ({
          body: {
            query: JSON.stringify({ $set: { is_active: false, reversal_date: new Date(), reversal_status: "REVERSED" } }),
            queryKey: JSON.stringify({ redemptionId: r.redemptionId })
          }
        }));
      }
    }
  }

  @PutMongo({ pos: { x: 1500, y: 0 } })
  @Relation(r => dao.isSuccess(), 'FinalReversalResponse')
  async UpdateUtilisedPNRStatus() {
    return {
      collectionName: `{{MONGO_UTILISED_PNR_COLLECTION}}`,
      mode: `update`,
      query: r => getBody().body.query,
      queryKey: r => getBody().body.queryKey,
    };
  }

  @Script({ pos: { x: 1800, y: 0 } })
  async FinalReversalResponse() {
    const script = {
      execute: () => ({
        http: { res: { status: 200, json: { status: true, message: "Coupon reversal completed successfully",
          reversedCount: getBody("CheckForRedemptions").body.redemptions.length },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } }
      })
    }
  }

  @Script({ pos: { x: 900, y: -200 } })
  async ReversalErrorBlock() {
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
