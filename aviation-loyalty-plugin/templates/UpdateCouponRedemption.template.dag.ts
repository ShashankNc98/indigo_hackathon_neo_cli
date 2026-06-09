// PLACEHOLDERS (5) — replace with values from your <airline>.config.json
// {{DAG_URL}}                            — e.g. "update-coupon-redemption"
// {{COUPON_REDEEM_URL}}                  — e.g. "https://apac.api.capillarytech.com/v2/coupon/redeem"
// {{CUSTOMER_LOOKUP_URL}}                — e.g. "https://apac.api.capillarytech.com/v2/customers/{customerId}"
// {{MONGO_COUPON_REDEMPTION_COLLECTION}} — e.g. "CouponRedemptions"
// {{APP_VERSION}}                        — e.g. "1.0.0"

import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getError, getIn, getStatus } = dao;

@Dag({ method: "PUT", url: "{{DAG_URL}}" })
class UpdateCouponRedemption {
  constructor() {
    this.VersionConfig();
  }

  @Script({ pos: { x: -300, y: 0 } })
  @Relation(r => dao.isSuccess(), 'ValidRequestExecution')
  async VersionConfig() {
    const script = {
      execute: () => {
        const appVersion = "{{APP_VERSION}}";
        logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`);
        return { body: { APP_VERSION: appVersion } };
      }
    }
  }

  @Script({ pos: { x: 0, y: 0 } })
  @Relation(r => dao.isSuccess(), 'UpdateExternalCouponRedeemApi')
  async ValidRequestExecution() {
    const script = {
      execute: () => ({
        headers: { "Content-Type": "application/json", ...getEffectiveHeaders() },
        body: JSON.stringify(getApiRequest().body)
      })
    }
  }

  @ApiRequest({ pos: { x: 300, y: 0 } })
  @Relation(r => dao.hasError(), 'FailedExternalCouponRedeemResp')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.success), 'prepareCustomerLookupApiRequest')
  @Relation(r => dao.isSuccess() && !(dao.getBody()?.success), 'NoRedemptionAvailable')
  async UpdateExternalCouponRedeemApi() {
    return { url: `{{COUPON_REDEEM_URL}}`, method: `PUT` };
  }

  @Script({ pos: { x: 600, y: 0 } })
  @Relation(r => dao.isSuccess(), 'GetCustomerDetails')
  async prepareCustomerLookupApiRequest() {
    const script = {
      execute: () => {
        const requestPayload = getApiRequest().body;
        const identifierValue = requestPayload?.entity?.identifierValue;
        const identifierType = requestPayload?.entity?.identifierType;
        logger.info(`Customer lookup for identifierType=${identifierType}, identifierValue=${identifierValue}`);
        return {
          headers: getEffectiveHeaders(),
          queryParams: { identifierName: identifierType, identifierValue, source: "INSTORE" }
        };
      }
    }
  }

  @ApiRequest({ pos: { x: 900, y: 0 } })
  @Relation(r => dao.isSuccess(), 'QueryPrepareForUpsert')
  @Relation(r => dao.hasError(), 'CustomerLookupErrorBlock')
  async GetCustomerDetails() {
    return { url: `{{CUSTOMER_LOOKUP_URL}}`, method: `GET` };
  }

  @Script({ pos: { x: 1200, y: 0 } })
  @Relation(r => dao.isSuccess(), 'UpsertCouponRedemption')
  async QueryPrepareForUpsert() {
    const script = {
      execute: () => {
        const requestPayload = getApiRequest().body;
        const currentDate = new Date();
        const couponRedemptionResponse = getBody("UpdateExternalCouponRedeemApi");
        const redemptions = couponRedemptionResponse.entity?.redemptions || [];
        const customerGetResponse = getBody();
        const pointsSummary = customerGetResponse.pointsSummary;
        const customerSlab = pointsSummary?.slabSNo;
        const couponDetails = {
          redemptionId: requestPayload?.redemptionId,
          billNumber: requestPayload?.billNumber,
          identifierValue: requestPayload?.entity?.identifierValue,
          redemptions,
          customerSlab,
          updatedAt: currentDate
        };
        logger.info(`Upserting coupon redemption for billNumber=${requestPayload?.billNumber}`);
        return { status: 200, body: { couponDetails, query: JSON.stringify({ $set: couponDetails }), queryKey: JSON.stringify({ billNumber: requestPayload?.billNumber }) } };
      }
    }
  }

  @PutMongo({ pos: { x: 1500, y: 0 } })
  @Relation(r => dao.isSuccess(), 'SuccessResponse')
  @Relation(r => dao.hasError(), 'MongoUpsertErrorBlock')
  async UpsertCouponRedemption() {
    return {
      collectionName: `{{MONGO_COUPON_REDEMPTION_COLLECTION}}`,
      mode: `upsert`,
      query: r => getBody().body.query,
      queryKey: r => getBody().body.queryKey,
    };
  }

  @Script({ pos: { x: 1800, y: 0 } })
  async SuccessResponse() {
    const script = {
      execute: () => ({
        http: { res: { status: 200, json: { status: true, message: "Coupon redemption updated successfully",
          billNumber: getApiRequest().body?.billNumber },
          headers: { "App-Version": getBody("VersionConfig")?.body.APP_VERSION } } }
      })
    }
  }

  @Script({ pos: { x: 300, y: -200 } })
  async NoRedemptionAvailable() {
    const script = {
      execute: () => ({
        http: { res: { status: 200, json: { status: false, message: "No redemption available for this request" },
          headers: { "App-Version": getBody("VersionConfig")?.body.APP_VERSION } } }
      })
    }
  }

  @Script({ pos: { x: 300, y: 200 } })
  async FailedExternalCouponRedeemResp() {
    const script = {
      execute: () => {
        const code = getBody()?.code || 500;
        const message = getBody()?.err?.message || "Coupon redeem API failed";
        return { http: { res: { status: code, json: { status: false, code, message },
          headers: { "App-Version": getBody("VersionConfig")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 900, y: 200 } })
  async CustomerLookupErrorBlock() {
    const script = {
      execute: () => {
        const code = getBody()?.code || 500;
        const message = getBody()?.err?.message || "Customer lookup failed";
        return { http: { res: { status: code, json: { status: false, code, message },
          headers: { "App-Version": getBody("VersionConfig")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 1500, y: 200 } })
  async MongoUpsertErrorBlock() {
    const script = {
      execute: () => {
        const code = getBody()?.code || 500;
        const message = getBody()?.err?.message || "MongoDB upsert failed";
        return { http: { res: { status: code, json: { status: false, code, message },
          headers: { "App-Version": getBody("VersionConfig")?.body.APP_VERSION } } } };
      }
    }
  }
}
