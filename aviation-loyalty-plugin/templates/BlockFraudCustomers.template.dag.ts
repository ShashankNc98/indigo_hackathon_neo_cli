// PLACEHOLDERS (8) — replace with values from your <airline>.config.json
// {{DAG_URL}}                           — e.g. "v1/block-fraud-customers"
// {{CUSTOMER_LOOKUP_URL}}               — e.g. "https://apac.api.capillarytech.com/v2/customers/{customerId}"
// {{UPDATE_CUSTOMER_STATUS_URL}}        — e.g. "https://apac.api.capillarytech.com/v2/customers/lookup/status"
// {{EMAIL_API_URL}}                     — e.g. "https://apac.api.capillarytech.com/v1.1/communications/email"
// {{MONGO_EMAIL_BLACKLIST_COLLECTION}}  — e.g. "emailDomainBlacklist"
// {{MONGO_UTILISED_PNR_COLLECTION}}     — e.g. "UtilisedPNR"
// {{FRAUD_CUSTOMER_EMAIL_CONFIG_KEY}}   — Neo KV key for customer-add fraud email config
// {{FRAUD_TRANSACTION_EMAIL_CONFIG_KEY}}— Neo KV key for transaction-add fraud email config

import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getMultiBody, getOut, getValueByKey } = dao;

@Dag({ method: "POST", url: "{{DAG_URL}}" })
class BlockFraudCustomers {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: 251, y: -17 } })
  @Relation(r => dao.isSuccess(), 'StaticConfiguration')
  async AppConfigurations() {
    const script = {
      execute: () => {
        const appVersion = "{{APP_VERSION}}";
        logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`);
        return { body: { APP_VERSION: appVersion } };
      }
    }
  }

  @Script({ pos: { x: 485, y: -30 } })
  @Relation(r => dao.isSuccess(), 'CreateHeadersForCustomerLookup')
  @Relation(r => dao.hasError(), 'error')
  async StaticConfiguration() {
    const script = {
      execute: async () => {
        const body = getApiRequest("Trigger")?.body;
        const eventName = body?.attributes?.eventName;
        const customerAddEmailDetails = await JSON.parse(await getValueByKey("{{FRAUD_CUSTOMER_EMAIL_CONFIG_KEY}}"));
        const transactionAddEmailDetails = await JSON.parse(await getValueByKey("{{FRAUD_TRANSACTION_EMAIL_CONFIG_KEY}}"));
        const pnrAlert = (await getValueByKey("PNR_NOTIFICATION_THRESHOLD"));
        const updateStatusPayload = await JSON.parse(await getValueByKey("FRAUD_CUSTOMER_STATUS_UPDATE"));
        const emailDetails = eventName === "transactionAdded" ? transactionAddEmailDetails : customerAddEmailDetails;
        const literals = {
          "customerAddedEvent": "customerAdded",
          "pointsRedeemedEvent": "pointsRedeemed",
          "transactionAddedEvent": "transactionAdded",
          "source": "INSTORE",
          "identifierName": "externalId",
          "emailIdentifierType": "email",
          "format": "json",
          "updateCustomerStatusApiBody": { "label": updateStatusPayload.label, "reason": updateStatusPayload.reason },
          "to": emailDetails.to,
          "cc": emailDetails.cc,
          "from": emailDetails.from,
          "pnrAlert": Number(pnrAlert),
          "customerAddedSubject": "Fraudulent Alert on Customer Add",
          "pointsRedeemedSubject": "Fraudulent Alert on Points Redemption",
          "transcationAddedSubject": "Multiple PNRs claimed for same departure date",
          "pointRedeemedSubject": "Customer Redemption"
        };
        return { body: literals };
      }
    }
  }

  @Script({ pos: { x: 730, y: -21 } })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.eventName !== dao.getBody("StaticConfiguration")?.body?.transactionAddedEvent), 'GetCustomerDetails')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.eventName === dao.getBody("StaticConfiguration")?.body?.transactionAddedEvent), 'checkMongo')
  async CreateHeadersForCustomerLookup() {
    const script = {
      execute: () => {
        const body = getApiRequest("Trigger")?.body;
        const literals = getBody("StaticConfiguration")?.body;
        const customerId = body?.attributes?.data?.customerIdentifiers?.customerId;
        const eventName = body?.attributes?.eventName;
        const headers = getEffectiveHeaders();
        return { headers, queryParams: { source: literals.source }, pathParams: { customerId }, eventName };
      }
    }
  }

  @ApiRequest({ pos: { x: 1009, y: 35 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().errors?.length), 'GetCustomerErrorHandler')
  @Relation(r => dao.isSuccess() && !(dao.getBody().errors?.length) && (dao.getBody("CreateHeadersForCustomerLookup")?.eventName === dao.getBody("StaticConfiguration")?.body?.customerAddedEvent), 'GetMongo')
  @Relation(r => dao.isSuccess() && !(dao.getBody().errors?.length) && (dao.getBody("CreateHeadersForCustomerLookup")?.eventName === dao.getBody("StaticConfiguration")?.body?.pointsRedeemedEvent), 'ValidateCustomerRegisteredDate')
  @Relation(r => dao.hasError(), 'HasErrorHandler')
  async GetCustomerDetails() {
    return { url: `{{CUSTOMER_LOOKUP_URL}}`, method: `GET` };
  }

  @GetMongo({ pos: { x: 1284, y: -17 } })
  @Relation(r => dao.isSuccess(), 'ValidateEmail')
  async GetMongo() {
    return { collectionName: `{{MONGO_EMAIL_BLACKLIST_COLLECTION}}`, query: `{}`, sort: `{}` };
  }

  @Script({ pos: { x: 1544, y: -38 } })
  @Relation(r => dao.isSuccess(), 'CreateHeadersForUpdateCustomerStatusApi')
  async ValidateEmail() {
    const script = {
      execute: () => {
        const literals = getBody("StaticConfiguration")?.body;
        const emailDomains = getMultiBody("GetMongo")[0]?.emailDomains;
        const identifiers = getBody("GetCustomerDetails")?.profiles[0]?.identifiers;
        const email = identifiers.filter(d => d?.type === literals.emailIdentifierType)[0]?.value;
        const emailDomain = email?.split("@")[1];
        if (emailDomains?.includes(emailDomain)) {
          return { fraudFlag: true };
        }
        return { http: { res: { json: getBody("GetCustomerDetails"), headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 1403, y: 102 } })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.fraudFlag === true), 'CreateHeadersForUpdateCustomerStatusApi')
  @Relation(r => dao.isSuccess(), 'CreatePayloadForEmailCommunicationApi')
  async ValidateCustomerRegisteredDate() {
    const script = {
      execute: () => {
        const epochTime = getApiRequest("Trigger")?.body?.attributes?.createdAt;
        const pointsRedeemedDate = new Date(new Date(epochTime).toISOString().split("T")[0] + "T00:00:00Z");
        const customerCreatedDate = new Date(getBody("GetCustomerDetails")?.profiles[0]?.attribution?.createDate.split("T")[0] + "T00:00:00Z");
        const diffDays = (pointsRedeemedDate - customerCreatedDate) / (1000 * 60 * 60 * 24);
        const fraudFlag = diffDays >= 0 && diffDays <= 2;
        return fraudFlag ? { customerCreatedDate, pointsRedeemedDate, fraudFlag } : { fraudFlag };
      }
    }
  }

  @Script({ pos: { x: 1724, y: 74 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'UpdateCustomerStatusApi')
  async CreateHeadersForUpdateCustomerStatusApi() {
    const script = {
      execute: () => {
        const literals = getBody("StaticConfiguration")?.body;
        const identifiers = getBody("GetCustomerDetails")?.profiles[0]?.identifiers;
        const externalId = identifiers.filter(d => d?.type === literals.identifierName)[0]?.value;
        return { headers: getEffectiveHeaders(), queryParams: { source: literals.source, identifierName: literals.identifierName, identifierValue: externalId },
          body: JSON.stringify({ label: literals.updateCustomerStatusApiBody?.label, reason: literals.updateCustomerStatusApiBody?.reason }) };
      }
    }
  }

  @ApiRequest({ pos: { x: 2037, y: -56 } })
  @Relation(r => dao.isSuccess(), 'CreatePayloadForCommunicationApi')
  @Relation(r => dao.hasError(), 'UpdateStatusHasErrorHandler')
  async UpdateCustomerStatusApi() {
    return { url: `{{UPDATE_CUSTOMER_STATUS_URL}}`, method: `PUT` };
  }

  @Script({ pos: { x: 2411, y: 24 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'EmailCommunicationApi')
  async CreatePayloadForCommunicationApi() {
    const script = {
      execute: () => {
        const literals = getBody("StaticConfiguration")?.body;
        const profile = getBody("GetCustomerDetails")?.profiles[0];
        const identifiers = profile?.identifiers;
        const externalId = identifiers.filter(d => d?.type === literals.identifierName)[0]?.value;
        const email = identifiers.filter(d => d?.type === literals.emailIdentifierType)[0]?.value;
        const eventName = getBody("CreateHeadersForCustomerLookup")?.eventName;
        const requestBody = getApiRequest("Trigger")?.body;
        let payload = {};
        let subject = "";
        if (eventName === literals.customerAddedEvent) {
          payload = { FFN: externalId, firstName: profile?.firstName, lastName: profile?.lastName, registrationDate: profile?.attribution?.createDate, customerEmailId: email };
          subject = literals.customerAddedSubject;
        } else if (eventName === literals.pointsRedeemedEvent) {
          payload = { FFN: externalId, firstName: profile?.firstName, lastName: profile?.lastName, registrationDate: profile?.attribution?.createDate,
            pointsRedeemed: requestBody?.attributes?.data?.totalPointsRedeemed, billNumber: requestBody?.attributes?.data?.redemptionBillNumber };
          subject = literals.pointsRedeemedSubject;
        }
        return { headers: getEffectiveHeaders(), queryParams: { format: literals.format },
          body: JSON.stringify({ root: { email: [{ to: literals.to, cc: literals.cc, from: literals.from, subject, body: payload }] } }) };
      }
    }
  }

  @ApiRequest({ pos: { x: 2728, y: -82 } })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.response?.status?.success === true), 'FinalSuccessResponse')
  @Relation(r => dao.hasError(), 'EmailCommunicationHasErrorHandler')
  async EmailCommunicationApi() {
    return { url: `{{EMAIL_API_URL}}`, method: `POST` };
  }

  // --- Transaction Added event path ---

  @Script({ pos: { x: 629, y: 645 } })
  @Relation(r => dao.isSuccess(), 'checkDataInMongo')
  async checkMongo() {
    const script = {
      execute: () => {
        const data = getApiRequest("Trigger")?.body?.attributes?.data;
        const customerId = data?.customerIdentifiers?.instore?.externalId;
        const departureDate = data?.extendedFields?.find(item => item.key === "departure_date")?.value;
        return { body: { query: { departureDate: departureDate.trim(), FFN: customerId.trim() } } };
      }
    }
  }

  @GetMongo({ pos: { x: 886, y: 691 } })
  @Relation(r => dao.isSuccess(), 'preparePNRSummary')
  async checkDataInMongo() {
    return { collectionName: `{{MONGO_UTILISED_PNR_COLLECTION}}`, query: r => getBody().body.query, sort: `{"_id":-1}` };
  }

  @Script({ pos: { x: 1211, y: 695 } })
  @Relation(r => dao.isSuccess() && dao.getBody()?.uniquePNRs.length >= Number(dao.getBody()?.pnrAlert), 'createPayload')
  async preparePNRSummary() {
    const script = {
      execute: () => {
        const res = getOut() || [];
        const pnrAlert = getBody("StaticConfiguration")?.body?.pnrAlert;
        const uniquePNRsSet = new Set();
        const pnrOriginList = [];
        let firstRecord = null;
        for (const record of res) {
          if (!firstRecord) firstRecord = record;
          uniquePNRsSet.add(record.PNR);
          pnrOriginList.push({ PNR: record.PNR, origin: record.origin, FFN: record.FFN, destination: record.destination,
            departureDate: record.departureDate, arrivalDate: record.arrivalDate,
            departureTime: record.departureTime || 'N/A', arrivalTime: record.arrivalTime || 'N/A' });
        }
        const uniquePNRs = Array.from(uniquePNRsSet);
        if (uniquePNRs.length <= 1) {
          return { http: { res: { json: { status: 200, data: "No duplicate PNR transaction found" },
            headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
        }
        const emailBody = firstRecord ? { FFN: firstRecord.FFN, Fname: firstRecord.firstName, Lname: firstRecord.lastName,
          PNR: firstRecord.PNR, DepartureDate: firstRecord.departureDate, ArrivalDate: firstRecord.arrivalDate } : {};
        return { emailBody, uniquePNRs, pnrAlert, pnrOriginList };
      }
    }
  }

  @Script({ pos: { x: 1509, y: 770 } })
  @Relation(r => dao.isSuccess(), 'CommunicationEmail')
  async createPayload() {
    const script = {
      execute: () => {
        const literals = getBody("StaticConfiguration")?.body;
        const getPNRResponse = getBody("preparePNRSummary");
        return { headers: getEffectiveHeaders(), queryParams: { format: literals.format },
          body: JSON.stringify({ root: { email: [{ to: literals.to, cc: literals.cc, from: literals.from,
            subject: literals?.transcationAddedSubject, body: getPNRResponse?.emailBody }] } }) };
      }
    }
  }

  @ApiRequest({ pos: { x: 1790, y: 741 } })
  @Relation(r => dao.isSuccess(), 'FinalSuccessResponse')
  @Relation(r => dao.hasError(), 'EmailCommunicationHasErrorHandler')
  async CommunicationEmail() {
    return { url: `{{EMAIL_API_URL}}`, method: `POST` };
  }

  // --- Shared terminal blocks ---

  @Script({ pos: { x: 3514, y: -80 } })
  async FinalSuccessResponse() {
    const script = {
      execute: () => ({
        http: { res: { status: 200, json: getBody("EmailCommunicationApi"),
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } }
      })
    }
  }

  @Script({ pos: { x: 3511, y: 92 } })
  async EmailCommunicationHasErrorHandler() {
    const script = {
      execute: () => {
        const errors = getBody();
        const status = errors.code >= 500 ? 500 : errors.code || 500;
        return { http: { res: { status, json: errors, headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 834, y: -208 } })
  async HasErrorHandler() {
    const script = {
      execute: () => {
        const errors = getBody();
        const status = errors.code >= 500 ? 500 : errors.code || 500;
        return { http: { res: { status, json: errors, headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 1295, y: -158 } })
  async GetCustomerErrorHandler() {
    const script = {
      execute: () => ({
        http: { res: { json: getBody("GetCustomerDetails"), headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } }
      })
    }
  }

  @Script({ pos: { x: 2395, y: 164 } })
  async UpdateStatusHasErrorHandler() {
    const script = {
      execute: () => ({
        http: { res: { json: getBody("UpdateCustomerStatusApi"), headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } }
      })
    }
  }

  @Script({ pos: { x: 440, y: 242 } })
  async error() {
    const script = { execute: () => ({ res: getOut() }) }
  }

  @Script({ pos: { x: 2355, y: 346 } })
  @Relation(r => dao.isSuccess(), 'FinalSuccessResponse')
  async CreatePayloadForEmailCommunicationApi() {
    const script = {
      execute: () => {
        const literals = getBody("StaticConfiguration")?.body;
        const profile = getBody("GetCustomerDetails")?.profiles[0];
        const externalId = profile?.identifiers.filter(d => d?.type === literals.identifierName)[0]?.value;
        const requestBody = getApiRequest("Trigger")?.body;
        const body = { root: { email: [{ to: literals.to, cc: literals.cc, from: literals.from, subject: literals.pointRedeemedSubject,
          body: { FFN: externalId, firstName: profile?.firstName, lastName: profile?.lastName, registrationDate: profile?.attribution?.createDate,
            pointsRedeemed: requestBody?.attributes?.data?.totalPointsRedeemed, billNumber: requestBody?.attributes?.data?.redemptionBillNumber } }] } };
        return { headers: getEffectiveHeaders(), queryParams: { format: literals.format }, body: JSON.stringify(body) };
      }
    }
  }
}
