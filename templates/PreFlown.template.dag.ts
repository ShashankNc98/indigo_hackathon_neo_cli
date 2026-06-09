// PLACEHOLDERS (12) — replace with values from your <airline>.config.json
// {{DAG_METHOD}}                        — e.g. "POST"
// {{DAG_URL}}                           — e.g. "preFlownBookings"
// {{AIRLINE_NAME}}                      — e.g. "IndiGo"
// {{LOYALTY_CURRENCY_NAME}}             — e.g. "BluChips"
// {{MAX_PAYLOAD_SIZE}}                  — e.g. 20
// {{STAFF_CLASS_CODES}}                 — e.g. ['x','g','g2']
// {{STAFF_PAX_TYPES}}                   — e.g. ['stf']
// {{CODESHARE_SOURCES}}                 — e.g. ['oc']
// {{VALIDATE_ALIAS_URL}}                — e.g. "http://neo-a.default:3000/api/v1/<org>/execute/ValidateFFN"
// {{TRANSACTION_BULK_ADD_URL}}          — e.g. "https://apac.api.capillarytech.com/v2/transactions/bulk"
// {{MONGO_BOOKING_COLLECTION}}          — e.g. "Bookings"
// {{APP_VERSION}}                       — e.g. "1.0.0"

import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn, getMultiBody, getOut } = dao;

@Dag({ method: "{{DAG_METHOD}}", url: "{{DAG_URL}}" })
class PreFlown {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: -800, y: -400 } })
  @Relation(r => dao.isSuccess(), 'PayloadSizeValidation')
  async AppConfigurations() {
    const script = {
      execute: () => {
        const appVersion = "{{APP_VERSION}}";
        logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`);
        const requestBody = getApiRequest()?.body?.[0];
        logger.info(`PreFlown trigger: identifierValue=${requestBody?.identifierValue}, billNumber=${requestBody?.billNumber}`);
        return { body: { APP_VERSION: appVersion } };
      }
    }
  }

  @Script({ pos: { x: -500, y: -400 } })
  @Relation(r => dao.isSuccess(), 'PayloadValidation')
  async PayloadSizeValidation() {
    const script = {
      execute: () => {
        const requestBody = getApiRequest().body;
        const errorArray = [];
        if (!Array.isArray(requestBody)) {
          errorArray.push({ status: false, code: 400, message: "The payload must be an array", path: "/body" });
        } else if (requestBody.length === 0) {
          errorArray.push({ status: false, code: 400, message: "The payload must contain at least one item", path: "/body" });
        } else if (requestBody.length > {{MAX_PAYLOAD_SIZE}}) {
          errorArray.push({ status: false, code: 400, message: "The payload cannot contain more than {{MAX_PAYLOAD_SIZE}} items", path: "/body" });
        }
        if (errorArray.length > 0) {
          return { http: { res: { json: { errors: errorArray }, status: 400,
            headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
        }
        return requestBody;
      }
    }
  }

  @Schema({ pos: { x: -200, y: -400 } })
  @Relation(r => dao.isSuccess(), 'ExtractValidTransactions')
  @Relation(r => dao.hasError(), 'ExtractValidTransactions')
  async PayloadValidation() {
    return {
      definitions: [],
      spec: {
        type: "object",
        properties: {
          body: {
            type: "array", minItems: 1, maxItems: {{MAX_PAYLOAD_SIZE}},
            items: {
              type: 'object',
              properties: {
                identifierType: { type: 'string', transform: ['toLowerCase'], enum: ['externalid'],
                  errorMessage: { enum: "identifierType must be 'externalId'" } },
                identifierValue: { minLength: 1, errorMessage: { minLength: "identifierValue must not be empty" } },
                source:    { minLength: 1, errorMessage: { minLength: "source must not be empty" } },
                type:      { minLength: 1, errorMessage: { minLength: "type must not be empty" } },
                billNumber:{ minLength: 1, errorMessage: { minLength: "billNumber must not be empty" } },
                billingDate:{ minLength: 1, format: "date-time", errorMessage: { format: "billingDate must be ISO 8601" } },
                billAmount: { type: 'number', errorMessage: { type: "billAmount must be a number" } },
                extendedFields: {
                  type: 'object',
                  properties: {
                    booking_first_name: { type: 'string', minLength: 1, errorMessage: { minLength: "booking_first_name must not be empty" } },
                    booking_last_name:  { type: 'string', minLength: 1, errorMessage: { minLength: "booking_last_name must not be empty" } },
                    pnr_number:         { type: 'string', minLength: 1, errorMessage: { minLength: "pnr_number must not be empty" } },
                    flight_number:      { type: 'string', minLength: 1, errorMessage: { minLength: "flight_number must not be empty" } },
                    departure_date:     { type: 'string', format: "date-time", minLength: 1, errorMessage: { format: "departure_date must be ISO 8601" } },
                    arrival_date:       { type: 'string', format: "date-time", minLength: 1, errorMessage: { format: "arrival_date must be ISO 8601" } },
                    booking_date:       { type: 'string', format: "date", minLength: 1, errorMessage: { format: "booking_date must be yyyy-mm-dd" } },
                    airline_code:       { type: 'string', minLength: 1, errorMessage: { minLength: "airline_code must not be empty" } }
                  },
                  required: ['booking_first_name','booking_last_name','pnr_number','flight_number','departure_date','arrival_date','booking_date','airline_code'],
                  errorMessage: { required: { booking_first_name: "booking_first_name is missing", booking_last_name: "booking_last_name is missing",
                    pnr_number: "pnr_number is missing", flight_number: "flight_number is missing",
                    departure_date: "departure_date is missing", arrival_date: "arrival_date is missing",
                    booking_date: "booking_date is missing", airline_code: "airline_code is missing" } }
                },
                customFields: {
                  type: 'object',
                  properties: {
                    origin:      { type: 'string', minLength: 1, errorMessage: { minLength: "origin must not be empty" } },
                    destination: { type: 'string', minLength: 1, errorMessage: { minLength: "destination must not be empty" } },
                    prod_class_code: { transform: ['trim','toLowerCase'], not: { enum: {{STAFF_CLASS_CODES}} },
                      errorMessage: { not: "Staff travel PNRs not eligible for {{LOYALTY_CURRENCY_NAME}} earning." } },
                    pax_type: { transform: ['trim','toLowerCase'], not: { enum: {{STAFF_PAX_TYPES}} },
                      errorMessage: { not: "Staff travel PNRs not eligible for {{LOYALTY_CURRENCY_NAME}} earning." } },
                    transaction_source: { transform: ['trim','toLowerCase'], not: { enum: {{CODESHARE_SOURCES}} },
                      errorMessage: { not: "Codeshare marketed flight PNRs not eligible for {{LOYALTY_CURRENCY_NAME}} earning." } }
                  },
                  required: ['origin','destination'],
                  errorMessage: { required: { origin: "origin is missing", destination: "destination is missing" } }
                },
                lineItemsV2: {
                  type: 'array', minItems: 1,
                  items: { type: 'object', required: ['itemCode','amount'], properties: {
                    itemCode: { type: 'string', minLength: 1 }, amount: { type: 'number' } } }
                }
              },
              required: ['identifierType','identifierValue','source','type','billNumber','billingDate','billAmount','extendedFields','customFields','lineItemsV2']
            }
          }
        }
      }
    }
  }

  @Script({ pos: { x: 100, y: -400 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().body.validTransactions.length === 0), 'AllValidationsFailed')
  @Relation(r => dao.isSuccess() && (dao.getBody().body.validTransactions.length > 0), 'PrepareAliasValidationRequests')
  async ExtractValidTransactions() {
    const script = {
      execute: () => {
        const input = getApiRequest().body;
        const schemaErrors = getIn()?.err || [];
        const validTransactions = [];
        const validationSuccessMap = {};
        const validationFailureMap = {};
        const billNumberList = [];
        input.forEach((item, index) => {
          const bn = item.billNumber;
          billNumberList.push(bn);
          const errors = schemaErrors
            .filter(e => e.instancePath.includes(`/body/${index}`))
            .map(e => ({ status: false, code: 400, message: e.message, path: e.instancePath.replace(`/body/${index}`, '/body') }));
          if (errors.length === 0) { validTransactions.push(item); validationSuccessMap[bn] = item; }
          else { validationFailureMap[bn] = { body: item, errors }; }
        });
        return { status: 200, body: { validTransactions, validationSuccessMap, validationFailureMap, billNumberList } };
      }
    }
  }

  @Script({ pos: { x: 400, y: -400 } })
  @Relation(r => dao.isSuccess(), 'ValidateAliasApiCall')
  async PrepareAliasValidationRequests() {
    const script = {
      execute: () => {
        return getBody().body.validTransactions.map(tx => ({
          headers: getEffectiveHeaders(),
          queryParams: { FFN: tx.identifierValue, Fname: tx.extendedFields.booking_first_name, lname: tx.extendedFields.booking_last_name }
        }));
      }
    }
  }

  @ApiRequest({ pos: { x: 700, y: -400 } })
  @Relation(r => dao.isSuccess(), 'BuildTransactionAddPayload')
  @Relation(r => dao.hasError(), 'AliasValidationErrorBlock')
  async ValidateAliasApiCall() {
    return { url: `{{VALIDATE_ALIAS_URL}}`, method: `GET` };
  }

  @Script({ pos: { x: 1000, y: -400 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().body.aliasValidPayload.length === 0), 'BuildResponseNoValidAlias')
  @Relation(r => dao.isSuccess() && (dao.getBody().body.aliasValidPayload.length > 0), 'TransactionAddBulkApiCall')
  async BuildTransactionAddPayload() {
    const script = {
      execute: () => {
        const aliasCheckResponse = getMultiBody("ValidateAliasApiCall");
        const validTransactions = getBody("ExtractValidTransactions").body.validTransactions;
        const aliasValidPayload = [];
        const aliasFailureMap = {};
        validTransactions.forEach((tx, i) => {
          const aliasCheck = aliasCheckResponse[i];
          if (aliasCheck?.status) { aliasValidPayload.push(tx); }
          else { aliasFailureMap[tx.billNumber] = { transaction: tx, aliasCheck }; }
        });
        return { status: 200, body: { aliasValidPayload, aliasFailureMap,
          transactionAddBody: JSON.stringify(aliasValidPayload) } };
      }
    }
  }

  @ApiRequest({ pos: { x: 1300, y: -400 } })
  @Relation(r => dao.isSuccess(), 'BuildFinalResponse')
  @Relation(r => dao.hasError(), 'BuildErrorResponse')
  async TransactionAddBulkApiCall() {
    return { url: `{{TRANSACTION_BULK_ADD_URL}}`, method: `POST` };
  }

  @Script({ pos: { x: 1600, y: -400 } })
  async BuildFinalResponse() {
    const script = {
      execute: () => {
        const transactionResponse = getIn();
        const { validationSuccessMap, validationFailureMap, billNumberList } = getBody("ExtractValidTransactions").body;
        const { aliasFailureMap } = getBody("BuildTransactionAddPayload").body;
        const finalResponse = billNumberList.map(bn => {
          if (!validationSuccessMap[bn]) return { result: validationFailureMap[bn].body, errors: validationFailureMap[bn].errors, warnings: [] };
          if (aliasFailureMap[bn]) return { result: aliasFailureMap[bn].transaction, errors: [{ status: false, code: aliasFailureMap[bn].aliasCheck.code, message: aliasFailureMap[bn].aliasCheck.message }], warnings: [] };
          const match = transactionResponse?.response?.find(t => t?.result?.billNumber === bn);
          return match || { result: validationSuccessMap[bn], errors: [{ status: false, code: 500, message: "Transaction not found in response" }], warnings: [] };
        });
        return { http: { res: { status: 200, json: { response: finalResponse },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 1600, y: -200 } })
  async BuildErrorResponse() {
    const script = {
      execute: () => {
        const code = getBody()?.code || 500;
        const message = getBody()?.err?.message || "Internal server error";
        const { validTransactions } = getBody("ExtractValidTransactions").body;
        const finalResponse = validTransactions.map(tx => ({ result: tx, errors: [{ status: false, code, message }], warnings: [] }));
        return { http: { res: { status: code, json: { response: finalResponse },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 400, y: -200 } })
  async AllValidationsFailed() {
    const script = {
      execute: () => {
        const { validationFailureMap, billNumberList } = getBody("ExtractValidTransactions").body;
        const finalResponse = billNumberList.map(bn => ({ result: validationFailureMap[bn].body, errors: validationFailureMap[bn].errors, warnings: [] }));
        return { http: { res: { status: 400, json: { response: finalResponse },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 700, y: -200 } })
  async AliasValidationErrorBlock() {
    const script = {
      execute: () => {
        const code = getBody()?.code || 500;
        const message = getBody()?.err?.message || "Alias validation error";
        const { validTransactions } = getBody("ExtractValidTransactions").body;
        const finalResponse = validTransactions.map(tx => ({ result: tx, errors: [{ status: false, code, message }], warnings: [] }));
        return { http: { res: { status: code, json: { response: finalResponse },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 1300, y: -200 } })
  async BuildResponseNoValidAlias() {
    const script = {
      execute: () => {
        const { aliasFailureMap } = getBody("BuildTransactionAddPayload").body;
        const { validationSuccessMap, validationFailureMap, billNumberList } = getBody("ExtractValidTransactions").body;
        const finalResponse = billNumberList.map(bn => {
          if (!validationSuccessMap[bn]) return { result: validationFailureMap[bn].body, errors: validationFailureMap[bn].errors, warnings: [] };
          const { transaction, aliasCheck } = aliasFailureMap[bn] || {};
          return { result: transaction || validationSuccessMap[bn], errors: aliasCheck ? [{ status: false, code: aliasCheck.code, message: aliasCheck.message }] : [], warnings: [] };
        });
        return { http: { res: { status: 200, json: { response: finalResponse },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }
}
