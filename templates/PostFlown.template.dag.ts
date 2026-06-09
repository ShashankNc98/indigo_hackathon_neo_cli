// PLACEHOLDERS (13) — replace with values from your <airline>.config.json
// {{DAG_URL}}                         — e.g. "postFlownBookings"
// {{LOYALTY_CURRENCY_NAME}}           — e.g. "BluChips"
// {{MAX_PAYLOAD_SIZE}}                — e.g. 20
// {{MAX_BILLING_DAYS_LOOKBACK}}       — e.g. 90
// {{CODESHARE_SOURCES}}               — e.g. ['oc']
// {{STAFF_CLASS_CODES}}               — e.g. ['x','g','g2','zh','zl','zm']
// {{STAFF_PAX_TYPES}}                 — e.g. ['stf']
// {{STAFF_SOURCE_ORG_CODES}}          — e.g. ['6eadm','6eapt',...]
// {{VALIDATE_ALIAS_URL}}              — e.g. "http://neo-a.default:3000/api/v1/<org>/execute/ValidateFFN"
// {{TRANSACTION_BULK_ADD_URL}}        — e.g. "https://apac.api.capillarytech.com/v2/transactions/bulk"
// {{MONGO_ALIAS_REJECTION_COLLECTION}}— e.g. "postflown_alias_rejection"
// {{MONGO_PNR_TRANSACTIONS_COLLECTION}}— e.g. "PNR_Transactions"
// {{MONGO_UTILISED_PNR_COLLECTION}}   — e.g. "UtilisedPNR"

import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn, getMultiBody, getOut } = dao;

@Dag({ method: "POST", url: "{{DAG_URL}}" })
class PostFlown {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: -1136, y: -530 } })
  @Relation(r => dao.isSuccess(), 'checkMongo')
  async AppConfigurations() {
    const script = {
      execute: () => {
        const appVersion = "{{APP_VERSION}}";
        logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`);
        const requestBody = getApiRequest()?.body?.[0];
        const externalId = requestBody?.identifierValue;
        const billNumber = requestBody?.billNumber;
        const isgRequestId = `/{{DAG_URL}}_${externalId}_${billNumber}`;
        logger.info(`IsgRequestId : ${JSON.stringify(isgRequestId)}`);
        return { body: { APP_VERSION: appVersion } };
      }
    }
  }

  @Script({ pos: { x: -858, y: -545 } })
  @Relation(r => dao.isSuccess(), 'checkDataInMongo')
  async checkMongo() {
    const script = {
      execute: () => {
        const requestPayload = getApiRequest()?.body;
        const payloadArray = [];
        const billNumberArray = [];
        for (let data of requestPayload) {
          const pnr = data?.extendedFields?.["pnr_number"].trim();
          const firstName = data?.extendedFields?.["booking_first_name"].trim();
          const lastName = data?.extendedFields?.["booking_last_name"].trim();
          const org = data?.customFields?.origin.trim();
          const dest = data?.customFields?.destination.trim();
          const { date: departureDate } = processDateTime(data?.extendedFields?.["departure_date"]);
          const modifiedDepartureDate = departureDate.split("-").join("");
          const billNumber = data?.billNumber;
          const newBillNumber = `${pnr}${firstName}${lastName}${org}${dest}${modifiedDepartureDate}`.split(" ").join("").toLowerCase();
          logger.info(`PNR_KEY: ${newBillNumber}`);
          payloadArray.push({ pnrKey: newBillNumber, billNumber });
          billNumberArray.push(newBillNumber);
        }
        return { body: { query: { "PNR_KEY": { "$in": billNumberArray } } }, payloadArray };
      }
    }
    function processDateTime(datetimeStr) {
      const d = new Date(datetimeStr);
      d.setMinutes(d.getMinutes() + 330);
      return {
        date: `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`,
        time: `${String(d.getUTCHours()).padStart(2,'0')}:${String(d.getUTCMinutes()).padStart(2,'0')}:${String(d.getUTCSeconds()).padStart(2,'0')}`
      };
    }
  }

  @GetMongo({ pos: { x: -544, y: -665 } })
  @Relation(r => dao.isSuccess(), 'filteringRequestObject')
  async checkDataInMongo() {
    return {
      collectionName: `{{MONGO_UTILISED_PNR_COLLECTION}}`,
      query: r => getBody().body.query,
      sort: `{}`,
    };
  }

  @Script({ pos: { x: -264, y: -714 } })
  @Relation(r => dao.isSuccess(), 'PayloadSizeValidationFailureBlock')
  async filteringRequestObject() {
    const script = {
      execute: () => {
        const pnrKey = getOut("checkDataInMongo").map(data => data.PNR_KEY);
        const payloadArray = getBody("checkMongo")?.payloadArray;
        return { payloadArray, pnrKey };
      }
    }
  }

  @Script({ pos: { x: 48, y: -721 } })
  @Relation(r => dao.isSuccess(), 'PayloadValidation')
  async PayloadSizeValidationFailureBlock() {
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
            headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
              "x-cap-custom-entity": errorArray?.[0]?.code, "x-cap-custom-message": errorArray?.[0]?.message } } } };
        }
        return requestBody;
      }
    }
  }

  @Schema({ pos: { x: 308, y: -711 } })
  @Relation(r => dao.isSuccess(), 'ValidationFailureBlock')
  @Relation(r => dao.hasError(), 'ValidationFailureBlock')
  async PayloadValidation() {
    return {
      definitions: [],
      spec: {
        type: "object",
        properties: {
          body: {
            type: "array", minItems: 1, maxItems: {{MAX_PAYLOAD_SIZE}},
            errorMessage: {
              type: "The payload must be an array",
              minItems: "The payload must contain at least one item",
              maxItems: "The payload cannot contain more than {{MAX_PAYLOAD_SIZE}} items"
            },
            items: {
              type: 'object',
              properties: {
                identifierType: { type: 'string', transform: ['toLowerCase'], enum: ['externalid'],
                  errorMessage: { enum: "identifierType must be 'externalId'" } },
                identifierValue: { minLength: 1, errorMessage: { minLength: "identifierValue must not be empty" } },
                source: { minLength: 1, errorMessage: { minLength: "source must not be empty" } },
                type: { minLength: 1, errorMessage: { minLength: "type must not be empty" } },
                billNumber: { minLength: 1, errorMessage: { minLength: "billNumber must not be empty" } },
                billingDate: { minLength: 1, format: "date-time",
                  errorMessage: { minLength: "billingDate must not be empty", format: "billingDate must be ISO 8601" } },
                billAmount: { minLength: 1, errorMessage: { minLength: "billAmount must not be empty" } },
                extendedFields: {
                  type: 'object',
                  properties: {
                    booking_first_name: { type: 'string', minLength: 1, errorMessage: { minLength: "booking_first_name must not be empty" } },
                    booking_last_name:  { type: 'string', minLength: 1, errorMessage: { minLength: "booking_last_name must not be empty" } },
                    pnr_status: { type: 'string', transform: ['trim','toLowerCase'],
                      not: { enum: ['utilized','cancelled','hold'] },
                      minLength: 1, errorMessage: { not: "pnr_status cannot be 'utilized'/'cancelled'/'hold'", minLength: "pnr_status must not be empty" } },
                    pnr_number:     { type: 'string', minLength: 1, errorMessage: { minLength: "pnr_number must not be empty" } },
                    flight_number:  { type: 'string', minLength: 1, errorMessage: { minLength: "flight_number must not be empty" } },
                    departure_date: { type: 'string', format: "date-time", minLength: 1,
                      errorMessage: { minLength: "departure_date must not be empty", format: "departure_date must be ISO 8601" } },
                    arrival_date:   { type: 'string', format: "date-time", minLength: 1,
                      errorMessage: { minLength: "arrival_date must not be empty", format: "arrival_date must be ISO 8601" } },
                    boarding_status:{ minLength: 1, not: { enum: ['3'] },
                      errorMessage: { not: "No show by the passenger", minLength: "boarding_status must not be empty" } },
                    booking_date:   { type: 'string', format: "date", minLength: 1,
                      errorMessage: { minLength: "booking_date must not be empty", format: "booking_date must be yyyy-mm-dd" } },
                    airline_code:   { type: 'string', minLength: 1, errorMessage: { minLength: "airline_code must not be empty" } }
                  },
                  required: ['booking_first_name','booking_last_name','pnr_status','booking_date',
                    'pnr_number','boarding_status','arrival_date','flight_number','departure_date','airline_code'],
                  errorMessage: { required: {
                    booking_first_name: "booking_first_name is missing", booking_last_name: "booking_last_name is missing",
                    pnr_status: "pnr_status is missing", booking_date: "booking_date is missing",
                    arrival_date: "arrival_date is missing", boarding_status: "boarding_status is missing",
                    pnr_number: "pnr_number is missing", flight_number: "flight_number is missing",
                    departure_date: "departure_date is missing", airline_code: "airline_code is missing" } }
                },
                customFields: {
                  type: 'object',
                  properties: {
                    origin:      { type: 'string', minLength: 1, errorMessage: { minLength: "origin must not be empty" } },
                    destination: { type: 'string', minLength: 1, errorMessage: { minLength: "destination must not be empty" } },
                    prod_class_code: { transform: ['trim','toLowerCase'],
                      not: { enum: {{STAFF_CLASS_CODES}} },
                      errorMessage: { not: "Staff travel PNRs not eligible for {{LOYALTY_CURRENCY_NAME}} earning." } },
                    pax_type: { transform: ['trim','toLowerCase'],
                      not: { enum: {{STAFF_PAX_TYPES}} },
                      errorMessage: { not: "Staff travel PNRs not eligible for {{LOYALTY_CURRENCY_NAME}} earning." } },
                    source_org_code: { transform: ['trim','toLowerCase'],
                      not: { enum: {{STAFF_SOURCE_ORG_CODES}} },
                      errorMessage: { not: "Staff duty travel PNRs not eligible for {{LOYALTY_CURRENCY_NAME}} earning." } },
                    transaction_source: { transform: ['trim','toLowerCase'],
                      not: { enum: {{CODESHARE_SOURCES}} },
                      errorMessage: { not: "Codeshare marketed flight PNRs not eligible for {{LOYALTY_CURRENCY_NAME}} earning." } },
                    flight_status: { transform: ['trim','toLowerCase'], enum: ['flown','',null],
                      errorMessage: { enum: "flight_status must be 'flown'" } }
                  },
                  required: ['origin','destination'],
                  errorMessage: { required: { origin: "origin customField is missing", destination: "destination customField is missing" } }
                },
                lineItemsV2: {
                  type: 'array', minItems: 1,
                  items: { type: 'object', properties: {
                    itemCode: { type: 'string', minLength: 1, errorMessage: { minLength: "itemCode must not be empty" } },
                    amount:   { type: 'number', errorMessage: { type: "amount must be a number" } }
                  }, required: ['itemCode','amount'], errorMessage: { required: { itemCode: "itemCode is missing", amount: "amount is missing" } } }
                }
              },
              required: ['identifierType','identifierValue','source','type','billNumber','billingDate','billAmount','extendedFields','customFields','lineItemsV2'],
              errorMessage: { required: {
                identifierType: "identifierType is missing", identifierValue: "identifierValue is missing",
                source: "source is missing", type: "type is missing", billNumber: "billNumber is missing",
                billingDate: "billingDate is missing", billAmount: "billAmount is missing",
                extendedFields: "extendedFields are missing", customFields: "customFields are missing",
                lineItemsV2: "lineItemsV2 are missing" } }
            }
          }
        }
      }
    }
  }

  @Script({ pos: { x: 664, y: -685 } })
  @Relation(r => dao.isSuccess(), 'ExtractValidationSuccessfulTransactions')
  async ValidationFailureBlock() {
    const script = {
      execute: () => {
        let billNumberList = [];
        const input = getApiRequest().body;
        const { payloadArray, pnrKey } = getBody("filteringRequestObject");
        const billNumbers = new Set();
        const duplicates = new Set();
        const DuplicateDataPresentInMongo = [];
        for (let item of payloadArray) {
          if (pnrKey.includes(item.pnrKey)) DuplicateDataPresentInMongo.push(item.billNumber);
        }
        for (let payload of input) {
          const bn = payload.billNumber;
          if (billNumbers.has(bn)) duplicates.add(bn);
          else billNumbers.add(bn);
        }
        const respArr = input.map((item, index) => {
          const bn = item.billNumber;
          billNumberList.push(bn);
          const errors = [];
          const errorMessages = getIn()?.err || [];
          if (duplicates.has(bn)) errors.push({ status: false, code: 400, message: "billNumber must be unique", path: `/body/billNumber` });
          if (DuplicateDataPresentInMongo.includes(bn)) errors.push({ status: false, message: "Duplicate transaction number", code: 604 });
          if (isBillingDateTooOld(item.billingDate)) errors.push({ status: false, code: 6005, message: `billingDate cannot be older than {{MAX_BILLING_DAYS_LOOKBACK}} days`, path: `/body/billingDate` });
          if (item.customFields?.burn_pnr_flag?.trim().toLowerCase().includes('bt-burn')) {
            errors.push({ status: false, code: 6011, message: "Redemption PNRs not eligible for {{LOYALTY_CURRENCY_NAME}} earning.", path: `/body/customFields/burn_pnr_flag` });
          }
          errorMessages.forEach(error => {
            if (error.instancePath.includes(`/body/${index}`)) {
              errors.push({ status: false, code: 400, message: error.message, path: error.instancePath.replace(`/body/${index}`, '/body') });
            }
          });
          return { billNumber: bn, body: item, errors };
        });
        return { status: 200, body: { billNumberList, respArr } };
      }
    }
    function isBillingDateTooOld(billingDate) {
      const billingDateObj = new Date(billingDate);
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - {{MAX_BILLING_DAYS_LOOKBACK}});
      return billingDateObj < cutoff;
    }
  }

  @Script({ pos: { x: 309, y: -419 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().body.validationSuccessTransactions.length == 0), 'ValidationFailureForAllPayloads')
  @Relation(r => dao.isSuccess() && !(dao.getBody().body.validationSuccessTransactions.length == 0), 'prepareValidateAliasApiBlock')
  async ExtractValidationSuccessfulTransactions() {
    const script = {
      execute: () => {
        const input = getBody().body.respArr;
        const validationSuccessTransactions = [];
        const validationSuccessMap = {};
        const validationFailureMap = {};
        for (let payload of input) {
          const bn = payload.billNumber;
          if (payload.errors.length == 0) { validationSuccessTransactions.push(payload.body); validationSuccessMap[bn] = payload; }
          else { validationFailureMap[bn] = payload; }
        }
        return { status: 200, body: { validationSuccessTransactions, validationSuccessMap, validationFailureMap } };
      }
    }
  }

  @Script({ pos: { x: 661, y: -188 } })
  @Relation(r => dao.isSuccess(), 'validateAliasApiCall')
  async prepareValidateAliasApiBlock() {
    const script = {
      execute: () => {
        const requestBody = getBody().body.validationSuccessTransactions;
        return requestBody.map(tx => ({
          headers: (() => { const h = getEffectiveHeaders(); delete h["x-cap-neo-test-variant-id"]; return h; })(),
          queryParams: { "FFN": tx.identifierValue, "Fname": tx.extendedFields.booking_first_name, "lname": tx.extendedFields.booking_last_name }
        }));
      }
    }
  }

  @ApiRequest({ pos: { x: 926, y: -169 } })
  @Relation(r => dao.isSuccess(), 'ExtractTransactionsWithValidAlias')
  @Relation(r => dao.hasError(), 'ValidateAliasErrorBlock')
  async validateAliasApiCall() {
    return { url: `{{VALIDATE_ALIAS_URL}}`, method: `GET` };
  }

  @Script({ pos: { x: 1137, y: -487 } })
  async ExtractTransactionsWithValidAlias() {
    const script = {
      execute: () => {
        const aliasCheckResponse = getMultiBody("validateAliasApiCall");
        const transactionAddPayload = getBody("ExtractValidationSuccessfulTransactions").body.validationSuccessTransactions;
        const aliasValidPayload = [];
        const aliasFailureMap = {};
        const boardingStatusMap = {};
        const transactionMap = {};
        const transactionBillNumberList = [];
        for (let index = 0; index < transactionAddPayload.length; index++) {
          const aliasCheck = aliasCheckResponse[index];
          const transaction = transactionAddPayload[index];
          const { date: depDate, time: depTime } = processDateTime(transaction.extendedFields?.departure_date);
          const { date: arrDate, time: arrTime } = processDateTime(transaction.extendedFields?.arrival_date);
          transaction.extendedFields.departure_date = depDate;
          transaction.extendedFields.arrival_date = arrDate;
          if (!transaction.customFields) transaction.customFields = {};
          transaction.customFields.departure_time = depTime;
          transaction.customFields.arrival_time = arrTime;
          transaction.customFields["tran_posting_date"] = getCurrentISTISOString();
          const warnings = [];
          const bn = transaction.billNumber;
          const payload = { transaction, aliasCheck, warnings };
          transactionMap[bn] = payload;
          transactionBillNumberList.push(bn);
          if (aliasCheck?.status) {
            transaction.extendedFields['pnr_status'] = "Utilized";
            if (Number(transaction.extendedFields.boarding_status) !== 2) { boardingStatusMap[bn] = payload; }
            else { aliasValidPayload.push(transaction); }
          } else { aliasFailureMap[bn] = payload; }
        }
        return { status: 200, body: { transactionMap, transactionBillNumberList, transactionAddPayload: aliasValidPayload, boardingStatusMap, aliasFailureMap } };
      }
    }
    function getCurrentISTISOString() {
      const now = new Date();
      const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
      const pad = n => String(n).padStart(2, "0");
      return `${ist.getFullYear()}-${pad(ist.getMonth()+1)}-${pad(ist.getDate())}T${pad(ist.getHours())}:${pad(ist.getMinutes())}:${pad(ist.getSeconds())}+05:30`;
    }
    function processDateTime(datetimeStr) {
      const d = new Date(datetimeStr);
      d.setMinutes(d.getMinutes() + 330);
      return {
        date: `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`,
        time: `${String(d.getUTCHours()).padStart(2,'0')}:${String(d.getUTCMinutes()).padStart(2,'0')}:${String(d.getUTCSeconds()).padStart(2,'0')}`
      };
    }
  }

  @Script({ pos: { x: 1801, y: -342 } })
  @Relation(r => dao.isSuccess(), 'PutMongoAliasRejectionCollection')
  async AddFailedAliasToRejectionCollection() {
    const script = {
      execute: () => {
        const currentDate = new Date();
        const aliasFailureMap = getIn().body.aliasFailureMap;
        const rejectionPutMongo = [];
        for (const bn in aliasFailureMap) {
          const { transaction, aliasCheck } = aliasFailureMap[bn];
          rejectionPutMongo.push({ bill_number: bn, ffn_number: transaction.identifierValue,
            rejection_reason: aliasCheck.message, alias_check: aliasCheck, request_payload: transaction,
            date_created: currentDate, date_updated: currentDate });
        }
        return { headers: getEffectiveHeaders(), status: 200, body: { rejectionPutMongoQuery: JSON.stringify(rejectionPutMongo) } };
      }
    }
  }

  @PutMongo({ pos: { x: 2057, y: -447 } })
  @Relation(r => dao.isSuccess(), 'PrepareTransactionAddAfterRejectionCollection')
  async PutMongoAliasRejectionCollection() {
    return {
      collectionName: `{{MONGO_ALIAS_REJECTION_COLLECTION}}`,
      mode: `insert`,
      query: r => getBody().body.rejectionPutMongoQuery,
      queryKey: `{}`,
    };
  }

  @Script({ pos: { x: 1760, y: 138 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().body.length === 0), 'BuildFinalRespWithoutTransactionAdd')
  @Relation(r => dao.isSuccess() && !(dao.getBody().body.length === 0), 'TransactionAddBulkApiCall')
  async PreapareTransactionAddPayload() {
    const script = {
      execute: () => {
        const inputPayload = getBody("ExtractTransactionsWithValidAlias").body;
        return { status: 200, headers: { "Content-Type": "application/json", ...getEffectiveHeaders() }, body: JSON.stringify(inputPayload.transactionAddPayload) };
      }
    }
  }

  @ApiRequest({ pos: { x: 2104, y: 377 } })
  @Relation(r => dao.isSuccess(), 'BuildFinalResponseAfterTransactionAdd')
  @Relation(r => dao.hasError(), 'BuildFinalResponseAfterTransactionAddFails')
  async TransactionAddBulkApiCall() {
    return { url: `{{TRANSACTION_BULK_ADD_URL}}`, method: `POST` };
  }

  @Script({ pos: { x: 2531, y: 268 } })
  @Relation(r => dao.isSuccess(), 'BuildQueryForPnrCollectionUpdation')
  async BuildFinalResponseAfterTransactionAdd() {
    const script = {
      execute: () => {
        const mongoFlightStatusUpdation = [];
        const finalResponse = [];
        const transactionAddResponse = getIn();
        const aliasOutput = getBody("ExtractTransactionsWithValidAlias").body;
        const validations = getBody("ExtractValidationSuccessfulTransactions").body;
        const billNumberList = getBody("ValidationFailureBlock").body.billNumberList;
        for (const bn of billNumberList) {
          if (validations.validationSuccessMap[bn]) {
            if (aliasOutput.aliasFailureMap[bn]) {
              const { transaction, aliasCheck } = aliasOutput.aliasFailureMap[bn];
              finalResponse.push({ result: transaction, errors: [{ status: aliasCheck.status, code: aliasCheck.code, message: aliasCheck.message }], warnings: [] });
            } else if (aliasOutput.boardingStatusMap[bn]) {
              finalResponse.push({ result: aliasOutput.boardingStatusMap[bn].transaction, errors: [], warnings: [] });
            } else {
              const match = transactionAddResponse?.response?.find(t => t?.result?.billNumber === bn);
              if (match?.entityId) mongoFlightStatusUpdation.push({ bill_number: bn, flight_status: "FLOWN" });
              finalResponse.push(match);
            }
          } else {
            const { body, errors } = validations.validationFailureMap[bn];
            finalResponse.push({ result: body, errors, warnings: [] });
          }
        }
        if (mongoFlightStatusUpdation.length === 0) {
          return { http: { res: { status: 200, json: { response: finalResponse },
            headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
        }
        return { status: 200, body: { mongoFlightStatusUpdation, response: finalResponse },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } };
      }
    }
  }

  @Script({ pos: { x: 2954, y: 247 } })
  @Relation(r => dao.isSuccess(), 'UpdatePnrTransaction')
  async BuildQueryForPnrCollectionUpdation() {
    const script = {
      execute: () => {
        const currentDate = new Date();
        return getBody("BuildFinalResponseAfterTransactionAdd").body.mongoFlightStatusUpdation.map(item => ({
          body: {
            query: JSON.stringify({ $set: { flight_status: item.flight_status, flight_status_updation_date: currentDate, is_active: false } }),
            queryKey: JSON.stringify({ bill_number: item.bill_number })
          }
        }));
      }
    }
  }

  @PutMongo({ pos: { x: 3309, y: 214 } })
  @Relation(r => dao.isSuccess(), 'dbPayloadForUtilisedPNR')
  async UpdatePnrTransaction() {
    return {
      collectionName: `{{MONGO_PNR_TRANSACTIONS_COLLECTION}}`,
      mode: `update`,
      query: r => getBody().body.query,
      queryKey: r => getBody().body.queryKey,
    };
  }

  @Script({ pos: { x: 3657, y: 208 } })
  @Relation(r => dao.isSuccess(), 'InsertInUtilisedPNR')
  async dbPayloadForUtilisedPNR() {
    const script = {
      execute: () => {
        const response = getBody("BuildFinalResponseAfterTransactionAdd").body.response;
        return response.filter(d => d?.errors?.length === 0).map(r => {
          const pnr = r?.result?.extendedFields?.["pnr_number"].trim();
          const firstName = r?.result?.extendedFields?.["booking_first_name"].trim();
          const lastName = r?.result?.extendedFields?.["booking_last_name"].trim();
          const org = r?.result?.customFields?.origin.trim();
          const dest = r?.result?.customFields?.destination.trim();
          const departureDate = r?.result?.extendedFields?.["departure_date"];
          const newBillNumber = `${pnr}${firstName}${lastName}${org}${dest}${departureDate.split("-").join("")}`.split(" ").join("").toLowerCase();
          return {
            PNR: pnr, PNR_KEY: newBillNumber, billNumber: r?.result?.billNumber,
            ticketNumber: r?.result?.customFields?.eticket, FFN: r?.result?.identifierValue,
            sourceStore: r?.result?.extendedFields?.["store_associate_id"],
            origin: org, destination: dest, departureDate,
            arrivalDate: r?.result?.extendedFields?.["arrival_date"],
            arrivalTime: r?.result?.customFields?.["arrival_time"],
            departureTime: r?.result?.customFields?.["departure_time"],
            firstName, lastName,
            source: r?.result?.customFields?.["retro_or_auto"]?.toLowerCase(),
            passengerid: r?.result?.customFields?.passengerid,
            bookingid: r?.result?.customFields?.bookingid,
            eticket: r?.result?.customFields?.eticket,
            splitFromPnr: r?.result?.customFields?.["split_from_pnr"],
            distanceTravelled: "", deviceId: "",
            creationDate: new Date(), modifiedDate: new Date(), isActive: true
          };
        });
      }
    }
  }

  @PutMongo({ pos: { x: 3955, y: 196 } })
  @Relation(r => dao.isSuccess(), 'BuildFinalResponseAfterPNRTransactionUpdate')
  async InsertInUtilisedPNR() {
    return { collectionName: `{{MONGO_UTILISED_PNR_COLLECTION}}`, mode: `insert`, query: r => getBody() };
  }

  @Script({ pos: { x: 4257, y: 204 } })
  async BuildFinalResponseAfterPNRTransactionUpdate() {
    const script = {
      execute: () => ({
        http: { res: { status: 200, json: { response: getBody("BuildFinalResponseAfterTransactionAdd").body.response },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } }
      })
    }
  }

  @Script({ pos: { x: 749, y: -472 } })
  async ValidationFailureForAllPayloads() {
    const script = {
      execute: () => {
        const { billNumberList } = getBody("ValidationFailureBlock").body;
        const { validationFailureMap } = getBody("ExtractValidationSuccessfulTransactions").body;
        const finalResponse = billNumberList.map(bn => ({ result: validationFailureMap[bn].body, errors: validationFailureMap[bn].errors, warnings: [] }));
        return { http: { res: { status: 400, json: { response: finalResponse },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
            "x-cap-custom-entity": finalResponse[0]?.errors[0]?.code, "x-cap-custom-message": finalResponse[0]?.errors[0]?.message } } } };
      }
    }
  }

  @Script({ pos: { x: 2350, y: -411 } })
  async BuildFinalRespWithoutTransactionAdd() {
    const script = {
      execute: () => {
        const finalResponse = [];
        const aliasOutput = getBody("ExtractTransactionsWithValidAlias").body;
        const validations = getBody("ExtractValidationSuccessfulTransactions").body;
        const { billNumberList } = getBody("ValidationFailureBlock").body;
        for (const bn of billNumberList) {
          if (validations.validationSuccessMap[bn]) {
            if (aliasOutput.aliasFailureMap[bn]) {
              const { transaction, aliasCheck } = aliasOutput.aliasFailureMap[bn];
              finalResponse.push({ result: transaction, errors: [{ status: aliasCheck.status, code: aliasCheck.code, message: aliasCheck.message }], warnings: [] });
            } else if (aliasOutput.boardingStatusMap[bn]) {
              finalResponse.push({ result: aliasOutput.boardingStatusMap[bn].transaction, errors: [], warnings: [] });
            }
          } else {
            finalResponse.push({ result: validations.validationFailureMap[bn].body, errors: validations.validationFailureMap[bn].errors, warnings: [] });
          }
        }
        return { http: { res: { status: 200, json: { response: finalResponse },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }

  @Script({ pos: { x: 2481, y: 543 } })
  async BuildFinalResponseAfterTransactionAddFails() {
    const script = {
      execute: () => {
        const code = getBody()?.code || 500;
        const message = getBody()?.err?.message || "Internal server error";
        const finalResponse = getBody("ExtractValidationSuccessfulTransactions").body.validationSuccessTransactions
          .map(tx => ({ result: tx, errors: [{ status: false, code, message }], warnings: [] }));
        return { http: { res: { status: code, json: { response: finalResponse },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
            "x-cap-custom-entity": code, "x-cap-custom-message": message } } } };
      }
    }
  }

  @Script({ pos: { x: 1246, y: 470 } })
  async ValidateAliasErrorBlock() {
    const script = {
      execute: () => {
        const requestPayload = getApiRequest()?.body;
        const code = getBody()?.code || 500;
        const message = getBody()?.err?.message || "Internal server error";
        const response = requestPayload.map(data => ({ result: data, errors: [{ code, status: false, message }], warnings: [] }));
        return { http: { res: { status: code, json: { response },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
            "x-cap-custom-entity": code, "x-cap-custom-message": message } } } };
      }
    }
  }
}
