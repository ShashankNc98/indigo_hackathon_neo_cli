import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getError } = dao;

@Dag({ method: "POST", url: "v1/finance/spend-report" })
class FinanceSpendReport {
  constructor() {
    this.appConfigurations();
  }

  @Script({ pos: { x: 260, y: -33 } })
  @Relation(r => dao.isSuccess(), 'validateSchema')
  async appConfigurations() {
    const script = {

        execute: () => {

            //Write your code here.
            const appVersion = "1.8";
            logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`)

            return {
                body:
                {
                    APP_VERSION: appVersion
                }
            };

        }
    }
  }

  @Schema({ pos: { x: 573, y: -93 } })
  @Relation(r => dao.isSuccess(), 'cmsApiPayload')
  @Relation(r => dao.hasError(), 'schemaValidationFail')
  async validateSchema() {
    return {
        definitions: [],
        spec: {
            type: "object",
            properties: {
                headers: {
                    type: 'object',
                    properties: {
                        'x-cap-api-oauth-token': {
                            minLength: 1,
                            errorMessage: {
                                minLength: "x-cap-api-oauth-token header must not be empty"
                            }
                        },
                        'x-cap-api-attribution-entity-type': {
                            minLength: 1,
                            errorMessage: {
                                minLength: "x-cap-api-attribution-entity-type must not be empty"
                            }
                        },
                        'x-cap-api-attribution-entity-code': {
                            minLength: 1,
                            errorMessage: {
                                minLength: "x-cap-api-attribution-entity-code must not be empty"
                            }
                        },
                        'partner': {
                            minLength: 1,
                            errorMessage: {
                                minLength: "partner must not be empty"
                            }
                        },
                        'connectplusdataflowid': {
                            minLength: 1,
                            errorMessage: {
                                minLength: "connectPlusDataflowId must not be empty"
                            }
                        }
                    }, required: ['x-cap-api-oauth-token', 'x-cap-api-attribution-entity-type', 'x-cap-api-attribution-entity-code', 'partner', 'connectplusdataflowid'],
                    errorMessage: {
                        required: {
                            'x-cap-api-oauth-token': "x-cap-api-oauth-token request header is missing",
                            'x-cap-api-attribution-entity-type': "x-cap-api-attribution-entity-type request header is missing",
                            'x-cap-api-attribution-entity-code': "x-cap-api-attribution-entity-code request header is missing",
                            'partner': "partner request header is missing",
                            'connectplusdataflowid': "connectPlusDataflowId request header is missing"
                        }
                    }
                },
                body: {
                    type: "array", minItems: 1, maxItems: 1,
                    errorMessage: {
                        type: "The payload must be an array",
                        minItems: "The payload must contain atleast one item",
                        maxItems: "The payload cannot contain more than one item"
                    }
                }
            }
        }
    }
  }

  @Script({ pos: { x: 900, y: 56 } })
  async schemaValidationFail() {
    const script = {

        execute: () => {

            //Write your code here.
            const errors = [];
            const validationErrors = getError("validateSchema")?.err || [];
            validationErrors.forEach((validationError) => {
                const error = {
                    status: false,
                    code: 400,
                    message: validationError.message
                };
                errors.push(error);
            });

            logger.info(`Schema Validation Error: ${JSON.stringify(errors)}`);

            return {
                http: {
                    res: {
                        json: {
                            response: {
                                status: errors[0]
                            }
                        },
                        status: 200,
                        headers: {
                            "App-Version": getBody("appConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 901, y: -186 } })
  @Relation(r => dao.isSuccess(), 'cmsApiCall')
  async cmsApiPayload() {
    const script = {

        execute: () => {

            //Write your code here.
            const apiRequestHeaders = getApiRequest().headers;

            const requestHeaders = {
                ...(getEffectiveHeaders())
            }
            delete requestHeaders["x-cap-neo-test-variant-id"];

            const params = {
                partner: apiRequestHeaders.partner,
                connectPlusDataflowId: apiRequestHeaders["connectplusdataflowid"]
            };

            return {
                headers: requestHeaders,
                queryParams: params
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 1242, y: -178 } })
  @Cachable({ cachable: true, key: r => dao.getBody("cmsApiPayload").queryParams.connectPlusDataflowId, ttl: 600 })
  @Relation(r => dao.isSuccess() && !(dao.getBody().errors?.length), 'mappingBlock')
  @Relation(r => dao.isSuccess() && (dao.getBody().errors?.length), 'handleSchemaNotFoundError')
  @Relation(r => dao.hasError(), 'cmsErrorHandler')
  async cmsApiCall() {
  return {
        url: `http://neo-a.default:3000/api/v1/xto6x/execute/cms`,
        method: `GET`,
      };
  }

  @Script({ pos: { x: 1576, y: -233 } })
  @Relation(r => dao.isSuccess(), 'validateMappedBlock')
  async mappingBlock() {
    const script = {

        execute: () => {

            //Write your code here.
            const requestBody = getApiRequest().body[0];
            const cmsConfigData = getBody("cmsApiCall");
            const requestBodyKeys = Object.keys(requestBody);

            let mappedPayload = {};
            let columnMappings = cmsConfigData.columnMappings.apiAttribute;

            columnMappings.forEach((mapping) => {
                const columnMappingKey = mapping.columnMapping?.trim().toLowerCase();
                const columnMappingValue = mapping.PlatformkeyValue?.trim().toLowerCase();

                const requestBodyKey = requestBodyKeys.find(key => key.trim().toLowerCase() === columnMappingKey);
                if (requestBodyKey) {
                    mappedPayload[columnMappingValue] = requestBody[requestBodyKey]?.trim()?.toLowerCase();
                }
            });

            return {
                mappedPayload
            };

        }
    }
  }

  @Script({ pos: { x: 1578, y: -61 } })
  async handleSchemaNotFoundError() {
    const script = {

        execute: () => {

            //Write your code here.
            const errors = getBody("cmsApiCall").errors;
            logger.info(`Schema not found error: ${JSON.stringify(errors)}`);

            return {
                http: {
                    res: {
                        json: {
                            response: {
                                status: errors[0]
                            }
                        },
                        status: 200,
                        headers: {
                            "App-Version": getBody("appConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 1592, y: 118 } })
  async cmsErrorHandler() {
    const script = {
        execute: () => {
            const cmsResponse = getBody("cmsApiCall");
            const versionHeaders = getBody("appConfigurations")?.headers || {
                "Content-Type": "application/json"
            };

            // Handle 500 error from CMS API
            if (cmsResponse?.code >= 500) {
                logger.error("CMS API Timeout/Error", cmsResponse);

                return buildHttpResponse(500, 502, "Timeout Error", versionHeaders);
            }

            // Default error response
            return buildHttpResponse(200, 9003, "Error occurred while invoking CMS Api", versionHeaders);
        }
    };

    // Helper function to build structured HTTP responses
    const buildHttpResponse = (httpStatus, code, message, headers) => ({
        http: {
            res: {
                json: {
                    response: {
                        status: {
                            status: false,
                            code,
                            message
                        }
                    }
                },
                status: httpStatus,
                headers
            }
        }
    });
  }

  @Script({ pos: { x: 1896, y: -233 } })
  @Relation(r => dao.isSuccess(), 'mongoDbPayload')
  async validateMappedBlock() {
    const script = {

        execute: () => {

            //Write your code here.
            const mappedPayload = getBody("mappingBlock").mappedPayload;
            const mappedPayloadKeys = Object.keys(mappedPayload);

            const keysToValidate = {
                "PRODUCT": 9038,
                "MERCHANT_CATEGORY_CODE": 9053,
                "MERCHANT_CATEGORY": 9054,
                "NETWORK": 9039,
                "TOTAL_SPEND": 9056,
                "ONUS_SPEND": 9057,
                "OFFUS_SPEND": 9058,
                "DOMESTIC_SPEND": 9059,
                "INTERNATIONAL_SPEND": 9060,
                "MONTH": 9061,
                "YEAR": 9062
            }

            const missingKeys = [];

            Object.keys(keysToValidate).forEach((key) => {
                if (mappedPayloadKeys.includes(key.toLowerCase()) === false ||
                    mappedPayload[key.toLowerCase()] == null ||
                    (!(mappedPayload[key.toLowerCase()].trim().length > 0))
                ) {
                    missingKeys.push(key);
                }
            });

            if (missingKeys.length > 0) {
                let errors = [];
                missingKeys.forEach((missingKey) => {
                    const error = {
                        status: false,
                        code: keysToValidate[missingKey],
                        message: `${missingKey} is missing`
                    }
                    errors.push(error);
                });
                logger.info(`Missing keys error: ${JSON.stringify(errors)}`);

                return buildAndReturnError(errors);
            }

            const numberKeysToValidate = {
                "MERCHANT_CATEGORY_CODE": 9063,
                "TOTAL_SPEND": 9064,
                "ONUS_SPEND": 9065,
                "OFFUS_SPEND": 9066,
                "DOMESTIC_SPEND": 9067,
                "INTERNATIONAL_SPEND": 9068,
                "MONTH": 9069,
                "YEAR": 9070
            }
            const fieldsAllowingZeroAndNegative = ["TOTAL_SPEND", "ONUS_SPEND", "OFFUS_SPEND", "DOMESTIC_SPEND", "INTERNATIONAL_SPEND"];

            const invalidIntegerValues = [];
            Object.keys(numberKeysToValidate).forEach((key) => {
                const value = mappedPayload[key.toLowerCase()];
                if (value) {
                    if (!(isValidInteger(value))) {
                        // Regex failure.
                        invalidIntegerValues.push(key);
                    } else {
                        const intValue = parseInt(value);
                        if (!(fieldsAllowingZeroAndNegative.includes(key)) && intValue <= 0) {
                            // Value must be greater than 0 for other fields
                            invalidIntegerValues.push(key);
                        }
                    }
                }
            });
            if (invalidIntegerValues.length > 0) {
                let errors = [];
                invalidIntegerValues.forEach((invalidInteger) => {
                    const error = {
                        status: false,
                        code: numberKeysToValidate[invalidInteger],
                        message: `${invalidInteger} format is incorrect`
                    }
                    errors.push(error);
                });
                logger.info(`Integer value errors: ${JSON.stringify(errors)}`);

                return buildAndReturnError(errors);
            }

            const networkField = "NETWORK";
            const networkValue = mappedPayload[networkField.toLowerCase()].trim();
            if (!(isValidNetwork(networkValue))) {
                const error = [{
                    status: false,
                    code: 9071,
                    message: `${networkField} contains invalid value`
                }];
                logger.info(`Network invalid value errors: ${JSON.stringify(error)}`);

                return buildAndReturnError(error);
            }

            const monthField = "MONTH";
            const monthValue = mappedPayload[monthField.toLowerCase()].trim();
            if (!(isInRange(monthValue, 1, 12))) {
                const error = [{
                    status: false,
                    code: 9072,
                    message: `${monthField} contains invalid value`
                }];
                logger.info(`Month invalid value errors: ${JSON.stringify(error)}`);

                return buildAndReturnError(error);
            }

            const productField = "PRODUCT";
            const productValue = mappedPayload[productField.toLowerCase()].trim();
            if (!(isValidProduct(productValue))) {
                const error = [{
                    status: false,
                    code: 9073,
                    message: `${productField} contains invalid value`
                }];
                logger.info(`Product invalid value errors: ${JSON.stringify(error)}`);

                return buildAndReturnError(error);
            }

            return {
                body: {
                    message: "Mapping validation successfull"
                }
            };

        }
    }

    function isValidInteger(value) {
        return /^[+-]?\d+$/.test(value);
    }

    function isValidNetwork(value) {
        if ((value !== "visa") && (value !== "mastercard") && (value !== "rupay")) {
            return false;
        }
        return true;
    }

    function isValidProduct(value) {
        if ((value !== "base") && (value !== "xl") && (value !== "xxl")) {
            return false;
        }
        return true;
    }

    function isInRange(value, min, max) {
        const num = Number(value);
        return num >= min && num <= max;
    }

    function buildAndReturnError(errors) {
        return {
            http: {
                res: {
                    json: {
                        "response": {
                            "status": errors[0]
                        }
                    },
                    status: 200,
                    headers: {
                        "App-Version": getBody("appConfigurations")?.body.APP_VERSION
                    }
                }
            }
        }
    }
  }

  @Script({ pos: { x: 2216, y: -233 } })
  @Relation(r => dao.isSuccess(), 'insertInMongo')
  async mongoDbPayload() {
    const script = {

        execute: () => {

            //Write your code here.
            const mappedPayload = getBody("mappingBlock").mappedPayload;
            const cmsConfigData = getBody("cmsApiCall");

            const requestHeaders = {
                ...(getEffectiveHeaders())
            }

            const product_value = mappedPayload["product"];
            const merchant_category_code_value = mappedPayload["merchant_category_code"];
            const merchant_category_value = mappedPayload["merchant_category"];
            const network_value = mappedPayload["network"];
            const total_spend_value = mappedPayload["total_spend"];
            const onus_spend_value = mappedPayload["onus_spend"];
            const offus_spend_value = mappedPayload["offus_spend"];
            const domestic_spend_value = mappedPayload["domestic_spend"];
            const international_spend_value = mappedPayload["international_spend"];
            const month_value = mappedPayload["month"];
            const year_value = mappedPayload["year"];
            const store_code_value = requestHeaders['x-cap-api-attribution-entity-code'];
            const concept_code_value = cmsConfigData.conceptCode;
            const zone_code_value = cmsConfigData.zoneCode;

            const payload = {
                "PRODUCT": product_value,
                "MERCHANT_CATEGORY_CODE": parseInt(merchant_category_code_value)    ,
                "MERCHANT_CATEGORY": merchant_category_value,
                "NETWORK": network_value,
                "TOTAL_SPEND": parseInt(total_spend_value),
                "ONUS_SPEND": parseInt(onus_spend_value),
                "OFFUS_SPEND": parseInt(offus_spend_value),
                "DOMESTIC_SPEND": parseInt(domestic_spend_value),
                "INTERNATIONAL_SPEND": parseInt(international_spend_value),
                "MONTH": parseInt(month_value),
                "YEAR": parseInt(year_value),
                "storeCode": store_code_value,
                "conceptCode": concept_code_value,
                "zoneCode": zone_code_value,
                "creationDate": new Date().toISOString(),
                "modifiedDate": new Date().toISOString(),
                "isActive": true
            }

            return {
                body: {
                    query: {
                        payload
                    }
                }
            };

        }
    }
  }

  @PutMongo({ pos: { x: 2536, y: -233 } })
  @Relation(r => dao.isSuccess(), 'finalResponseForDbSuccess')
  @Relation(r => dao.hasError(), 'finalResponseForDbFailure')
  async insertInMongo() {
  return {
        collectionName: `financial_spend_report`,
        mode: `insert`,
        query: r => getBody().body.query.payload,
      };
  }

  @Script({ pos: { x: 2918, y: -303 } })
  async finalResponseForDbSuccess() {
    const script = {

        execute: () => {

            //Write your code here.
            const mongoInsertedId = getBody("insertInMongo").insertedId;
            // const mongoInsertedId = mongoInsertionResponse.insertedId;

            return {
                http: {
                    res: {
                        json: {
                            message: "Record captured successfully",
                            response: {
                                status: {
                                    code: 1100,
                                    message: "Record captured successfully"
                                },
                                requestId: Object.values(mongoInsertedId.buffer).map(byte => byte.toString(16).padStart(2, '0')).join('')
                            }
                        },
                        status: 200,
                        headers: {
                            "App-Version": getBody("appConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            }
        }
    }
  }

  @Script({ pos: { x: 2919, y: -66 } })
  async finalResponseForDbFailure() {
    const script = {

        execute: () => {

            //Write your code here.
            const mongoInsertionResponse = getBody("insertInMongo");

            return {
                http: {
                    res: {
                        json: {
                            message: "Error inserting in Mongo",
                            response: {
                                status: {
                                    code: mongoInsertionResponse.code,
                                    message: mongoInsertionResponse.message
                                }
                            }
                        },
                        status: 200,
                        headers: {
                            "App-Version": getBody("appConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            }

        }
    }
  }
}
