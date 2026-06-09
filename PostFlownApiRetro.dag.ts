import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn, getMultiBody, getOut } = dao;

@Dag({ method: "POST", url: "retro/postFlownBookings" })
class PostFlownApiRetro {
  constructor() {
    this.AppConfigurations();
  }

  @Schema({ pos: { x: -387.09338298317294, y: -412.3452773364686 } })
  @Relation(r => dao.isSuccess(), 'ValidationFailureBlock')
  @Relation(r => dao.hasError(), 'ValidationFailureBlock')
  async PayloadValidation() {
    return {
      definitions: [],
      spec: {
        type: "object",
        "properties": {
          "body": {
            "type": "array", "minItems": 1, "maxItems": 20,
            "errorMessage": {
              type: "The payload must be an array",
              minItems: "The payload must contain atleast one item",
              maxItems: "The payload cannot contain more than 20 items"
            },
            "items": {
              type: 'object',
              properties: {
                identifierType: {
                  type: 'string',
                  transform: ['toLowerCase'],
                  enum: ['externalid'],
                  errorMessage: {
                    enum: "The identifierType property must be 'externalId'"
                  }
                },
                identifierValue: {
                  minLength: 1,
                  "errorMessage": {
                    minLength: "identifierValue must not be empty"
                  }
                },
                source: {
                  minLength: 1,
                  "errorMessage": {
                    minLength: "source must not be empty"
                  }
                },
                type: {
                  minLength: 1,
                  "errorMessage": {
                    minLength: "type must not be empty"
                  }
                },
                billNumber: {
                  minLength: 1,
                  "errorMessage": {
                    minLength: "billNumber must not be empty"
                  }
                },
                billingDate: {
                  minLength: 1,
                  format: "date-time",
                  "errorMessage": {
                    minLength: "billingDate must not be empty",
                    format: "billingDate must be in ISO 8601 format"
                  }
                },
                billAmount: {
                  minLength: 1,
                  "errorMessage": {
                    minLength: "billAmount must not be empty"
                  }
                },
                extendedFields: {
                  type: 'object',
                  properties: {
                    booking_first_name: {
                      type: 'string',
                      minLength: 1,
                      "errorMessage": {
                        minLength: "booking_first_name must not be empty"
                      }
                    },
                    transaction_source: {
                      transform: ['trim', 'toLowerCase'],
                      not: {
                        enum: ['oc'],
                      },
                      "errorMessage": {
                        not: "Codeshare marketed flight PNRs not eligible for earning IndiGo BluChips."
                      }
                    },
                    flight_status: {
                      transform: ['trim', 'toLowerCase'],
                      enum: ['flown', '', null],
                      "errorMessage": {
                        enum: "flight_status must be 'flown'"
                      }
                    },
                    pnr_status: {
                      type: 'string',
                      transform: ['trim', 'toLowerCase'],
                      not: {
                        enum: ['utilized', 'cancelled', 'hold'],
                      },
                      minLength: 1,
                      "errorMessage": {
                        not: "pnr_status cannot be 'utilized'",
                        minLength: "pnr_status must not be empty"
                      }
                    },
                    booking_last_name: {
                      type: 'string',
                      minLength: 1,
                      "errorMessage": {
                        minLength: "booking_last_name must not be empty"
                      }
                    },
                    boarding_status: {
                      not: {
                        enum: ['3'],
                      },
                      minLength: 1,
                      "errorMessage": {
                        not: "No show by the passenger",
                        minLength: "boarding_status must not be empty"
                      }
                    },
                    booking_date: {
                      type: 'string',
                      format: "date",
                      minLength: 1,
                      "errorMessage": {
                        minLength: "booking_date must not be empty",
                        format: "booking_date format must be yyyy-mm-dd"
                      }
                    },
                    arrival_date: {
                      type: 'string',
                      format: "date-time",
                      minLength: 1,
                      "errorMessage": {
                        minLength: "arrival_date must not be empty",
                        format: "arrival_date must be in ISO 8601 format"
                      }
                    },
                    pnr_number: {
                      type: 'string',
                      minLength: 1,
                      "errorMessage": {
                        minLength: "pnr_number must not be empty"
                      }
                    },
                    flight_number: {
                      type: 'string',
                      minLength: 1,
                      "errorMessage": {
                        minLength: "flight_number must not be empty"
                      }
                    },
                    departure_date: {
                      type: 'string',
                      format: "date-time",
                      minLength: 1,
                      "errorMessage": {
                        minLength: "departure_date must not be empty",
                        format: "departure_date must be in ISO 8601 format"
                      }
                    },
                    airline_code: {
                      type: 'string',
                      minLength: 1,
                      "errorMessage": {
                        minLength: "airline_code must not be empty"
                      }
                    }
                  }, required: ['booking_first_name', 'booking_last_name', 'pnr_status', 'booking_date', 'arrival_date', 'pnr_number', 'boarding_status', 'flight_number', 'departure_date', 'airline_code'],
                  errorMessage: {
                    required: {
                      booking_first_name: "booking_first_name extendedField is missing",
                      booking_last_name: "booking_last_name extendedField is missing",
                      pnr_status: "pnr_status extendedField is missing",
                      booking_date: "booking_date extendedField is missing",
                      arrival_date: "arrival_date extendedField is missing",
                      boarding_status: "boarding_status extendedField is missing",
                      pnr_number: "pnr_number extendedField is missing",
                      flight_number: "flight_number extendedField is missing",
                      departure_date: "departure_date extendedField is missing",
                      airline_code: "airline_code extendedField is missing"
                    }
                  }
                },
                customFields: {
                  type: 'object',
                  properties: {
                    origin: {
                      type: 'string',
                      minLength: 1,
                      "errorMessage": {
                        minLength: "origin must not be empty"
                      }
                    },
                    destination: {
                      type: 'string',
                      minLength: 1,
                      "errorMessage": {
                        minLength: "destination must not be empty"
                      }
                    },
                    prod_class_code: {
                      transform: ['trim', 'toLowerCase'],
                      not: {
                        enum: ['x', 'g', 'g2', 'zh','zl','zm'],
                      },
                      "errorMessage": {
                        not: "Staff travel PNRs not eligible for earning IndiGo BluChips.",
                      }

                    },
                    pax_type: {
                      transform: ['trim', 'toLowerCase'],
                      not: {
                        enum: ['stf'],
                      },
                      "errorMessage": {
                        not: "Staff travel PNRs not eligible for earning IndiGo BluChips.",
                      }
                    },
                    source_org_code: {
                      transform: ['trim', 'toLowerCase'],
                      not: {
                        enum: ['6eadm', '6eapt', '6eapttr', '6eceo', '6ecoo', '6ecldca', '6ecomm', '6enetplan', '6esales', '6ecorpaf', '6eeng', '6efin', '6eflo', '6eflosim', '6efsf', '6ehfltops', '6ehinfgflt', '6ehraocs', '6ehrr', '6eifly', '6eift', '6eiit', '6elegal', '6eatr', '6elthq', '6eocc', '6esh1', '6eslt', '6eigcargo', '6ecargoint', '6eigsst', '6edigital', 'iarn0001', 'iars0002', 'igcargo', 'igeltd', '6emdoffice', '6egrc', '6esmartlin', '6ecustexp', '6efinaaf', 'gbaptbc', 'gbaptsm', 'xocorp', 'igmt01', 'igmt02', 'igsh01', 'igsh02', 'igsh03', '6esh2', 'gstss116', '9900001', '6einfadt',
                        '6ehapt', '6exocorp', '6epass', '6ehrcsr','6eigthr', '6eduty', '6e duty', '6ecld', '6eloyalty', '6enorse', '6eppa', '6edgtl', '6eaptbc', '6eaptsm', '6echqcorp']
                      },
                      "errorMessage": {
                        not: "Staff duty travel PNRs not eligible for earning IndiGo BluChips."
                      }
                    }
                  }, required: ['origin', 'destination'],
                  errorMessage: {
                    required: {
                      origin: "origin customField is missing",
                      destination: "destination customField is missing"
                    }
                  }
                },
                lineItemsV2: {
                  type: 'array',
                  minItems: 1,
                  items: {
                    type: 'object',
                    properties: {
                      itemCode: {
                        type: 'string',
                        minLength: 1,
                        errorMessage: {
                          minLength: "itemCode must not be empty"
                        }
                      },
                      amount: {
                        type: 'number',
                        minLength: 1,
                        errorMessage: {
                          minLength: "amount must not be empty"
                        }
                      }
                    },
                    required: ['itemCode', 'amount'],
                    errorMessage: {
                      required: {
                        itemCode: "itemCode is missing",
                        amount: "amount is missing"
                      }
                    }
                  }
                }
              },
              required: ['identifierType', 'identifierValue', 'source', 'type', 'billNumber', 'billingDate', 'billAmount', 'extendedFields', 'customFields', 'lineItemsV2'],
              errorMessage: {
                required: {
                  identifierType: "identifierType is missing",
                  identifierValue: "identifierValue is missing",
                  source: "source is missing",
                  type: "type is missing",
                  billNumber: "billNumber is missing",
                  billingDate: "billingDate is missing",
                  billAmount: "billAmount is missing",
                  extendedFields: "extendedFields are missing",
                  customFields: "customFields are missing",
                  lineItemsV2: "lineItemsV2 are missing"
                }
              }
            }
          }
        }
      }
    }
  }

  @Script({ pos: { x: 2.1481968705743384, y: -484.9277787540708 } })
  @Relation(r => dao.isSuccess(), 'ExtractValidationSuccessfulTransactions')
  async ValidationFailureBlock() {
    const script = {
      execute: () => {
        let billNumberList = [];
        let input = getApiRequest().body;
        let { payloadArray, pnrKey } = getBody("filteringRequestObject")
        const billNumbers = new Set();
        const duplicates = new Set();
        const DuplicateDataPresentInMongo = []

        for (let i = 0; i < payloadArray.length; i++) {
          const item = payloadArray[i];
          if (pnrKey.includes(item.pnrKey)) {
            DuplicateDataPresentInMongo.push(item.billNumber);
          }
        }
        for (let payload of input) {
          let billNumber = payload.billNumber;
          if (billNumbers.has(billNumber)) {
            duplicates.add(billNumber);
          } else {
            billNumbers.add(billNumber);
          }

        }
        const respArr = input.map((item, index) => {
          let billNumber = item.billNumber;
          billNumberList.push(billNumber);
          const errors = [];
          const errorMessages = getIn()?.err || [];
          // Check for unique billNumber
          if (duplicates.has(billNumber)) {
            errors.push({
              status: false,
              code: 400,
              message: "billNumber must be unique",
              path: `/body/billNumber`
            });
          }
          if (DuplicateDataPresentInMongo.includes(billNumber)) {
            errors.push({
              "status": false,
              "message": "Duplicate transaction number",
              "code": 604
            });
          }
          let billDate = item.billingDate;
          if (isBillingDateMoreThan90Days(billDate)) {
            errors.push({
              status: false,
              code: 6005,
              message: "billingDate can't be less than 90 days",
              path: `/body/billingDate`
            });
          }

          if (item.customFields.burn_pnr_flag) {
            let burnPnrFlagValue = item.customFields.burn_pnr_flag.trim().toLowerCase();
            if (burnPnrFlagValue.includes('bt-burn')) {
              errors.push({
                status: false,
                code: 6011,
                message: "Redemption PNRs not eligible for earning IndiGo BluChips.",
                path: `/body/customFields/burn_pnr_flag`
              })
            }
          }

          errorMessages.forEach(error => {
            if (error.instancePath.includes(`/body/${index}`)) {
              let instancePath = error.instancePath.replace(`/body/${index}`, '/body');

              let errorType = instancePath.split('/').pop();
              let statusCode = 400;
              let message = error.message

              if (errorType === 'source_org_code') {
                if (item.customFields.source_org_code.trim().toLowerCase() === '9900001') {
                  statusCode = 6008;
                  message = 'Not eligible for earning IndiGo BluChips.'
                } else {
                  statusCode = 6001
                }
              }

              if (errorType === 'pax_type') {
                statusCode = 6003
              }

              if (errorType === 'transaction_source') {
                statusCode = 6006
              }

              if (errorType === 'boarding_status') {
                statusCode = 6009
              }

              if (errorType === 'prod_class_code' && item.customFields.prod_class_code.trim().toLowerCase() === 'g') {
                statusCode = 6002;
                message = 'Group booking fares not eligible for earning IndiGo BluChips.'
              }

              if (errorType === 'prod_class_code' && item.customFields.prod_class_code.trim().toLowerCase() === 'x') {
                statusCode = 6003;
                message = 'Staff Travel / MyId Travel PNRs not eligible for earning IndiGo BluChips.'
              }

              if (errorType === 'prod_class_code' && item.customFields.prod_class_code.trim().toLowerCase() === 'g2') {
                statusCode = 6008;
                message = 'Not eligible for earning IndiGo BluChips.'
              }

              if (errorType === 'pnr_status' && item.extendedFields.pnr_status.trim().toLowerCase() === 'cancelled') {
                statusCode = 6007;
                message = 'Cancelled PNR not eligible for Retro Claim.'
              }

              if (errorType === 'pnr_status' && item.extendedFields.pnr_status.trim().toLowerCase() === 'hold') {
                statusCode = 6010;
                message = 'Hold PNRs status are not eligible'
              }

              errors.push({
                status: false,
                code: statusCode,
                message: message,
                path: instancePath,
              });
            }
          });
          return {
            billNumber: billNumber,
            body: item,
            errors: errors,
          };
        });
        return {
          status: 200,
          body: {
            "billNumberList": billNumberList,
            "respArr": respArr
          }
        }
      }
    }
    function isBillingDateMoreThan90Days(billingDate) {
      const billingDateObj = new Date(billingDate);
      const currentDate = new Date();
      const ninetyDays = new Date();
      ninetyDays.setDate(currentDate.getDate() - 90);
      if (billingDateObj < ninetyDays) return true
      else return false;
    }
  }

  @ApiRequest({ pos: { x: 2104.301215873735, y: 377.04175451040635 } })
  @Relation(r => dao.isSuccess(), 'BuildFinalResponseAfterTransactionAdd')
  @Relation(r => dao.hasError(), 'BuildFinalResponseAfterTransactionAddFails')
  async TransactionAddBulkApiCall() {
  return {
        url: `https://apac.api.capillarytech.com/v2/transactions/bulk`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 2531.4417291936415, y: 268.1850277719414 } })
  @Relation(r => dao.isSuccess(), 'BuildQueryForPnrCollectionUpdation')
  async BuildFinalResponseAfterTransactionAdd() {
    const script = {

        execute: () => {
            let mongoFlightStatusUpdation = [];
            let finalResponse = [];
            let transactionAddResponse = getIn();

            let outputFromAliasValidationBlock = getBody("ExtractTransactionsWithValidAlias").body;
            let boardingStatusMap = outputFromAliasValidationBlock.boardingStatusMap;

            let aliasFailureMap = outputFromAliasValidationBlock.aliasFailureMap;

            let outputFromValidationFailureBlock = getBody("ValidationFailureBlock").body;

            let bodyWithValidations = getBody("ExtractValidationSuccessfulTransactions").body;
            let validationSuccessMap = bodyWithValidations.validationSuccessMap;
            let validationFailureMap = bodyWithValidations.validationFailureMap;
            let billNumberList = outputFromValidationFailureBlock.billNumberList;

            let transactionMap = outputFromAliasValidationBlock.transactionMap;
            for (const billNumber of billNumberList) {
                if (validationSuccessMap[billNumber]) {
                    if (aliasFailureMap[billNumber]) {
                        let failedTransaction = aliasFailureMap[billNumber].transaction;
                        let aliasCheck = aliasFailureMap[billNumber].aliasCheck;
                        let errors = [
                            {
                                status: aliasCheck.status,
                                code: aliasCheck.code,
                                message: aliasCheck.message
                            }
                        ]
                        let warnings = transactionMap[billNumber].warnings;
                        let aliasCheckFailureResponse = {
                            "result": failedTransaction,
                            "errors": errors,
                            "warnings": warnings
                        }
                        finalResponse.push(aliasCheckFailureResponse);
                    } else if (boardingStatusMap[billNumber]) {
                        let boardingStatusTransaction = boardingStatusMap[billNumber].transaction;
                        let errors = [];
                        let boardingStatusResponse = {
                            "result": boardingStatusTransaction,
                            "errors": errors,
                            "warnings": []
                        }
                        finalResponse.push(boardingStatusResponse);
                    } else {
                        let matchingTransaction = transactionAddResponse?.response?.find(matchingTransaction => matchingTransaction?.result?.billNumber === billNumber);
                        const successresponse = validationSuccessMap[billNumber]
                        if (matchingTransaction?.entityId) {
                            let mongoUpdation = {
                                "bill_number": billNumber,
                                "flight_status": "FLOWN"
                            }
                            mongoFlightStatusUpdation.push(mongoUpdation)
                        }

                        let warnings = transactionMap[billNumber].warnings;
                        let warning = warnings[0];
                        if (warning) {
                            matchingTransaction?.warnings?.push(warning);
                        }
                        matchingTransaction["result"]["billNumber"] = successresponse?.body?.billNumber?.trim()
                        matchingTransaction["result"]["customFields"]["eticket"] = successresponse?.body?.customFields?.eticket?.trim();
                        matchingTransaction["result"]["customFields"]["origin"] = successresponse?.body?.customFields?.origin?.trim();
                        matchingTransaction["result"]["customFields"]["destination"] = successresponse?.body?.customFields?.destination?.trim();
                        matchingTransaction["result"]["customFields"]["retro_or_auto"] = successresponse?.body?.customFields?.["retro_or_auto"]?.toLowerCase();
                        matchingTransaction["result"]["customFields"]["passengerid"] = successresponse?.body?.customFields?.passengerid?.trim();
                        matchingTransaction["result"]["customFields"]["bookingid"] = successresponse?.body?.customFields?.bookingid?.trim();
                        matchingTransaction["result"]["customFields"]["split_from_pnr"] = successresponse?.body?.customFields?.["split_from_pnr"]?.trim();
                        matchingTransaction["result"]["identifierValue"] = successresponse?.body?.identifierValue.trim();
                        matchingTransaction["result"]["extendedFields"]["store_associate_id"] = successresponse?.body?.extendedFields?.["store_associate_id"];
                        matchingTransaction["result"]["extendedFields"]["departure_date"] = successresponse?.body?.extendedFields?.["departure_date"]?.trim();
                        matchingTransaction["result"]["extendedFields"]["booking_first_name"] = successresponse?.body?.extendedFields?.["booking_first_name"]?.trim();
                        matchingTransaction["result"]["extendedFields"]["booking_last_name"] = successresponse?.body?.extendedFields?.["booking_last_name"]?.trim();
                        matchingTransaction["result"]["extendedFields"]["arrival_date"] = successresponse?.body?.extendedFields?.["arrival_date"]?.trim();
                        if (matchingTransaction?.result?.customFields?.tran_posting_date) {
                            delete matchingTransaction?.result?.customFields.tran_posting_date;
                        }
                        finalResponse.push(matchingTransaction)
                    }
                } else {
                    let failedTransaction = validationFailureMap[billNumber].body;
                    let errors = validationFailureMap[billNumber].errors;
                    let validationFailureResponse = {
                        "result": failedTransaction,
                        "errors": errors,
                        "warnings": []
                    }
                    finalResponse.push(validationFailureResponse);
                }
            }
            let firstErrorCode =
                finalResponse?.[0]?.errors?.[0]?.code || 500; // default to 500 if missing
            let firstErrorMessage =
                finalResponse?.[0]?.errors?.[0]?.message ||
                "Unknown Error";

            if (mongoFlightStatusUpdation.length === 0) {

                return {
                    http: {
                        "res": {
                            status: 200,
                            "json": {
                                "response": finalResponse,
                            },
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                                "x-cap-custom-entity": firstErrorCode,
                                "x-cap-custom-message": firstErrorMessage
                            },
                        }
                    }
                }
            } else {
                return {
                    status: 200,
                    body: {
                        "mongoFlightStatusUpdation": mongoFlightStatusUpdation,
                        "response": finalResponse
                    },
                    "headers": {
                        "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                        "x-cap-custom-entity": firstErrorCode,
                        "x-cap-custom-message": firstErrorMessage
                    },
                }
            }
        }

    }
  }

  @Script({ pos: { x: 663.6823839127524, y: -170.22746169363097 } })
  @Relation(r => dao.isSuccess(), 'validateFfnApiCall')
  async prepareValidateFFNApiBlock() {
    const script = {
        execute: () => {
            const requestBody = getBody().body.validationSuccessTransactions;

            let apiRequestArr = [];
            for (const transactionAddPayload of requestBody) {

                let extendedFields = transactionAddPayload.extendedFields;

                let firstName = extendedFields.booking_first_name;
                let lastName = extendedFields.booking_last_name;

                let queryParameters = {
                    "FFN": transactionAddPayload.identifierValue,
                    "Fname": firstName,
                    "lname": lastName
                }

                let requestHeaders = getEffectiveHeaders();
                delete requestHeaders["x-cap-neo-test-variant-id"];
                delete requestHeaders["x-cap-api-attribution-entity-type"]
                delete requestHeaders["x-cap-neo-test-variant-id"];
                delete requestHeaders["x-cap-api-attribution-entity-type"];
                delete requestHeaders["x-cap-api-attribution-entity-code"];
                delete requestHeaders["x-cap-api-attribution-till-code"];
                let headers = requestHeaders
                let apiRequestObject = {
                    headers,
                    queryParams: queryParameters
                }
                apiRequestArr.push(apiRequestObject);
            };

            return apiRequestArr;
        }
    }
  }

  @ApiRequest({ pos: { x: 982.7632229564765, y: -285.3328497541718 } })
  @Relation(r => dao.isSuccess(), 'ExtractTransactionsWithValidAlias')
  @Relation(r => dao.hasError(), 'validateFFNErrorBlock')
  async validateFfnApiCall() {
  return {
        url: `http://neo-a.default:3000/api/v1/xto6x/execute/ValidateFFN`,
        method: `GET`,
      };
  }

  @Script({ pos: { x: 1597.7593139944763, y: -180.77331332273764 } })
  @Relation(r => dao.isSuccess() && (JSON.stringify(dao.getBody().body?.aliasFailureMap) != '{}'), 'AddFailedAliasToRejectionCollection')
  @Relation(r => dao.isSuccess() && !(JSON.stringify(dao.getBody().body?.aliasFailureMap) != '{}'), 'PreapareTransactionAddPayload')
  async ExtractTransactionsWithValidBoardingStatus() {
    const script = {

        execute: () => {
            let transactionsWithValidAlias = getBody("ExtractTransactionsWithValidAlias").body;

            return {
                status : 200,
                body : transactionsWithValidAlias
            }

        }

    }
  }

  @Script({ pos: { x: 1760.434864171699, y: 138.62281817194992 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().body.length === 0), 'BuildFinalRespWithoutRejectionCollectionWithoutTransactionAdd')
  @Relation(r => dao.isSuccess() && !(dao.getBody().body.length === 0), 'TransactionAddBulkApiCall')
  async PreapareTransactionAddPayload() {
    const script = {
      execute: () => {
        const inputPayload = getBody("ExtractTransactionsWithValidAlias").body;
        const headers = { ...getEffectiveHeaders() };
        delete headers["x-cap-neo-test-variant-id"];

        // Parse and clean payload
        let transactionPayload = inputPayload?.transactionAddPayload || [];

        // Ensure it's an array
        if (typeof transactionPayload === "string") {
          transactionPayload = JSON.parse(transactionPayload);
        }

        // Remove 'pnrkey' field from each object
        const cleanedPayload = transactionPayload.map(obj => {
          const newObj = { ...obj };
          delete newObj?.pnrkey;
          return newObj;
        });

        return {
          status: 200,
          headers: {
            "Content-Type": "application/json",
            ...headers,
          },
          body: JSON.stringify(cleanedPayload),
        };
      },
    };
  }

  @Script({ pos: { x: 1801.4200453176245, y: -342.3525666126693 } })
  @Relation(r => dao.isSuccess(), 'PutMongoRejectionCollection')
  async AddFailedAliasToRejectionCollection() {
    const script = {

        execute: () => {
            const currentDate = new Date();

            let aliasFailureMap = getIn().body.aliasFailureMap;

            let rejectionPutMongo = [];
            for (const billNumber in aliasFailureMap) {
                let transactionWithAliasStatus = aliasFailureMap[billNumber];
                let transactionPayload = transactionWithAliasStatus.transaction;
                let aliasCheck = transactionWithAliasStatus.aliasCheck;
                const rejectionCollection = {
                    bill_number: billNumber,
                    ffn_number: transactionPayload.identifierValue,
                    rejection_reason: aliasCheck.message,
                    alias_check: aliasCheck,
                    request_payload: transactionPayload,
                    date_created: currentDate,
                    date_updated: currentDate
                };
                rejectionPutMongo.push(rejectionCollection);
            }

            return {
                headers : getEffectiveHeaders(),
                status : 200,
                body : {
                    "rejectionPutMongoQuery" : JSON.stringify(rejectionPutMongo)
                }
            }
        }

    }
  }

  @PutMongo({ pos: { x: 2057.80550269208, y: -447.9443781432292 } })
  @Relation(r => dao.isSuccess(), 'PrepareTransactionAddAfterRejectionCollection')
  async PutMongoRejectionCollection() {
  return {
        collectionName: `post_flown_alias_rejection`,
        mode: `insert`,
        query: r => getBody().body.rejectionPutMongoQuery,
        queryKey: `{}`,
      };
  }

  @Script({ pos: { x: 2074.9817291566374, y: -234.14535180064496 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().body.length === 0), 'BuildFinalRespWithoutTransactionAdd')
  @Relation(r => dao.isSuccess() && (dao.getBody().body.length != 0), 'TransactionAddApiCallAfterRejectionCollection')
  async PrepareTransactionAddAfterRejectionCollection() {
    const script = {

        execute: () => {
            let inputPayload = getBody("ExtractTransactionsWithValidAlias").body;

            const headers = { ...getEffectiveHeaders() };
            delete headers["x-cap-neo-test-variant-id"];

            // Parse and clean payload
            let transactionPayload = inputPayload?.transactionAddPayload || [];

            // Ensure it's an array
            if (typeof transactionPayload === "string") {
                transactionPayload = JSON.parse(transactionPayload);
            }

            // Remove 'pnrkey' field from each object
            const cleanedPayload = transactionPayload.map(obj => {
                const newObj = { ...obj };
                delete newObj?.pnrkey;
                return newObj;
            });

            return {
                status: 200,
                headers: {
                    "Content-Type": "application/json",
                    ...headers,
                },
                body: JSON.stringify(cleanedPayload),
            };
        },
    };
  }

  @ApiRequest({ pos: { x: 2351.745743420475, y: -179.67785584367476 } })
  @Relation(r => dao.isSuccess(), 'BuildResponseAfterTransactionAddRejectionCollection')
  @Relation(r => dao.hasError(), 'BuildResponseAfterTransactionAddRejectionCollectionFails')
  async TransactionAddApiCallAfterRejectionCollection() {
  return {
        url: `https://apac.api.capillarytech.com/v2/transactions/bulk`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 2671.516814363711, y: -263.6234577056687 } })
  @Relation(r => dao.isSuccess(), 'BuildQueryForPnrCollectionUpdationAfterRejectionCollection')
  async BuildResponseAfterTransactionAddRejectionCollection() {
    const script = {

        execute: () => {
            let mongoFlightStatusUpdation = [];
            let finalResponse = [];
            let transactionAddResponse = getIn();

            let outputFromAliasValidationBlock = getBody("ExtractTransactionsWithValidAlias").body;
            let boardingStatusMap = outputFromAliasValidationBlock.boardingStatusMap;

            let aliasFailureMap = outputFromAliasValidationBlock.aliasFailureMap;

            let outputFromValidationFailureBlock = getBody("ValidationFailureBlock").body;

            let bodyWithValidations = getBody("ExtractValidationSuccessfulTransactions").body;
            let validationSuccessMap = bodyWithValidations.validationSuccessMap;
            let validationFailureMap = bodyWithValidations.validationFailureMap;
            let billNumberList = outputFromValidationFailureBlock.billNumberList;

            let transactionMap = outputFromAliasValidationBlock.transactionMap;

            for (const billNumber of billNumberList) {
                if (validationSuccessMap[billNumber]) {
                    if (aliasFailureMap[billNumber]) {
                        let failedTransaction = aliasFailureMap[billNumber].transaction;
                        let aliasCheck = aliasFailureMap[billNumber].aliasCheck;
                        let errors = [
                            {
                                status: aliasCheck.status,
                                code: aliasCheck.code,
                                message: aliasCheck.message
                            }
                        ]
                        let warnings = transactionMap[billNumber].warnings;
                        let aliasCheckFailureResponse = {
                            "result": failedTransaction,
                            "errors": errors,
                            "warnings": warnings
                        }
                        finalResponse.push(aliasCheckFailureResponse);
                    } else if (boardingStatusMap[billNumber]) {
                        let boardingStatusTransaction = boardingStatusMap[billNumber].transaction;
                        let errors = [];
                        let boardingStatusResponse = {
                            "result": boardingStatusTransaction,
                            "errors": errors,
                            "warnings": []
                        }
                        finalResponse.push(boardingStatusResponse);
                    } else {
                        let matchingTransaction = transactionAddResponse?.response?.find(matchingTransaction => matchingTransaction?.result?.billNumber === billNumber);
                        const successresponse = validationSuccessMap[billNumber]
                        if (matchingTransaction?.entityId) {
                            let mongoUpdation = {
                                "bill_number": billNumber,
                                "flight_status": "FLOWN"
                            }
                            mongoFlightStatusUpdation.push(mongoUpdation)
                        }

                        let warnings = transactionMap[billNumber].warnings;
                        let warning = warnings[0];
                        if (warning) {
                            matchingTransaction?.warnings?.push(warning);
                        }
                        matchingTransaction["result"]["billNumber"] = successresponse?.body?.billNumber?.trim()
                        matchingTransaction["result"]["customFields"]["eticket"] = successresponse?.body?.customFields?.eticket?.trim();
                        matchingTransaction["result"]["customFields"]["origin"] = successresponse?.body?.customFields?.origin?.trim();
                        matchingTransaction["result"]["customFields"]["destination"] = successresponse?.body?.customFields?.destination?.trim();
                        matchingTransaction["result"]["customFields"]["retro_or_auto"] = successresponse?.body?.customFields?.["retro_or_auto"]?.toLowerCase();
                        matchingTransaction["result"]["customFields"]["passengerid"] = successresponse?.body?.customFields?.passengerid?.trim();
                        matchingTransaction["result"]["customFields"]["bookingid"] = successresponse?.body?.customFields?.bookingid?.trim();
                        matchingTransaction["result"]["customFields"]["split_from_pnr"] = successresponse?.body?.customFields?.["split_from_pnr"]?.trim();
                        matchingTransaction["result"]["identifierValue"] = successresponse?.body?.identifierValue.trim();
                        matchingTransaction["result"]["extendedFields"]["store_associate_id"] = successresponse?.body?.extendedFields?.["store_associate_id"];
                        matchingTransaction["result"]["extendedFields"]["departure_date"] = successresponse?.body?.extendedFields?.["departure_date"]?.trim();
                        matchingTransaction["result"]["extendedFields"]["booking_first_name"] = successresponse?.body?.extendedFields?.["booking_first_name"]?.trim();
                        matchingTransaction["result"]["extendedFields"]["booking_last_name"] = successresponse?.body?.extendedFields?.["booking_last_name"]?.trim();
                        matchingTransaction["result"]["extendedFields"]["arrival_date"] = successresponse?.body?.extendedFields?.["arrival_date"]?.trim();
                        if (matchingTransaction?.result?.customFields?.tran_posting_date) {
                            delete matchingTransaction?.result?.customFields.tran_posting_date;
                        }
                        finalResponse.push(matchingTransaction)
                    }
                } else {
                    let failedTransaction = validationFailureMap[billNumber].body;
                    let errors = validationFailureMap[billNumber].errors;
                    let validationFailureResponse = {
                        "result": failedTransaction,
                        "errors": errors,
                        "warnings": []
                    }
                    finalResponse.push(validationFailureResponse);
                }
            }
            let firstErrorCode =
                finalResponse?.[0]?.errors?.[0]?.code ||
                500; // default to 500 if missing
            let firstErrorMessage =
                finalResponse?.[0]?.errors?.[0]?.message ||
                "Unknown Error";

            if (mongoFlightStatusUpdation.length === 0) {

                return {
                    http: {
                        "res": {
                            status: 200,
                            "json": {
                                "response": finalResponse
                            },
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                                "x-cap-custom-entity": firstErrorCode,
                                "x-cap-custom-message": firstErrorMessage
                            }
                        }
                    }
                }
            } else {
                return {
                    status: 200,
                    body: {
                        "mongoFlightStatusUpdation": mongoFlightStatusUpdation,
                        "response": finalResponse
                    },
                    "headers": {
                        "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                        "x-cap-custom-entity": firstErrorCode,
                        "x-cap-custom-message": firstErrorMessage
                    }
                }
            }
        }

    }
  }

  @Script({ pos: { x: 2350.663501461865, y: -411.4393246963692 } })
  async BuildFinalRespWithoutTransactionAdd() {
    const script = {

        execute: () => {
            let finalResponse = [];

            let outputFromAliasValidationBlock = getBody("ExtractTransactionsWithValidAlias").body;
            let aliasFailureMap = outputFromAliasValidationBlock.aliasFailureMap;
            let boardingStatusMap = outputFromAliasValidationBlock.boardingStatusMap;

            let outputFromValidationFailureBlock = getBody("ValidationFailureBlock").body;

            let bodyWithValidations = getBody("ExtractValidationSuccessfulTransactions").body;
            let validationSuccessMap = bodyWithValidations.validationSuccessMap;
            let validationFailureMap = bodyWithValidations.validationFailureMap;
            let billNumberList = outputFromValidationFailureBlock.billNumberList;

            for (const billNumber of billNumberList) {
                if (validationSuccessMap[billNumber]) {
                    if (aliasFailureMap[billNumber]) {
                        let failedTransaction = aliasFailureMap[billNumber].transaction;
                        let aliasCheck = aliasFailureMap[billNumber].aliasCheck;
                        let errors = [
                            {
                                status : aliasCheck.status,
                                code : aliasCheck.code,
                                message : aliasCheck.message
                            }
                        ]
                        let aliasCheckFailureResponse = {
                            "result" : failedTransaction,
                            "errors" : errors,
                            "warnings": []
                        }
                        finalResponse.push(aliasCheckFailureResponse);
                    } else if (boardingStatusMap[billNumber]) {
                        let boardingStatusTransaction = boardingStatusMap[billNumber].transaction;
                        let errors = [];
                        let boardingStatusResponse = {
                            "result" : boardingStatusTransaction,
                            "errors" : errors,
                            "warnings": []
                        }
                        finalResponse.push(boardingStatusResponse);
                    }
                } else {
                    let failedTransaction = validationFailureMap[billNumber].body;
                    let errors = validationFailureMap[billNumber].errors;
                    let validationFailureResponse = {
                        "result" : failedTransaction,
                        "errors" : errors,
                        "warnings": []
                    }
                    finalResponse.push(validationFailureResponse);
                }
            }

            return {
               http: {
                   "res": {
                        status : 200,
                        json : {
                            "response" : finalResponse
                        },
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                            "x-cap-custom-entity": finalResponse?.errors?.[0]?.code,
                            "x-cap-custom-message": finalResponse?.errors?.[0]?.message
                        }
                   }
                }
            }
        }

    }
  }

  @Script({ pos: { x: 2959.5193145340927, y: -268.933515323206 } })
  @Relation(r => dao.isSuccess(), 'UpdatePnrTransactionAfterRejectionCollection')
  async BuildQueryForPnrCollectionUpdationAfterRejectionCollection() {
    const script = {

        execute: () => {
            let currentDate = new Date();

            let mongoUpdationQuery = getBody("BuildResponseAfterTransactionAddRejectionCollection").body.mongoFlightStatusUpdation

            const bulkOps = mongoUpdationQuery.map(item => ({
                body: {
                    query:JSON.stringify({
                        $set: {"flight_status": item.flight_status,"flight_status_updation_date": currentDate, "is_active" : false} 
                        }),
                    queryKey: JSON.stringify({"bill_number": item.bill_number})
                }
            }));

            return bulkOps;
        }

    }
  }

  @Script({ pos: { x: 2989.4417291936415, y: 226.1850277719414 } })
  @Relation(r => dao.isSuccess(), 'UpdatePnrTransaction')
  async BuildQueryForPnrCollectionUpdation() {
    const script = {

        execute: () => {
            let currentDate = new Date();

            let mongoUpdationQuery = getBody("BuildFinalResponseAfterTransactionAdd").body.mongoFlightStatusUpdation

            const bulkOps = mongoUpdationQuery.map(item => ({
                body: {
                    query:JSON.stringify({
                        $set: {"flight_status": item.flight_status,"flight_status_updation_date": currentDate, "is_active" : false} 
                        }),
                    queryKey: JSON.stringify({"bill_number": item.bill_number})
                }
            }));

            return bulkOps;
        }

    }
  }

  @PutMongo({ pos: { x: 3250.1489957292274, y: -257.13258925097034 } })
  @Relation(r => dao.isSuccess(), 'dbPayloadForUtilisedPNRForAfterRejectionTransactionAdd')
  async UpdatePnrTransactionAfterRejectionCollection() {
  return {
        collectionName: `PNR_Transactions`,
        mode: `update`,
        query: r => getBody().body.query,
        queryKey: r => getBody().body.queryKey,
      };
  }

  @Script({ pos: { x: 4094.7049976204958, y: -266.31606710069457 } })
  async BuildFinalResponseAfterRejectionCollectionAndPNRTransactionUpdate() {
    const script = {

        execute: () => {
            return {
                http: {
                    "res": {
                        status: 200,
                        "json": {
                            "response": getBody("BuildResponseAfterTransactionAddRejectionCollection").body.response
                        },
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                            "x-cap-custom-entity": getBody("BuildFinalResponseAfterTransactionAdd")?.body?.response?.errors?.[0]?.code,
                            "x-cap-custom-message": getBody("BuildFinalResponseAfterTransactionAdd")?.body?.response?.errors?.[0]?.message
                        }
                    }
                }
            };
        }

    }
  }

  @Script({ pos: { x: 4169.441729193642, y: 206.1850277719414 } })
  async BuildFinalResponseAfterPNRTransactionUpdate() {
    const script = {

        execute: () => {
            return {
                http: {
                    "res": {
                        status: 200,
                        "json": {
                            "response": getBody("BuildFinalResponseAfterTransactionAdd").body.response
                        },
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                            "x-cap-custom-entity": getBody("BuildFinalResponseAfterTransactionAdd")?.body?.response?.errors?.[0]?.code,
                            "x-cap-custom-message": getBody("BuildFinalResponseAfterTransactionAdd")?.body?.response?.errors?.[0]?.message
                        }
                    }
                }
            };
        }

    }
  }

  @PutMongo({ pos: { x: 3337.4417291936415, y: 208.1850277719414 } })
  @Relation(r => dao.isSuccess(), 'dbPayloadForUtilisedPNRForBulkApiTransactionAdd')
  async UpdatePnrTransaction() {
  return {
        collectionName: `PNR_Transactions`,
        mode: `update`,
        query: r => getBody().body.query,
        queryKey: r => getBody().body.queryKey,
      };
  }

  @Script({ pos: { x: 2522.459143938218, y: 434.97525848325586 } })
  async BuildFinalResponseAfterTransactionAddFails() {
    const script = {

      execute: () => {
        let finalResponse = [];

        let outputFromAliasValidationBlock = getBody("ExtractTransactionsWithValidAlias").body;
        let aliasFailureMap = outputFromAliasValidationBlock.aliasFailureMap;
        let boardingStatusMap = outputFromAliasValidationBlock.boardingStatusMap;

        let outputFromValidationFailureBlock = getBody("ValidationFailureBlock").body;

        let bodyWithValidations = getBody("ExtractValidationSuccessfulTransactions").body;
        let validationSuccessMap = bodyWithValidations.validationSuccessMap;
        let validationFailureMap = bodyWithValidations.validationFailureMap;
        let billNumberList = outputFromValidationFailureBlock.billNumberList;
        for (const billNumber of billNumberList) {
          if (validationSuccessMap[billNumber]) {
            if (aliasFailureMap[billNumber]) {
              let failedTransaction = aliasFailureMap[billNumber].transaction;
              let aliasCheck = aliasFailureMap[billNumber].aliasCheck;
              let errors = [
                {
                  status: aliasCheck.status,
                  code: aliasCheck.code,
                  message: aliasCheck.message
                }
              ]
              let aliasCheckFailureResponse = {
                "result": failedTransaction,
                "errors": errors,
                "warnings": []
              }
              finalResponse.push(aliasCheckFailureResponse);
            } else if (boardingStatusMap[billNumber]) {
              let boardingStatusTransaction = boardingStatusMap[billNumber].transaction;
              let errors = [];
              let boardingStatusResponse = {
                "result": boardingStatusTransaction,
                "errors": errors,
                "warnings": []
              }
              finalResponse.push(boardingStatusResponse);
            }
            else {
              let code, message
              if (getBody().code >= 500 && getBody().code <= 599) {
                code = getBody().code
                message = getBody()
              } if (getBody().code === 401) {
                code = parseXml(getBody().err?.message).code;
                message = parseXml(getBody().err?.message).message;
              } else {
                code = getBody().code
                message = getBody().err?.message
              }
              let boardingStatusTransaction = validationSuccessMap[billNumber].body;
              let errors = [{
                status: false,
                code: code,
                message: message
              }]

              let boardingStatusResponse = {
                "result": boardingStatusTransaction,
                "errors": errors,
                "warnings": []
              }
              finalResponse.push(boardingStatusResponse);
            }
          } else {
            let failedTransaction = validationFailureMap[billNumber].body;
            let errors = validationFailureMap[billNumber].errors;
            let code, message
            if (getBody().code >= 500 && getBody().code <= 599) {
              code = getBody().code
              message = getBody()
            } if (getBody().code === 401) {
              code = parseXml(getBody().err?.message).code;
              message = parseXml(getBody().err?.message).message;
            } else {
              code = getBody().code
              message = getBody().err?.message
            }
            let error = {
              status: false,
              code: code,
              message: message
            }
            errors.push(error)
            let validationFailureResponse = {
              "result": failedTransaction,
              "errors": errors,
              "warnings": []
            }
            finalResponse.push(validationFailureResponse);
          }
        }
        // ✅ Extract first error message safely for header
        const firstErrorMessage =
          finalResponse?.[0]?.errors?.[0]?.message ||
          "No error message found";
        return {
          http: {
            "res": {
              status: getBody()?.code || 500,
              json: {
                "response": finalResponse
              },
              "headers": {
                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                "x-cap-custom-entity": finalResponse?.[0]?.errors?.[0]?.code || 500,
                "x-cap-custom-message": firstErrorMessage
              },
            }
          }
        }

      }
    };

    function parseXml(xml) {
      const successMatch = xml?.match(/<success>(.*?)<\/success>/);
      const codeMatch = xml?.match(/<code>(.*?)<\/code>/);
      const messageMatch = xml?.match(/<message>(.*?)<\/message>/);

      const status = successMatch ? successMatch[1] === "true" : null;
      const code = codeMatch ? parseInt(codeMatch[1], 10) : null;
      const message = messageMatch ? messageMatch[1] : null;

      return { status, code, message };
    }
  }

  @Script({ pos: { x: 2685.745743420475, y: -85.67785584367476 } })
  async BuildResponseAfterTransactionAddRejectionCollectionFails() {
    const script = {

      execute: () => {
        let finalResponse = [];
        let outputFromAliasValidationBlock = getBody("ExtractTransactionsWithValidAlias").body;
        let aliasFailureMap = outputFromAliasValidationBlock.aliasFailureMap;
        let boardingStatusMap = outputFromAliasValidationBlock.boardingStatusMap;

        let outputFromValidationFailureBlock = getBody("ValidationFailureBlock").body;

        let bodyWithValidations = getBody("ExtractValidationSuccessfulTransactions").body;
        let validationSuccessMap = bodyWithValidations.validationSuccessMap;
        let validationFailureMap = bodyWithValidations.validationFailureMap;
        let billNumberList = outputFromValidationFailureBlock.billNumberList;
        for (const billNumber of billNumberList) {
          if (validationSuccessMap[billNumber]) {
            if (aliasFailureMap[billNumber]) {
              let failedTransaction = aliasFailureMap[billNumber].transaction;
              let aliasCheck = aliasFailureMap[billNumber].aliasCheck;
              let errors = [
                {
                  status: aliasCheck.status,
                  code: aliasCheck.code,
                  message: aliasCheck.message
                }
              ]
              let aliasCheckFailureResponse = {
                "result": failedTransaction,
                "errors": errors,
                "warnings": []
              }
              finalResponse.push(aliasCheckFailureResponse);
            } else if (boardingStatusMap[billNumber]) {
              let boardingStatusTransaction = boardingStatusMap[billNumber].transaction;
              let errors = [];
              let boardingStatusResponse = {
                "result": boardingStatusTransaction,
                "errors": errors,
                "warnings": []
              }
              finalResponse.push(boardingStatusResponse);
            }
            else {
              let code, message
              if (getBody().code >= 500 && getBody().code <= 599) {
                code = getBody().code
                message = getBody()
              } if (getBody().code === 401) {
                code = parseXml(getBody().err?.message).code;
                message = parseXml(getBody().err?.message).message;
              } else {
                code = getBody().code
                message = getBody().err?.message
              }
              let boardingStatusTransaction = validationSuccessMap[billNumber].body;
              let errors = [{
                status: false,
                code: code,
                message: message
              }]

              let boardingStatusResponse = {
                "result": boardingStatusTransaction,
                "errors": errors,
                "warnings": []
              }
              finalResponse.push(boardingStatusResponse);
            }
          } else {
            let failedTransaction = validationFailureMap[billNumber].body;
            let errors = validationFailureMap[billNumber].errors;
            let code, message
            if (getBody().code >= 500 && getBody().code <= 599) {
              code = getBody().code
              message = getBody()
            } if (getBody().code === 401) {
              code = parseXml(getBody().err?.message).code;
              message = parseXml(getBody().err?.message).message;
            } else {
              code = getBody().code
              message = getBody().err?.message
            }
            let error = {
                status: false,
                code: code,
                message: message
              }
            errors.push(error)
            let validationFailureResponse = {
              "result": failedTransaction,
              "errors": errors,
              "warnings": []
            }
            finalResponse.push(validationFailureResponse);
          }
        }
        return {
          http: {
            "res": {
              status: getBody()?.code || 500,
              json: {
                "response": finalResponse
              },
              "headers": {
                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                "x-cap-custom-entity": getBody()?.code || 500,
                "x-cap-custom-message": getBody().err?.message
              }
            }
          }
        }

      }
    };

    function parseXml(xml) {
      const successMatch = xml?.match(/<success>(.*?)<\/success>/);
      const codeMatch = xml?.match(/<code>(.*?)<\/code>/);
      const messageMatch = xml?.match(/<message>(.*?)<\/message>/);

      const status = successMatch ? successMatch[1] === "true" : null;
      const code = codeMatch ? parseInt(codeMatch[1], 10) : null;
      const message = messageMatch ? messageMatch[1] : null;

      return { status, code, message };
    }
  }

  @Script({ pos: { x: 383.4272827187759, y: -387.309530878351 } })
  @Relation(r => dao.isSuccess() && (dao.getBody().body.validationSuccessTransactions.length == 0), 'ValidationFailureForAllPayloads')
  @Relation(r => dao.isSuccess() && !(dao.getBody().body.validationSuccessTransactions.length == 0), 'prepareValidateFFNApiBlock')
  async ExtractValidationSuccessfulTransactions() {
    const script = {
        execute: () => {
            let input = getBody().body.respArr;
            let validationSuccessTransactions = [];
            let validationSuccessMap = {};
            let validationFailureMap = {};
            for (let payload of input) {
                let billNumber = payload.billNumber;
                if (payload.errors.length == 0) {
                    validationSuccessTransactions.push(payload.body);
                    validationSuccessMap[billNumber] = payload;
                } else {
                    validationFailureMap[billNumber] = payload;
                }
            }
            return {
                status : 200,
                body : {
                    "validationSuccessTransactions" : validationSuccessTransactions,
                    "validationSuccessMap" : validationSuccessMap,
                    "validationFailureMap" : validationFailureMap
                }
            }
        }
    }
  }

  @Script({ pos: { x: 749.1618739873022, y: -472.7863313470567 } })
  async ValidationFailureForAllPayloads() {
    const script = {
        execute: () => {
            let outputFromValidationFailureBlock = getBody("ValidationFailureBlock").body;

            let finalResponse = [];

            let bodyWithValidations = getBody("ExtractValidationSuccessfulTransactions").body;
            let validationFailureMap = bodyWithValidations.validationFailureMap;
            let billNumberList = outputFromValidationFailureBlock.billNumberList;

            for (billNumber of billNumberList) {

                let failedTransaction = validationFailureMap[billNumber].body;

                let errors = validationFailureMap[billNumber].errors;
                let warnings = [];
                let validationFailureResponse = {
                    "result" : failedTransaction,
                    "errors" : errors,
                    "warnings": warnings
                }

                finalResponse.push(validationFailureResponse);
            }
            const responseWithErrors = finalResponse
                .filter(entry => Array.isArray(entry.errors) && entry.errors.length > 0);
            return {
                http: {
                    res : {
                        status : 400,
                        json : {
                            "response" : finalResponse
                        },
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                            "x-cap-custom-entity": responseWithErrors.length > 0 ? responseWithErrors?.[0]?.errors[0].code : null,
                            "x-cap-custom-message": responseWithErrors.length > 0 ? responseWithErrors?.[0]?.errors[0].message : null
                        }
                    }
                }
            }
        }
    }
  }

  @Script({ pos: { x: -747.9132163161837, y: -481.1480642507423 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'PayloadValidation')
  async PayloadSizeValidationFailureBlock() {
    const script = {

        execute: () => {
            const errorArray = [];
            const requestBody = getApiRequest()?.body

            let isError = false;
            if (!Array.isArray(requestBody)) {
                const error = {
                    "status" : false,
                    "code" : 400,
                    "message" : "The payload must be an array",
                    "path" : "/body"
                }
                isError = true;
                errorArray.push(error);
            } else if (requestBody.length === 0) {
                const error = {
                    "status" : false,
                    "code" : 400,
                    "message" : "The payload must contain atleast one item",
                    "path" : "/body"
                }
                isError = true;
                errorArray.push(error);
            } else if (requestBody.length > 20) {
                const error = {
                    "status" : false,
                    "code" : 400,
                    "message" : "The payload cannot contain more than 20 items",
                    "path" : "/body"
                }
                isError = true;
                errorArray.push(error);
            }

            if (isError) {
                return {
                    http: {
                        "res": {
                            "json": {
                                "errors" : errorArray
                            },
                            "status": 400,
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                                "x-cap-custom-entity": errorArray?.[0]?.code,
                                "x-cap-custom-message": errorArray?.[0]?.message
                            }
                        }
                    }
                };
            } else {
                return requestBody
            }

        }
    }
  }

  @Script({ pos: { x: 1137.2513724074624, y: -487.36004451360293 } })
  @Relation(r => dao.isSuccess(), 'ExtractTransactionsBasedOnBoardingStatus')
  async ExtractTransactionsWithValidAlias() {
    const script = {

        execute: () => {
            let aliasCheckResponse = getMultiBody("validateFfnApiCall");

            let transactionAddPayload = getBody("ExtractValidationSuccessfulTransactions").body.validationSuccessTransactions;

            let transactionBillNumberList = [];
            let aliasValidPayload = [];
            let aliasFailureMap = {};
            let transactionMap = {};
            let boardingStatusMap = {};
            for (let index = 0; index < transactionAddPayload.length; index++) {
                let aliasCheck = aliasCheckResponse[index];
                let transaction = transactionAddPayload[index];

                // ✅ Extract and format date/time using updated logic
                let departureDateStr = transaction.extendedFields?.departure_date;
                let arrivalDateStr = transaction.extendedFields?.arrival_date;
                //return processDateTime(departureDateStr);
                const { date: depDate, time: depTime } = processDateTime(departureDateStr);
                const { date: arrDate, time: arrTime } = processDateTime(arrivalDateStr);
                // Store dates in extendedFields
                transaction.extendedFields.departure_date = depDate;
                transaction.extendedFields.arrival_date = arrDate;

                // Store times in customFields
                if (!transaction.customFields) {
                    transaction.customFields = {};
                }
                transaction.customFields.departure_time = depTime;
                transaction.customFields.arrival_time = arrTime;
                transaction.customFields["tran_posting_date"] = getCurrentISTISOString();
                transaction.customFields["split_from_pnr"] = transaction?.customFields?.split_from_pnr
                let warnings = checkArrivalDate(transaction.extendedFields);
                let payload = {
                    "transaction": transaction,
                    "aliasCheck": aliasCheck,
                    "warnings": warnings
                };

                let billNumber = transaction.billNumber;
                transactionMap[billNumber] = payload;
                transactionBillNumberList.push(billNumber);

                if (aliasCheck?.status) {
                    let transactionAddWithUpdatedPnr = transaction;
                    transactionAddWithUpdatedPnr.extendedFields['pnr_status'] = "Utilized";

                    if (Number(transaction.extendedFields.boarding_status) !== 2) {
                        boardingStatusMap[billNumber] = payload;
                    } else {
                        aliasValidPayload.push(transactionAddWithUpdatedPnr);
                    }
                } else {
                    aliasFailureMap[billNumber] = payload;
                }
            }

            return {
                status: 200,
                body: {
                    transactionMap: transactionMap,
                    transactionBillNumberList: transactionBillNumberList,
                    transactionAddPayload: aliasValidPayload,
                    boardingStatusMap: boardingStatusMap,
                    aliasFailureMap: aliasFailureMap
                }
            };
        }
    };
    function getCurrentISTISOString() {
        const now = new Date();

        // Convert to IST
        const ist = new Date(
            now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" })
        );

        const pad = (n) => String(n).padStart(2, "0");

        return (
            ist.getFullYear() + "-" +
            pad(ist.getMonth() + 1) + "-" +
            pad(ist.getDate()) + "T" +
            pad(ist.getHours()) + ":" +
            pad(ist.getMinutes()) + ":" +
            pad(ist.getSeconds()) +
            "+05:30"
        );
    }
    function processDateTime(datetimeStr) {
        const d = new Date(datetimeStr)
        // Add 5h30m manually
        d.setMinutes(d.getMinutes() + 330);
        let yyyy = d.getUTCFullYear();
        let mm = String(d.getUTCMonth() + 1).padStart(2, "0");
        let dd = String(d.getUTCDate()).padStart(2, "0");

        let hh = String(d.getUTCHours()).padStart(2, "0");
        let min = String(d.getUTCMinutes()).padStart(2, "0");
        let ss = String(d.getUTCSeconds()).padStart(2, "0");
        return {
            date: `${yyyy}-${mm}-${dd}`,
            time: `${hh}:${min}:${ss}`
        };
    }


    /**
     * Validates arrival_date format and returns warning if invalid.
     */
    function checkArrivalDate(extendedFields) {
        let warnings = [];
        let arrivalDate = extendedFields.arrival_date;

        if (arrivalDate != undefined) {
            if (typeof arrivalDate !== 'string') {
                warnings.push({
                    "status": false,
                    "code": 6005,
                    "message": "arrival_date must be a string"
                });
                return warnings;
            }

            if (arrivalDate.length == 0) {
                warnings.push({
                    "status": false,
                    "code": 6005,
                    "message": "arrival_date must not be blank"
                });
                return warnings;
            }

            const regex = /^\d{4}-\d{2}-\d{2}$/;
            if (!regex.test(arrivalDate)) {
                warnings.push({
                    "status": false,
                    "code": 6005,
                    "message": "arrival_date must be in 'YYYY-MM-DD' format"
                });
                return warnings;
            }

            const [year, month, day] = arrivalDate.split('-').map(Number);
            const date = new Date(year, month - 1, day);

            if (date.getFullYear() !== year || date.getMonth() !== month - 1 || date.getDate() !== day) {
                warnings.push({
                    "status": false,
                    "code": 6005,
                    "message": "arrival_date must be a valid date"
                });
            }
        }

        return warnings;
    }
  }

  @Script({ pos: { x: 1262.497662373065, y: -186.46925103985325 } })
  @Relation(r => dao.isSuccess(), 'ExtractTransactionsWithValidBoardingStatus')
  @Relation(r => dao.isSuccess() && dao.getBody().body.areNoShowTransactionsPresent, 'PrepareQueryForNoShowTransactions')
  async ExtractTransactionsBasedOnBoardingStatus() {
    const script = {

        execute: () => {
            let currentDate = new Date();

            let inputBody = getBody("ExtractTransactionsWithValidAlias").body;

            let boardingStatusMap = inputBody.boardingStatusMap;

            let queryItems = []
            let respArr = []
            for (let key in boardingStatusMap) {
                let item = {
                    "bill_number" : key,
                    "flight_status": "FLOWN",
                    "flight_status_updation_date": currentDate
                }
                queryItems.push(item)
                let respObj = {
                    body: {
                        query:JSON.stringify({
                            $set: {"flight_status": item.flight_status,"flight_status_updation_date": currentDate, "is_active" : false} 
                            }),
                        queryKey: JSON.stringify({"bill_number": item.bill_number})
                    }
                }
                respArr.push(respObj)
            }

            if (queryItems.length == 0) {
                return {
                    body : {
                        "areNoShowTransactionsPresent": false
                    }
                }
            } else {
                return {
                    body: {
                        "areNoShowTransactionsPresent": true,
                        "respArr" : respArr
                    }
                }
            }

        }

    }
  }

  @Script({ pos: { x: 2172.434864171699, y: 102.6228181719498 } })
  async BuildFinalRespWithoutRejectionCollectionWithoutTransactionAdd() {
    const script = {

        execute: () => {
            let finalResponse = [];

            let outputFromAliasValidationBlock = getBody("ExtractTransactionsWithValidAlias").body;
            let aliasFailureMap = outputFromAliasValidationBlock.aliasFailureMap;
            let boardingStatusMap = outputFromAliasValidationBlock.boardingStatusMap;

            let outputFromValidationFailureBlock = getBody("ValidationFailureBlock").body;

            let bodyWithValidations = getBody("ExtractValidationSuccessfulTransactions").body;
            let validationSuccessMap = bodyWithValidations.validationSuccessMap;
            let validationFailureMap = bodyWithValidations.validationFailureMap;
            let billNumberList = outputFromValidationFailureBlock.billNumberList;

            for (const billNumber of billNumberList) {
                if (validationSuccessMap[billNumber]) {
                    if (aliasFailureMap[billNumber]) {
                        let failedTransaction = aliasFailureMap[billNumber].transaction;
                        let aliasCheck = aliasFailureMap[billNumber].aliasCheck;
                        let errors = [
                            {
                                status: aliasCheck.status,
                                code: aliasCheck.code,
                                message: aliasCheck.message
                            }
                        ]
                        let aliasCheckFailureResponse = {
                            "result": failedTransaction,
                            "errors": errors,
                            "warnings": []
                        }
                        finalResponse.push(aliasCheckFailureResponse);
                    } else if (boardingStatusMap[billNumber]) {
                        let boardingStatusTransaction = boardingStatusMap[billNumber].transaction;
                        let errors = [];
                        let boardingStatusResponse = {
                            "result": boardingStatusTransaction,
                            "errors": errors,
                            "warnings": []
                        }
                        finalResponse.push(boardingStatusResponse);
                    }
                } else {
                    let failedTransaction = validationFailureMap[billNumber].body;
                    let errors = validationFailureMap[billNumber].errors;
                    let validationFailureResponse = {
                        "result": failedTransaction,
                        "errors": errors,
                        "warnings": []
                    }
                    finalResponse.push(validationFailureResponse);
                }
            }

            return {
                http: {
                    "res": {
                        status: 200,
                        json: {
                            "response": finalResponse
                        },
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                            "x-cap-custom-entity": finalResponse?.errors?.[0]?.code,
                            "x-cap-custom-message": finalResponse?.errors?.[0]?.message
                        },
                    }
                }
            }
        }

    }
  }

  @PutMongo({ pos: { x: 1328.497662373065, y: 287.53074896014675 } })
  async UpdatePnrTransactionForNoShowTransactions() {
  return {
        collectionName: `PNR_Transactions`,
        mode: `update`,
        query: r => getBody().body.query,
        queryKey: r => getBody().body.queryKey,
      };
  }

  @Script({ pos: { x: 1322.497662373065, y: 67.53074896014675 } })
  @Relation(r => dao.isSuccess(), 'UpdatePnrTransactionForNoShowTransactions')
  async PrepareQueryForNoShowTransactions() {
    const script = {

        execute: () => {

            return getBody().body.respArr;

        }
    }
  }

  @Script({ pos: { x: -1755.3005495333928, y: -413.6464115865037 } })
  @Relation(r => dao.isSuccess(), 'checkDataInMongo')
  async checkMongo() {
    const script = {

        execute: () => {
            const requestPayload = getApiRequest()?.body;
            const payloadArray = []
            const billNumberArray = []


            for (let data of requestPayload) {
                const pnr = data?.extendedFields?.["pnr_number"].trim();
                const firstName = data?.extendedFields?.["booking_first_name"].trim();
                const lastName = data?.extendedFields?.["booking_last_name"].trim();
                const org = data?.customFields?.origin.trim();
                const dest = data?.customFields?.destination.trim();
                const departureDatestr = data?.extendedFields?.["departure_date"]
                const { date: departureDate } = processDateTime(departureDatestr);
                const modifiedDepartureDate = departureDate.split("-").join("");
                const billNumber = data?.billNumber

                // Creating new billNumber by concatenating multiple fields
                const newBillNumber = `${pnr}${firstName}${lastName}${org}${dest}${modifiedDepartureDate}`.split(" ").join("").toLowerCase();

                logger.info(`Updated Bill Number is: ${newBillNumber}`);
                payloadArray.push({
                    pnrKey: newBillNumber,
                    billNumber: billNumber
                })

                billNumberArray.push(newBillNumber)
            }

            return {
                body: {
                    query: {
                        "PNR_KEY": { "$in": billNumberArray }
                    }
                },
                payloadArray
            };
        }
    }


    function processDateTime(datetimeStr) {
        const d = new Date(datetimeStr)
        // Add 5h30m manually
        d.setMinutes(d.getMinutes() + 330);
        let yyyy = d.getUTCFullYear();
        let mm = String(d.getUTCMonth() + 1).padStart(2, "0");
        let dd = String(d.getUTCDate()).padStart(2, "0");

        let hh = String(d.getUTCHours()).padStart(2, "0");
        let min = String(d.getUTCMinutes()).padStart(2, "0");
        let ss = String(d.getUTCSeconds()).padStart(2, "0");
        return {
            date: `${yyyy}-${mm}-${dd}`,
            time: `${hh}:${min}:${ss}`
        };
    }
  }

  @GetMongo({ pos: { x: -1414.1379136138894, y: -444.79849303240667 } })
  @Relation(r => dao.isSuccess(), 'filteringRequestObject')
  async checkDataInMongo() {
  return {
        collectionName: `UtilisedPNR`,
        query: r => getBody().body.query,
        sort: `{}`,
      };
  }

  @Script({ pos: { x: 3533.531370227985, y: -262.5776750188457 } })
  @Relation(r => dao.isSuccess(), 'bulkInsert')
  async dbPayloadForUtilisedPNRForAfterRejectionTransactionAdd() {
    const script = {
      execute: () => {
        logger.info(
          `Payload Array: ${JSON.stringify(getBody("filteringRequestObject")?.payloadArray)}`
        );
        const payloadArray = getBody?.("filteringRequestObject")?.payloadArray ?? [];
        logger.info(
          `Payload Array: ${JSON.stringify(getBody("BuildResponseAfterTransactionAddRejectionCollection")?.body?.response)}`
        );
        const response = getBody?.("BuildResponseAfterTransactionAddRejectionCollection")?.body?.response ?? [];
        // Filter only success responses (no errors)
        const filteredResponse =
          response?.filter(
            (data) =>
              Array.isArray(data?.errors) &&
              (data?.errors?.length ?? 0) === 0
          ) ?? [];
        const bulkOps = [];
        const currentDate = new Date().toISOString();
        for (const res of filteredResponse) {
          const pnrKey = payloadArray
            ?.filter((d) => d?.billNumber === res?.result?.billNumber)
            ?.map((d) => d?.pnrKey)
            ?.[0];
          if (!pnrKey) continue;
          const record = {
            PNR: res?.result?.extendedFields?.["pnr_number"],
            PNR_KEY: pnrKey,
            billNumber: res?.result?.billNumber,
            ticketNumber: res?.result?.customFields?.eticket,
            FFN: res?.result?.identifierValue,
            sourceStore: res?.result?.extendedFields?.["store_associate_id"],
            origin: res?.result?.customFields?.origin,
            destination: res?.result?.customFields?.destination,
            distanceTravelled: "",
            departureDate: res?.result?.extendedFields?.["departure_date"],
            firstName: res?.result?.extendedFields?.["booking_first_name"],
            lastName: res?.result?.extendedFields?.["booking_last_name"],
            source:
              res?.result?.customFields?.["retro_or_auto"]
                ?.toLowerCase(),
            deviceId: "",
            passengerid: res?.result?.customFields?.passengerid,
            bookingid: res?.result?.customFields?.bookingid,
            eticket: res?.result?.customFields?.eticket,
            arrivalDate: res?.result?.extendedFields?.["arrival_date"],
            arrivalTime: res?.result?.customFields?.["arrival_time"],
            departureTime: res?.result?.customFields?.["departure_time"],
            splitFromPnr: response?.result?.customFields?.["split_from_pnr"],
            creationDate: currentDate,
            isActive: true,
            modifiedDate: currentDate,
            status: "success",
          };

          bulkOps?.push?.({
            filter: JSON.stringify({
              PNR_KEY: pnrKey,
            }),
            query: JSON.stringify({
              $set: record,
            }),
            upsert: true,
          });
        }

        const uniqueOps = Array.from(
          new Map(
            bulkOps?.map((op) => [
              JSON.parse(op?.filter ?? "{}")?.PNR_KEY,
              op,
            ]) ?? []
          ).values()
        );

        return { data: uniqueOps ?? [] };
      },
    };
  }

  @Script({ pos: { x: 3617.4417291936415, y: 194.1850277719414 } })
  @Relation(r => dao.isSuccess(), 'insertInUtilisedPnrForBulkApiTransactionAdd')
  async dbPayloadForUtilisedPNRForBulkApiTransactionAdd() {
    const script = {
        execute: () => {

            const payloadArray =
                getBody?.("filteringRequestObject")?.payloadArray ?? [];

            logger.info(
                `Payload Array: ${JSON.stringify(payloadArray ?? [])}`
            );

            const buildResponseBlock =
                getBody?.("BuildFinalResponseAfterTransactionAdd") ?? {};

            const mongoUpdationQuery =
                buildResponseBlock?.body?.mongoFlightStatusUpdation;

            const response =
                buildResponseBlock?.body?.response ?? [];

            logger.info(
                `Response: ${JSON.stringify(response ?? [])}`
            );

            // Filter only success responses (no errors)
            const filteredResponse =
                response?.filter?.(
                    (data) =>
                        Array.isArray(data?.errors) &&
                        (data?.errors?.length ?? 0) === 0
                ) ?? [];

            const bulkOps = [];
            const currentDate = new Date()?.toISOString?.() ?? "";

            for (const res of filteredResponse ?? []) {

                const pnrKey = payloadArray
                    ?.filter?.(
                        (d) => d?.billNumber === res?.result?.billNumber
                    )
                    ?.map?.((d) => d?.pnrKey)
                    ?.[0];

                if (!pnrKey) continue;

                const record = {
                    PNR: res?.result?.extendedFields?.["pnr_number"],
                    PNR_KEY: pnrKey,
                    billNumber: res?.result?.billNumber,
                    ticketNumber: res?.result?.customFields?.eticket,
                    FFN: res?.result?.identifierValue,
                    sourceStore: res?.result?.extendedFields?.["store_associate_id"],
                    origin: res?.result?.customFields?.origin,
                    destination: res?.result?.customFields?.destination,
                    distanceTravelled: "",
                    departureDate: res?.result?.extendedFields?.["departure_date"],
                    firstName: res?.result?.extendedFields?.["booking_first_name"],
                    lastName: res?.result?.extendedFields?.["booking_last_name"],
                    source:
                        res?.result?.customFields?.["retro_or_auto"]
                            ?.toLowerCase?.() ?? undefined,
                    deviceId: "",
                    passengerid: res?.result?.customFields?.passengerid,
                    bookingid: res?.result?.customFields?.bookingid,
                    eticket: res?.result?.customFields?.eticket,
                    splitFromPnr: response?.result?.customFields?.["split_from_pnr"],
                    arrivalDate: res?.result?.extendedFields?.["arrival_date"],
                    arrivalTime: res?.result?.customFields?.["arrival_time"],
                    departureTime: res?.result?.customFields?.["departure_time"],
                    creationDate: currentDate,
                    isActive: true,
                    modifiedDate: currentDate,
                    status: "success",
                };

                bulkOps?.push?.({
                    filter: JSON.stringify({
                        PNR_KEY: pnrKey,
                    }),
                    query: JSON.stringify({
                        $set: record,
                    }),
                    upsert: true,
                });
            }

            const uniqueOps = Array.from(
                new Map(
                    bulkOps?.map?.((op) => {
                        let parsedFilter = {};
                        try {
                            parsedFilter = JSON.parse(op?.filter ?? "{}");
                        } catch (e) {
                            parsedFilter = {};
                        }

                        return [parsedFilter?.PNR_KEY, op];
                    }) ?? []
                ).values()
            );

            return { data: uniqueOps ?? [] };
        },
    };
  }

  @Script({ pos: { x: -1086.1379136138894, y: -478.79849303240667 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'PayloadSizeValidationFailureBlock')
  async filteringRequestObject() {
    const script = {

        execute: () => {

            const mongoResponse = getOut()
            const pnrKey = mongoResponse.map(data => {
                return data.PNR_KEY
            })

            // const payloadArray=getBody("checkMongo")?.payloadArray
            const requestPayload = getApiRequest()?.body;
            const payloadArray = []

            for (let data of requestPayload) {
                const pnr = data?.extendedFields?.["pnr_number"].trim();
                const firstName = data?.extendedFields?.["booking_first_name"].trim();
                const lastName = data?.extendedFields?.["booking_last_name"].trim();
                const org = data?.customFields?.origin.trim();
                const dest = data?.customFields?.destination.trim();
                const departureDatestr = data?.extendedFields?.["departure_date"]
                const { date: departureDate } = processDateTime(departureDatestr);
                const modifiedDepartureDate = departureDate.split("-").join("");
                const billNumber = data?.billNumber

                // Creating new billNumber by concatenating multiple fields
                const newBillNumber = `${pnr}${firstName}${lastName}${org}${dest}${modifiedDepartureDate}`.split(" ").join("").toLowerCase();

                logger.info(`Updated Bill Number is: ${newBillNumber}`);
                payloadArray.push({
                    pnrKey: newBillNumber,
                    billNumber: billNumber
                })
            }


            //Write your code here.
            return {
                payloadArray,
                pnrKey
            };

        }
    }


    function processDateTime(datetimeStr) {
        const d = new Date(datetimeStr)
        // Add 5h30m manually
        d.setMinutes(d.getMinutes() + 330);
        let yyyy = d.getUTCFullYear();
        let mm = String(d.getUTCMonth() + 1).padStart(2, "0");
        let dd = String(d.getUTCDate()).padStart(2, "0");

        let hh = String(d.getUTCHours()).padStart(2, "0");
        let min = String(d.getUTCMinutes()).padStart(2, "0");
        let ss = String(d.getUTCSeconds()).padStart(2, "0");
        return {
            date: `${yyyy}-${mm}-${dd}`,
            time: `${hh}:${min}:${ss}`
        };
    }
  }

  @Script({ pos: { x: -2056, y: -380 } })
  @Relation(r => dao.isSuccess(), 'checkMongo')
  async AppConfigurations() {
    const script = {

        execute: () => {

            const appVersion = "2.4.0";   
            const developer="DIvya"
            const branch="PSV-30719"   
            const trigger = "retro/postFlownBookings" 
            const requestBody = getApiRequest()?.body?.[0];
            const externalId = requestBody?.identifierValue
            const billNumber = requestBody?.billNumber
            const isgRequestId = `${trigger}_${externalId}_${billNumber}`;
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

  @Script({ pos: { x: 1302.7632229564765, y: 514.6671502458282 } })
  async validateFFNErrorBlock() {
    const script = {

      execute: () => {
        try {
          const requestPayload = getApiRequest()?.body;
          const response = [];
          if (getBody()?.code === 401) {
            try {
              const error = parseXml(getBody().err?.message);
              //return error
              // errors.push(error);
              for (let data of requestPayload) {
                response.push({
                  result: data,
                  errors: [error],
                  warnings: [],
                });
              }
              return {
                http: {
                  res: {
                    status: getBody()?.code,
                    json: {
                      response: response,
                    },
                    headers: {
                      "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                      "x-cap-custom-entity": getBody()?.code,
                      "x-cap-custom-message": getBody().err?.message
                    },
                  },
                },
              };
            } catch (innerError) {
              return {
                http: {
                  res: {
                    status: 500,
                    json: { error: "Error processing XML response", details: innerError?.message },
                    headers: {
                      "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                      "x-cap-custom-entity": 500,
                      "x-cap-custom-message": "Error processing XML response"
                    },
                  },
                },
              };
            }
          } else {
            const message = getBody()?.err?.message;
            const err = (typeof message === 'string' && (() => { try { const p = JSON.parse(message); return typeof p === 'object' && p ? p : message; } catch { return message; } })()) || message;
            //return {err : null, message : `$$$ + ${getBody()}`}
            for (let data of requestPayload) {
              response.push({
                result: data,
                errors: [{
                  code: err?.code || 500,
                  status: "false",
                  message: err?.message || "Internal server error"
                }],
                warnings: [],
              });
            }
            //return getBody().err?.message
            return {
              http: {
                res: {
                  status: err?.code || 500,
                  json: response,
                  headers: {
                    "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                    "x-cap-custom-entity": err?.code || 500,
                    "x-cap-custom-message": err?.message || "Internal server error"
                  },
                },
              },
            };
          }
        } catch (error) {
          return {
            http: {
              res: {
                status: 500,
                json: { error: "Unexpected error occurred", details: error?.message },
                headers: {
                  "App-Version": getBody("AppConfigurations")?.body.APP_VERSION,
                  "x-cap-custom-entity": 500,
                  "x-cap-custom-message": "Unexpected error occurred"
                },
              },
            },
          };
        }
      },
    };

    function parseXml(xml) {
      const successMatch = xml?.match(/<success>(.*?)<\/success>/);
      const codeMatch = xml?.match(/<code>(.*?)<\/code>/);
      const messageMatch = xml?.match(/<message>(.*?)<\/message>/);

      const status = successMatch ? successMatch[1] === "true" : null;
      const code = codeMatch ? parseInt(codeMatch[1], 10) : null;
      const message = messageMatch ? messageMatch[1] : null;

      return { status, code, message };
    }
  }

  @BulkMongo({ pos: { x: 3806, y: -290 } })
  @Relation(r => dao.isSuccess(), 'BuildFinalResponseAfterRejectionCollectionAndPNRTransactionUpdate')
  async bulkInsert() {
  return {
        collectionName: `UtilisedPNR`,
        mode: `bulkUpsert`,
        query: r => getIn("dbPayloadForUtilisedPNRForAfterRejectionTransactionAdd").data,
        options: ``,
      };
  }

  @BulkMongo({ pos: { x: 3888, y: 188 } })
  @Relation(r => dao.isSuccess(), 'BuildFinalResponseAfterPNRTransactionUpdate')
  async insertInUtilisedPnrForBulkApiTransactionAdd() {
  return {
        collectionName: `UtilisedPNR`,
        mode: `bulkUpsert`,
        query: r => getIn("dbPayloadForUtilisedPNRForBulkApiTransactionAdd").data,
        options: ``,
      };
  }
}
