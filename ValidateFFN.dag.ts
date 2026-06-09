import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn } = dao;

@Dag({ method: "GET", url: "ValidateFFN" })
class ValidateFFN {
  constructor() {
    this.AppConfigurations();
  }

  @Schema({ pos: { x: 254.7615876219424, y: 38.17490634183102 } })
  @Relation(r => dao.hasError(), 'queryParamValidationFailureBlock')
  @Relation(r => dao.isSuccess(), 'prepareCustomerLookupApiScript')
  async queryParamValidationBlock() {
    return {
      definitions : [],
      spec:{
        type : "object",
        "properties": {
          "queryParams" :{
            type: 'object',
            properties: {
              FFN: {
                type: 'string',
                minLength: 1,
                "errorMessage": {
                  minLength: "FFN must not be empty"
                }
              },
              Fname: {
                type: 'string',
                minLength: 1,
                "errorMessage": {
                  minLength: "Fname must not be empty"
                }
              },
              lname: {
                type: 'string',
                minLength: 1,
                "errorMessage": {
                  minLength: "lname must not be empty"
                }
              }
            },
            required: ['FFN', 'Fname', 'lname'],
            errorMessage: {
              required: {
                FFN: "FFN is missing",
                Fname: "Fname is missing",
                lname: "lname is missing"
              }
            }
          }
        },
        required: ['queryParams'],
        errorMessage: {
          required: {
            queryParams: "queryParams are missing"
          }
        }
      }
    }
  }

  @Script({ pos: { x: 517.1001466979629, y: 203.15922953553775 } })
  @Relation(r => dao.isSuccess(), 'CustomerLookupApiCall')
  async prepareCustomerLookupApiScript() {
    const script = {
        execute: () => {
            const requestQueryParams = getApiRequest()?.queryParams;
            let requestHeaders = getEffectiveHeaders();
            // Remove these headers because customer lookup api will throw error incase invalid values are passed here
            delete requestHeaders["x-cap-neo-test-variant-id"];
            delete requestHeaders["x-cap-api-attribution-entity-type"];
            delete requestHeaders["x-cap-api-attribution-entity-code"];
            delete requestHeaders["x-cap-api-attribution-till-code"];
            logger.info(`[prepareCustomerLookup] Incoming queryParams: ${JSON.stringify(requestQueryParams)}`);
            let queryParameters = {
                "identifierName" : "externalId",
                "identifierValue" : requestQueryParams.FFN,
                "source" : requestQueryParams.source ? requestQueryParams.source.toUpperCase() : "INSTORE"
            }
            logger.info(`[prepareCustomerLookup] FFN: ${requestQueryParams?.FFN}, source: ${requestQueryParams?.source}`);
            logger.info(`[prepareCustomerLookup] queryParams to API: ${JSON.stringify(queryParameters)}`);
            return {
                headers : requestHeaders,
                queryParams : queryParameters
            };
        }
    }
  }

  @Script({ pos: { x: 518.4891791010231, y: -60.69435025808147 } })
  async queryParamValidationFailureBlock() {
    const script = {

        execute: () => {
            let errorArr = [];
            let error;
            const validationErrors = getIn()?.err;
            validationErrors?.forEach(validationError => {
                error = {
                    "status": false,
                    "code": 6001,
                    "message" : validationError.message,
                    "path" : validationError.instancePath
                }
                errorArr.push(error);
            });
            return {
               http: {
                   "res": {
                        status : 400,
                        "json": {
                            "errors" : errorArr
                        }
                   }
               }
            }
        }

    }
  }

  @ApiRequest({ pos: { x: 815.0382471140103, y: 66.31097007017263 } })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.errors?.length), 'CustomerGetError')
  @Relation(r => dao.isSuccess() && !(dao.getBody()?.errors?.length), 'AliasValidationBlock')
  @Relation(r => dao.hasError(), 'handleError')
  async CustomerLookupApiCall() {
  return {
        url: `https://apac.api.capillarytech.com/v2/customers/lookup/customerDetails`,
        method: `GET`,
        queryParams: {
    "embed": "points,mlp"
  },
      };
  }

  @Script({ pos: { x: 1130.4016624233807, y: 141.43258049805655 } })
  async AliasValidationBlock() {
    const script = {

        execute: () => {
            let customerGetResponse = getBody("CustomerLookupApiCall") || {};
            const warnings = customerGetResponse?.warnings || [];

            const hasMergedCustomer = warnings.some(
                (warning) => warning?.code === 8069
            );

            if (hasMergedCustomer) {
                logger.info("Merged customer found (code 8069)");
                return {
                    http: {
                        res: {
                            json: {
                                status: false,
                                code: 8015,
                                message: "Customer Not Found",
                                FFN: getApiRequest()?.queryParams?.FFN
                            },
                            status: 200,
                            headers: {
                                "App-Version": getBody("AppConfigurations")?.body?.APP_VERSION
                            }
                        }
                    }
                };
            }
            let requestQueryParams = getApiRequest()?.queryParams;
            logger.info(`[AliasCheck] Incoming queryParams: ${requestQueryParams}`);

            let ffnNumber = requestQueryParams?.FFN;

            let firstNameFromRequestBody = convertToAlphabeticAndRemoveSpace(requestQueryParams?.Fname?.trim()?.toLowerCase());
            let lastNameFromRequestBody = convertToAlphabeticAndRemoveSpace(requestQueryParams?.lname?.trim()?.toLowerCase());
            logger.info(`[AliasCheck] CustomerLookupApiCall response: ${JSON.stringify(customerGetResponse)}`);

            let profile = customerGetResponse?.profiles?.[0];
            let loyaltyProgramDetails = customerGetResponse?.loyaltyProgramDetails;

            let firstNameFromCustomerProfile = profile?.firstName;
            let lastNameFromCustomerProfile = profile?.lastName;

            let customFields = profile?.fields;

            let alias1FirstName = customFields?.alias1_fname;
            let alias1LastName = customFields?.alias1_lname;

            let alias2FirstName = customFields?.alias2_fname;
            let alias2LastName = customFields?.alias2_lname;

            let alias3FirstName = customFields?.alias3_fname;
            let alias3LastName = customFields?.alias3_lname;

            const aliasBody = {
                "alias1FirstName": convertToAlphabeticAndRemoveSpace(alias1FirstName?.trim()?.toLowerCase()),
                "alias1LastName": convertToAlphabeticAndRemoveSpace(alias1LastName?.trim()?.toLowerCase()),
                "alias2FirstName": convertToAlphabeticAndRemoveSpace(alias2FirstName?.trim()?.toLowerCase()),
                "alias2LastName": convertToAlphabeticAndRemoveSpace(alias2LastName?.trim()?.toLowerCase()),
                "alias3FirstName": convertToAlphabeticAndRemoveSpace(alias3FirstName?.trim()?.toLowerCase()),
                "alias3LastName": convertToAlphabeticAndRemoveSpace(alias3LastName?.trim()?.toLowerCase()),
                "firstNameFromCustomerProfile": convertToAlphabeticAndRemoveSpace(firstNameFromCustomerProfile?.trim()?.toLowerCase()),
                "lastNameFromCustomerProfile": convertToAlphabeticAndRemoveSpace(lastNameFromCustomerProfile?.trim()?.toLowerCase()),
            };
            if (isAliasValid(firstNameFromRequestBody, lastNameFromRequestBody, aliasBody)) {

                const slabDetails = [];
                // Loop through the array and extract the required fields
                loyaltyProgramDetails.forEach(program => {
                    const {
                        programId,
                        currentSlab,
                        currentSlabDescription,
                        nextSlab,
                        nextSlabSerialNumber,
                        nextSlabDescription,
                        slabSNo,
                        slabExpiryDate
                    } = program;

                    // Push the extracted details into the slabDetails array
                    slabDetails.push({
                        programId,
                        currentSlab,
                        currentSlabDescription,
                        nextSlab,
                        nextSlabSerialNumber,
                        nextSlabDescription,
                        slabSNo,
                        slabExpiryDate
                    });
                });


                return {
                    http: {
                        "res": {
                            "json": {
                                "status": true,
                                "code": 200,
                                "message": "Alias Check Successful",
                                "FFN": ffnNumber,
                                "slabDetails": slabDetails
                            },
                            "status": 200
                        }
                    }
                };
            } else {
                return {
                    http: {
                        "res": {
                            status: 200,
                            "json": {
                                "status": false,
                                "code": 6000,
                                "message": "Alias Check Failed",
                                "FFN": ffnNumber
                            }
                        }
                    }
                }
            }
        }

    }

    function isAliasValid(firstNameFromRequestBody, lastNameFromRequestBody, aliasBody) {
        let firstNameFromCustomerProfile = aliasBody.firstNameFromCustomerProfile;
        let lastNameFromCustomerProfile = aliasBody.lastNameFromCustomerProfile;
        let alias1FirstName = aliasBody.alias1FirstName;
        let alias1LastName = aliasBody.alias1LastName;
        let alias2FirstName = aliasBody.alias2FirstName;
        let alias2LastName = aliasBody.alias2LastName;
        let alias3FirstName = aliasBody.alias3FirstName;
        let alias3LastName = aliasBody.alias3LastName;

        return (
            (firstNameFromRequestBody + lastNameFromRequestBody == firstNameFromCustomerProfile + lastNameFromCustomerProfile) ||
            (firstNameFromRequestBody + lastNameFromRequestBody == lastNameFromCustomerProfile + firstNameFromCustomerProfile) ||
            (firstNameFromRequestBody + lastNameFromRequestBody == alias1FirstName + alias1LastName) ||
            (firstNameFromRequestBody + lastNameFromRequestBody == alias2FirstName + alias2LastName) ||
            (firstNameFromRequestBody + lastNameFromRequestBody == alias3FirstName + alias3LastName)
        );
    }


    function convertToAlphabeticAndRemoveSpace(inputValue) {
        let convertedValue = inputValue?.replace(/[^a-zA-Z]+/g, "") || "";
        return convertedValue;
    }
  }

  @Script({ pos: { x: 1120.5819229817632, y: -24.980686764301737 } })
  async CustomerGetError() {
    const script = {

        execute: () => {
            let customerGetResponse = getIn();
            logger.info(`[AliasCheck] CustomerLookupApiCall response: ${JSON.stringify(customerGetResponse)}`);
            let requestQueryParams = getApiRequest().queryParams;
            let error = customerGetResponse.errors?.[0];
            error['FFN'] = requestQueryParams.FFN;
            return {
                http: {
                    "res": {
                        status : 200,
                        "json" : error
                    }
                }
            }
        }

    }
  }

  @Script({ pos: { x: -75.3110930040591, y: 71.71193632278263 } })
  @Relation(r => dao.isSuccess(), 'queryParamValidationBlock')
  async AppConfigurations() {
    const script = {

        execute: () => {

            const appVersion = "1.2.0";
            logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`)
            const developer = "Adarsh"
            const branch = "PSV-27138"

            return {
                body:
                {
                    APP_VERSION: appVersion
                }
            };

        }
    }
  }

  @Script({ pos: { x: 1135.0382471140103, y: 386.31097007017263 } })
  async handleError() {
    const script = {

        execute: () => {

            const errors = getBody("CustomerLookupApiCall")
            logger.info(`[AliasCheck] Error: ${errors}`);
            if (errors.code >= 500 && errors.code <= 599) {
                return {
                    http: {
                        res: {
                            status: 500,
                            json: errors,
                            headers: {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            //Write your code here.
            return {
                http: {
                    res: {
                        json: {
                            response: {
                                status: {
                                    success: false,
                                    code: 9001,
                                    message: "Error in fetching customer details"
                                }
                            }
                        },
                        status: 200,
                        headers: {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            }
        }
    }
  }
}
