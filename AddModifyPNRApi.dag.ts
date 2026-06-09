import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn, getMultiBody, getMultiError, getOut } = dao;

@Dag({ method: "POST", url: "preFlownBookings" })
class AddModifyPNRApi {
  constructor() {
    this.AppConfigurations();
  }

  @Schema({ pos: { x: -1004.8005671394229, y: 125.49782934051723 } })
  @Relation(r => dao.isSuccess(), 'requestRejectionRule')
  @Relation(r => dao.hasError(), 'requestRejectionRule')
  async RequestValidator() {
    return {
      definitions: [],
      spec: {
        type: "object",
        //schema

        properties: {
          body: {
            type: "array",
            minItems: 1,
            maxItems: 20,
            errorMessage: {
              type: "The payload must be an array",
              minItems: "The payload must contain atleast one item",
              maxItems: "The payload cannot contain more than 20 items",
            },
            items: {
              type: "object",
              properties: {
                identifierType: {
                  type: "string",
                  transform: ["toLowerCase"],
                  enum: ["externalid"],
                  errorMessage: {
                    enum: "The identifierType property must be 'externalId'",
                  },
                },
                identifierValue: {
                  minLength: 1,
                  transform: ["trim"],
                  errorMessage: {
                    minLength: "identifierValue must not be empty",
                  },
                },
                source: {
                  minLength: 1,
                  transform: ["trim"],
                  errorMessage: {
                    minLength: "source must not be empty",
                  },
                },
                type: {
                  minLength: 1,
                  transform: ["trim"],
                  errorMessage: {
                    minLength: "type must not be empty",
                  },
                },
                billNumber: {
                  minLength: 1,
                  transform: ["trim"],
                  errorMessage: {
                    minLength: "billNumber must not be empty",
                  },
                },
                billAmount: {
                  minLength: 1,
                  transform: ["trim"],
                  errorMessage: {
                    minLength: "billAmount must not be empty",
                  },
                },
                billingDate: {
                  minLength: 1,
                  type: "string",
                  format: "date-time",
                  // anyOf: [
                  //     { type: "string", format: "date" },
                  //     { type: "string", format: "date-time" }
                  //   ],
                  transform: ["trim"],
                  errorMessage: {
                    minLength: "billingDate must not be empty",
                  },
                },
                lineItemsV2: {
                  type: "array",
                  minItems: 1,
                  items: {
                    type: "object",
                    properties: {
                      itemCode: {
                        type: "string",
                        minLength: 1,
                        errorMessage: {
                          minLength: "itemCode must not be empty",
                        },
                      },
                      amount: {
                        minLength: 1,
                        errorMessage: {
                          minLength: "amount must not be empty",
                        },
                      },
                    },
                    required: ["itemCode", "amount"],
                    errorMessage: {
                      required: {
                        itemCode: "itemCode is missing",
                        amount: "amount is missing",
                      },
                    },
                  },
                },
                extendedFields: {
                  type: "object",
                  properties: {
                    airline_code: {
                      type: "string",
                      minLength: 1,
                      errorMessage: {
                        minLength: "airline_code must not be empty",
                      },
                    },
                    transaction_source: {
                      transform: ['trim', 'toLowerCase'],
                      not: {
                        enum: ['oc'],
                      },
                      "errorMessage": {
                        not: "Codeshare marketed flight PNRs not eligible for earning IndiGo BluChips.",
                      }
                    },
                    boarding_status: {
                      minLength: 1,
                      errorMessage: {
                        minLength: "boarding_status must not be empty",
                      },
                    },
                    pnr_status: {
                      type: "string",
                      minLength: 1,
                      errorMessage: {
                        minLength: "pnr_status must not be empty",
                      },
                    },
                    flight_status: {
                      transform: ['trim', 'toLowerCase'],
                      not : {
                          enum: ['flown'],
                        },
                      errorMessage: {
                        not: "flight_status cannot be 'flown'"
                      },
                    },
                    booking_date: {
                      type: "string",
                      format: "date",
                      minLength: 1,
                      errorMessage: {
                        minLength: "booking_date must not be empty",
                      },
                    },
                    pnr_number: {
                      type: "string",
                      minLength: 1,
                      errorMessage: {
                        minLength: "pnr_number must not be empty",
                      },
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
                    arrival_date: {
                      type: 'string',
                      format: "date-time",
                      minLength: 1,
                      "errorMessage": {
                        minLength: "arrival_date must not be empty",
                        format: "arrival_date must be in ISO 8601 format"
                      }
                    },
                    booking_first_name: {
                      type: "string",
                      minLength: 1,
                      errorMessage: {
                        minLength: "booking_first_name must not be empty",
                      },
                    },
                    booking_last_name: {
                      type: "string",
                      minLength: 1,
                      errorMessage: {
                        minLength: "booking_last_name must not be empty",
                      },
                    },
                  },
                  required: [
                    "airline_code",
                    "booking_date",
                    "booking_first_name",
                    "booking_last_name",
                    "departure_date",
                    "arrival_date",
                    "pnr_status",
                    "pnr_number",
                  ],
                  errorMessage: {
                    required: {
                      airline_code: "airline_code extendedField is missing",
                      pnr_status: "pnr_status extendedField is missing",
                      booking_date: "booking_date extendedField is missing",
                      pnr_number: "pnr_number extendedField is missing",
                      departure_date: "departure_date extendedField is missing",
                      arrival_date: "arrival_date extendedField is missing",
                      booking_first_name:
                        "booking_first_name extendedField is missing",
                      booking_last_name:
                        "booking_last_name extendedField is missing",
                    },
                  },
                },
                customFields: {
                  type: "object",
                  properties: {
                    origin: {
                      type: "string",
                      minLength: 1,
                      errorMessage: {
                        minLength: "origin must not be empty",
                      },
                    },
                    destination: {
                      type: "string",
                      minLength: 1,
                      errorMessage: {
                        minLength: "destination must not be empty",
                      },
                    },
                    prod_class_code: {
                      transform: ['trim', 'toLowerCase'],
                      not: {
                        enum: ['x', 'g', 'zh','zl', 'zm'],
                      },
                      "errorMessage": {
                        not: "prod_class_code cannot be 'x' or 'g' or 'zh' or 'zl' or 'zm'",
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
                        enum: ['6eadm', '6eapt', '6eapttr', '6eceo', '6ecoo', '6ecldca', '6ecomm', '6enetplan', '6esales', '6ecorpaf', '6eeng', '6efin', '6eflo', '6eflosim', '6efsf', '6ehfltops', '6ehinfgflt', '6ehraocs', '6ehrr', '6eifly', '6eift', '6eiit', '6elegal', '6eatr', '6elthq', '6eocc', '6esh1', '6eslt', '6eigcargo', '6ecargoint', '6eigsst', '6edigital', 'iarn0001', 'iars0002', 'igcargo', 'igeltd', '6emdoffice', '6egrc', '6esmartlin', '6ecustexp', '6efinaaf', 'gbaptbc', 'gbaptsm', 'xocorp', 'igmt01', 'igmt02', 'igsh01', 'igsh02', 'igsh03', '6esh2', 'gstss116'],
                      },
                      "errorMessage": {
                        not: "Staff duty travel PNRs not eligible for earning IndiGo BluChips."
                      }
                    }
                  },
                  required: ["destination", "origin"],
                  errorMessage: {
                    required: {
                      origin: "origin customField is missing",
                      destination: "destination customField is missing",
                    },
                  },
                },
              },
              required: [
                "identifierType",
                "identifierValue",
                "source",
                "type",
                "billNumber",
                "billAmount",
                "billingDate",
                "extendedFields",
                "lineItemsV2",
                "customFields",
              ],
              errorMessage: {
                required: {
                  identifierType: "identifierType is missing",
                  identifierValue: "identifierValue is missing",
                  source: "source is missing",
                  type: "type is missing",
                  billNumber: "billNumber is missing",
                  billAmount: "billAmount is missing",
                  billingDate: "billingDate is missing",
                  extendedFields: "extendedFields are missing",
                  customFields: "customFields are missing",
                  lineItemsV2: "lineItemsV2 are missing",
                },
              },
            },
            //uniqueItemProperties: ['extendedFields.pnr_number']
          },
        },
      },
    }
  }

  @Script({ pos: { x: 1543.8859365165924, y: 230.7758337965996 } })
  @ExecutionStrategy('or')
  @Cachable({ cachable: false })
  @Relation(r => dao.isSuccess(), 'CheckPNRPresent')
  async PayloadTransformer() {
    const script = {
      execute: () => {

        const currentDate = new Date();
        let isAliasCheckFailedForAll = false;
        const requestBody = getBody("AliasCheckedDataSagregation")?.body?.passedAliasPNRRequests;

        if (!Array.isArray(requestBody) || requestBody.length === 0) {
          isAliasCheckFailedForAll = true;
          const errors = [{
            success: false,
            code: 8006,
            message: "alias check failed for all requests",
          }];
          return {
            headers: getEffectiveHeaders(),
            body: {
              "isAliasCheckFailedForAll":isAliasCheckFailedForAll,
              errors: errors,
              //body: requestBody
            }
          };
        }

        const pnrNumber = requestBody[0]?.extendedFields?.pnr_number;

        const getPNR = {
          pnr_number : pnrNumber,
          is_active : true
        };

        // Create the PNR object
        const PNR = {
          pnr_number: pnrNumber,
          payload: requestBody,
          date_created: currentDate,
          date_updated: currentDate,
          is_active: true
        };

        const simulationResponse = getMultiBody("SimulationAPICall");
        const generateSimulationRequest = getOut("GenerateSimulationRequest");

        const billNumberSimulationMap = {};

        simulationResponse?.map((resp, index) => {
          let simulationRequestBody = generateSimulationRequest[index]?.body;
          let billNumber = JSON.parse(simulationRequestBody)?.billNumber;
          if (!(billNumber)) {
            throw new Error("Error extracting billNumber from request");
          }
          let simulatedPointsBreakup = resp.simulatedPointsBreakup;
          if (simulatedPointsBreakup == null) {
            resp['is_rejected'] = true;
          } else {
            resp['is_rejected'] = false;
          }

          return billNumberSimulationMap[billNumber] = resp;
        });

        const PNR_Transactions = requestBody?.map(item => ({
          identifier_value: item.identifierValue,
          bill_number: item.billNumber,
          pnr_number: item.extendedFields.pnr_number,
          parentPNR_id: 0,
          flight_status: item.extendedFields.flight_status,
          departure_date: new Date(item.extendedFields.departure_date),
          transaction_payload: item,
          simulation_response: billNumberSimulationMap[item.billNumber],
          date_created: currentDate,
          date_updated: currentDate,
          is_active : true
        }));


        return {
          headers: getEffectiveHeaders(),
          body: {
            getPNRQuery : JSON.stringify(getPNR),
            insertPNRQuery : JSON.stringify(PNR),
            PNR,
            PNR_Transactions,
            pnrNumber
          }
        };
      }
    };
  }

  @Script({ pos: { x: -480.359119119355, y: 416.1789782120893 } })
  async HandleValidationFailure() {
    const script = {
        execute: () => {
            return {
                http: {
                    res: {
                        json: {
                            success: false,
                            errors: getBody("requestRejectionRule")?.body?.errors
                        },
                        status: 400,
                        headers: getEffectiveHeaders()
                    }
                }
            };
        }

    }
  }

  @PutMongo({ pos: { x: 2296.9394351094174, y: -51.957141496003715 } })
  @Relation(r => dao.isSuccess(), 'PNRTransactionInsertion')
  async InsertPNR() {
  return {
        collectionName: `PNR`,
        mode: `insert`,
        query: r => getBody("PayloadTransformer")?.body?.insertPNRQuery,
        queryKey: `{}`,
      };
  }

  @Script({ pos: { x: 2528.7440945819358, y: -55.85593308183921 } })
  @Relation(r => dao.isSuccess(), 'InsertPNRTransactions')
  async PNRTransactionInsertion() {
    const script = {
      execute: () => {
        const pnr_transactions = getOut("PayloadTransformer")?.[0]?.body?.PNR_Transactions;
        const parentPNR_id = Object.values(getBody("InsertPNR")?.insertedId.buffer).map(byte => byte.toString(16).padStart(2, '0')).join('');
        pnr_transactions?.forEach(transaction => {
          // Assign the parentPNR_id to each transaction object
          transaction.parentPNR_id = parentPNR_id;
        });

        return {
          status: 200,
          body: {
            "queryPNRTransaction": JSON.stringify(pnr_transactions),
          }
        };
      }
    };
  }

  @PutMongo({ pos: { x: 2759.2437412984145, y: -49.29241421791832 } })
  @Relation(r => dao.isSuccess(), 'ReturnSimulationResponse')
  async InsertPNRTransactions() {
  return {
        collectionName: `PNR_Transactions`,
        mode: `insert`,
        query: r => getBody("PNRTransactionInsertion")?.body?.queryPNRTransaction,
        queryKey: `{}`,
      };
  }

  @Script({ pos: { x: 473.3819535007739, y: 408.1129944286826 } })
  @Cachable({ cachable: false })
  @Relation(r => dao.isSuccess(), 'GenerateSimulationRequest')
  async StaticConfigurations() {
    const script = {

      execute: () => {
        return {
          body: {
            simulationAPIRequestType : "REGULAR",
            simulationAPIRequestnote : "ind"
          }
        };

      }
    }
  }

  @Script({ pos: { x: 704.7099634613137, y: 271.4913276447076 } })
  @Relation(r => dao.isSuccess(), 'SimulationAPICall')
  async GenerateSimulationRequest() {
    const script = {
      execute: () => {
        try {
          const requestPayload = getBody("AliasCheckedDataSagregation")?.body?.passedAliasPNRRequests;
          if (!requestPayload || !Array.isArray(requestPayload) || requestPayload.length === 0) {
            throw new Error('requestPayload is missing or empty');
          }
          const simulationRequests = requestPayload.map(payload => {


            const departureTime =payload?.customFields?.departure_time
            const arrivalTime = payload?.customFields?.arrival_time

            console.log("Extracted departure_time:", departureTime);
            console.log("Extracted arrival_time:", arrivalTime);

            const customFields = {
              ...payload.customFields,
              ffn: payload.identifierValue,
              departure_time: departureTime,
              arrival_time: arrivalTime
            };

            return {
              headers: {
                "Content-Type": "application/json",
                ...(getEffectiveHeaders())
              },
              queryParams: {
                identifierName: "externalId",
                identifierValue: payload.identifierValue
              },
              body: JSON.stringify({
                type: getBody("StaticConfigurations")?.body?.simulationAPIRequestType,
                billNumber: payload.billNumber,
                billAmount: payload.billAmount,
                billingDate: payload.billingDate,
                note: payload?.note,
                source: payload.extendedFields?.transaction_source || "",
                paymentModes: payload?.paymentModes?.map(paymentMode => ({
                  mode: paymentMode.mode,
                  value: paymentMode.value,
                  notes: paymentMode.notes,
                  attributes: {
                    amount: paymentMode.attributes?.value || ""
                  }
                })),
                customFields,
                extendedFields: payload.extendedFields,
                lineItemsV2: payload.lineItemsV2
              })
            };
          });
          return simulationRequests;


        } catch (error) {
          // Log any errors that occur during execution
          console.error(`Error in script execution: ${error.message}`);
          const errors = [{
            "status": false,
            "message": error.message,
            "code": 500
          }]

          // Return an error response
          return {
            status: 200,
            body: {
              success: false,
              errors
            }
          };
        }
      }
    };
  }

  @ApiRequest({ pos: { x: 938.5277777777778, y: 274.0277777777778 } })
  @Relation(r => dao.isSuccess(), 'checkFlow')
  @Relation(r => dao.hasError(), 'SimulationRequestFailed')
  async SimulationAPICall() {
  return {
        url: `https://apac.api.capillarytech.com/v2/simulation/transactions`,
        method: `POST`,
      };
  }

  @GetMongo({ pos: { x: 1786.4874031307468, y: 225.98825627047864 } })
  @Relation(r => dao.isSuccess(), 'checkIfPNRPresent')
  async CheckPNRPresent() {
  return {
        collectionName: `PNR`,
        query: r => getBody("PayloadTransformer")?.body?.getPNRQuery,
        sort: `{"_id":-1}`,
        limit: 1,
      };
  }

  @Script({ pos: { x: 2039.2574351115986, y: 196.1562092067673 } })
  @Relation(r => dao.getBody()?.body?.isEmpty, 'InsertPNR')
  @Relation(r => !dao.getBody()?.body?.isEmpty, 'UpdatePNRFlow')
  async checkIfPNRPresent() {
    const script = {
      execute: () => {

        const alreadyPresentPNR = getOut("CheckPNRPresent");

        //const isEmpty = !alreadyPresentPNR || Object.keys(alreadyPresentPNR).length === 0;
        const isEmpty = Object.keys(alreadyPresentPNR).length === 0 ? true : false;
        // return {
        //   http: {
        //     "res": {
        //       "json": {
        //         len:alreadyPresentPNR?.length,
        //         alreadyPresentPNR,
        //         isEmpty,
        //         //out:getOut("CheckPNRPresent"),
        //         body :getBody("CheckPNRPresent"),
        //         //aa:getBody("PayloadTransformer")?.body, //?.insertPNRQuery},
        //         alreadyPresentPNR,
        //         isEmpty
        //       },
        //       "status": 200,
        //     }
        //   }
        // };

        return {
          status: 200,
          body: {
            alreadyPresentPNR,
            isEmpty
          }
        };
      }
    };
  }

  @Script({ pos: { x: 2287.713957967776, y: 265.73074361886944 } })
  @Relation(r => dao.isSuccess(), 'DeactivateOldPNRRecords')
  async UpdatePNRFlow() {
    const script = {
      execute: () => {
        // try{
        const currentDate = new Date();
        const requestBody = getOut("checkIfPNRPresent")?.[0]?.body;

    // return {
    //       http: {
    //         "res": {
    //           "json": {
    //             //'showmsg': getOut(),
    //             //oldPNRRecord,
    //             //'msg3': getOut("PayloadTransformer").body.pnrNumber,
    //             'msg4': getOut("checkIfPNRPresent")
    //           },
    //           "status": 200,
    //         }
    //       }
    //     };

        // Assuming oldPNRRecord is an array, get the first element
        const oldPNRRecord = requestBody.alreadyPresentPNR[0];



        const pnrNumber = getBody("PayloadTransformer")?.body?.pnrNumber;


        // // Check if oldPNRRecord exists and has the 'version' property
        if (!oldPNRRecord || oldPNRRecord.is_active === "false") {
          logger.info('No active old PNR Record found to update, adding new');
        }

        // Extract the latestVersion from oldPNRRecord
        // const latestVersion = oldPNRRecord.version+1;

        //   const deactivateOldPNRRecords = {
        //          // Query to find matching bill_number
        //          $set: { "pnr_number": pnrNumber ,"is_active": "false" }  // Use $set to update simulation_response

        // }

        const deactivateOldPNRRecord = JSON.stringify({
          $set: { "is_active": false, "date_updated": currentDate }
        });
        const deactivateOldPNRQueryKey = JSON.stringify({ "pnr_number": pnrNumber, "is_active": true });


        return {
          body: {
            deactivateOldPNRRecord,
            deactivateOldPNRQueryKey
          }
        }
        // }catch (error) {
        //   // Log any errors that occur during execution
        //   logger.error(`Error in script execution: ${error.message}`);

        //   // Return an error response
        //   return {
        //     status: 500,
        //     body: {
        //       error: error.message
        //     }
        //   };
        // }
      }
    }
  }

  @PutMongo({ pos: { x: 2532.216052227172, y: 265.2512643291783 } })
  @Relation(r => dao.isSuccess(), 'DeactivateOldPNRTransactions')
  async DeactivateOldPNRRecords() {
  return {
        collectionName: `PNR`,
        mode: `update`,
        query: r => getBody("UpdatePNRFlow")?.body?.deactivateOldPNRRecord,
        queryKey: r => getBody("UpdatePNRFlow")?.body?.deactivateOldPNRQueryKey,
      };
  }

  @Script({ pos: { x: 1207.6436250869076, y: 374.20926806760997 } })
  async SimulationRequestFailed() {
    const script = {
        execute: () => {
            const multiError = getMultiError("SimulationAPICall");

            const parseXMLString = (xmlString) => {
                const codeMatch = xmlString.match(/<code>(.*?)<\/code>/);
                const messageMatch = xmlString.match(/<message>(.*?)<\/message>/);

                const code = codeMatch ? codeMatch[1] : '';
                const message = messageMatch ? messageMatch[1] : '';

                return {
                    status: false,
                    message: message,
                    code: parseInt(code, 10)
                };
            };

            const formattedErrors = multiError?.map(err => {
                const xmlMessage = err.message;
                return parseXMLString(xmlMessage);
            });


            return {
                http: {
                    "res": {
                        "json": {
                            success: false,
                            errors: formattedErrors
                        },
                        "status": 200,
                    }
                }
            };
        }
    }
  }

  @Script({ pos: { x: -204.60371920408917, y: 166.81671339890417 } })
  @Relation(r => dao.isSuccess(), 'AliasCheckApi')
  async AliasCheckPayload() {
    const script = {
      execute: () => {
        try {
          const requestPayload = getBody("AdditionalValidationBlock")?.body?.validPayload;

          if (!requestPayload || !Array.isArray(requestPayload) || requestPayload.length === 0) {
            throw new Error('requestPayload is missing or empty');
          }

          let requestHeaders = getEffectiveHeaders();
          delete requestHeaders["x-cap-neo-test-variant-id"];
          const commonHeaders = {
            //"x-cap-neo-test-variant-id" : "fa8e5964-6479-4d5e-a720-e4e56751acb61",
            ...(requestHeaders)
          };

          const aliasCheckRequests = requestPayload.map(payload => ({
            headers: commonHeaders,
            queryParams: {
              FFN: payload.identifierValue, 
              Fname: payload.extendedFields.booking_first_name,
              lname: payload.extendedFields.booking_last_name
            }
          }));

          return aliasCheckRequests

        } catch (error) {
          // Log any errors that occur during execution
          console.error(`Error in script execution: ${error.message}`);

          // Return an error response
          return {
            status: 500,
            body: {
              success: false,
              error: error.message
            }
          };
        }
      }
    };
  }

  @ApiRequest({ pos: { x: 36.45900785758545, y: 170.00893549538807 } })
  @Relation(r => dao.hasError(), 'AliasCheckFailed')
  @Relation(r => dao.isSuccess(), 'AliasCheckedDataSagregation')
  async AliasCheckApi() {
  return {
        url: `http://neo-a.default:3000/api/v1/xto6x/execute/ValidateFFN`,
        method: `GET`,
      };
  }

  @Script({ pos: { x: 268.25101870267116, y: 158.93111696872194 } })
  async AliasCheckFailed() {
    const script = {
      execute: () => {

        const errors = getMultiError("AliasCheckApi")
          .filter((error, index) => error !== null)
          .map(error => {
            const errorMessage = JSON.parse(error.message);
            return {

              status: errorMessage.status,
              message: errorMessage.message,
              code: errorMessage.code
            };
          });

        return {
          http: {
            "res": {
              "json": {
                success: false,
                "block": "AliasCheckFailed",
                errors
              },
              "status": 200,
            }
          }
        };


      }
    }
  }

  @PutMongo({ pos: { x: 2780.1903687087893, y: 250.98335182951928 } })
  @Relation(r => dao.isSuccess(), 'InsertPNRInUpdateFlow')
  async DeactivateOldPNRTransactions() {
  return {
        collectionName: `PNR_Transactions`,
        mode: `update`,
        query: r => getBody("UpdatePNRFlow")?.body?.deactivateOldPNRRecord,
        queryKey: r => getBody("UpdatePNRFlow")?.body?.deactivateOldPNRQueryKey,
      };
  }

  @PutMongo({ pos: { x: 3024.4836885240147, y: 263.1598837743047 } })
  @Relation(r => dao.isSuccess(), 'PNRTransactionInsertionInUpdateFlow')
  async InsertPNRInUpdateFlow() {
  return {
        collectionName: `PNR`,
        mode: `insert`,
        query: r => getBody("PayloadTransformer")?.body?.insertPNRQuery,
        queryKey: `{}`,
      };
  }

  @Script({ pos: { x: 3250.399155582119, y: 253.44650772140102 } })
  @Relation(r => dao.isSuccess(), 'InsertPNRTransactionInUpdateFlow')
  async PNRTransactionInsertionInUpdateFlow() {
    const script = {
      execute: () => {

        const pnr_transactions = getOut("PayloadTransformer")?.[0]?.body?.PNR_Transactions;
        //const parentPNR_id = JSON.parse(getBody().insertedId.toString()).value;


        const parentPNR_id = Object.values(getBody("InsertPNRInUpdateFlow")?.insertedId.buffer).map(byte => byte.toString(16).padStart(2, '0')).join('');

        pnr_transactions?.forEach(transaction => {
          // Assign the parentPNR_id to each transaction object
          transaction.parentPNR_id = parentPNR_id;
        });

        return {
          status: 200,
          // putMongoSpecs: {
          //   query: JSON.stringify(pnr_transaction)
          // }
          body:{
            queryPNRTransaction : JSON.stringify(pnr_transactions),
          }
        };
      }
    };
  }

  @PutMongo({ pos: { x: 3489.2173552357226, y: 252.77176727433016 } })
  @Relation(r => dao.isSuccess(), 'ReturnSimulationResponseInUpdateFlow')
  async InsertPNRTransactionInUpdateFlow() {
  return {
        collectionName: `PNR_Transactions`,
        mode: `insert`,
        query: r => getBody("PNRTransactionInsertionInUpdateFlow")?.body?.queryPNRTransaction,
        queryKey: `{}`,
      };
  }

  @Script({ pos: { x: 2999.7135649587703, y: -48.326549507015386 } })
  async ReturnSimulationResponse() {
    const script = {
      execute: () => {
        const errorResponse = Object.values(getBody("AdditionalValidationBlock")?.body?.errorMap || {});
        const validationWarnings = getBody("AdditionalValidationBlock")?.body?.warnings || {};
        const simulationResponse = getMultiBody("SimulationAPICall");
        const aliasFailures = getBody("AliasCheckedDataSagregation")?.body?.failedAliasErrors;

        const combinedLength = simulationResponse.length + Object.keys(aliasFailures).length;

        const combinedResponse = Array(combinedLength).fill(null);

        Object.keys(aliasFailures).forEach(index => {
          const failureIndex = parseInt(index, 10);
          if (failureIndex >= 0 && failureIndex < combinedLength) {
            const { source, type, billingDate, note, paymentModes, redemptions, customFields, extendedFields, lineItemsV2, ...filteredData } = aliasFailures[index];
            combinedResponse[failureIndex] = filteredData;   //{...aliasFailures[index]};
            combinedResponse[failureIndex].warnings = [];
            combinedResponse[failureIndex].sideEffects = [];
            combinedResponse[failureIndex].simulatedPointsBreakup = null;
          }
        });

        // Place simulationResponse in remaining null positions in combinedResponse
        simulationResponse.forEach((response, index) => {
          let insertIndex = combinedResponse?.findIndex(item => item === null);
          if (insertIndex !== -1) {
            delete response.is_rejected;
            combinedResponse[insertIndex] = response;
            combinedResponse[insertIndex].identifierType = "externalId";
            combinedResponse[insertIndex].identifierValue = response?.simulatedPointsBreakup?.customFields?.find(field => field.name === "ffn")?.value || null;
            combinedResponse[insertIndex].billNumber = response?.simulatedPointsBreakup?.billDetails?.billNumber;
            combinedResponse[insertIndex].billAmount = response?.simulatedPointsBreakup?.billDetails?.billAmount;
          }
        });

        if (Object.keys(validationWarnings)?.length > 0) {
          for (let i = 0; i < combinedResponse?.length; i++) {
            if (validationWarnings[combinedResponse[i]?.billNumber]) {
              combinedResponse[i].warnings = [...combinedResponse[i].warnings, ...validationWarnings[combinedResponse[i]?.billNumber]];
            }
          }
        }


         return {
          http: {
            "res": {
              "json": [...combinedResponse, ...errorResponse?.flat()],
              "status": 200,
            }
          }
        };
        //g return {
        //   body: [...combinedResponse, ...errorResponse?.flat()]
        // };
      }
    }
  }

  @Script({ pos: { x: 3780.952237024208, y: 255.34314565840123 } })
  async ReturnSimulationResponseInUpdateFlow() {
    const script = {
      execute: () => {

        const errorResponse = Object.values(getBody("AdditionalValidationBlock")?.body?.errorMap || {});
        const validationWarnings = getBody("AdditionalValidationBlock")?.body?.warnings || {};
        const simulationResponse = getMultiBody("SimulationAPICall");
        const aliasFailures = getBody("AliasCheckedDataSagregation")?.body?.failedAliasErrors;

        const combinedLength = simulationResponse.length + Object.keys(aliasFailures).length;

        const combinedResponse = Array(combinedLength).fill(null);

        Object.keys(aliasFailures).forEach(index => {
          const failureIndex = parseInt(index, 10);
          if (failureIndex >= 0 && failureIndex < combinedLength) {
            const { source, type, billingDate, note, paymentModes, redemptions, customFields, extendedFields, lineItemsV2, ...filteredData } = aliasFailures[index];
            combinedResponse[failureIndex] = filteredData;   //{...aliasFailures[index]};
            combinedResponse[failureIndex].warnings = [];
            combinedResponse[failureIndex].sideEffects = [];
            combinedResponse[failureIndex].simulatedPointsBreakup = null;
          }
        });

        // Place simulationResponse in remaining null positions in combinedResponse
        simulationResponse.forEach((response, index) => {
          let insertIndex = combinedResponse?.findIndex(item => item === null);
          if (insertIndex !== -1) {
            delete response.is_rejected;
            combinedResponse[insertIndex] = response;
            combinedResponse[insertIndex].identifierType = "externalId";
            combinedResponse[insertIndex].identifierValue = response?.simulatedPointsBreakup?.customFields?.find(field => field.name === "ffn")?.value || null;
            combinedResponse[insertIndex].billAmount = response?.simulatedPointsBreakup?.billDetails?.billAmount;
            combinedResponse[insertIndex].billNumber = response?.simulatedPointsBreakup?.billDetails?.billNumber;
          }
        });
        if (Object.keys(validationWarnings)?.length > 0) {
          for (let i = 0; i < combinedResponse?.length; i++) {
            if (validationWarnings[combinedResponse[i]?.billNumber]) {
              combinedResponse[i].warnings = [...combinedResponse[i].warnings, ...validationWarnings[combinedResponse[i]?.billNumber]];
            }
          }
        }

      return {
          http: {
            "res": {
              "json": [...combinedResponse, ...errorResponse?.flat()],
              "status": 200,
            }
          }
        };

        // return {
        //   body: [...combinedResponse, ...errorResponse?.flat()]
        // };
      }
    }
  }

  @Script({ pos: { x: 256.2126727787505, y: 284.24544712382203 } })
  @Relation(r => dao.isSuccess() && (dao.getBody("AliasCheckedDataSagregation")?.body?.isAliasFailedForAll), 'AllRequestFailures')
  @Relation(r => dao.isSuccess() && !(dao.getBody("AliasCheckedDataSagregation")?.body?.isAliasFailedForAll), 'StaticConfigurations')
  async AliasCheckedDataSagregation() {
    const script = {
      execute: () => {
        const multiBody = getMultiBody("AliasCheckApi");
        const apiRequests = getBody("AdditionalValidationBlock")?.body?.validPayload;
        const multiError = getMultiBody("AliasCheckApi");

        let isAliasFailedForAll = false;

        // Step 1: Determine which FFNs passed
        const validFFNs = new Set(
          multiBody?.filter(item => item.status).map(item => item.FFN)
        );

        // Step 2: Filter successful requests
        let successfulRequests = apiRequests?.filter(item =>
          validFFNs.has(item.identifierValue)
        );

        // Step 3: Track errors
        let errorMessages = [];

        for (let apiRequest of apiRequests) {
          let errorAlias = multiBody?.find(
            error => error.FFN === apiRequest.identifierValue
          );

          if (errorAlias && !errorAlias.status) {
            apiRequest.errors = [
              {
                status: errorAlias.status,
                code: errorAlias.code,
                message: errorAlias.message
              }
            ];
            errorMessages.push(apiRequest);
          }
        }

        if (!successfulRequests || successfulRequests.length === 0) {
          isAliasFailedForAll = true;
        }

        // ✅ Step 4: Transform arrival/departure into correct fields
        successfulRequests = successfulRequests.map(req => {
          const ext = req.extendedFields || {};
          const cust = req.customFields || {};


          const { date: depDate, time: depTime } = processDateTime(ext.departure_date);
          const { date: arrDate, time: arrTime } = processDateTime(ext.arrival_date);
          // Inject times into customFields
          cust.departure_time = depTime
          cust.arrival_time = arrTime;

          // Replace datetime with date-only in extendedFields
          ext.departure_date = depDate
          ext.arrival_date = arrDate
          return {
            ...req,
            customFields: cust,
            extendedFields: ext
          };
        });

        // ✅ Final Return
        return {
          body: {
            passedAliasPNRRequests: successfulRequests,
            failedAliasErrors: errorMessages,
            isAliasFailedForAll: isAliasFailedForAll
          }
        };
      }
    };

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

  @Script({ pos: { x: 475.21433974130775, y: 280.8891435645567 } })
  async AllRequestFailures() {
    const script = {
        execute: () => {

            const errorResponse = Object.values(getBody("AdditionalValidationBlock")?.body?.errorMap || {});
            const extractedData = getBody('AliasCheckedDataSagregation')?.body?.failedAliasErrors?.map(({ identifierType, identifierValue, billNumber, billAmount, errors }) => ({
                identifierType,
                identifierValue,
                billNumber,
                billAmount,
                errors,
            }));


            return {
                http: {
                    "res": {
                        "json": [...extractedData,...errorResponse?.flat()],
                        "status": 200,
                    }
                }
            };

        }
    };
  }

  @Script({ pos: { x: -704.9516473012814, y: 127.62937932271558 } })
  @Relation(r => dao.isSuccess() && !(dao.getBody("requestRejectionRule")?.body?.wholePayloadError), 'AdditionalValidationBlock')
  @Relation(r => dao.isSuccess() && (dao.getBody("requestRejectionRule")?.body?.wholePayloadError), 'HandleValidationFailure')
  async requestRejectionRule() {
    const script = {
      execute: () => {
        let errors = [];
        let validPayload = [];
        let errorMap = {};
        let wholePayloadError = false;
        let pnrNumberMissing = false;
        let uniquePnrIdentified = false;
        let invalidPayloadList = new Set();
        const requestPayload = getApiRequest()?.body;

        let validationErrors = getIn("requestRejectionRule")?.err || [];


        if (validationErrors?.length > 0) {
          validationErrors?.forEach((validationError) => {
            const error = {
              status: false,
              code: 400,
              message: validationError.message,
              path: validationError.instancePath,
            };
            errors.push(error);
            if (validationError.instancePath === "/body") {
              wholePayloadError = true;
            }
            if (validationError?.instancePath?.split("/")?.[2]) {
              invalidPayloadList.add(
                Number(validationError?.instancePath?.split("/")?.[2])
              );
            }
            if (errorMap[Number(validationError?.instancePath?.split("/")?.[2])]) {
              errorMap[Number(validationError?.instancePath?.split("/")?.[2])].push(
                error
              );
            } else {
              errorMap[Number(validationError?.instancePath?.split("/")?.[2])] = [
                error,
              ];
            }
          });
          if (getApiRequest()?.body?.length === invalidPayloadList?.size) {
            wholePayloadError = true;
          }
        }
        Object.keys(errorMap)?.forEach((key, index) => {
          errorMap[Number(key)] = [
            {
              identifierType:
                getApiRequest()?.body?.[
                  key
                ]?.identifierType,
              identifierValue:
                getApiRequest()?.body?.[
                  key
                ]?.identifierValue,
              billAmount:
                getApiRequest()?.body?.[
                  key
                ]?.billAmount,
              billNumber:
                getApiRequest()?.body?.[
                  key
                ]?.billNumber,
              errors: errorMap[Number(key)],
              warnings: []
            },
          ];
        });
        const firstPnrNumber = requestPayload?.[0]?.extendedFields?.pnr_number;
        requestPayload?.map((payloadObj) => {
          if (!payloadObj?.extendedFields?.pnr_number) {
            pnrNumberMissing = true;
          }
        });
        // Check if all pnr_numbers are the same
        uniquePnrIdentified = requestPayload.every(
          (request) => request.extendedFields.pnr_number === firstPnrNumber
        );

        if (!uniquePnrIdentified || pnrNumberMissing) {
          wholePayloadError = true;
          errors.push({
            success: false,
            code: 3001,
            message: "PNR number must be the same for all requests",
          });
        }
        if (!wholePayloadError) {
          validPayload = getApiRequest()?.body?.filter(
            (item, index) => !invalidPayloadList.has(index)
          );
        }

        return {
          body: {
            success: false,
            uniquePnrIdentified,
            errors,
            validPayload,
            wholePayloadError,
            errorMap,
          },
        };
      },
    };
  }

  @Script({ pos: { x: -449.79233111877227, y: 143.71614845123196 } })
  @Relation(r => dao.isSuccess(), 'AliasCheckPayload')
  async AdditionalValidationBlock() {
    // Check if date string is a valid ISO 8601 datetime
    const isValidISODate = (dateString) => {
      try {
        // Use Date.parse which parses ISO 8601 string, returns NaN if invalid
        const timestamp = Date.parse(dateString);
        if (isNaN(timestamp)) return false;

        // Additional check: ensure the string is actually in ISO format (basic)
        // Regex checks for ISO 8601 datetime e.g. 2025-08-13T03:10:49+05:30 or Z timezone
        const isoRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})$/;
        return isoRegex.test(dateString);
      } catch (e) {
        return false;
      }
    }

    const script = {
      execute: () => {
        let warnings = {};
        let validPayload = getBody("requestRejectionRule")?.body?.validPayload;
        for (let i = 0; i < validPayload?.length; i++) {
          if (validPayload[i]?.extendedFields?.arrival_date) {
            if (!isValidISODate(validPayload[i]?.extendedFields?.arrival_date)) {
              delete validPayload[i]?.extendedFields?.arrival_date;
              warnings[validPayload[i]?.billNumber] = [{
                status: false,
                code: 400,
                message: "Invalid arrival_date",
              }]
            }
          }
        }
        return {
          body: {
            ...getBody("requestRejectionRule")?.body,
            validPayload,
            warnings,
          },
        };
      },
    };
  }

  @Script({ pos: { x: -1643.1472648265444, y: 131.65591373350247 } })
  @Relation(r => dao.isSuccess(), 'checkPayload')
  async AppConfigurations() {
    const script = {

        execute: () => {

            const appVersion = "1.0.0";
            logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`)
            const developer = "Adarsh"
            const branch = "PSV-28853"
            const trigger = "/preFlownBookings"
            const requestBody = this.dao.getApiRequest()?.body?.[0];
            const externalId = requestBody?.identifierValue;
            const billNumber = requestBody?.billNumber;
            const isgRequestId = `${trigger}_${externalId}_${billNumber}`;
            logger.info(`IsgRequestId : ${JSON.stringify(isgRequestId)}`);

            return {
                body:
                {
                    APP_VERSION: appVersion
                }
            };

        }
    }
  }

  @Script({ pos: { x: -1347.1472648265444, y: 125.65591373350247 } })
  @Relation(r => dao.isSuccess(), 'RequestValidator')
  async checkPayload() {
    const script = {

        execute: () => {

            const payload = getApiRequest()?.body || [];
            const errors = [];

            if (!Array.isArray(payload) || payload.length === 0) {
                errors.push({
                    status: false,
                    code: 400,
                    message: "Invalid payload. Expected non-empty array.",
                    path: "/body"
                });
            }

            if (errors.length === 0) {

                const splitPresenceArray = payload.map(item =>
                    Boolean(item?.customFields?.split_from_pnr?.trim())
                );

                const allHaveSplit = splitPresenceArray.every(Boolean);
                const noneHaveSplit = splitPresenceArray.every(val => !val);

                if (!allHaveSplit && !noneHaveSplit) {
                    errors.push({
                        status: false,
                        code: 3002,
                        message: "Mixed payload not allowed. Either all transactions must have split_from_pnr or none.",
                        path: "/body"
                    });
                }
            }

            // ❌ ERROR FLOW
            if (errors.length > 0) {

                const responseBody = {
                    success: false,
                    errors
                };

                // ✅ Add requestBody only if 3002 exists
                if (errors.some(err => err.code === 3002)) {
                    responseBody.requestBody = payload;
                }

                return {
                    http: {
                        res: {
                            status: 400,
                            json: responseBody,
                            headers: getEffectiveHeaders()
                        }
                    }
                };
            }

            // ✅ VALID FLOW
            const splitPresenceArray = payload.map(item =>
                Boolean(item?.customFields?.split_from_pnr?.trim())
            );

            const allHaveSplit = splitPresenceArray.every(Boolean);

            return {
                body: {
                    success: true,
                    flowType: allHaveSplit ? "splitFlow" : "regularFlow"
                }
            };
        }
    };
  }

  @Script({ pos: { x: 1191.4722222222222, y: 146.93518518518522 } })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.body?.flowType === "splitFlow"), 'prepareDeactivatePNRAPI')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.body?.flowType === "regularFlow"), 'PayloadTransformer')
  async checkFlow() {
    const script = {

        execute: () => {
            let flowType = getBody("checkPayload")?.body?.flowType

            return {
                body: {
                    flowType
                }
            };

        }
    }
  }

  @Script({ pos: { x: 1412.6203703703702, y: -17.40740740740739 } })
  @Relation(r => dao.isSuccess(), 'DeactivatePNRAPICall')
  async prepareDeactivatePNRAPI() {
    const script = {

        execute: () => {

            let req = getApiRequest();
            let payload = req?.body || [];
            let reqHeaders = req?.headers || {};
            delete reqHeaders["x-cap-neo-test-variant-id"]
            const transformedArray = payload.map(item => ({
                pnr_number: item?.extendedFields?.pnr_number,
                split_bill_numbers: item?.customFields?.split_bill_numbers,
                billNumber: item?.billNumber,
                identifierValue: item?.identifierValue,
                is_active: false,
                deactivation_reason: "SPLIT",
                split_to_pnr: item?.customFields?.split_from_pnr
            }));

            logger.info("Transformed Payload: " + JSON.stringify(transformedArray));

            return {
                headers: reqHeaders,
                body: JSON.stringify(transformedArray)
            };
        }
    };
  }

  @ApiRequest({ pos: { x: 1639.3703703703702, y: -30.666666666666686 } })
  @Relation(r => dao.isSuccess(), 'DeactivateErrorInAddModify')
  @Relation(r => dao.hasError(), 'DeactivateErrorInAddModify')
  async DeactivatePNRAPICall() {
  return {
        url: `http://neo-a.default:3000/api/v1/xto6x/execute/deactivatePNR`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 1900.7518945117308, y: -31.83844807013915 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess(), 'PayloadTransformer')
  async DeactivateErrorInAddModify() {
    const script = {
        execute: () => {
            const data = getBody();
            logger.error('deactivating SplitTransactions', data);
            return data

            // return {
            //     http: {
            //         res: {
            //             status: 200,
            //             "headers": {
            //                 "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
            //             },
            //             json: {
            //                 success: false,
            //                 errors: [
            //                     {
            //                         status: false,
            //                         code: 400,
            //                         message: errorMessage,
            //                         path: "/body"
            //                     }
            //                 ]
            //             }
            //         }
            //     }
            // };
        }
    };
  }
}
