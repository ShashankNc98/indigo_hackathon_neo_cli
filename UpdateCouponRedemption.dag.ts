import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getError, getIn, getStatus } = dao;

@Dag({ method: "PUT", url: "update-coupon-redemption" })
class UpdateCouponRedemption {
  constructor() {
    this.VersionConfig();
  }

  @Script({ pos: { x: 333.1748249154049, y: 21.30620416892424 } })
  @Relation(r => dao.isSuccess(), 'UpdateExternalCouponRedeemApi')
  async ValidRequestExecution() {
    const script = {

        execute: () => {

            return {
                headers: {
                    "Content-Type": "application/json",
                    ...(getEffectiveHeaders())
                },
                body: JSON.stringify(getApiRequest().body)
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 597.9196547746348, y: -145.1965477463478 } })
  @Relation(r => dao.hasError(), 'FailedExternalCouponRedeemResp')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.success), 'prepareCustomerLookupApiRequest')
  @Relation(r => dao.isSuccess() && !(dao.getBody()?.success), 'NoRedemptionAvailable')
  async UpdateExternalCouponRedeemApi() {
  return {
        url: `https://apac.api.capillarytech.com/v2/coupon/redeem`,
        method: `PUT`,
      };
  }

  @Script({ pos: { x: 1537.2486692022444, y: -50.92560806313577 } })
  @Relation(r => dao.isSuccess(), 'UpsertCouponRedemption')
  async QueryPrepareForUpsert() {
    const script = {

        execute: () => {
            let requestPayload = getApiRequest().body;
            let currentDate = new Date();

            let couponRedemptionUpdateRespone = getBody("UpdateExternalCouponRedeemApi");
            let redemptions = couponRedemptionUpdateRespone.entity.redemptions;

            // Extract customer slab
            let customerGetResponse = getBody();
            let pointsSummary = customerGetResponse.pointsSummary;
            let customerSlab = pointsSummary.slabSNo;

            let bulkOps = [];
            try {
                redemptions.forEach(redemption => {
                    if (redemption.redemptionStatus.status == true) {
                        // Create the upsert query key
                        let upsertQueryKey = {
                            redemption_id: redemption?.redemptionId,
                            pnr_number: redemption?.transactionNumber,
                            ffn: requestPayload?.user?.externalId
                        };

                        if (upsertQueryKey.redemption_id == undefined ||
                            upsertQueryKey.pnr_number == undefined ||
                            upsertQueryKey.ffn == undefined) {
                            throw new Error('Error building query key');
                        }
                        let upsertQuery = JSON.stringify({
                            $set: {
                                is_reversed: false,
                                ...upsertQueryKey,
                                ...(redemption?.customFields),
                                "modified_date": currentDate
                            },
                            $setOnInsert: {
                                "customer_slab": customerSlab,
                                "created_date": currentDate
                            }
                        });

                        let bulkOp = {
                            query: upsertQuery,
                            queryKey: upsertQueryKey,
                        };

                        bulkOps.push(bulkOp);
                    }
                });

            } catch (err) {
                return buildQueryValidationError();
            }

            return bulkOps;
        }
    }

    function buildQueryValidationError() {
        return {
            http: {
                "res": {
                    "json": {
                        errors: [{
                            status: false,
                            "message": "Error updating coupon redemption details",
                            "code": 400
                        }]
                    },
                    "status": 400,
                    "headers": getBody("VersionConfig").headers
                }
            }
        };
    }
  }

  @Script({ pos: { x: 902.1730287226715, y: -208.8142004208925 } })
  async FailedExternalCouponRedeemResp() {
    const script = {

        execute: () => {

            return {
                http: {
                    res: {
                        status: getBody().code,
                        json: getBody().err.message,
                        "headers": getBody("VersionConfig").headers
                    }
                }
            }

        }
    }
  }

  @Script({ pos: { x: 2081.653304364363, y: -63.22340515356046 } })
  @ExecutionStrategy('or')
  async ApiEndResponseUpdate() {
    const script = {

        execute: () => {

            return {
               http: {
                   "res": {
                       "json": getBody("UpdateExternalCouponRedeemApi"),
                        "headers": getBody("VersionConfig").headers
                   }
               }
           };

        }
    }
  }

  @Script({ pos: { x: 902.0803452253651, y: 44.18241328167329 } })
  async NoRedemptionAvailable() {
    const script = {

        execute: () => {

            return {
               http: {
                   "res": {
                       "json": getBody("UpdateExternalCouponRedeemApi"),
                       "status": getStatus("UpdateExternalCouponRedeemApi"),
                        "headers": getBody("VersionConfig").headers
                   }
               }
           };

        }
    }
  }

  @Script({ pos: { x: 321.13137925351936, y: -281.2748084359887 } })
  async ValidationFailures() {
    const script = {
      execute: () => {
        const errorArray = [];
        const validationErrors = getError("RequestValidator")?.err;
        validationErrors?.forEach(validationError => {
          const error = {
            status: false,
            "message": validationError.message,
            "code": 400
          }
          errorArray.push(error)
        });

        return {
          http: {
            "res": {
              "json": {
                errors: errorArray
              },
              "status": 400,
              "headers": getBody("VersionConfig").headers
            }
          }
        };




      }
    }
  }

  @Schema({ pos: { x: 0.9510505075702724, y: -208.4279105204508 } })
  @Relation(r => dao.hasError(), 'ValidationFailures')
  @Relation(r => dao.isSuccess(), 'ValidRequestExecution')
  async RequestValidator() {
    return {
        definitions: [],
        spec: {
            type: "object",
            properties: {
                body: {
                    type: 'object',
                    properties: {
                        redemptions: {
                            type: 'array',
                            minItems: 1,
                            maxItems: 20,
                            items: {
                                type: 'object',
                                properties: {
                                    redemptionId: {
                                        minLength: 1,
                                        "errorMessage": {
                                            minLength: "redemptionId must not be empty"
                                        }
                                    },
                                    transactionNumber: {
                                        type: 'string',
                                        transform: ['trim'],
                                        minLength: 1,
                                        "errorMessage": {
                                            minLength: "transactionNumber must not be empty"
                                        }
                                    }

                                },
                                required: ['transactionNumber', 'redemptionId',],
                                errorMessage: {
                                    required: {
                                        transactionNumber: "transactionNumber is missing",
                                        redemptionId: "redemptionId is missing"
                                    }
                                }
                            }
                        },
                        user: {
                            type: "object",
                            properties: {
                                externalId: {
                                    type: "string",
                                    minLength: 1,
                                    errorMessage: {
                                        minLength: "externalId must not be empty",
                                    },
                                }
                            },
                            required: ["externalId"],
                            errorMessage: {
                                required: {
                                    externalId: "externalId is missing"
                                },
                            },
                        },
                    },
                    required: ['user', 'redemptions'],
                    errorMessage: {
                        required: {
                            user: "user details are missing",
                            redemptions: "redemptions details are missing"
                        }
                    }
                }
            },
            required: ['body'],
            errorMessage: {
                required: {
                    body: "Payload is missing"
                }
            }
        }
    }
  }

  @Script({ pos: { x: 921.9196547746348, y: -87.19654774634773 } })
  @Relation(r => dao.isSuccess(), 'CustomerLookupApiCall')
  async prepareCustomerLookupApiRequest() {
    const script = {
        execute: () => {

            let requestPayload = getApiRequest().body;

            let requestHeaders = getEffectiveHeaders();
            delete requestHeaders["x-cap-neo-test-variant-id"];

            let queryParameters = {
                "identifierName" : "externalId",
                "identifierValue" : requestPayload?.user?.externalId,
                "source" : "INSTORE"
            }
            return {
                headers : requestHeaders,
                queryParams : queryParameters
            };
        }
    }
  }

  @ApiRequest({ pos: { x: 1208.1386803721198, y: -143.07314804916444 } })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.errors?.length), 'CustomerGetError')
  @Relation(r => dao.isSuccess() && !(dao.getBody()?.errors?.length), 'QueryPrepareForUpsert')
  @Relation(r => dao.hasError(), 'CustomerLookupApiCallFailure')
  async CustomerLookupApiCall() {
  return {
        url: `https://apac.api.capillarytech.com/v2/customers/lookup/customerDetails`,
        method: `GET`,
        queryParams: {
    "embed": "points"
  },
      };
  }

  @Script({ pos: { x: 1526.1386803721198, y: 141.7700241148189 } })
  async CustomerLookupApiCallFailure() {
    const script = {

        execute: () => {
            return {
                http: {
                    res: {
                        status: getBody().code,
                        json: getBody().err?.message,
                        "headers": getBody("VersionConfig").headers
                    }
                }
            }
        }

    }
  }

  @Script({ pos: { x: 1506.1386803721198, y: -231.70245104473787 } })
  async CustomerGetError() {
    const script = {

        execute: () => {
            let customerGetResponse = getIn();
            let error = customerGetResponse.errors?.[0];
            return {
                http: {
                    "res": {
                        status : 200,
                        "json" : error,
                        "headers": getBody("VersionConfig").headers
                    }
                }
            }
        }

    }
  }

  @PutMongo({ pos: { x: 1805.2486692022444, y: -62.92560806313577 } })
  @Relation(r => dao.isSuccess(), 'ApiEndResponseUpdate')
  async UpsertCouponRedemption() {
  return {
        collectionName: `Coupon_Redemption`,
        mode: `upsert`,
        query: r => getBody().query,
        queryKey: r => getBody().queryKey,
      };
  }

  @Script({ pos: { x: -556, y: 28 } })
  @Relation(r => dao.isSuccess(), 'RequestValidator')
  async VersionConfig() {
    const script = {
        execute: () => {
            return {
                "headers": {
                    "x-cap-isg-neo-verison": 1.1
                }
            }
        }
    }
  }
}
