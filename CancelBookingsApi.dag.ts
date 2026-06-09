import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn, getMultiBody, getOut } = dao;

@Dag({ method: "POST", url: "cancelBookings" })
class CancelBookingsApi {
  constructor() {
    this.validatePayload();
  }

  @Schema({ pos: { x: -1005, y: 203 } })
  @Relation(r => dao.hasError(), 'validationFailureBlock')
  @Relation(r => dao.isSuccess(), 'MongoGetPnrSpec')
  async validatePayload() {
    return {
      definitions: [],
      spec: {
        type: 'object',
        properties: {
          body: {
            type: 'array',
            minItems: 1,
            maxItems: 1,
            errorMessage: {
              type: 'The payload must be an array',
              minItems: 'The payload must contain atleast one item',
              maxItems: 'The payload must contain atmost one item',
            },
            items: {
              type: 'object',
              // maxProperties: 3,
              // minProperties: 3,
              properties: {
                returnType: {
                  type: 'string',
                  enum: ['FULL'],
                  errorMessage: {
                    enum: 'The returnType property must be `FULL`'
                  }
                },
                type: {
                  type: 'string',
                  enum: ['RETURN'],
                  errorMessage: {
                    enum: 'The type property must be `RETURN`'
                  }
                },
                extendedFields: {
                  type: 'object',
                  // minProperties: 1,
                  // maxProperties: 1,
                  properties: {
                    pnrnumber: {
                      type: 'string',
                      minLength: 1,
                    }
                  },
                  required: ['pnrnumber'],
                  errorMessage: {
                    required: {
                      pnrnumber: 'pnrnumber extendedField is missing',
                    },
                  },
                }
              },
              required: ['returnType', 'type', 'extendedFields'],
              errorMessage: {
                required: {
                  returnType: 'returnType is missing',
                  type: 'type is missing',
                  extendedFields: 'extendedFields are missing',
                },
              },
            },
          },
        },
      },
    }
  }

  @Script({ pos: { x: -635.1243283341903, y: 48.83206508229915 } })
  async validationFailureBlock() {
    const script = {
      execute: () => {
        const errorArray = [];
        const validationErrors = getIn()?.err;
        validationErrors?.forEach((validationError) => {
          const error = {
            status: false,
            message: `${validationError.message} at ${validationError.instancePath}`,
            code: 1006,
          };
          errorArray.push(error);
        });

        return {
          http: {
            res: {
              status: 400,
              json: {
                errors: errorArray
              }
            }
          }
        }
      }
    };
  }

  @Script({ pos: { x: -557.3684313333888, y: 319.72450402789775 } })
  @Relation(r => dao.isSuccess(), 'MongoGetPnrBlock')
  async MongoGetPnrSpec() {
    const script = {
      execute: () => {
        let pnrList = getApiRequest()?.body?.map((pnrElement) => {
          return pnrElement?.extendedFields?.pnrnumber;
        });

        let pnrTransactionsGetMongoQuery = {
          pnr_number: { "$in": pnrList },
          is_active: true
        };

        let pnrGetMongoQuery = {
          pnr_number: { "$in": pnrList },
          is_active: true,
          "payload.extendedFields.flight_status": { $not: { $regex: '^flown$', $options: 'i' } },
        };

        return {
          body: {
            getPNRQuery: JSON.stringify(pnrGetMongoQuery),
            getPNRTransactionsQuery: JSON.stringify(pnrTransactionsGetMongoQuery),
            pnrList,
          },
        };
      },
    };
  }

  @GetMongo({ pos: { x: -240.3067171479588, y: 266.78339219241445 } })
  @Relation(r => dao.isSuccess() && dao.getOut("MongoGetPnrBlock")?.length > 0, 'DBGetPnrResponseHandlingAndSpecs')
  @Relation(r => dao.isSuccess() && !dao.getOut("MongoGetPnrBlock")?.length > 0, 'CheckIfPNRIsPostFlownOrCancelled')
  async MongoGetPnrBlock() {
  return {
        collectionName: `PNR_Transactions`,
        query: r => getBody().body.getPNRTransactionsQuery,
        sort: `{}`,
      };
  }

  @Script({ pos: { x: 298.39453016986636, y: 177.25286628553704 } })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.body?.validPnrList?.length == 0), 'ValidationErrorHandlingBlock')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.body?.validPnrList?.length > 0 && dao.getBody()?.body?.redemptionIds?.length > 0), 'GetCouponRedemption')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.body?.validPnrList?.length > 0 && dao.getBody()?.body?.redemptionIds?.length == 0), 'MongoSetPnrTransactionsBlockClone')
  async DBGetPnrResponseHandlingAndSpecs() {
    const script = {
      execute: () => {
        let reqBody = getBody("MongoGetPnrSpec");
        let dbPnrList = [...new Set(getOut()?.map((dbPnr) => dbPnr.pnr_number))];

        let validPnrList = dbPnrList?.filter((pnrObj) =>
          reqBody?.body?.pnrList?.includes(pnrObj)
        );
        let invalidPnrList = reqBody?.body?.pnrList?.filter(
          (pnrObj) => !dbPnrList?.includes(pnrObj)
        );

        let getPNRQuery = getBody("MongoGetPnrSpec")?.body?.getPNRQuery;

        let getPNRTransactionsQuery = getBody("MongoGetPnrSpec")?.body?.getPNRTransactionsQuery;
        let setQuery = JSON.stringify({
          $set: { is_active: false, is_cancelled: true },
        });


        let redemptionIds = [];

        for (let i = 0; i < getOut()?.length; i++) {
          if (
            getOut()?.[i]?.transaction_payload?.redemptions?.couponRedemptions &&
            Array.isArray(
              getOut()?.[i]?.transaction_payload?.redemptions?.couponRedemptions
            ) &&
            getOut()?.[i]?.transaction_payload?.redemptions?.couponRedemptions?.length >
            0
          ) {
            if (getOut()?.[i].transaction_payload.extendedFields.flight_status != "FLOWN") {
              redemptionIds = [
                ...redemptionIds,
                ...getOut()?.[i]?.transaction_payload?.redemptions?.couponRedemptions,
              ];
            }
          }
        }

        return {
          body: {
            getPNRQuery,
            getPNRTransactionsQuery,
            setQuery,
            validPnrList,
            dbPnrList,
            invalidPnrList,
            queryOptions: JSON.stringify({ multi: true }),
            redemptionIds,
            couponReversalRequired: validPnrList?.length > 0 && redemptionIds?.length > 0,
            getCouponQuery: JSON.stringify({ is_reversed: false, redemption_id: { $in: redemptionIds }, pnr_number: { $in: getBody("MongoGetPnrSpec")?.body?.pnrList } }),
            getCouponByPNRQuery: JSON.stringify({ is_reversed: false, pnr_number: { $in: getBody("MongoGetPnrSpec")?.body?.pnrList } }),
            setCouponQuery: JSON.stringify({ $set: { is_reversed: true } })
          },
        };
      },
    };
  }

  @PutMongo({ pos: { x: 3679.458852091485, y: 164.2706113019408 } })
  @Relation(r => dao.isSuccess(), 'MongoSetPnrBlock')
  async MongoSetPnrTransactionsBlock() {
  return {
        collectionName: `PNR_Transactions`,
        mode: `update`,
        query: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.setQuery,
        queryKey: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.getPNRTransactionsQuery,
        options: ``,
      };
  }

  @Script({ pos: { x: 1215.8474474877767, y: 84.93582602033223 } })
  async ValidationErrorHandlingBlock() {
    const script = {
      execute: () => {
        let reqBody = getBody('DBGetPnrResponseHandlingAndSpecs');
        return {
          http: {
            res: {
              json: {
                status: reqBody?.body?.validPnrList?.length > 0 ? 'success' : 'failed',
                code: reqBody?.body?.validPnrList?.length > 0 ? 200 : 400,
                message: reqBody?.body?.validPnrList?.length > 0 ? 'PNR cancellation is done successfully' : 'PNR cancellation failed',
                data: reqBody?.body?.validPnrList?.map((pnrnumber) => {
                  return pnrnumber;
                }),
                errors: reqBody?.body?.invalidPnrList?.map((pnrnumber) => {
                  return { success: false, code: 1007, message: 'Invalid PNR', pnrnumber };
                }),
                warnings: []
              }
            }
          }
        }
      }
    };
  }

  @PutMongo({ pos: { x: 3930.0205631892522, y: 156.24829219507802 } })
  @Relation(r => dao.isSuccess(), 'MongoPutCouponRedemptionBlock1')
  async MongoSetPnrBlock() {
  return {
        collectionName: `PNR`,
        mode: `update`,
        query: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.setQuery,
        queryKey: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.getPNRQuery,
      };
  }

  @Script({ pos: { x: 3032.8212477558236, y: 168.17666523466067 } })
  @Relation(r => dao.isSuccess(), 'reactivateApiCall')
  async reactivateCouponReqBlock() {
    const script = {
      execute: () => {
        const filteredRedemptionIds = getBody("FilterOutFlightCoupons").body.filteredRedemptionIds;

        return [{
          headers: {
            "Content-Type": "application/json",
            ...(getEffectiveHeaders())
          },
          body: JSON.stringify({ redemptionIds: filteredRedemptionIds }),
        }];
      }
    }
  }

  @ApiRequest({ pos: { x: 3351.9726546872657, y: 154.9408371025275 } })
  @Relation(r => dao.isSuccess(), 'MongoSetPnrTransactionsBlock')
  async reactivateApiCall() {
  return {
        url: `https://apac.api.capillarytech.com/v2/coupon/reactivate`,
        method: `POST`,
      };
  }

  @PutMongo({ pos: { x: 1296.1743048514168, y: 613.9747585185523 } })
  @Relation(r => dao.isSuccess(), 'MongoSetPnrBlockClone')
  async MongoSetPnrTransactionsBlockClone() {
  return {
        collectionName: `PNR_Transactions`,
        mode: `update`,
        query: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.setQuery,
        queryKey: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.getPNRTransactionsQuery,
      };
  }

  @PutMongo({ pos: { x: 1539.073922610183, y: 613.7158596451446 } })
  @Relation(r => dao.isSuccess(), 'MongoGetCouponRedemptionBlock3')
  async MongoSetPnrBlockClone() {
  return {
        collectionName: `PNR`,
        mode: `update`,
        query: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.setQuery,
        queryKey: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.getPNRQuery,
      };
  }

  @Script({ pos: { x: 4445.835164033948, y: 161.20688636958852 } })
  async ResponseWithCouponReversalBlock() {
    const script = {
      execute: () => {
        let reqBody = getBody('DBGetPnrResponseHandlingAndSpecs');
        let couponReactiveResp = getOut("reactivateApiCall")?.[0];
        return {
          http: {
            res: {
              json: {
                status: reqBody?.body?.validPnrList?.length > 0 ? 'success' : 'failed',
                code: reqBody?.body?.validPnrList?.length > 0 ? 200 : 400,
                message: reqBody?.body?.validPnrList?.length > 0 ? 'PNR cancellation is done successfully' : 'PNR cancellation failed',
                data: reqBody?.body?.validPnrList?.map((pnrnumber) => {
                  return pnrnumber;
                }),
                errors: reqBody?.body?.invalidPnrList?.map((pnrnumber) => {
                  return { success: false, code: 1007, message: 'Invalid PNR', pnrnumber };
                }),
                warnings: couponReactiveResp?.success ? [] : couponReactiveResp?.errors ?? []
              }
            }
          }
        }
      }
    };
  }

  @Script({ pos: { x: 2078.711928869633, y: 795.4263439361331 } })
  async ResponseHandlingBlock() {
    const script = {
      execute: () => {
        let reqBody = getBody('DBGetPnrResponseHandlingAndSpecs');
        return {
          http: {
            res: {
              json: {
                status: reqBody?.body?.validPnrList?.length > 0 ? 'success' : 'failed',
                code: reqBody?.body?.validPnrList?.length > 0 ? 200 : 400,
                message: reqBody?.body?.validPnrList?.length > 0 ? 'PNR cancellation is done successfully' : 'PNR cancellation failed',
                data: reqBody?.body?.validPnrList?.map((pnrnumber) => {
                  return pnrnumber;
                }),
                errors: reqBody?.body?.invalidPnrList?.map((pnrnumber) => {
                  return { success: false, code: 1007, message: 'Invalid PNR', pnrnumber };
                }),
                warnings: []
              }
            }
          }
        }
      }
    };
  }

  @Script({ pos: { x: 450.45136522205325, y: 698.0527927991347 } })
  @Relation(r => dao.isSuccess(), 'MongoGetCouponRedemptionBlock')
  async MongoGetCouponRedemptionSpec() {
    const script = {
        execute: () => {
            return {
                getQuery: { is_reversed: false, pnr_number: { $in: getBody("MongoGetPnrSpec")?.body?.pnrList } },
                updateQuery: { $set: { is_reversed: true } }
            };
        }
    }
  }

  @GetMongo({ pos: { x: 899.8107788320355, y: 757.8851243803032 } })
  @Relation(r => dao.isSuccess() && dao.getOut("MongoGetCouponRedemptionBlock")?.length > 0, 'ExtractFfnBasedOnRedemptionId')
  @Relation(r => (dao.hasError() || dao.getOut("MongoGetCouponRedemptionBlock")?.length == 0), 'InvalidPNRFailureHandlingBlock')
  async MongoGetCouponRedemptionBlock() {
  return {
        collectionName: `Coupon_Redemption`,
        query: r => getBody("MongoGetCouponRedemptionSpec")?.getQuery,
        sort: `{}`,
      };
  }

  @Script({ pos: { x: 1528.1306216522107, y: 1162.5259129937554 } })
  async InvalidPNRFailureHandlingBlock() {
    const script = {
        execute: () => {
            return {
                http: {
                    res: {
                        json: {
                            status: 'failed',
                            code: 400,
                            message: 'PNR cancellation failed',
                            errors: [{ success: false, code: 1007, message: 'Invalid PNR', pnrnumber: getBody("MongoGetPnrSpec")?.body?.pnrList?.join(", ") }],
                            warnings: []
                        }
                    }
                }
            }
        }
    }
  }

  @PutMongo({ pos: { x: 3460.039215144699, y: 846.9486468569291 } })
  @Relation(r => dao.isSuccess(), 'OtherTransactionResponseWithCouponReversalBlock')
  async MongoPutCouponRedemptionBlock() {
  return {
        collectionName: `Coupon_Redemption`,
        mode: `update`,
        query: r => getBody("FilterOutRedemptionIds")?.body.setCouponQuery,
        queryKey: r => getBody("FilterOutRedemptionIds")?.body.getCouponQuery,
      };
  }

  @Script({ pos: { x: 3722.610290828319, y: 840.0084318072504 } })
  async OtherTransactionResponseWithCouponReversalBlock() {
    const script = {
        execute: () => {
            return {
                http: {
                    res: {
                        json: {
                            status: 'success',
                            code: 200,
                            message: 'PNR cancellation is done successfully',
                            data: getBody("MongoGetPnrSpec")?.body?.pnrList,
                            errors: [],
                            warnings: []
                        }
                    }
                }
            }
        }
    }
  }

  @Script({ pos: { x: 2944.770416442704, y: 864.1292680770459 } })
  @Relation(r => dao.isSuccess(), 'reactivateOtherApiCall')
  async reactivateOtherCouponReqBlock() {
    const script = {
        execute: () => {
            const filteredRedemptionIds = getBody().body.filteredRedemptionIds;

            return [{
                headers: {
                    "Content-Type": "application/json",
                    ...(getEffectiveHeaders())
                },
                body: JSON.stringify({ redemptionIds: filteredRedemptionIds }),
            }]
        }
    }
  }

  @ApiRequest({ pos: { x: 3200.746723723457, y: 855.0362966052792 } })
  @Relation(r => dao.isSuccess(), 'MongoPutCouponRedemptionBlock')
  async reactivateOtherApiCall() {
  return {
        url: `https://apac.api.capillarytech.com/v2/coupon/reactivate`,
        method: `POST`,
      };
  }

  @PutMongo({ pos: { x: 4168.491781886051, y: 151.84364332628712 } })
  @Relation(r => dao.isSuccess(), 'ResponseWithCouponReversalBlock')
  async MongoPutCouponRedemptionBlock1() {
  return {
        collectionName: `Coupon_Redemption`,
        mode: `update`,
        query: r => getBody("FilterOutFlightCoupons").body.setCouponQuery,
        queryKey: r => getBody("FilterOutFlightCoupons").body.getCouponQuery,
      };
  }

  @GetMongo({ pos: { x: 1808.5819899390078, y: 611.9747585185524 } })
  @Relation(r => dao.isSuccess() && dao.getOut("MongoGetCouponRedemptionBlock3")?.length > 0, 'ExtractFfnBasedOnRedemptionIdClone')
  @Relation(r => (dao.hasError() || dao.getOut("MongoGetCouponRedemptionBlock3")?.length == 0), 'ResponseHandlingBlock')
  async MongoGetCouponRedemptionBlock3() {
  return {
        collectionName: `Coupon_Redemption`,
        query: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.getCouponByPNRQuery,
        sort: `{}`,
      };
  }

  @Script({ pos: { x: 3582.90839046633, y: 574.6689484210664 } })
  @Relation(r => dao.isSuccess(), 'reactivateOtherApiCall2')
  async reactivateCouponReqBlock2() {
    const script = {
        execute: () => {
            const filteredRedemptionIds = getBody().body.filteredRedemptionIds;

            return [{
                headers: {
                    "Content-Type": "application/json",
                    ...(getEffectiveHeaders())
                },
                body: JSON.stringify({ redemptionIds : filteredRedemptionIds })
            }]
        }
    }
  }

  @ApiRequest({ pos: { x: 3885.431601561955, y: 562.4605018825989 } })
  @Relation(r => dao.isSuccess(), 'MongoPutCouponRedemptionBlock2')
  async reactivateOtherApiCall2() {
  return {
        url: `https://apac.api.capillarytech.com/v2/coupon/reactivate`,
        method: `POST`,
      };
  }

  @PutMongo({ pos: { x: 4170.956301451875, y: 568.4190568552211 } })
  @Relation(r => dao.isSuccess(), 'ResponseHandlingBlock2')
  async MongoPutCouponRedemptionBlock2() {
  return {
        collectionName: `Coupon_Redemption`,
        mode: `update`,
        query: r => getBody("FilterOutFlightCouponsClone").body.setCouponQuery,
        queryKey: r => getBody("FilterOutFlightCouponsClone").body.getCouponQuery,
      };
  }

  @Script({ pos: { x: 4478.038162935681, y: 558.3485843596418 } })
  async ResponseHandlingBlock2() {
    const script = {
      execute: () => {
        let reqBody = getBody('DBGetPnrResponseHandlingAndSpecs');
        return {
          http: {
            res: {
              json: {
                status: reqBody?.body?.validPnrList?.length > 0 ? 'success' : 'failed',
                code: reqBody?.body?.validPnrList?.length > 0 ? 200 : 400,
                message: reqBody?.body?.validPnrList?.length > 0 ? 'PNR cancellation is done successfully' : 'PNR cancellation failed',
                data: reqBody?.body?.validPnrList?.map((pnrnumber) => {
                  return pnrnumber;
                }),
                errors: reqBody?.body?.invalidPnrList?.map((pnrnumber) => {
                  return { success: false, code: 1007, message: 'Invalid PNR', pnrnumber };
                }),
                warnings: []
              }
            }
          }
        }
      }
    };
  }

  @GetMongo({ pos: { x: 860.3945301698664, y: 231.25286628553704 } })
  @Relation(r => dao.isSuccess() && dao.getOut()?.length > 0, 'ExtractFfnBasedOnRedemptionIdClone2')
  @Relation(r => dao.isSuccess() && dao.getOut()?.length == 0, 'MongoSetPnrTransactionsBlock3')
  async GetCouponRedemption() {
  return {
        collectionName: `Coupon_Redemption`,
        query: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.getCouponQuery,
        sort: `{}`,
      };
  }

  @Script({ pos: { x: 2712.3945301698664, y: 157.25286628553704 } })
  @Relation(r => dao.isSuccess()&& (dao.getBody().body.filteredRedemptionIds?.length > 0), 'reactivateCouponReqBlock')
  @Relation(r => dao.isSuccess()&& (dao.getBody().body.filteredRedemptionIds?.length == 0), 'MongoSetPnrTransactionsBlock2')
  async FilterOutFlightCoupons() {
    const script = {
      execute: () => {

        let mongoQueryResponse = getOut("GetCouponRedemption");

        let redemptionIdMap = getBody().body.redemptionIdMap;

        // Filter redemptionIds where flight_voucher is false or missing
        const filteredRedemptionIds = mongoQueryResponse
          .filter(record => {
            let flightVoucher = record.flight_voucher;

            if (flightVoucher) {
              if (typeof flightVoucher === 'string') {
                flightVoucher = flightVoucher.trim().toLowerCase() === "true";
              } else if (typeof flightVoucher !== 'boolean') {
                flightVoucher = false;
              }
            }

            let isRefundable = record?.is_refundable;
            let isRefundableFlag = true;

            if (typeof isRefundable === 'string') {
              isRefundableFlag = isRefundable.trim().toLowerCase() !== "false";
            } else if (typeof isRefundable === 'boolean') {
              isRefundableFlag = isRefundable !== false;
            }

            let slabDuringCouponRedemption = record.customer_slab;
            let currentSlab = redemptionIdMap[record.redemption_id];
            return slabDuringCouponRedemption == currentSlab && flightVoucher !== true && isRefundableFlag === true;
          })
          .map(record => record.redemption_id);

        return {
          body: {
            setCouponQuery: JSON.stringify({ $set: { is_reversed: true } }),
            getCouponQuery: JSON.stringify({ is_reversed: false, redemption_id: { $in: filteredRedemptionIds } }),
            "filteredRedemptionIds": filteredRedemptionIds
          }
        }
      }
    }
  }

  @Script({ pos: { x: 3284.6372499755116, y: 583.850423436419 } })
  @Relation(r => dao.isSuccess(), 'reactivateCouponReqBlock2')
  async FilterOutFlightCouponsClone() {
    const script = {
      execute: () => {
        let mongoQueryResponse = getOut("MongoGetCouponRedemptionBlock3");

        let redemptionIdMap = getBody().body.redemptionIdMap;

        // Filter redemptionIds where flight_voucher is false or missing
        const filteredRedemptionIds = mongoQueryResponse
          .filter(record => {
            let flightVoucher = record.flight_voucher;
            if (flightVoucher) {
              if (typeof flightVoucher === 'string') {
                flightVoucher = flightVoucher.trim().toLowerCase() === "true";
              } else if (typeof flightVoucher !== 'boolean') {
                flightVoucher = false;
              }
            }

            let slabDuringCouponRedemption = record.customer_slab;
            let currentSlab = redemptionIdMap[record.redemption_id];
            return slabDuringCouponRedemption == currentSlab && flightVoucher !== true
          })
          .map(record => record.redemption_id);

        return {
          body: {
            setCouponQuery: JSON.stringify({ $set: { is_reversed: true } }),
            getCouponQuery: JSON.stringify({ is_reversed: false, redemption_id: { $in: filteredRedemptionIds }}),
            "filteredRedemptionIds" : filteredRedemptionIds
          }
        }
      }
    }
  }

  @Script({ pos: { x: 3683.443868454936, y: 312.6529898161681 } })
  async ResponseAfterCouponFiltration() {
    const script = {
      execute: () => {
        let reqBody = getBody('DBGetPnrResponseHandlingAndSpecs');
        return {
          http: {
            res: {
              json: {
                status: reqBody?.body?.validPnrList?.length > 0 ? 'success' : 'failed',
                code: reqBody?.body?.validPnrList?.length > 0 ? 200 : 400,
                message: reqBody?.body?.validPnrList?.length > 0 ? 'PNR cancellation is done successfully' : 'PNR cancellation failed',
                data: reqBody?.body?.validPnrList?.map((pnrnumber) => {
                  return pnrnumber;
                }),
                errors: reqBody?.body?.invalidPnrList?.map((pnrnumber) => {
                  return { success: false, code: 1007, message: 'Invalid PNR', pnrnumber };
                }),
                warnings: []
              }
            }
          }
        }
      }
    };
  }

  @PutMongo({ pos: { x: 3034.3945301698664, y: 327.25286628553704 } })
  @Relation(r => dao.isSuccess(), 'MongoSetPnrBlock2')
  async MongoSetPnrTransactionsBlock2() {
  return {
        collectionName: `PNR_Transactions`,
        mode: `update`,
        query: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.setQuery,
        queryKey: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.getPNRTransactionsQuery,
      };
  }

  @PutMongo({ pos: { x: 3321.9945301698667, y: 333.25286628553704 } })
  @Relation(r => dao.isSuccess(), 'ResponseAfterCouponFiltration')
  async MongoSetPnrBlock2() {
  return {
        collectionName: `PNR`,
        mode: `update`,
        query: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.setQuery,
        queryKey: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.getPNRQuery,
      };
  }

  @Script({ pos: { x: 2633.9532149626416, y: 886.1752395719477 } })
  @Relation(r => dao.isSuccess()&& (dao.getBody().body.filteredRedemptionIds?.length > 0), 'reactivateOtherCouponReqBlock')
  @Relation(r => dao.isSuccess()&& (dao.getBody().body.filteredRedemptionIds?.length == 0), 'MongoPutCouponRedemptionBlock3')
  async FilterOutRedemptionIds() {
    const script = {
      execute: () => {
        let redemptionIdMap = getBody().body.redemptionIdMap;
        let mongoQueryResponse = getOut("MongoGetCouponRedemptionBlock");

        // Filter redemptionIds where flight_voucher is false or missing
        const filteredRedemptionIds = mongoQueryResponse
          .filter(record => {
            let flightVoucher = record.flight_voucher;

            if (flightVoucher) {
              if (typeof flightVoucher === 'string') {
                flightVoucher = flightVoucher.trim().toLowerCase() === "true";
              } else if (typeof flightVoucher !== 'boolean') {
                flightVoucher = false;
              }
            }

            let isRefundable = record?.is_refundable;
            let isRefundableFlag = true;

            if (typeof isRefundable === 'string') {
              isRefundableFlag = isRefundable.trim().toLowerCase() !== "false";
            } else if (typeof isRefundable === 'boolean') {
              isRefundableFlag = isRefundable !== false;
            }

            let slabDuringCouponRedemption = record.customer_slab;
            let currentSlab = redemptionIdMap[record.redemption_id];
            return slabDuringCouponRedemption == currentSlab && flightVoucher !== true && isRefundableFlag === true;
          })
          .map(record => record.redemption_id);

        return {
          body: {
            setCouponQuery: JSON.stringify({ $set: { is_reversed: true } }),
            getCouponQuery: JSON.stringify({ is_reversed: false, redemption_id: { $in: filteredRedemptionIds } }),
            "filteredRedemptionIds": filteredRedemptionIds
          }
        }
      }
    }
  }

  @Script({ pos: { x: 3218.2709601725383, y: 1072.3410196814589 } })
  async FinalResponseAfterCouponFiltration() {
    const script = {
        execute: () => {
            let pnrList = getBody("MongoGetPnrSpec")?.body?.pnrList;
            let warnings = [];
            let warning = { success: false, code: 1009, message: 'no coupon to reactivate for the PNR', pnrnumber : pnrList }
            warnings.push(warning);
            return {
                http: {
                    res: {
                        json: {
                            status: 'success',
                            code: 200,
                            message: 'PNR cancellation is done successfully',
                            data: getBody("MongoGetPnrSpec")?.body?.pnrList,
                            errors: [],
                            warnings: warnings
                        }
                    }
                }
            }
        }
    }
  }

  @PutMongo({ pos: { x: 2940.29859019079, y: 1062.3824647088368 } })
  @Relation(r => dao.isSuccess(), 'FinalResponseAfterCouponFiltration')
  async MongoPutCouponRedemptionBlock3() {
  return {
        collectionName: `Coupon_Redemption`,
        mode: `update`,
        query: r => getBody("MongoGetCouponRedemptionSpec")?.updateQuery,
        queryKey: r => getBody("MongoGetCouponRedemptionSpec")?.getQuery,
      };
  }

  @Script({ pos: { x: 1541.6141923810128, y: 939.1040215066448 } })
  @Relation(r => dao.isSuccess(), 'BuildCustomerLookupRequest')
  async ExtractFfnBasedOnRedemptionId() {
    const script = {
      execute: () => {

        let mongoQueryResponse = getOut("MongoGetCouponRedemptionBlock");

        let redemptionIdList = [];
        let redemptionIdMap = {};
        let ffnMap = {};
        let ffnList = [];
        let customerLookupRequest = [];
        // Filter redemptionIds where flight_voucher is false or missing
        for (let record of mongoQueryResponse) {
            let redemptionId = record.redemption_id;
            let ffn = record.ffn;

            redemptionIdList.push(redemptionId);
            redemptionIdMap[redemptionId] = ffn;

            if (ffnMap[ffn]) {
              continue;
            }

            ffnMap[ffn] = ffn;
            ffnList.push (ffn);
            let requestHeaders = getEffectiveHeaders();
            // Remove these headers because customer lookup api will throw error incase invalid values are passed here
            delete requestHeaders["x-cap-neo-test-variant-id"];
            delete requestHeaders["x-cap-api-attribution-entity-type"];
            delete requestHeaders["x-cap-api-attribution-entity-code"];
            delete requestHeaders["x-cap-api-attribution-till-code"];

            let queryParameters = {
                "identifierName" : "externalId",
                "identifierValue" : ffn,
                "source" : "INSTORE"
            }
            let apiRequest = {
                headers : requestHeaders,
                queryParams : queryParameters
            };

            customerLookupRequest.push(apiRequest);
        }
        return {
          body : {
            "redemptionIdMap" : redemptionIdMap,
            "redemptionIdList" : redemptionIdList,
            "ffnMap" : ffnMap,
            "ffnList" : ffnList,
            "customerLookupRequest" : customerLookupRequest
          }
        }
      }
    }
  }

  @ApiRequest({ pos: { x: 2108.332572855561, y: 925.3250616526595 } })
  @Cachable({ cachable: false, key: "" })
  @Relation(r => dao.isSuccess(), 'ExtractCustomerSlab')
  async CustomerLookupApiCall() {
  return {
        url: `https://apac.api.capillarytech.com/v2/customers/lookup/customerDetails`,
        method: `GET`,
        queryParams: {
    "embed": "points"
  },
      };
  }

  @Script({ pos: { x: 2374.7055781019617, y: 919.3526916709113 } })
  @Relation(r => dao.isSuccess(), 'FilterOutRedemptionIds')
  async ExtractCustomerSlab() {
    const script = {

        execute: () => {
            let redemptionIdWithFFNMap = getBody("ExtractFfnBasedOnRedemptionId").body.redemptionIdMap;

            let redemptionIdList = getBody("ExtractFfnBasedOnRedemptionId").body.redemptionIdList;
            let ffnList = getBody("ExtractFfnBasedOnRedemptionId").body.ffnList;
            let customerLookupResponseList = getMultiBody("CustomerLookupApiCall");


            let ffnMap = {};
            for (let index = 0; index < ffnList.length; index++) {
                let ffn = ffnList[index];
                let customerLookupResponse = customerLookupResponseList[index];
                ffnMap[ffn] = customerLookupResponse;
            }

            let redemptionIdMap = {};
            for (let index = 0; index < redemptionIdList.length; index++) {
                let redemptionId = redemptionIdList[index];
                let ffn = redemptionIdWithFFNMap[redemptionId];
                let customerLookupResponse = ffnMap[ffn];
                let pointsSummary = customerLookupResponse.pointsSummary;
                let currentSlabNo = pointsSummary.slabSNo;
                redemptionIdMap[redemptionId] = currentSlabNo;
            }

            return {
                body : {
                    "redemptionIdMap" : redemptionIdMap
                }
            };

        }
    }
  }

  @Script({ pos: { x: 1825.8628625452795, y: 937.1178365157707 } })
  @Relation(r => dao.isSuccess(), 'CustomerLookupApiCall')
  async BuildCustomerLookupRequest() {
    const script = {

        execute: () => {

            //Write your code here.
            return getBody().body.customerLookupRequest;

        }
    }
  }

  @Script({ pos: { x: 2116.581989939008, y: 613.9747585185523 } })
  @Relation(r => dao.isSuccess(), 'BuildCustomerLookupRequestClone')
  async ExtractFfnBasedOnRedemptionIdClone() {
    const script = {
      execute: () => {

        let mongoQueryResponse = getOut("MongoGetCouponRedemptionBlock3");

        let redemptionIdList = [];
        let redemptionIdMap = {};
        let ffnMap = {};
        let ffnList = [];
        let customerLookupRequest = [];
        // Filter redemptionIds where flight_voucher is false or missing
        for (let record of mongoQueryResponse) {
            let redemptionId = record.redemption_id;
            let ffn = record.ffn;

            redemptionIdList.push(redemptionId);
            redemptionIdMap[redemptionId] = ffn;

            if (ffnMap[ffn]) {
              continue;
            }

            ffnMap[ffn] = ffn;
            ffnList.push (ffn);
            let requestHeaders = getEffectiveHeaders();
            // Remove these headers because customer lookup api will throw error incase invalid values are passed here
            delete requestHeaders["x-cap-neo-test-variant-id"];
            delete requestHeaders["x-cap-api-attribution-entity-type"];
            delete requestHeaders["x-cap-api-attribution-entity-code"];
            delete requestHeaders["x-cap-api-attribution-till-code"];

            let queryParameters = {
                "identifierName" : "externalId",
                "identifierValue" : ffn,
                "source" : "INSTORE"
            }
            let apiRequest = {
                headers : requestHeaders,
                queryParams : queryParameters
            };

            customerLookupRequest.push(apiRequest);
        }
        return {
          body : {
            "redemptionIdMap" : redemptionIdMap,
            "redemptionIdList" : redemptionIdList,
            "ffnMap" : ffnMap,
            "ffnList" : ffnList,
            "customerLookupRequest" : customerLookupRequest
          }
        }
      }
    }
  }

  @Script({ pos: { x: 2406.581989939008, y: 601.9747585185523 } })
  @Relation(r => dao.isSuccess(), 'CustomerLookupApiCallClone')
  async BuildCustomerLookupRequestClone() {
    const script = {

        execute: () => {

            //Write your code here.
            return getBody().body.customerLookupRequest;

        }
    }
  }

  @ApiRequest({ pos: { x: 2682.581989939008, y: 591.9747585185523 } })
  @Cachable({ cachable: false, key: "" })
  @Relation(r => dao.isSuccess(), 'ExtractCustomerSlabClone')
  async CustomerLookupApiCallClone() {
  return {
        url: `https://apac.api.capillarytech.com/v2/customers/lookup/customerDetails`,
        method: `GET`,
        queryParams: {
    "embed": "points"
  },
      };
  }

  @Script({ pos: { x: 3000.6786950028895, y: 583.8918684637968 } })
  @Relation(r => dao.isSuccess(), 'FilterOutFlightCouponsClone')
  async ExtractCustomerSlabClone() {
    const script = {

        execute: () => {
            let redemptionIdWithFFNMap = getBody("ExtractFfnBasedOnRedemptionIdClone").body.redemptionIdMap;

            let redemptionIdList = getBody("ExtractFfnBasedOnRedemptionIdClone").body.redemptionIdList;
            let ffnList = getBody("ExtractFfnBasedOnRedemptionIdClone").body.ffnList;
            let customerLookupResponseList = getMultiBody("CustomerLookupApiCallClone");


            let ffnMap = {};
            for (let index = 0; index < ffnList.length; index++) {
                let ffn = ffnList[index];
                let customerLookupResponse = customerLookupResponseList[index];
                ffnMap[ffn] = customerLookupResponse;
            }

            let redemptionIdMap = {};
            for (let index = 0; index < redemptionIdList.length; index++) {
                let redemptionId = redemptionIdList[index];
                let ffn = redemptionIdWithFFNMap[redemptionId];
                let customerLookupResponse = ffnMap[ffn];
                let pointsSummary = customerLookupResponse.pointsSummary;
                let currentSlabNo = pointsSummary.slabSNo;
                redemptionIdMap[redemptionId] = currentSlabNo;
            }

            return {
                body : {
                    "redemptionIdMap" : redemptionIdMap
                }
            };

        }
    }
  }

  @Script({ pos: { x: 1246.3945301698664, y: 217.25286628553704 } })
  @Relation(r => dao.isSuccess(), 'BuildCustomerLookupRequestClone2')
  async ExtractFfnBasedOnRedemptionIdClone2() {
    const script = {
      execute: () => {

        let mongoQueryResponse = getOut("GetCouponRedemption");

        let redemptionIdList = [];
        let redemptionIdMap = {};
        let ffnMap = {};
        let ffnList = [];
        let customerLookupRequest = [];
        // Filter redemptionIds where flight_voucher is false or missing
        for (let record of mongoQueryResponse) {
            let redemptionId = record.redemption_id;
            let ffn = record.ffn;

            redemptionIdList.push(redemptionId);
            redemptionIdMap[redemptionId] = ffn;

            if (ffnMap[ffn]) {
              continue;
            }

            ffnMap[ffn] = ffn;
            ffnList.push (ffn);
            let requestHeaders = getEffectiveHeaders();
            // Remove these headers because customer lookup api will throw error incase invalid values are passed here
            delete requestHeaders["x-cap-neo-test-variant-id"];
            delete requestHeaders["x-cap-api-attribution-entity-type"];
            delete requestHeaders["x-cap-api-attribution-entity-code"];
            delete requestHeaders["x-cap-api-attribution-till-code"];

            let queryParameters = {
                "identifierName" : "externalId",
                "identifierValue" : ffn,
                "source" : "INSTORE"
            }
            let apiRequest = {
                headers : requestHeaders,
                queryParams : queryParameters
            };

            customerLookupRequest.push(apiRequest);
        }
        return {
          body : {
            "redemptionIdMap" : redemptionIdMap,
            "redemptionIdList" : redemptionIdList,
            "ffnMap" : ffnMap,
            "ffnList" : ffnList,
            "customerLookupRequest" : customerLookupRequest
          }
        }
      }
    }
  }

  @Script({ pos: { x: 1614.3945301698664, y: 218.052866285537 } })
  @Relation(r => dao.isSuccess(), 'CustomerLookupApiCallClone2')
  async BuildCustomerLookupRequestClone2() {
    const script = {

        execute: () => {

            //Write your code here.
            return getBody().body.customerLookupRequest;

        }
    }
  }

  @ApiRequest({ pos: { x: 1950.3945301698664, y: 211.65286628553702 } })
  @Relation(r => dao.isSuccess(), 'ExtractCustomerSlabClone2')
  async CustomerLookupApiCallClone2() {
  return {
        url: `https://apac.api.capillarytech.com/v2/customers/lookup/customerDetails`,
        method: `GET`,
        queryParams: {
    "embed": "points"
  },
      };
  }

  @Script({ pos: { x: 2402.3945301698664, y: 209.25286628553704 } })
  @Relation(r => dao.isSuccess(), 'FilterOutFlightCoupons')
  async ExtractCustomerSlabClone2() {
    const script = {

        execute: () => {
            let redemptionIdWithFFNMap = getBody("ExtractFfnBasedOnRedemptionIdClone2").body.redemptionIdMap;

            let redemptionIdList = getBody("ExtractFfnBasedOnRedemptionIdClone2").body.redemptionIdList;
            let ffnList = getBody("ExtractFfnBasedOnRedemptionIdClone2").body.ffnList;
            let customerLookupResponseList = getMultiBody("CustomerLookupApiCallClone2");


            let ffnMap = {};
            for (let index = 0; index < ffnList.length; index++) {
                let ffn = ffnList[index];
                let customerLookupResponse = customerLookupResponseList[index];
                ffnMap[ffn] = customerLookupResponse;
            }

            let redemptionIdMap = {};
            for (let index = 0; index < redemptionIdList.length; index++) {
                let redemptionId = redemptionIdList[index];
                let ffn = redemptionIdWithFFNMap[redemptionId];
                let customerLookupResponse = ffnMap[ffn];
                let pointsSummary = customerLookupResponse.pointsSummary;
                let currentSlabNo = pointsSummary.slabSNo;
                redemptionIdMap[redemptionId] = currentSlabNo;
            }

            return {
                body : {
                    "redemptionIdMap" : redemptionIdMap
                }
            };

        }
    }
  }

  @PutMongo({ pos: { x: 1256.0537855242019, y: 404.1359614750694 } })
  @Relation(r => dao.isSuccess(), 'MongoSetPnrBlock3')
  async MongoSetPnrTransactionsBlock3() {
  return {
        collectionName: `PNR_Transactions`,
        mode: `update`,
        query: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.setQuery,
        queryKey: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.getPNRTransactionsQuery,
      };
  }

  @PutMongo({ pos: { x: 1598.9160583301534, y: 392.5398788905237 } })
  @Relation(r => dao.isSuccess(), 'ResponseWhenNoRedemptionIdPresentInCouponCollection')
  async MongoSetPnrBlock3() {
  return {
        collectionName: `PNR`,
        mode: `update`,
        query: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.setQuery,
        queryKey: r => getBody("DBGetPnrResponseHandlingAndSpecs").body.getPNRQuery,
      };
  }

  @Script({ pos: { x: 1973.2013062568853, y: 382.42447391467954 } })
  async ResponseWhenNoRedemptionIdPresentInCouponCollection() {
    const script = {
      execute: () => {
        let reqBody = getBody('DBGetPnrResponseHandlingAndSpecs');
        return {
          http: {
            res: {
              json: {
                status: reqBody?.body?.validPnrList?.length > 0 ? 'success' : 'failed',
                code: reqBody?.body?.validPnrList?.length > 0 ? 200 : 400,
                message: reqBody?.body?.validPnrList?.length > 0 ? 'PNR cancellation is done successfully' : 'PNR cancellation failed',
                data: reqBody?.body?.validPnrList?.map((pnrnumber) => {
                  return pnrnumber;
                }),
                errors: reqBody?.body?.invalidPnrList?.map((pnrnumber) => {
                  return { success: false, code: 1007, message: 'Invalid PNR', pnrnumber };
                }),
                warnings: []
              }
            }
          }
        }
      }
    };
  }

  @Script({ pos: { x: -312.3067171479588, y: 640.7833921924145 } })
  @Relation(r => dao.isSuccess(), 'CheckIfPnrIsActive')
  async CheckIfPNRIsPostFlownOrCancelled() {
    const script = {
        execute: () => {
            let pnrList = getBody("MongoGetPnrSpec")?.body?.pnrList;

            let pnrGetTransactionMongoQuery = {
                pnr_number: { "$in": pnrList },
                "$or": [
                    { is_cancelled: true },
                    { flight_status: "FLOWN" }
                ]
            };

            return {
                body: {
                    pnrList,
                    getTransactionQuery: JSON.stringify(pnrGetTransactionMongoQuery)
                },
            };
        },
    };
  }

  @GetMongo({ pos: { x: 3.693282852041193, y: 644.7833921924145 } })
  @Relation(r => dao.isSuccess() && dao.getOut()?.length == 0, 'MongoGetCouponRedemptionSpec')
  @Relation(r => dao.isSuccess() && dao.getOut()?.length > 0, 'FinalResponseWhenPNRAlreadyFlown')
  async CheckIfPnrIsActive() {
  return {
        collectionName: `PNR_Transactions`,
        query: r => getBody().body.getTransactionQuery,
        sort: `{}`,
      };
  }

  @Script({ pos: { x: 323.6932828520412, y: 964.7833921924145 } })
  async FinalResponseWhenPNRAlreadyFlown() {
    const script = {
        execute: () => {
            return {
                http: {
                    res: {
                        json: {
                            status: 'failed',
                            code: 400,
                            message: 'PNR is not active',
                            errors: [{ success: false, code: 1007, message: 'Invalid PNR', pnrnumber: getBody("CheckIfPNRIsPostFlownOrCancelled")?.body?.pnrList?.join(", ") }],
                            warnings: []
                        }
                    }
                }
            }
        }
    }
  }
}
