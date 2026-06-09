import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getError, getOut, getValueByKey } = dao;

@Dag({ method: "POST", url: "deactivate-nominee" })
class RemoveNomineeApi {
  constructor() {
    this.versionConfig();
  }

  @Schema({ pos: { x: 824.9491973279105, y: -1.6782757249279427 } })
  @Relation(r => dao.hasError(), 'ValidationFailureBlock')
  @Relation(r => dao.isSuccess(), 'ValidationSuccessBlock')
  async requestSchemaValidation() {
    return {
      definitions: [],
      spec: {
        type: "object",
        properties: {
          body: {
            type: "object",
            properties: {
              identifierType: {
                type: "string",
                minLength: 1,
                transform: ["trim"],
                errorMessage: {
                  minLength: "identifierType cannot be empty",
                },
              },
              identifierValue: {
                type: "string",
                minLength: 1,
                transform: ["trim"],
                errorMessage: {
                  minLength: "identifierValue cannot be empty",
                },
              },
              nomineeId: {
                type: "string",
                minLength: 1,
                transform: ["trim"],
                errorMessage: {
                  minLength: "nomineeId cannot be empty",
                },
              },
            },
            required: ["identifierType", "identifierValue", "nomineeId"],
            errorMessage: {
              required: {
                identifierType: "identifierType is missing",
                identifierValue: "identifierValue is missing",
                nomineeId: "nomineeId is missing",
              },
            },
          },
        },
        required: ["body"],
        errorMessage: {
          required: {
            body: "Payload is missing",
          },
        },
      },
    }
  }

  @Script({ pos: { x: 1058.3303814399917, y: -118.73382074086074 } })
  async ValidationFailureBlock() {
    const script = {
      execute: () => {
        const errorArray = [];
        const validationErrors = getError("requestSchemaValidation")?.err;
        validationErrors?.forEach((validationError) => {
          const error = {
            success: false,
            code: 1006,
            message: `${validationError.message} at ${validationError.instancePath}`,
          };
          errorArray.push(error);
        });
        return {
          http: {
            res: {
              json: {
                errors: errorArray,
              },
              status: 200,
              headers: getEffectiveHeaders()
            }
          }
        };
      },
    };
  }

  @Script({ pos: { x: 1062.4938459619193, y: 153.57740301743638 } })
  @Relation(r => (dao.isSuccess() && dao.getBody("ValidationSuccessBlock")?.body?.isError), 'ErrorResponseHandlingBlock')
  @Relation(r => (dao.isSuccess() && !dao.getBody("ValidationSuccessBlock")?.body?.isError), 'MongoGetNomineeValidationBlock')
  async ValidationSuccessBlock() {
    const checkDuplicateId = (idString) => {
      const objectIdPattern = /^[0-9a-fA-F]{24}$/;
      let invalidIds = [];
      let duplicateIdSet = new Set();
      const nomineeIdArray = idString.split(",");
      const nomineeIdSet = new Set();
      for (let nomineeId of nomineeIdArray) {
        if (!objectIdPattern.test(nomineeId)) {
          invalidIds.push(nomineeId);
        }
        if (nomineeIdSet.has(nomineeId.trim())) {
          duplicateIdSet.add(nomineeId.trim());
        }
        nomineeIdSet.add(nomineeId.trim());
      }
      return {
        isDuplicate: nomineeIdSet.size !== nomineeIdArray.length,
        idList: nomineeIdArray.filter((id) => !invalidIds.includes(id)),
        invalidIds,
        duplicateIdList: Array.from(duplicateIdSet),
        idCount: nomineeIdArray.length,
      };
    };
    const script = {
      execute: () => {
        const { identifierType, identifierValue, nomineeId } = getApiRequest()?.body;
        const {
          isDuplicate = false,
          idList,
          invalidIds,
          duplicateIdList,
          idCount,
        } = checkDuplicateId(nomineeId);
        logger.info(`Delete nominee | identifierValue: ${identifierValue} | idCount: ${idCount} | invalidIds: ${invalidIds.length} | isDuplicate: ${isDuplicate} | isError: ${isDuplicate || idList?.length == 0 || idCount > 5}`);

        let mongoQueryKey = {
          identifierType,
          identifierValue,
          isActive: true,
          _id: {
            $in: idList.map((id) => {
              return { $oid: id };
            }),
          },
        };
        return {
          headers: getEffectiveHeaders(),
          body: {
            updateQueryKey: JSON.stringify(mongoQueryKey),
            updateObj: JSON.stringify({ $set: { isActive: false , modifiedDate : new Date()} }),
            idList,
            invalidIds,
            isDuplicate,
            duplicateIdList,
            idCount,
            isError: isDuplicate || idList?.length == 0 || idCount > 5
          },
        };
      },
    };
  }

  @PutMongo({ pos: { x: 2047.7213873600979, y: 339.40748498548555 } })
  @Relation(r => dao.isSuccess(), 'DBGetResponseHandling')
  async MongoPutNomineeBlock() {
  return {
        collectionName: `NomineeDetails`,
        mode: `update`,
        query: r => getBody("PutMongoSpecs").updateObj,
        queryKey: r => getBody("PutMongoSpecs").criteria,
        options: `{}`,
      };
  }

  @Script({ pos: { x: 2359.2487930065854, y: 456.3876986176938 } })
  @ExecutionStrategy('or')
  async DBGetResponseHandling() {
    const script = {
      execute: () => {
        logger.info(JSON.stringify(getBody("PutMongoSpecs")));
        let data = getBody('PutMongoSpecs')
        let nomineeData = []
        const { identifierType, identifierValue } = getApiRequest()?.body;
        const { modifiedDateErrors } = getBody("validateModifiedDate")

        let {
          invalidIds = [],
          idList = [],
          duplicateIdList,
        } = getOut("ValidationSuccessBlock")?.[0]?.body;
        if (data?.skipUpdate === "update-db") {
          nomineeData = getOut("MongoGetNomineeValidationBlock")
        }
        let nomineeList = nomineeData?.map(
          (ele) => {
            let { gender, firstName, lastName, dob, createdDate } = ele;
            let _id = Object.values(ele?._id?.buffer)
              .map((byte) => byte.toString(16).padStart(2, "0"))
              .join("");
            idList = idList.filter((id) => id !== _id);
            return {
              nomineeId: _id,
              gender: gender ? gender : "",
              firstName: firstName ? firstName : "",
              lastName: lastName ? lastName : "",
              dob: dob ? dob : "",
              createdDate: createdDate ? createdDate : "",
            };
          }
        );

        let errors =
          [...invalidIds, ...idList]?.length > 0
            ? [
              {
                success: false,
                code: 1009,
                message: `Invalid Ids ${[...invalidIds, ...idList,].join(", ")}`,
                // invalidIds,
                // idList
              },
            ]
            : [];
        if (modifiedDateErrors.length > 0) {
          errors.push(...modifiedDateErrors)
        }
        if (duplicateIdList && duplicateIdList.length > 0) {
          errors.push({
            success: false,
            code: 1008,
            message: `Duplicate Ids ${duplicateIdList?.join(", ")}`,
          });
        }

        logger.info(`Delete nominee response | identifierValue: ${identifierValue} | nomineeCount: ${nomineeList.length} | invalidIds: ${invalidIds.length} | duplicates: ${duplicateIdList?.length || 0}`);

        return {
          http: {
            res: {
              json: {
                messageCode: "200",
                message: "Successful",
                identifierType,
                identifierValue,
                nominee: nomineeList,
                errors: errors
              },
              status: 200,
              headers: getEffectiveHeaders()
            }
          }
        };
      },
    };
  }

  @Script({ pos: { x: 1825.3929096171848, y: 70.69229944550472 } })
  async DBErrorResponseHandlingBlock() {
    const script = {
      execute: () => {
        const { identifierType, identifierValue } = getApiRequest()?.body;
        const {
          isDuplicate = false,
          idList = null,
          invalidIds = null,
          duplicateIdList,
          idCount,
        } = getOut("ValidationSuccessBlock")?.[0]?.body;
        const dbValidationResponse = getOut("MongoGetNomineeValidationBlock");
        let errors = [];
        if (idCount > 5) {
          errors.push({
            success: false,
            code: 1006,
            message: `Payload cannot contain more than 5 Id's`,
          });
        }
        if (isDuplicate) {
          errors.push({
            success: false,
            code: 1008,
            message: `Payload contains duplicate Ids ${duplicateIdList.join(", ")}`,
          });
        }
        if (idList?.length == 0 || invalidIds?.length > 0) {
          if (dbValidationResponse?.length == 0 && idList?.length != 0) {
            errors.push({
              success: false,
              code: 1009,
              message: `Invalid Ids ${[...idList, ...invalidIds].join(", ")}`,
            });
          } else {
            errors.push({
              success: false,
              code: 1010,
              message: `Invalid Ids ${invalidIds.join(", ")}`,
            });
          }
        } else if (dbValidationResponse?.length == 0 && idList?.length != 0) {
          errors.push({
            success: false,
            code: 1011,
            message: `Invalid Ids ${idList.join(", ")}`,
          });
        }

        return {
          http: {
            res: {
              json: {
                messageCode: "400",
                message: "failed",
                identifierType,
                identifierValue,
                nominee: [],
                errors,
              },
              status: 400,
              headers: getEffectiveHeaders()
            }
          }
        };
      },
    };
  }

  @GetMongo({ pos: { x: 1343.726770201326, y: 244.15547205713682 } })
  @Relation(r => (dao.isSuccess() && dao.getOut("MongoGetNomineeValidationBlock")?.length == 0), 'DBErrorResponseHandlingBlock')
  @Relation(r => (dao.isSuccess() && !(dao.getOut("MongoGetNomineeValidationBlock")?.length == 0)), 'validateModifiedDate')
  async MongoGetNomineeValidationBlock() {
  return {
        collectionName: `NomineeDetails`,
        query: r => getBody("ValidationSuccessBlock")?.body?.updateQueryKey,
        sort: `{}`,
      };
  }

  @Script({ pos: { x: 1278.307019808069, y: 29.786373457383178 } })
  async ErrorResponseHandlingBlock() {
    const script = {
      execute: () => {
        const { identifierType, identifierValue } = getApiRequest()?.body;
        const {
          isDuplicate = false,
          idList = null,
          invalidIds = null,
          duplicateIdList,
          idCount
        } = getBody("ValidationSuccessBlock")?.body;
        const dbValidationResponse = getBody();
        let errors = [];
        if (idCount > 5) {
          errors.push({
            success: false,
            code: 1006,
            message: `Payload cannot contain more than 5 Id's`,
          });
        }
        if (isDuplicate) {
          errors.push({
            success: false,
            code: 1008,
            message: `Payload contains duplicate Ids ${duplicateIdList.join(", ")}`,
          });
        }
        if (idList?.length == 0) {
          errors.push({
            success: false,
            code: 1009,
            message: `Invalid Ids ${invalidIds.join(", ")}`,
          });
        }

        if (dbValidationResponse == 0 && idList?.length != 0) {
          errors.push({
            success: false,
            code: 1009,
            message: `Invalid Ids ${idList.join(", ")}`,
          });
        }
        return {
          http: {
            res: {
              json: {
                messageCode: "400",
                message: "failed",
                identifierType,
                identifierValue,
                nominee: [],
                errors,
              },
              status: 400,
              headers: getEffectiveHeaders()
            }
          }
        };
      },
    };
  }

  @Script({ pos: { x: 1817.1125108062988, y: 328.7502235757494 } })
  @Relation(r => dao.getBody("PutMongoSpecs")?.skipUpdate === "update-db", 'MongoPutNomineeBlock')
  @Relation(r => dao.getBody("PutMongoSpecs")?.skipUpdate === "skip-update", 'DBGetResponseHandling')
  async PutMongoSpecs() {
    const script = {
      execute: () => {
        const validNominees = getBody('validateModifiedDate')?.validNominees || [];
        const validIds = validNominees.map(n => n._id);


        logger.info(`[PutMongoSpecs] validNominees: ${validNominees.length} | validIds: ${validIds.length}`);

        if (validIds.length === 0) {
          return {
            criteria: JSON.stringify({ _id: { $in: [] } }),
            updateObj: JSON.stringify({ $set: { isActive: false, modifiedDate: new Date() } }),
            skipUpdate: "skip-update"
          };
        }
        else {
          return {
            criteria: JSON.stringify({ _id: { $in: validIds.map(id => ({ $oid: id })) } }),
            updateObj: JSON.stringify({ $set: { isActive: false, modifiedDate: new Date() } }),
            skipUpdate: "update-db"
          };
        }

      }
    };
  }

  @Script({ pos: { x: 1582.9497942058993, y: 465.6854055289268 } })
  @Relation(r => dao.isSuccess(), 'PutMongoSpecs')
  async validateModifiedDate() {
    const script = {
      execute: () => {
        const rawNominees = getOut('MongoGetNomineeValidationBlock') || [];
        const nominees = JSON.parse(JSON.stringify(rawNominees));

        if (!Array.isArray(nominees) || nominees.length === 0) {
          return { validNominees: [], modifiedDateErrors: [] };
        }

        // x-api-key bypass — only if config key is non-empty AND matches
        const requestHeaders = getApiRequest()?.headers || {};
        const apiKeyHeader = requestHeaders?.['x-api-key'] || '';
        const nomineeXAPIKeyFromConfig = getBody('StaticConfiguration')?.body?.nomineeXApiKey || '';

        if (nomineeXAPIKeyFromConfig && apiKeyHeader === nomineeXAPIKeyFromConfig) {
          logger.info(`x-api-key match — skipping 365-day validation | count: ${nominees.length}`);
          return { validNominees: nominees, modifiedDateErrors: [] };
        }

        // 365-day cutoff — same pattern as updateNominee
        const date365DaysAgo = new Date();
        date365DaysAgo.setUTCDate(date365DaysAgo.getUTCDate() - 365);
        date365DaysAgo.setUTCHours(0, 0, 0, 0);

        const validNominees = [];
        const modifiedDateErrors = [];

        for (const nominee of nominees) {
          const nomineeId = nominee._id || '';
          const modifiedDate = nominee?.modifiedDate;

          if (!modifiedDate) {
            modifiedDateErrors.push({ status: false, code: 400, message: `Modified date not found for nominee ${nomineeId}` });
            continue;
          }

          const modifiedUTC = new Date(modifiedDate);
          if (isNaN(modifiedUTC.getTime())) {
            modifiedDateErrors.push({ status: false, code: 400, message: `Invalid modified date format for nominee ${nomineeId}` });
            continue;
          }

          modifiedUTC.setUTCHours(0, 0, 0, 0);

          // Same condition as updateNominee: if modified MORE RECENTLY than 365 days ago → block
          if (modifiedUTC.getTime() > date365DaysAgo.getTime()) {
            modifiedDateErrors.push({ status: false, code: 1012, message: `Nominee ${nomineeId} can only be edited if last modification was more than 365 days ago` });
            continue;
          }

          validNominees.push(nominee);
        }

        logger.info(`[validateModifiedDate] total: ${nominees.length} | valid: ${validNominees.length} | errors: ${modifiedDateErrors.length}`);
        return { validNominees, modifiedDateErrors };
      }
    };
  }

  @Script({ pos: { x: 286.1237265401464, y: 12.139179348731403 } })
  @Relation(r => dao.isSuccess(), 'StaticConfiguration')
  async versionConfig() {
    const script = {

        execute: () => {
            const developer = "Adarsh"
            const branch = "PSV-29941"
            const trigger = "/deactivate-nominee"
            const requestBody = getApiRequest()?.body;
            const nomineeId = requestBody?.nomineeId
            const identifierValue = requestBody?.identifierValue
            const isgRequestId = `${trigger}_${nomineeId}_${identifierValue}`;
            logger.info(`IsgRequestId : ${JSON.stringify(isgRequestId)}`);
            //Write your code here.
            return {
                headers: {
                    "x-cap-isg-neo-verison": 1.3
                }
            }
        }
    }
  }

  @Script({ pos: { x: 568.6431536540924, y: 31.18463049036137 } })
  @Relation(r => dao.isSuccess(), 'requestSchemaValidation')
  async StaticConfiguration() {
    const script = {

        execute: async () => {
            let nomineeXApiKeyHeader = await getValueByKey("NOMINEE_X_API_KEY")
            const literals = {
                nomineeXApiKey: nomineeXApiKeyHeader,
            }

            return {
                body: literals
            };

        }
    }
  }
}
