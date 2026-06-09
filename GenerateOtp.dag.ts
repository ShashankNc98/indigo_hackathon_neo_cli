import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody } = dao;

@Dag({ method: "POST", url: "generateOtp" })
class GenerateOtp {
  constructor() {
    this.modifyMobileNumber();
  }

  @Script({ pos: { x: 320, y: 0 } })
  @Relation(r => dao.isSuccess(), 'OtpGenerateApiCall')
  async modifyMobileNumber() {
    const script = {

        execute: () => {
            let apiRequest = getApiRequest();

            let headers = apiRequest.headers;
            delete headers['x-cap-neo-test-variant-id'];

            headers = {
                "content-type": "application/json",
                ...(headers)
            }
            let queryParams = apiRequest.queryParams;
            let body = { ...(apiRequest.body) };

            let entityType = body.entityType;
            let entityValue = body.entityValue;
            let channels = body.channels;

            if (entityType?.trim()?.toUpperCase() == "MOBILE") {
                if (entityValue?.length == 10) {
                    let mobileNumberWithPrefix = "91" + entityValue;
                    body['entityValue'] = mobileNumberWithPrefix;
                }
            }

            if (channels) {
                let channelArr = [];
                for (let channel of channels) {
                    let channelType = channel['type'];
                    let channelValue = channel['value'];
                    if (channelType?.trim()?.toUpperCase() == "SMS") {
                        if (channelValue?.length == 10) {
                            channelValue = "91" + channelValue;
                        }
                    }
                    channel['type'] = channelType;
                    channel['value'] = channelValue;
                    channelArr.push(channel);
                }
                body['channels'] = channelArr;
            }

            return {
                "headers": headers,
                "queryParams": queryParams,
                "body": JSON.stringify(body)
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 640, y: 0 } })
  @Relation(r => dao.hasError(), 'HandleApiCallFailure')
  async OtpGenerateApiCall() {
  return {
        url: `https://apac.api.capillarytech.com/v2/otp/generate`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 960, y: 0 } })
  async HandleApiCallFailure() {
    const script = {

        execute: () => {
            if (getBody().code === 401) {
                let errors = [];
                const error = parseXml(getBody().err?.message)
                errors.push(error);
                return {
                    http: {
                        res: {
                            status : getBody().code,
                            json : {
                                "errors": errors
                            }
                        }
                    }
                }
            } else {
                return {
                    http: {
                        res: {
                            status : getBody().code,
                            json : getBody().err?.message
                        }
                    }
                }
            }
        }

    }

    function parseXml(xml) {
      const successMatch = xml?.match(/<success>(.*?)<\/success>/);
      const codeMatch = xml?.match(/<code>(.*?)<\/code>/);
      const messageMatch = xml?.match(/<message>(.*?)<\/message>/);

      const success = successMatch ? successMatch[1] === "true" : null;
      const code = codeMatch ? parseInt(codeMatch[1], 10) : null;
      const message = messageMatch ? messageMatch[1] : null;


      return { success, code, message };
    }
  }
}
