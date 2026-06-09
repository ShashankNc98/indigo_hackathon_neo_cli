import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn, getValueByKey } = dao;

@Dag({ method: "POST", url: "recon/utilised-pnr" })
class ReconUtilisedPNR {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: 15.924479166666629, y: -7.601888020833343 } })
  @Relation(r => dao.isSuccess(), 'StaticConfiguration')
  async AppConfigurations() {
    const script = {

        execute: () => {

            const appVersion = "1.1"; 
            const developer="Divya"
            const branch="PSV-30719"   
            const trigger = "/recon/utilised-pnr" 
            const requestBody = getApiRequest()?.body?.attributes?.data;
            const externalId = requestBody?.customerIdentifiers?.instore?.externalId
            const mobile = requestBody?.customerIdentifiers?.instore?.mobile
            const isgRequestId = `${trigger}_${externalId}_${mobile}`;
            logger.info(`IsgRequestId : ${JSON.stringify(isgRequestId)}`);     
            logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`)

            return {                       
                body:
                {
                    APP_VERSION : appVersion
                }
            };

        }
    }
  }

  @Script({ pos: { x: 640, y: 0 } })
  @Relation(r => dao.isSuccess() && dao.getBody()?.valid === true, 'getData')
  @Relation(r => dao.isSuccess() && dao.getBody()?.valid === false, 'createPayload')
  async validateCustomFields() {
    const script = {

        execute: () => {
            let payload = getApiRequest().body;
            let customFields = payload?.attributes?.data?.customFields;
            let extendedFields = payload?.attributes?.data?.extendedFields;
            let valid = true;
            let origin = null;
            let destination = null;
            let pnrNumber, firstName, lastName, departureDate;
            let newBillNumber;


            customFields?.forEach(field => {
                if (field.key === 'origin') {
                    origin = field?.value?.toString()?.trim();
                }
                if (field.key === 'destination') {
                    destination = field?.value?.toString()?.trim();
                }
            });
            //return {origin,destination}
            extendedFields?.forEach(field => {
                if (field.key === 'pnr_number') {
                    pnrNumber = field?.value?.toString()?.trim();
                }
                if (field.key === 'booking_first_name') {
                    firstName = field?.value?.toString()?.trim();
                }
                if (field.key === 'booking_last_name') {
                    lastName = field?.value?.toString()?.trim();
                }
                if (field.key === 'departure_date') {
                    departureDate = field?.value?.toString()?.trim();
                }
            })
            //return {pnrNumber,firstName,lastName,departureDate}
            if (origin && origin !== "0" && destination && destination !== "0") {
                const depDate = departureDate;
                const modifiedDepartureDate = depDate.split("-").join("");

                newBillNumber = `${pnrNumber}${firstName}${lastName}${origin}${destination}${modifiedDepartureDate}`.split(" ").join("").toLowerCase().trim()
                valid = true; // invalid
                let query = {
                    "PNR_KEY": newBillNumber
                }
                return {
                    query,
                    valid
                }
            }
            return {
                body: {
                    pnrNumber, firstName, lastName, departureDate,
                    pnrKey: newBillNumber
                },
                valid: false
            }
        }
    }
  }

  @GetMongo({ pos: { x: 960, y: 0 } })
  @ExecutionStrategy('or')
  @Relation(r => dao.isSuccess() && dao.getOut()?.length > 0, 'updateDBPayload')
  @Relation(r => dao.isSuccess() && dao.getOut()?.length === 0, 'createPayloadForInsert')
  async getData() {
  return {
        collectionName: `UtilisedPNR`,
        query: r => getBody().query,
        sort: `{}`,
        options: ``,
      };
  }

  @Script({ pos: { x: 1280, y: 0 } })
  @Relation(r => dao.isSuccess(), 'updateStatusToDB')
  async updateDBPayload() {
    const script = {

      execute: () => {
        let dbData = getIn()
        const { _id, ...data } = dbData;
        let mongoId = getIn()._id;
        let neoMongoId = Object.values(mongoId.buffer).map(byte => byte.toString(16).padStart(2, '0')).join('')
        const currentDate = new Date();
        // 🔹 Build update query
        const updateQuery = {
          query: JSON.stringify({
            $set: {
              status: "success",
              modifiedDate: currentDate,
            },
          }),
          queryKey: JSON.stringify({ _id: {"$oid":neoMongoId }}),
        };

        return updateQuery;
      }
    }
  }

  @Script({ pos: { x: 1280, y: 160 } })
  @Relation(r => dao.isSuccess(), 'InsertIntoUtilisedPNR')
  async createPayloadForInsert() {
    const script = {

        execute: () => {
            let payload = getApiRequest().body;
            let customFields = payload?.attributes?.data?.customFields;
            let extendedFields = payload?.attributes?.data?.extendedFields;
            let origin = null;
            let destination = null;
            let ffn = payload?.attributes?.data?.customerIdentifiers?.instore?.externalId?.trim()
            let pnrNumber, firstName, lastName, departureDate, passengerid, bookingid, arrivalDate, source;

            function normalizeToDateOnly(value) {
                try {
                    if (!value || typeof value !== "string") return "";
                    value = value
                        .replace(/\bIST\b/g, "")
                        .replace(/\bGMT\b/g, "")
                        .replace(/\bUTC\b/g, "")
                        .trim();
                    const date = new Date(value);
                    if (isNaN(date.getTime())) return ""; // invalid
                    return date.toISOString().split("T")[0]; // :white_tick: Return in YYYY-MM-DD
                } catch {
                    return "";
                }
            }









            customFields?.forEach(field => {
                if (field.key === 'origin') {
                    origin = field?.value?.toString()?.trim();
                }
                if (field.key === 'destination') {
                    destination = field?.value?.toString()?.trim();
                }
                if (field.key === 'passengerid') {
                    passengerid = field?.value?.toString()?.trim();
                }
                if (field.key === 'bookingid') {
                    bookingid = field?.value?.toString()?.trim();
                }
                if (field.key === 'retro_or_auto') {
                    source = field?.value?.toString()?.trim().toLowerCase();
                }
            });
            //return {origin,destination}
            extendedFields?.forEach(field => {
                if (field.key === 'pnr_number') {
                    pnrNumber = field?.value?.toString()?.trim();
                }
                if (field.key === 'booking_first_name') {
                    firstName = field?.value?.toString();
                }
                if (field.key === 'booking_last_name') {
                    lastName = field?.value?.toString();
                }
                if (field.key === 'departure_date') {
                    departureDate = field?.value?.toString()?.trim();
                }
                if (field.key === 'arrival_date') {
                    arrivalDate = field?.value?.toString()?.trim();
                    arrivalDate = normalizeToDateOnly(arrivalDate)
                }
            })

            const modifiedDepartureDate = departureDate.split("-").join("");

            //return modifiedDepartureDate

            const newBillNumber = `${pnrNumber}${firstName}${lastName}${origin}${destination}${modifiedDepartureDate}`.split(" ").join("").toLowerCase().trim()
            const dbPayload = {
                "PNR": pnrNumber,
                "PNR_KEY": newBillNumber,
                "billNumber": payload?.attributes?.data?.billNumber,
                "ticketNumber": payload?.attributes?.data?.customFields?.eticket || '',
                "FFN": ffn,
                "sourceStore": payload?.attributes?.data?.extendedFields?.["store_associate_id"] || '',
                "origin": origin,
                "destination": destination,
                "distanceTravelled": "",
                "departureDate": departureDate,
                "firstName": firstName,
                "lastName": lastName,
                "source": source,
                "deviceId": "",
                "passengerid": passengerid,
                "bookingid": bookingid,
                "eticket": payload?.attributes?.data?.customFields?.eticket || '',
                "arrivalDate": arrivalDate,
                "status": "success",
                "creationDate": new Date(),
                "isActive": true,
                "modifiedDate": new Date()
            };

            return {
                query: dbPayload
            };

        }
    }
  }

  @PutMongo({ pos: { x: 1600, y: 160 } })
  @Relation(r => dao.isSuccess(), 'InsertSuccess')
  async InsertIntoUtilisedPNR() {
  return {
        collectionName: `UtilisedPNR`,
        mode: `insert`,
        query: r => getBody().query,
      };
  }

  @Script({ pos: { x: 316.65208333333334, y: -9.28229166666668 } })
  @Relation(r => dao.isSuccess(), 'validateCustomFields')
  async StaticConfiguration() {
    const script = {

        execute: async () => {

            const customeraddEmailDetails = await JSON.parse(await getValueByKey("RECON_UTILISED_PNR_EMAIL"))
            const literals = {
                "to": customeraddEmailDetails.to,
                "cc": customeraddEmailDetails.cc,
                "from": customeraddEmailDetails.from,
                "subject": customeraddEmailDetails.subject
            }

            return {
                body: literals
            };

        }
    }
  }

  @Script({ pos: { x: 1019, y: 255 } })
  @Relation(r => dao.isSuccess(), 'CommunicationEmail')
  async createPayload() {
    const script = {

        execute: () => {
            const literals = getBody("StaticConfiguration")?.body
            const getResponse = getBody()?.body
            const queryParams = {
                "format": literals.format
            }
            const headers = getEffectiveHeaders()
            const body = {
                "root": {
                    "email": [
                        {
                            "to": literals.to,
                            "cc": literals.cc,
                            "from": literals.from,
                            "subject": literals.subject,
                            "body": getResponse
                        }
                    ]

                }

            }
            return {
                headers,
                queryParams,
                body: JSON.stringify(body)
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 1339, y: 415 } })
  @Relation(r => dao.isSuccess()&& (dao.getBody()?.response?.status?.success == true), 'successBlock')
  @Relation(r => dao.hasError(), 'errorBlock')
  async CommunicationEmail() {
  return {
        url: `https://apac.api.capillarytech.com/v1.1/communications/email`,
        method: `POST`,
      };
  }

  @Script({ pos: { x: 1659, y: 415 } })
  async successBlock() {
    const script = {

        execute: () => {
            let data = getBody()
            return {
                http:{
                    res:{
                        "json":data,
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }

            };

        }
    }
  }

  @Script({ pos: { x: 1920, y: 160 } })
  async InsertSuccess() {
    const script = {

        execute: () => {
            return {
                http:{
                    res:{
                        "json": getBody(),
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }

            };

        }
    }
  }

  @Script({ pos: { x: 1659, y: 575 } })
  async errorBlock() {
    const script = {

        execute: () => {

            const errors = getBody()
            if (errors.code >= 500 && errors.code <= 599) {
                return {
                    http: {
                        res: {
                            status: 500,
                            json: errors,
                            "headers": {
                                "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                            }
                        }
                    }
                }
            }

            return {
                http: {
                    res: {
                        "json": errors,
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @PutMongo({ pos: { x: 1600, y: 0 } })
  @Relation(r => dao.isSuccess(), 'finalRes')
  async updateStatusToDB() {
  return {
        collectionName: `UtilisedPNR`,
        mode: `update`,
        query: r => getBody().query,
        queryKey: r => getBody().queryKey,
        options: ``,
      };
  }

  @Script({ pos: { x: 1920, y: 0 } })
  async finalRes() {
    const script = {

        execute: () => {

            //Write your code here.
            return {
                http: {
                    res: {
                        status: 200,
                        json: {
                            status: true,
                            message: "Data already present in DB"
                        }
                    }
                }
            };
        }
    }
  }
}
