import pino from "pino";
import { pinoLambdaDestination } from 'pino-lambda';

const destination = pinoLambdaDestination()
const prettyPrint = 
  (process.env.LOG_PRETTY_PRINT === undefined || process.env.LOG_PRETTY_PRINT === null) 
    ? true 
    : process.env.LOG_PRETTY_PRINT.toLowerCase() === "true";
const logToCloudWatch = 
    (process.env.LOG_DESTINATION === undefined || process.env.LOG_DESTINATION === null) 
      ? false 
      : process.env.LOG_DESTINATION.toUpperCase() === "CLOUDWATCH";

const options = {
  level: process.env.LOG_LEVEL ?? "info",
  timestamp: pino.stdTimeFunctions.isoTime,
  formatters: {
    level: (label) => ({ level: label }),
    bindings: () => ({}),
  },
  ...(prettyPrint && {
    transport: {
      target: "pino-pretty",
      options: {
        colorize: false,
        levelFirst: true,
        translateTime: "UTC:mm/dd/yyyy, h:MM:ss.l TT Z",
      },
    },
  }),
}
const log = pino(options, logToCloudWatch ? destination : undefined);

export default log;
