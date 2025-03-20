import stream from "stream";
import util from "util";
import {
    BedrockAgentRuntimeClient,
    InvokeAgentCommand,
    OrchestrationTrace,
  } from "@aws-sdk/client-bedrock-agent-runtime";
import log from "./log.mjs";
import { v4 as uuidv4 } from 'uuid';

const pipeline = util.promisify(stream.pipeline);
const bedrockAgentClient = new BedrockAgentRuntimeClient({ region: "us-east-1" });

// Sample event payload
//   {
//     "sessionParams": {
//       "sessionId": "SESSION_ID"
//       "endSession": "false",
//       "memoryId": "MEMORY_ID"
//     }
//     "lambdaParams": {
//       "isStreaming": "true"
//     },
//     "agentParams": {
//       "agentId": "AGENT_ID",
//       "agentAliasId": "AGENT_ALIAS_ID",
//       "enableTrace": "true",
//       "inputText": "INPUT_TEXT"
//     }
//   }

// helpers
function getBody(event) {
    if ('body' in event) {
        let body = event.isBase64Encoded ? new TextDecoder("utf-8").decode(event.body) : event.body;
        body = JSON.parse(body)
        log.info(JSON.stringify(body));
        return body;
    }
    else {
        return event;
    }
}

function getInvokeAgentRequest(body) {
    return {
        agentId: body.agentParams?.agentId ?? process.env.ORCH_AGENT_ID,
        agentAliasId: body.agentParams?.agentAliasId ?? process.env.ORCH_AGENT_ALIAS_ID,
        sessionId: body.sessionParams?.sessionId ?? uuidv4(),
        endSession: body.sessionParams?.endSession?.toLowerCase() == 'true' ?? false,
        memoryId: body.sessionParams?.memoryId ?? uuidv4(),
        enableTrace: body.agentParams?.enableTrace?.toLowerCase() == 'true' ?? true,
        inputText: body.agentParams.inputText,
        streamingConfigurations: {
          streamFinalResponse: body.lambdaParams?.isStreaming?.toLowerCase() == 'true' ?? true
        },
    };
}

function extractOrchestrationTraceInfo(orchestrationTrace) {
  if (!orchestrationTrace) {
    return { type: "", traceId: "" };
  }
  const entry = Object.entries(orchestrationTrace)[0];
  if (!entry) {
    return { type: "", traceId: "" };
  }
  const [key, value] = entry;
  return { type: key, traceId: value.traceId };
}

async function doBedrockAgent(body, responseStream) {
    const command = new InvokeAgentCommand(getInvokeAgentRequest(body));
    try {
        const response = await bedrockAgentClient.send(command);
    
        if (response.completion === undefined) {
            throw new Error("Completion is undefined");
        }

        let chunkCount = 0;
        let traceCount = 0;
        const chunks = [];
        for await (const chunkEvent of response.completion) {
            if (chunkEvent.chunk) {
                chunkCount += 1;
                const chunk = chunkEvent.chunk;
                const decodedResponse = new TextDecoder("utf-8").decode(chunk.bytes);
                log.info(`Chunk ${chunkCount}: ${decodedResponse}`);
                chunks.push(decodedResponse);
                responseStream.write(decodedResponse);
            }
            // TODO: handle better tracing later. E.g. write to DB.
            if (chunkEvent.trace?.trace) {
                traceCount += 1;
                const orchestrationTraceInfo = extractOrchestrationTraceInfo(
                    chunkEvent.trace.trace.orchestrationTrace
                );
                log.info(
                    `TRACE ${traceCount}: ${orchestrationTraceInfo.type}, ${orchestrationTraceInfo.traceId}`
                );
                log.info(JSON.stringify(chunkEvent.trace.trace));
            }
        }
        const completion = chunks.join('')
        log.info(completion);
    } catch (err) {
        log.error(err)
    } finally {
        responseStream.end();
    }
}

export const handler = awslambda.streamifyResponse(async (event, responseStream, _context) => {
    log.info(JSON.stringify(event));
    const body = getBody(event);
    
    await doBedrockAgent(body, responseStream);
    
    log.info(JSON.stringify({"status": "complete"}));
});