import {
    initDatabase,
    findOrCreateCustomer,
    createJobForCall,
    saveTranscriptChunks
} from './db.js';

import Fastify from 'fastify';
import WebSocket from 'ws';
import dotenv from 'dotenv';
import fastifyFormBody from '@fastify/formbody';
import fastifyWs from '@fastify/websocket';


dotenv.config();


const {
    OPENAI_API_KEY
} = process.env;


if (!OPENAI_API_KEY) {
    console.error(
        'Missing OpenAI API key.'
    );

    process.exit(1);
}


const MODEL =
    'gpt-live-1';

const VOICE =
    'marin';

const USER_AGENT =
    'sv-ai-call-center/1.0';

const PORT =
    process.env.PORT || 5050;


// =========================================================
// MIA
// =========================================================

const OPENING =
    'Hi, this is Mia. How can I help you?';


const VOICE_PROMPT = `
You are Mia, the female phone receptionist for an HVAC and plumbing service company
serving New York City and surrounding areas.

COMMUNICATION STYLE:
- Sound natural, calm, confident, and conversational.
- Keep your answers very short and direct.
- Usually answer in one or two short sentences.
- Ask only one question at a time.
- Stay strictly focused on the customer's service request.
- Do not give long explanations unless the customer specifically asks.
- Do not repeat information the customer just told you.
- Do not unnecessarily summarize the conversation.
- Do not sound overly enthusiastic, scripted, salesy, or robotic.
- Use brief natural acknowledgements when appropriate, such as:
  "Okay", "Got it", "Sure", or "I see".
- Do not use an acknowledgement after every customer response.
- Use natural American English and contractions.
- If the caller interrupts you, stop speaking and listen.
- Do not fill silence with unnecessary speech.
- Never use jokes unless the customer is joking first.

IDENTITY AND LANGUAGE:
- Your name is Mia.
- Do not unnecessarily announce that you are an AI.
- Never falsely claim to be a human.
- If directly asked whether you are AI, say briefly:
  "I'm the company's virtual assistant."
- Speak in the same language the caller is using whenever possible.
- Do not randomly switch languages during a conversation.
- Your voice identity is female.
- In languages that use grammatical gender, refer to yourself using feminine forms.
- In Russian, use forms such as "я поняла", "я записала", and "я проверила".
- If directly asked who you are in Russian, say:
  "Я виртуальная помощница компании."

PRIMARY GOAL:
Quickly understand the customer's problem and collect only the information
needed to handle the service request.

COLLECT:
- Customer's name.
- What is wrong.
- Type of HVAC or plumbing equipment, if they know.
- Service address and ZIP code.
- Preferred appointment time.
- Whether the issue is urgent.

HVAC:
Ask only useful questions based on the customer's problem.

Examples:
- Is the system not cooling, not heating, leaking, frozen, making noise,
  or not turning on?
- Is it central AC, mini-split, furnace, boiler, or another system?
- How many units are affected?

Do not perform a long technical diagnosis over the phone.

PLUMBING:
Briefly determine what is leaking, clogged, broken, or not working.

If water is actively leaking, ask whether they can safely shut off the water.

DELEGATION POLICY:

Backend tools:
- None.

Never delegate any request.
Never search the web.
Never look up prices.
Never say "let me check", "let me look that up", or similar phrases.
Never wait for a backend result because no backend tools are currently available.

For any question about repair pricing:
- Do not search.
- Do not estimate.
- Do not give average prices.
- Do not give price ranges.
- The only authorized price is the $95 service call / diagnostic fee.

If the customer asks how much a repair will cost, answer immediately:

"I can't give you an exact repair price until the technician checks it.
The service call is $95, and if we do the repair, that $95 goes toward the cost of the work."

If the customer asks for a rough estimate, say:

"It depends on what's actually wrong, so I don't want to give you the wrong number.
The technician can give you the repair price after checking the system."

PRICING:
The service call and diagnostic fee is $95.

Near the end of the conversation, after you understand the problem,
address, and preferred appointment time, tell the customer naturally:

"Just so you know, the service call is $95. If the technician does the repair,
that $95 goes toward the cost of the work."

If no repair or service work is performed, the $95 service call fee still applies.

Do not imply that the entire repair costs $95.
Do not repeatedly mention the fee.

If the customer asks about the service call price earlier,
answer immediately and briefly.

SCHEDULING:
Do not guarantee an appointment time unless availability has been confirmed.

For now, collect the customer's preferred time and say it will be confirmed.

SAFETY:
If there is a gas smell, fire, smoke, carbon monoxide alarm,
or immediate danger, tell the customer to leave the area
and contact 911 or the appropriate utility.

ENDING:
Before ending the call, make sure you have:
- the customer's name,
- service issue,
- address,
- preferred time.

Do not give a long recap.
`;


// =========================================================
// FASTIFY
// =========================================================

const fastify =
    Fastify();


fastify.register(
    fastifyFormBody
);

fastify.register(
    fastifyWs
);


fastify.get(
    '/',
    async () => ({
        message:
            'AI Call Center is running!'
    })
);


// =========================================================
// XML SAFETY
// =========================================================

function escapeXml(
    value = ''
) {
    return String(value)
        .replace(
            /&/g,
            '&amp;'
        )
        .replace(
            /</g,
            '&lt;'
        )
        .replace(
            />/g,
            '&gt;'
        )
        .replace(
            /"/g,
            '&quot;'
        )
        .replace(
            /'/g,
            '&apos;'
        );
}


// =========================================================
// INCOMING TWILIO CALL
// =========================================================

fastify.all(
    '/incoming-call',

    async (
        request,
        reply
    ) => {

        const host =
            request.headers[
                'x-forwarded-host'
            ] ||
            request.headers.host;


        const callerPhone =
            request.body?.From ||
            request.query?.From ||
            '';


        reply
            .type(
                'text/xml'
            )
            .send(
`<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Connect>
        <Stream url="wss://${host}/media-stream">
            <Parameter
                name="From"
                value="${escapeXml(callerPhone)}"
            />
        </Stream>
    </Connect>
</Response>`
            );
    }
);


// =========================================================
// TWILIO MEDIA STREAM
// =========================================================

fastify.register(
    async (fastify) => {

        fastify.get(
            '/media-stream',

            {
                websocket: true
            },

            (connection) => {

                console.log(
                    'Twilio connected'
                );


                // =================================================
                // CALL STATE
                // =================================================

                let streamSid =
                    null;

                let sessionRequested =
                    false;

                let sessionReady =
                    false;

                let currentJobNumber =
                    null;

                let openAiSessionId =
                    null;

                let shuttingDown =
                    false;


                // =================================================
                // TRANSCRIPT STATE
                // =================================================

                let transcriptBuffer =
                    [];

                let transcriptSaveChain =
                    Promise.resolve();


                // =================================================
                // OPENAI LIVE WEBSOCKET
                // =================================================

                const openAiWs =
                    new WebSocket(
                        'wss://api.openai.com/v1/live/sessions',

                        {
                            headers: {
                                Authorization:
                                    `Bearer ${OPENAI_API_KEY}`,

                                'User-Agent':
                                    USER_AGENT
                            }
                        }
                    );


                // =================================================
                // SEND EVENT TO OPENAI
                // =================================================

                const send =
                    (event) => {

                        if (
                            openAiWs.readyState ===
                            WebSocket.OPEN
                        ) {
                            openAiWs.send(
                                JSON.stringify(
                                    event
                                )
                            );
                        }
                    };


                // =================================================
                // QUEUE TRANSCRIPT
                // =================================================

                const queueTranscript =
                    (
                        speaker,
                        text,
                        startMs = null,
                        endMs = null,
                        eventId = null
                    ) => {

                        if (
                            typeof text !==
                                'string' ||
                            text.length === 0
                        ) {
                            return;
                        }


                        transcriptBuffer.push({
                            speaker,
                            text,

                            startMs,
                            endMs,

                            eventId,

                            sessionId:
                                openAiSessionId
                        });
                    };


                // =================================================
                // SAVE TRANSCRIPT BUFFER TO POSTGRES
                // =================================================

                const flushTranscript =
                    () => {

                        if (
                            !currentJobNumber ||
                            transcriptBuffer.length === 0
                        ) {
                            return transcriptSaveChain;
                        }


                        const jobNumber =
                            currentJobNumber;


                        const chunks =
                            transcriptBuffer;


                        transcriptBuffer =
                            [];


                        transcriptSaveChain =
                            transcriptSaveChain
                                .then(
                                    async () => {

                                        await saveTranscriptChunks(
                                            jobNumber,
                                            chunks
                                        );
                                    }
                                )
                                .catch(
                                    (error) => {

                                        console.error(
                                            'Transcript save error:',
                                            error
                                        );


                                        // Put unsaved transcript back
                                        // into memory so we can retry.

                                        transcriptBuffer = [
                                            ...chunks,
                                            ...transcriptBuffer
                                        ];
                                    }
                                );


                        return transcriptSaveChain;
                    };


                // Save transcript roughly once every second.

                const transcriptTimer =
                    setInterval(
                        () => {
                            void flushTranscript();
                        },
                        1000
                    );


                // =================================================
                // CLOSE SOCKETS
                // =================================================

                const closeSockets =
                    () => {

                        if (
                            connection.readyState ===
                            WebSocket.OPEN
                        ) {
                            connection.close();
                        }


                        if (
                            openAiWs.readyState ===
                            WebSocket.OPEN
                        ) {
                            openAiWs.close();
                        }
                    };


                // =================================================
                // CLEAN SHUTDOWN
                // =================================================

                const shutdown =
                    async (
                        reason,
                        waitForFinalTranscript = false
                    ) => {

                        if (
                            shuttingDown
                        ) {
                            return;
                        }


                        shuttingDown =
                            true;


                        clearInterval(
                            transcriptTimer
                        );


                        // Give GPT-Live a short moment to send
                        // the final transcript fragments.

                        if (
                            waitForFinalTranscript &&
                            openAiWs.readyState ===
                                WebSocket.OPEN
                        ) {
                            await new Promise(
                                (resolve) =>
                                    setTimeout(
                                        resolve,
                                        500
                                    )
                            );
                        }


                        await flushTranscript();


                        console.log(
                            `Closing call resources: ${reason}`
                        );


                        closeSockets();
                    };


                // =================================================
                // START GPT-LIVE SESSION
                // =================================================

                const startSession =
                    () => {

                        if (
                            sessionRequested ||
                            !streamSid ||
                            openAiWs.readyState !==
                                WebSocket.OPEN
                        ) {
                            return;
                        }


                        sessionRequested =
                            true;


                        send({
                            type:
                                'session.start',

                            session: {
                                model:
                                    MODEL,

                                instructions:
                                    VOICE_PROMPT,

                                delegation: {
                                    type:
                                        'client'
                                },

                                audio: {
                                    format: {
                                        type:
                                            'audio/pcmu',

                                        rate:
                                            8000
                                    },

                                    output: {
                                        voice:
                                            VOICE
                                    }
                                }
                            }
                        });
                    };


                // =================================================
                // OPENAI CONNECTED
                // =================================================

                openAiWs.on(
                    'open',

                    () => {

                        console.log(
                            'Connected to GPT-Live-1'
                        );


                        startSession();
                    }
                );


                // =================================================
                // OPENAI EVENTS
                // =================================================

                openAiWs.on(
                    'message',

                    (data) => {

                        try {
                            const event =
                                JSON.parse(
                                    data
                                );


                            // =====================================
                            // SESSION STARTED
                            // =====================================

                            if (
                                event.type ===
                                'session.started'
                            ) {

                                sessionReady =
                                    true;


                                openAiSessionId =
                                    event.session?.id ||
                                    null;


                                console.log(
                                    'GPT-Live-1 session:',
                                    openAiSessionId
                                );


                                send({
                                    type:
                                        'session.instructions.append',

                                    delegation_id:
                                        null,

                                    content:
                                        `Your first spoken line on this call is exactly: "${OPENING}"`
                                });


                                send({
                                    type:
                                        'session.commentary.append',

                                    delegation_id:
                                        null,

                                    content:
                                        OPENING
                                });


                            // =====================================
                            // MIA AUDIO
                            // =====================================

                            } else if (
                                event.type ===
                                    'session.output_audio.delta' &&

                                streamSid &&

                                connection.readyState ===
                                    WebSocket.OPEN
                            ) {

                                connection.send(
                                    JSON.stringify({
                                        event:
                                            'media',

                                        streamSid,

                                        media: {
                                            payload:
                                                event.delta
                                        }
                                    })
                                );


                            // =====================================
                            // CUSTOMER TRANSCRIPT
                            // =====================================

                            } else if (
                                event.type ===
                                    'session.input_transcript.delta'
                            ) {

                                console.log(
                                    'Customer:',
                                    event.delta
                                );


                                queueTranscript(
                                    'customer',

                                    event.delta,

                                    event.start_ms ??
                                        null,

                                    event.end_ms ??
                                        null,

                                    event.event_id ??
                                        null
                                );


                            // =====================================
                            // MIA TRANSCRIPT
                            // =====================================

                            } else if (
                                event.type ===
                                    'session.output_transcript.delta'
                            ) {

                                console.log(
                                    'Assistant:',
                                    event.delta
                                );


                                queueTranscript(
                                    'assistant',

                                    event.delta,

                                    event.start_ms ??
                                        null,

                                    event.end_ms ??
                                        null,

                                    event.event_id ??
                                        null
                                );


                            // =====================================
                            // OPENAI ERROR
                            // =====================================

                            } else if (
                                event.type ===
                                    'error'
                            ) {

                                console.error(
                                    'GPT-Live-1 error:',
                                    event.error
                                );
                            }


                        } catch (error) {

                            console.error(
                                'OpenAI message error:',
                                error
                            );
                        }
                    }
                );


                // =================================================
                // TWILIO EVENTS
                // =================================================

                connection.on(
                    'message',

                    async (message) => {

                        try {
                            const data =
                                JSON.parse(
                                    message
                                );


                            // =====================================
                            // CUSTOMER AUDIO
                            // =====================================

                            if (
                                data.event ===
                                    'media' &&

                                sessionReady &&

                                openAiWs.readyState ===
                                    WebSocket.OPEN
                            ) {

                                send({
                                    type:
                                        'session.input_audio.append',

                                    audio:
                                        data.media.payload
                                });


                            // =====================================
                            // TWILIO STREAM START
                            // =====================================

                            } else if (
                                data.event ===
                                    'start'
                            ) {

                                streamSid =
                                    data.start.streamSid;


                                const callSid =
                                    data.start.callSid;


                                const callerPhone =
                                    data.start
                                        .customParameters
                                        ?.From ||
                                    null;


                                console.log(
                                    'Incoming Twilio stream:',
                                    streamSid
                                );


                                // Start Mia immediately.
                                // Database work should not delay
                                // the greeting.

                                startSession();


                                try {

                                    if (
                                        !callerPhone
                                    ) {

                                        console.error(
                                            'Caller phone number was not received'
                                        );


                                    } else {

                                        const customer =
                                            await findOrCreateCustomer(
                                                callerPhone
                                            );


                                        const job =
                                            await createJobForCall({
                                                customerId:
                                                    customer.id,

                                                callSid
                                            });


                                        currentJobNumber =
                                            job.job_number;


                                        console.log(
                                            `CRM job created: #${job.job_number}`
                                        );


                                        // Mia might already have spoken
                                        // while PostgreSQL was creating
                                        // the job, so save those fragments now.

                                        await flushTranscript();
                                    }


                                } catch (error) {

                                    console.error(
                                        'CRM create job error:',
                                        error
                                    );
                                }


                            // =====================================
                            // TWILIO STREAM STOP
                            // =====================================

                            } else if (
                                data.event ===
                                    'stop'
                            ) {

                                await shutdown(
                                    'twilio-stop',
                                    true
                                );
                            }


                        } catch (error) {

                            console.error(
                                'Twilio message error:',
                                error
                            );
                        }
                    }
                );


                // =================================================
                // TWILIO SOCKET CLOSED
                // =================================================

                connection.on(
                    'close',

                    () => {

                        void shutdown(
                            'twilio-close',
                            true
                        );


                        console.log(
                            'Caller disconnected'
                        );
                    }
                );


                // =================================================
                // TWILIO SOCKET ERROR
                // =================================================

                connection.on(
                    'error',

                    (error) => {

                        console.error(
                            'Twilio WebSocket error:',
                            error
                        );


                        void shutdown(
                            'twilio-error',
                            false
                        );
                    }
                );


                // =================================================
                // OPENAI SOCKET CLOSED
                // =================================================

                openAiWs.on(
                    'close',

                    (
                        code,
                        reason
                    ) => {

                        console.log(
                            'Disconnected from GPT-Live-1',
                            code,
                            reason.toString()
                        );


                        void shutdown(
                            'openai-close',
                            false
                        );
                    }
                );


                // =================================================
                // OPENAI SOCKET ERROR
                // =================================================

                openAiWs.on(
                    'error',

                    (error) => {

                        console.error(
                            'OpenAI WebSocket error:',
                            error
                        );


                        void shutdown(
                            'openai-error',
                            false
                        );
                    }
                );
            }
        );
    }
);


// =========================================================
// START SERVER
// =========================================================

async function start() {

    try {

        await initDatabase();


        await fastify.listen({
            port:
                PORT,

            host:
                '0.0.0.0'
        });


        console.log(
            `AI Call Center listening on port ${PORT}`
        );


    } catch (error) {

        console.error(
            'Startup error:',
            error
        );


        process.exit(1);
    }
}


start();
