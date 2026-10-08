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
    OPENAI_API_KEY,
    TWILIO_ACCOUNT_SID,
    TWILIO_AUTH_TOKEN
} = process.env;

if (!OPENAI_API_KEY) {
    console.error('Missing OPENAI_API_KEY.');
    process.exit(1);
}

if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) {
    console.error('Missing Twilio credentials.');
    process.exit(1);
}

const MODEL = 'gpt-live-1';
const VOICE = 'marin';
const USER_AGENT = 'sv-ai-call-center/1.0';
const PORT = process.env.PORT || 5050;

const OPENING = 'Hi, this is Mia. How can I help you?';

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
- Use brief natural acknowledgements only when useful.
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
- Is the system not cooling, not heating, leaking, frozen, making noise, or not turning on?
- Is it central AC, mini-split, furnace, boiler, or another system?
- How many units are affected?

Do not perform a long technical diagnosis over the phone.

PLUMBING:
Briefly determine what is leaking, clogged, broken, or not working.
If water is actively leaking, ask whether they can safely shut off the water.

NO EXTERNAL LOOKUPS:
- Do not search the web.
- Do not browse.
- Do not look up products, brands, prices, reviews, inventory, availability, or technical information.
- Do not say "let me check", "let me look that up", "I'll search for that", or similar phrases.
- Do not invent business inventory, supported brands, repair prices, or availability.
- Answer only from information explicitly provided in these instructions or by the caller.
- If a fact is not confirmed, say briefly that you do not have confirmed information.
- Do not delegate a request merely because you do not know the answer.

For example, if asked what faucet brands the company carries, say briefly:

"I don't have a confirmed inventory list, so I don't want to give you the wrong information. Is there a particular brand you're looking for?"

PRICING:
- The regular service call and diagnostic fee is $95.
- Do not estimate repair prices.
- Do not give average repair prices or price ranges.
- Do not imply that the entire repair costs $95.
- If the technician performs the repair or service work, the $95 service call goes toward the cost of the work.
- If no repair or service work is performed, the $95 service call still applies.

If asked how much a repair will cost, say briefly:

"I can't give you an exact repair price until the technician checks it. The service call is $95, and if we do the repair, that $95 goes toward the cost of the work."

Near the end of the conversation, after you understand the problem, address,
and preferred appointment time, mention the $95 service call once if it has not already been discussed.

SCHEDULING:
- Do not guarantee an appointment time unless availability has been confirmed.
- For now, collect the customer's preferred time and say it will be confirmed.
- Never claim a text, appointment confirmation, or other action was sent unless the backend has actually confirmed it.

SAFETY:
If there is a gas smell, fire, smoke, carbon monoxide alarm, or immediate danger,
tell the customer to leave the area and contact 911 or the appropriate utility.

ENDING:
- Before ending the call, try to have the customer's name, service issue, address, and preferred time.
- Do not give a long recap.
- If the caller clearly says goodbye, says that is all, or otherwise clearly ends the conversation, give one brief natural closing sentence.
- After a clear goodbye, do not ask another question and do not continue the conversation.
`;

const fastify = Fastify();

fastify.register(fastifyFormBody);
fastify.register(fastifyWs);

fastify.get('/', async () => ({
    message: 'AI Call Center is running!'
}));

function escapeXml(value = '') {
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;');
}

function normalizeForMatch(value = '') {
    return String(value)
        .toLowerCase()
        .replace(/[’]/g, "'")
        .replace(/[^a-zа-яё0-9'\s]/gi, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function containsExplicitGoodbye(value = '') {
    const text = normalizeForMatch(value);

    return (
        /(^|\s)(bye|goodbye|good bye|that's all|that is all|thanks bye|thank you bye|thanks goodbye|thank you goodbye)(\s|$)/i.test(text) ||
        /(^|\s)(до свидания|пока|всего доброго|всего хорошего|это всё|это все|всё спасибо|все спасибо|спасибо пока)(\s|$)/i.test(text)
    );
}

fastify.all('/incoming-call', async (request, reply) => {

    const host =
        request.headers['x-forwarded-host'] ||
        request.headers.host;

    const callerPhone =
        request.body?.From ||
        request.query?.From ||
        '';

    reply.type('text/xml').send(
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
});

fastify.register(async (fastify) => {

    fastify.get(
        '/media-stream',
        { websocket: true },

        (connection) => {

            console.log('Twilio connected');

            let streamSid = null;
            let currentCallSid = null;
            let currentJobNumber = null;
            let openAiSessionId = null;

            let sessionRequested = false;
            let sessionReady = false;
            let shuttingDown = false;
            let twilioHangupRequested = false;

            let transcriptBuffer = [];
            let transcriptSaveChain = Promise.resolve();

            let customerTextWindow = '';
            let assistantTextWindow = '';

            let customerRequestedEnd = false;
            let assistantSpokeAfterEnd = false;

            let lastAssistantAudioAt = 0;

            let gracefulEndTimer = null;
            let gracefulEndFallbackTimer = null;

            const openAiWs = new WebSocket(
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

            const send = (event) => {

                if (
                    openAiWs.readyState ===
                    WebSocket.OPEN
                ) {

                    openAiWs.send(
                        JSON.stringify(event)
                    );
                }
            };

            const queueTranscript = (
                speaker,
                text,
                startMs = null,
                endMs = null,
                eventId = null
            ) => {

                if (
                    typeof text !== 'string' ||
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

            const flushTranscript = () => {

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

                transcriptBuffer = [];

                transcriptSaveChain =
                    transcriptSaveChain

                        .then(async () => {

                            await saveTranscriptChunks(
                                jobNumber,
                                chunks
                            );

                            console.log(
                                `Transcript saved: Job #${jobNumber}, ${chunks.length} chunks`
                            );
                        })

                        .catch((error) => {

                            console.error(
                                'Transcript save error:',
                                error
                            );

                            transcriptBuffer = [
                                ...chunks,
                                ...transcriptBuffer
                            ];
                        });

                return transcriptSaveChain;
            };

            const transcriptTimer =
                setInterval(
                    () => {
                        void flushTranscript();
                    },
                    1000
                );

            const closeSockets = () => {

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

            const shutdown = async (
                reason,
                waitForFinalTranscript = false
            ) => {

                if (shuttingDown) {
                    return;
                }

                shuttingDown = true;

                clearInterval(
                    transcriptTimer
                );

                if (gracefulEndTimer) {

                    clearInterval(
                        gracefulEndTimer
                    );

                    gracefulEndTimer = null;
                }

                if (
                    gracefulEndFallbackTimer
                ) {

                    clearTimeout(
                        gracefulEndFallbackTimer
                    );

                    gracefulEndFallbackTimer =
                        null;
                }

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

                await transcriptSaveChain;

                console.log(
                    `Closing call resources: ${reason}`
                );

                closeSockets();
            };

            const endTwilioCall = async (
                reason = 'conversation-ended'
            ) => {

                if (
                    twilioHangupRequested ||
                    !currentCallSid
                ) {

                    return;
                }

                twilioHangupRequested = true;

                try {

                    const auth =
                        Buffer.from(
                            `${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`
                        ).toString(
                            'base64'
                        );

                    const body =
                        new URLSearchParams({
                            Status:
                                'completed'
                        });

                    const response =
                        await fetch(
                            `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(TWILIO_ACCOUNT_SID)}/Calls/${encodeURIComponent(currentCallSid)}.json`,
                            {
                                method:
                                    'POST',

                                headers: {
                                    Authorization:
                                        `Basic ${auth}`,

                                    'Content-Type':
                                        'application/x-www-form-urlencoded'
                                },

                                body
                            }
                        );

                    if (!response.ok) {

                        const responseText =
                            await response.text();

                        throw new Error(
                            `Twilio ${response.status}: ${responseText}`
                        );
                    }

                    console.log(
                        `Twilio call ended: ${currentCallSid} (${reason})`
                    );

                    setTimeout(
                        () => {

                            void shutdown(
                                'twilio-api-end',
                                true
                            );
                        },
                        800
                    );

                } catch (error) {

                    twilioHangupRequested =
                        false;

                    console.error(
                        'Twilio end call error:',
                        error
                    );
                }
            };

            const requestGracefulEnd = () => {

                if (
                    customerRequestedEnd
                ) {

                    return;
                }

                customerRequestedEnd = true;

                assistantSpokeAfterEnd =
                    false;

                assistantTextWindow =
                    '';

                console.log(
                    'Customer clearly ended the conversation'
                );

                send({
                    type:
                        'session.instructions.append',

                    delegation_id:
                        null,

                    content:
                        'The caller has clearly ended the conversation. Give exactly one brief, natural closing sentence now. Do not ask another question. Do not continue the conversation after the closing.'
                });

                gracefulEndTimer =
                    setInterval(
                        () => {

                            if (
                                assistantSpokeAfterEnd &&
                                lastAssistantAudioAt > 0 &&
                                Date.now() -
                                    lastAssistantAudioAt >=
                                    1800
                            ) {

                                clearInterval(
                                    gracefulEndTimer
                                );

                                gracefulEndTimer =
                                    null;

                                void endTwilioCall(
                                    'customer-goodbye'
                                );
                            }
                        },
                        250
                    );

                gracefulEndFallbackTimer =
                    setTimeout(
                        () => {

                            if (
                                !twilioHangupRequested
                            ) {

                                console.log(
                                    'Graceful end fallback reached'
                                );

                                void endTwilioCall(
                                    'customer-goodbye-fallback'
                                );
                            }
                        },
                        10000
                    );
            };

            const startSession = () => {

                if (
                    sessionRequested ||
                    !streamSid ||
                    openAiWs.readyState !==
                        WebSocket.OPEN
                ) {

                    return;
                }

                sessionRequested = true;

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

            openAiWs.on(
                'open',
                () => {

                    console.log(
                        'Connected to GPT-Live-1'
                    );

                    startSession();
                }
            );

            openAiWs.on(
                'message',
                (data) => {

                    try {

                        const event =
                            JSON.parse(data);

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

                        } else if (
                            event.type ===
                                'session.output_audio.delta' &&

                            streamSid &&

                            connection.readyState ===
                                WebSocket.OPEN
                        ) {

                            lastAssistantAudioAt =
                                Date.now();

                            if (
                                customerRequestedEnd
                            ) {

                                assistantSpokeAfterEnd =
                                    true;
                            }

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

                            customerTextWindow = (
                                customerTextWindow +
                                event.delta
                            ).slice(-300);

                            if (
                                !customerRequestedEnd &&
                                containsExplicitGoodbye(
                                    customerTextWindow
                                )
                            ) {

                                requestGracefulEnd();
                            }

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

                            assistantTextWindow = (
                                assistantTextWindow +
                                event.delta
                            ).slice(-300);

                        } else if (
                            event.type ===
                                'session.delegation.created'
                        ) {

                            const delegationId =
                                event.delegation?.id ||
                                null;

                            console.log(
                                'Delegation blocked:',
                                delegationId
                            );

                            if (delegationId) {

                                send({
                                    type:
                                        'session.commentary.append',

                                    event_id:
                                        `blocked_${Date.now()}`,

                                    delegation_id:
                                        delegationId,

                                    content:
                                        'No external lookup or backend search is available for this request. Do not say you are checking or searching. If the requested fact is not confirmed in the business instructions or by the caller, briefly say that you do not have confirmed information.'
                                });
                            }

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

            connection.on(
                'message',

                async (message) => {

                    try {

                        const data =
                            JSON.parse(message);

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

                        } else if (
                            data.event ===
                                'start'
                        ) {

                            streamSid =
                                data.start.streamSid;

                            currentCallSid =
                                data.start.callSid ||
                                null;

                            const callerPhone =
                                data.start
                                    .customParameters
                                    ?.From ||
                                null;

                            console.log(
                                'Incoming Twilio stream:',
                                streamSid
                            );

                            console.log(
                                'Twilio Call SID:',
                                currentCallSid
                            );

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

                                            callSid:
                                                currentCallSid
                                        });

                                    currentJobNumber =
                                        job.job_number;

                                    console.log(
                                        `CRM job created: #${job.job_number}`
                                    );

                                    await flushTranscript();
                                }

                            } catch (error) {

                                console.error(
                                    'CRM create job error:',
                                    error
                                );
                            }

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
});

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
