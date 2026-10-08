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
    TWILIO_AUTH_TOKEN,
    OWNER_PHONE_NUMBER
} = process.env;

if (!OPENAI_API_KEY) {
    console.error('Missing OPENAI_API_KEY.');
    process.exit(1);
}

if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) {
    console.error('Missing Twilio credentials.');
    process.exit(1);
}

if (!OWNER_PHONE_NUMBER) {
    console.error('Missing OWNER_PHONE_NUMBER.');
    process.exit(1);
}

const MODEL = 'gpt-live-1';
const VOICE = 'marin';
const USER_AGENT = 'sv-ai-call-center/1.0';
const PORT = process.env.PORT || 5050;

const SILENCE_CHECK_MS = 25_000;
const SILENCE_HANGUP_MS = 10_000;

const MAX_AI_CALL_MS = 5 * 60_000;
const MAX_CALL_WARNING_MS = 4 * 60_000 + 50_000;
const MAX_CALL_CLOSING_MS = 4 * 60_000 + 56_000;

const AUDIO_BURST_IDLE_MS = 350;

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
- Confirm whether we can contact the customer at the number they are calling from.

CALLBACK NUMBER:
- The backend already knows the phone number the customer is calling from.
- Never ask the customer to repeat that same number.
- Ask naturally:
  "Can we reach you back at this number?"
- If the customer says yes, do not ask for the number.
- If the customer says no, ask:
  "What's the best number to reach you at?"

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

For example, if asked what faucet brands the company carries, say:

"I don't have a confirmed inventory list, so I don't want to give you the wrong information. Is there a particular brand you're looking for?"

PRICING:
- The regular service call and diagnostic fee is $95.
- Do not estimate repair prices.
- Do not give average repair prices or price ranges.
- Do not imply that the entire repair costs $95.
- If the technician performs the repair or service work, the $95 service call goes toward the cost of the work.
- If no repair or service work is performed, the $95 service call still applies.

If asked how much a repair will cost, say:

"I can't give you an exact repair price until the technician checks it. The service call is $95, and if we do the repair, that $95 goes toward the cost of the work."

Near the end of the conversation, after you understand the problem,
address, and preferred appointment time, mention the $95 service call once
if it has not already been discussed.

SCHEDULING:
- Do not guarantee an appointment time unless availability has been confirmed.
- For now, collect the customer's preferred time and say it will be confirmed.
- Never claim a text, appointment confirmation, or other action was sent
  unless the backend has actually confirmed it.

HUMAN TRANSFER:
- If the caller asks to speak to a person, owner, manager, technician,
  representative, or human, agree briefly and say you will try to connect them.
- If the caller is upset, has a complaint or dispute, asks for a decision
  you cannot make, or the situation is outside the confirmed information you have,
  offer:
  "Would you like me to connect you with someone?"
- If you cannot understand the customer's request after two reasonable attempts,
  offer the same transfer.
- Do not say that the transfer succeeded until it actually succeeds.
- Do not keep asking questions after the customer clearly requests a person.

SAFETY:
If there is a gas smell, fire, smoke, carbon monoxide alarm, or immediate danger,
tell the customer to leave the area and contact 911 or the appropriate utility.

A transfer to the company must never replace emergency services.

ENDING:
- Before ending the call, try to have the customer's name,
  service issue, address, preferred time, and callback-number confirmation.
- Do not give a long recap.
- If the caller clearly says goodbye, says that is all,
  or otherwise clearly ends the conversation,
  give one brief natural closing sentence.
- After a clear goodbye, do not ask another question.
`;

const fastify = Fastify();

fastify.register(fastifyFormBody);
fastify.register(fastifyWs);

const acceptedTransfers = new Set();

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


function getPublicBaseUrl(request) {
    const proto =
        request.headers['x-forwarded-proto'] ||
        'https';

    const host =
        request.headers['x-forwarded-host'] ||
        request.headers.host;

    return `${proto}://${host}`;
}


function containsExplicitGoodbye(value = '') {
    const text =
        normalizeForMatch(value);

    return (
        /(^|\s)(bye|goodbye|good bye|that's all|that is all|thanks bye|thank you bye|thanks goodbye|thank you goodbye)(\s|$)/i.test(text) ||
        /(^|\s)(до свидания|пока|всего доброго|всего хорошего|это всё|это все|всё спасибо|все спасибо|спасибо пока)(\s|$)/i.test(text)
    );
}


function containsExplicitHumanRequest(value = '') {
    const text =
        normalizeForMatch(value);

    return (
        /\b(speak|talk|connect|transfer)\b.*\b(person|human|representative|manager|owner|technician|someone)\b/i.test(text) ||
        /\b(i want|i need|can i get)\b.*\b(human|person|representative|manager|owner|technician)\b/i.test(text) ||
        /\b(real person|live person|human agent)\b/i.test(text) ||

        /(соедините|переведите|хочу поговорить|можно поговорить).*(человек|оператор|менеджер|владелец|техник|мастер)/i.test(text) ||

        /(живой человек|живым человеком|оператором|менеджером|владельцем|техником|мастером)/i.test(text)
    );
}


function containsAffirmative(value = '') {
    const text =
        normalizeForMatch(value);

    return (
        /\b(yes|yeah|yep|sure|okay|ok|please|go ahead|absolutely)\b/i.test(text) ||
        /\b(да|ага|конечно|давайте|хорошо|пожалуйста)\b/i.test(text)
    );
}


function containsNegative(value = '') {
    const text =
        normalizeForMatch(value);

    return (
        /\b(no|nope|not now)\b/i.test(text) ||
        /\b(нет|не надо|не нужно)\b/i.test(text)
    );
}


// ======================================================
// INCOMING CALL
// ======================================================

fastify.all(
    '/incoming-call',

    async (
        request,
        reply
    ) => {

        const baseUrl =
            getPublicBaseUrl(request);

        const callerPhone =
            request.body?.From ||
            request.query?.From ||
            '';

        const callStartMs =
            Date.now();

        const wsBaseUrl =
            baseUrl.replace(
                /^http/,
                'ws'
            );

        reply
            .type(
                'text/xml'
            )
            .send(
`<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Connect>
        <Stream url="${escapeXml(wsBaseUrl)}/media-stream">

            <Parameter
                name="From"
                value="${escapeXml(callerPhone)}"
            />

            <Parameter
                name="BaseUrl"
                value="${escapeXml(baseUrl)}"
            />

            <Parameter
                name="CallStartMs"
                value="${callStartMs}"
            />

        </Stream>
    </Connect>
</Response>`
            );
    }
);


// ======================================================
// OWNER SCREENING
// ======================================================

fastify.all(
    '/owner-screen',

    async (
        request,
        reply
    ) => {

        const baseUrl =
            getPublicBaseUrl(request);

        const parentCallSid =
            request.query?.parent ||
            request.body?.parent ||
            '';

        const actionUrl =
            `${baseUrl}/owner-screen-result?parent=${encodeURIComponent(parentCallSid)}`;

        reply
            .type(
                'text/xml'
            )
            .send(
`<?xml version="1.0" encoding="UTF-8"?>
<Response>

    <Gather
        input="dtmf"
        numDigits="1"
        timeout="7"
        action="${escapeXml(actionUrl)}"
        method="POST">

        <Say>
            Service call transfer from Mia.
            Press 1 to accept.
        </Say>

    </Gather>

    <Say>
        Transfer not accepted.
        Goodbye.
    </Say>

    <Hangup/>

</Response>`
            );
    }
);


fastify.all(
    '/owner-screen-result',

    async (
        request,
        reply
    ) => {

        const digits =
            request.body?.Digits ||
            request.query?.Digits ||
            '';

        const parentCallSid =
            request.query?.parent ||
            request.body?.parent ||
            '';

        if (
            digits === '1' &&
            parentCallSid
        ) {

            acceptedTransfers.add(
                parentCallSid
            );

            console.log(
                `Owner accepted transfer for ${parentCallSid}`
            );

            reply
                .type(
                    'text/xml'
                )
                .send(
`<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say>
        Connecting you now.
    </Say>
</Response>`
                );

            return;
        }

        reply
            .type(
                'text/xml'
            )
            .send(
`<?xml version="1.0" encoding="UTF-8"?>
<Response>

    <Say>
        Transfer declined.
        Goodbye.
    </Say>

    <Hangup/>

</Response>`
            );
    }
);


// ======================================================
// TRANSFER RESULT
// ======================================================

fastify.all(
    '/transfer-result',

    async (
        request,
        reply
    ) => {

        const baseUrl =
            getPublicBaseUrl(request);

        const dialCallStatus =
            request.body?.DialCallStatus ||
            request.query?.DialCallStatus ||
            '';

        const callSid =
            request.body?.CallSid ||
            request.query?.CallSid ||
            '';

        const callerPhone =
            request.query?.from ||
            request.body?.From ||
            request.query?.From ||
            '';

        const startedRaw =
            request.query?.started ||
            request.body?.started ||
            '';

        const callStartMs =
            Number(startedRaw) ||
            Date.now();

        const ownerAccepted =
            callSid &&
            acceptedTransfers.has(
                callSid
            );

        console.log(
            `Transfer result: ${dialCallStatus || 'unknown'}, accepted=${ownerAccepted}`
        );

        if (
            ownerAccepted
        ) {

            acceptedTransfers.delete(
                callSid
            );
        }

        if (
            ownerAccepted &&
            (
                dialCallStatus ===
                    'completed' ||

                dialCallStatus ===
                    'answered'
            )
        ) {

            reply
                .type(
                    'text/xml'
                )
                .send(
`<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Hangup/>
</Response>`
                );

            return;
        }

        const wsBaseUrl =
            baseUrl.replace(
                /^http/,
                'ws'
            );

        reply
            .type(
                'text/xml'
            )
            .send(
`<?xml version="1.0" encoding="UTF-8"?>
<Response>

    <Connect>
        <Stream url="${escapeXml(wsBaseUrl)}/media-stream">

            <Parameter
                name="From"
                value="${escapeXml(callerPhone)}"
            />

            <Parameter
                name="BaseUrl"
                value="${escapeXml(baseUrl)}"
            />

            <Parameter
                name="CallStartMs"
                value="${callStartMs}"
            />

            <Parameter
                name="ResumeReason"
                value="transfer-unavailable"
            />

        </Stream>
    </Connect>

</Response>`
            );
    }
);


// ======================================================
// MEDIA STREAM
// ======================================================

fastify.register(
    async (fastify) => {

        fastify.get(
            '/media-stream',

            {
                websocket:
                    true
            },

            (
                connection
            ) => {

                console.log(
                    'Twilio connected'
                );


                // --------------------------------------
                // CALL STATE
                // --------------------------------------

                let streamSid =
                    null;

                let currentCallSid =
                    null;

                let currentCallerPhone =
                    null;

                let currentJobNumber =
                    null;

                let openAiSessionId =
                    null;

                let currentPublicBaseUrl =
                    null;

                let sessionRequested =
                    false;

                let sessionReady =
                    false;

                let shuttingDown =
                    false;

                let twilioHangupRequested =
                    false;

                let transferInProgress =
                    false;

                let callStartMs =
                    Date.now();

                let resumeReason =
                    null;


                // --------------------------------------
                // TRANSCRIPT
                // --------------------------------------

                let transcriptBuffer =
                    [];

                let transcriptSaveChain =
                    Promise.resolve();

                let customerTextWindow =
                    '';

                let assistantTextWindow =
                    '';

                let customerRequestedEnd =
                    false;

                let assistantOfferedTransferUntil =
                    0;


                // --------------------------------------
                // PLAYBACK / ACTIVITY
                // --------------------------------------

                let lastCustomerSpeechAt =
                    Date.now();

                let lastAssistantPlaybackEndedAt =
                    0;

                let hasAssistantPlaybackEnded =
                    false;

                let assistantPlaybackBusy =
                    false;


                let nextAssistantPurpose =
                    null;

                let currentAssistantPurpose =
                    null;

                let assistantMarkCounter =
                    0;

                let assistantMarkDebounceTimer =
                    null;


                const pendingPlaybackMarks =
                    new Map();

                let completedPlaybackPurposes =
                    [];

                const playbackWaiters =
                    new Map();

                const hangupByPurpose =
                    new Map();

                const hangupFallbackTimers =
                    new Map();


                // --------------------------------------
                // SILENCE STATE
                // --------------------------------------

                let silencePhase =
                    'none';

                let silenceCycle =
                    0;

                let activeSilenceCycle =
                    null;

                let silencePromptRequestedAt =
                    0;

                let silencePromptFinishedAt =
                    0;


                // --------------------------------------
                // TIMERS
                // --------------------------------------

                let silenceMonitor =
                    null;

                let maxWarningTimer =
                    null;

                let maxClosingTimer =
                    null;

                let maxHardTimer =
                    null;


                // --------------------------------------
                // OPENAI
                // --------------------------------------

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


                const send =
                    (
                        event
                    ) => {

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


                const sendToTwilio =
                    (
                        event
                    ) => {

                        if (
                            connection.readyState ===
                            WebSocket.OPEN
                        ) {

                            connection.send(
                                JSON.stringify(
                                    event
                                )
                            );
                        }
                    };


                // --------------------------------------
                // TRANSCRIPT
                // --------------------------------------

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

                            text.length ===
                                0
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


                const flushTranscript =
                    () => {

                        if (
                            !currentJobNumber ||

                            transcriptBuffer.length ===
                                0
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

                                        console.log(
                                            `Transcript saved: Job #${jobNumber}, ${chunks.length} chunks`
                                        );
                                    }
                                )

                                .catch(
                                    (
                                        error
                                    ) => {

                                        console.error(
                                            'Transcript save error:',
                                            error
                                        );

                                        transcriptBuffer = [
                                            ...chunks,
                                            ...transcriptBuffer
                                        ];
                                    }
                                );

                        return transcriptSaveChain;
                    };


                const transcriptTimer =
                    setInterval(
                        () => {

                            void flushTranscript();

                        },
                        1000
                    );


                // --------------------------------------
                // PLAYBACK HELPERS
                // --------------------------------------

                const clearHangupPurpose =
                    (
                        purpose
                    ) => {

                        hangupByPurpose.delete(
                            purpose
                        );

                        const timer =
                            hangupFallbackTimers.get(
                                purpose
                            );

                        if (
                            timer
                        ) {

                            clearTimeout(
                                timer
                            );

                            hangupFallbackTimers.delete(
                                purpose
                            );
                        }
                    };


                const clearPlaybackWaiters =
                    () => {

                        for (
                            const [
                                ,
                                waiter
                            ]
                            of playbackWaiters
                        ) {

                            clearTimeout(
                                waiter.timer
                            );

                            waiter.resolve(
                                false
                            );
                        }

                        playbackWaiters.clear();
                    };


                const clearAiTimers =
                    () => {

                        if (
                            silenceMonitor
                        ) {

                            clearInterval(
                                silenceMonitor
                            );
                        }

                        if (
                            maxWarningTimer
                        ) {

                            clearTimeout(
                                maxWarningTimer
                            );
                        }

                        if (
                            maxClosingTimer
                        ) {

                            clearTimeout(
                                maxClosingTimer
                            );
                        }

                        if (
                            maxHardTimer
                        ) {

                            clearTimeout(
                                maxHardTimer
                            );
                        }

                        silenceMonitor =
                            null;

                        maxWarningTimer =
                            null;

                        maxClosingTimer =
                            null;

                        maxHardTimer =
                            null;
                    };


                // --------------------------------------
                // CLOSE
                // --------------------------------------

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


                const shutdown =
                    async (
                        reason,
                        waitForFinalTranscript =
                            false
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

                        clearAiTimers();

                        clearPlaybackWaiters();


                        if (
                            assistantMarkDebounceTimer
                        ) {

                            clearTimeout(
                                assistantMarkDebounceTimer
                            );

                            assistantMarkDebounceTimer =
                                null;
                        }


                        for (
                            const [
                                ,
                                timer
                            ]
                            of hangupFallbackTimers
                        ) {

                            clearTimeout(
                                timer
                            );
                        }


                        hangupFallbackTimers.clear();

                        hangupByPurpose.clear();


                        if (
                            waitForFinalTranscript &&

                            openAiWs.readyState ===
                                WebSocket.OPEN
                        ) {

                            await new Promise(
                                (
                                    resolve
                                ) =>
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


                // --------------------------------------
                // TWILIO API
                // --------------------------------------

                const twilioAuthHeader =
                    () => {

                        return (
                            'Basic ' +

                            Buffer
                                .from(
                                    `${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`
                                )
                                .toString(
                                    'base64'
                                )
                        );
                    };


                const updateTwilioCall =
                    async (
                        callSid,
                        formValues
                    ) => {

                        const body =
                            new URLSearchParams(
                                formValues
                            );

                        const response =
                            await fetch(

                                `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(TWILIO_ACCOUNT_SID)}/Calls/${encodeURIComponent(callSid)}.json`,

                                {
                                    method:
                                        'POST',

                                    headers: {

                                        Authorization:
                                            twilioAuthHeader(),

                                        'Content-Type':
                                            'application/x-www-form-urlencoded'
                                    },

                                    body
                                }
                            );


                        if (
                            !response.ok
                        ) {

                            const responseText =
                                await response.text();

                            throw new Error(
                                `Twilio ${response.status}: ${responseText}`
                            );
                        }


                        return response;
                    };


                // --------------------------------------
                // END CALL
                // --------------------------------------

                const endTwilioCall =
                    async (
                        reason =
                            'conversation-ended'
                    ) => {

                        if (
                            twilioHangupRequested ||
                            !currentCallSid ||
                            transferInProgress
                        ) {

                            return;
                        }

                        twilioHangupRequested =
                            true;

                        clearAiTimers();


                        try {

                            await flushTranscript();

                            await transcriptSaveChain;


                            await updateTwilioCall(
                                currentCallSid,
                                {
                                    Status:
                                        'completed'
                                }
                            );


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
                                500
                            );


                        } catch (
                            error
                        ) {

                            twilioHangupRequested =
                                false;

                            console.error(
                                'Twilio end call error:',
                                error
                            );
                        }
                    };


                const armHangupOnPurpose =
                    (
                        purpose,
                        reason,
                        fallbackMs =
                            6000
                    ) => {

                        clearHangupPurpose(
                            purpose
                        );

                        hangupByPurpose.set(
                            purpose,
                            reason
                        );


                        const timer =
                            setTimeout(
                                () => {

                                    hangupFallbackTimers.delete(
                                        purpose
                                    );

                                    hangupByPurpose.delete(
                                        purpose
                                    );

                                    void endTwilioCall(
                                        `${reason}-fallback`
                                    );

                                },
                                fallbackMs
                            );


                        hangupFallbackTimers.set(
                            purpose,
                            timer
                        );
                    };


                const waitForPlaybackPurpose =
                    (
                        purpose,
                        timeoutMs =
                            3500
                    ) => {

                        return new Promise(
                            (
                                resolve
                            ) => {

                                const timer =
                                    setTimeout(
                                        () => {

                                            playbackWaiters.delete(
                                                purpose
                                            );

                                            resolve(
                                                false
                                            );

                                        },
                                        timeoutMs
                                    );


                                playbackWaiters.set(
                                    purpose,
                                    {
                                        resolve,
                                        timer
                                    }
                                );
                            }
                        );
                    };


                const speakCommentary =
                    (
                        content,
                        purpose =
                            'general'
                    ) => {

                        nextAssistantPurpose =
                            purpose;

                        send({

                            type:
                                'session.commentary.append',

                            delegation_id:
                                null,

                            content
                        });
                    };


                const clearTwilioAudio =
                    () => {

                        if (
                            streamSid &&

                            connection.readyState ===
                                WebSocket.OPEN
                        ) {

                            sendToTwilio({

                                event:
                                    'clear',

                                streamSid
                            });
                        }
                    };


                // --------------------------------------
                // TWILIO PLAYBACK MARK
                // --------------------------------------

                const onAssistantPlaybackFullyDrained =
                    () => {

                        assistantPlaybackBusy =
                            false;

                        hasAssistantPlaybackEnded =
                            true;

                        lastAssistantPlaybackEndedAt =
                            Date.now();


                        const purposes = [
                            ...new Set(
                                completedPlaybackPurposes
                            )
                        ];


                        completedPlaybackPurposes =
                            [];


                        for (
                            const purpose
                            of purposes
                        ) {


                            const waiter =
                                playbackWaiters.get(
                                    purpose
                                );


                            if (
                                waiter
                            ) {

                                clearTimeout(
                                    waiter.timer
                                );

                                playbackWaiters.delete(
                                    purpose
                                );

                                waiter.resolve(
                                    true
                                );
                            }


                            if (
                                activeSilenceCycle !==
                                    null &&

                                purpose ===
                                    `silence-check:${activeSilenceCycle}` &&

                                silencePhase ===
                                    'waiting_playback'
                            ) {

                                silencePhase =
                                    'waiting_response';

                                silencePromptFinishedAt =
                                    Date.now();

                                console.log(
                                    'Silence check finished playing; waiting 10 seconds'
                                );
                            }


                            const hangupReason =
                                hangupByPurpose.get(
                                    purpose
                                );


                            if (
                                hangupReason
                            ) {

                                clearHangupPurpose(
                                    purpose
                                );

                                void endTwilioCall(
                                    hangupReason
                                );

                                return;
                            }
                        }
                    };


                const sendAssistantPlaybackMark =
                    () => {

                        assistantMarkDebounceTimer =
                            null;


                        if (
                            !streamSid ||

                            connection.readyState !==
                                WebSocket.OPEN
                        ) {

                            return;
                        }


                        const markName =
                            `mia_${Date.now()}_${++assistantMarkCounter}`;


                        const purpose =
                            currentAssistantPurpose ||
                            'general';


                        currentAssistantPurpose =
                            null;


                        pendingPlaybackMarks.set(
                            markName,
                            purpose
                        );


                        sendToTwilio({

                            event:
                                'mark',

                            streamSid,

                            mark: {
                                name:
                                    markName
                            }
                        });
                    };


                const noteAssistantAudioChunk =
                    (
                        payload
                    ) => {

                        if (
                            nextAssistantPurpose
                        ) {

                            currentAssistantPurpose =
                                nextAssistantPurpose;

                            nextAssistantPurpose =
                                null;

                        } else if (
                            !currentAssistantPurpose
                        ) {

                            currentAssistantPurpose =
                                'general';
                        }


                        assistantPlaybackBusy =
                            true;


                        sendToTwilio({

                            event:
                                'media',

                            streamSid,

                            media: {

                                payload
                            }
                        });


                        if (
                            assistantMarkDebounceTimer
                        ) {

                            clearTimeout(
                                assistantMarkDebounceTimer
                            );
                        }


                        assistantMarkDebounceTimer =
                            setTimeout(

                                sendAssistantPlaybackMark,

                                AUDIO_BURST_IDLE_MS
                            );
                    };


                // --------------------------------------
                // GOODBYE
                // --------------------------------------

                const requestGracefulEnd =
                    () => {

                        if (
                            customerRequestedEnd ||
                            transferInProgress ||
                            twilioHangupRequested
                        ) {

                            return;
                        }


                        customerRequestedEnd =
                            true;


                        console.log(
                            'Customer clearly ended the conversation'
                        );


                        const purpose =
                            `customer-goodbye:${Date.now()}`;


                        nextAssistantPurpose =
                            purpose;


                        armHangupOnPurpose(
                            purpose,
                            'customer-goodbye',
                            6000
                        );


                        send({

                            type:
                                'session.instructions.append',

                            delegation_id:
                                null,

                            content:
                                'The caller has clearly ended the conversation. Give exactly one brief natural closing sentence now. Do not ask another question and do not continue the conversation afterward.'
                        });
                    };


                // --------------------------------------
                // TRANSFER
                // --------------------------------------

                const requestOwnerTransfer =
                    async (
                        reason =
                            'customer-request'
                    ) => {

                        if (
                            transferInProgress ||
                            twilioHangupRequested ||
                            shuttingDown ||
                            !currentCallSid ||
                            !currentPublicBaseUrl
                        ) {

                            return;
                        }


                        transferInProgress =
                            true;


                        clearAiTimers();


                        console.log(
                            `Human transfer requested: ${reason}`
                        );


                        const transferPurpose =
                            `transfer-announcement:${Date.now()}`;


                        speakCommentary(
                            "Sure, I'll try to connect you now.",
                            transferPurpose
                        );


                        await waitForPlaybackPurpose(
                            transferPurpose,
                            3500
                        );


                        try {

                            await flushTranscript();

                            await transcriptSaveChain;


                            const transferResultUrl =
                                `${currentPublicBaseUrl}/transfer-result` +
                                `?started=${encodeURIComponent(callStartMs)}` +
                                `&from=${encodeURIComponent(currentCallerPhone || '')}`;


                            const ownerScreenUrl =
                                `${currentPublicBaseUrl}/owner-screen?parent=${encodeURIComponent(currentCallSid)}`;


                            const transferTwiml =
`<Response>

    <Dial
        answerOnBridge="true"
        timeout="20"
        action="${escapeXml(transferResultUrl)}"
        method="POST">

        <Number
            url="${escapeXml(ownerScreenUrl)}"
            method="POST">${escapeXml(OWNER_PHONE_NUMBER)}</Number>

    </Dial>

</Response>`;


                            await updateTwilioCall(
                                currentCallSid,
                                {
                                    Twiml:
                                        transferTwiml
                                }
                            );


                            console.log(
                                `Transfer started for ${currentCallSid}`
                            );


                        } catch (
                            error
                        ) {

                            transferInProgress =
                                false;


                            console.error(
                                'Human transfer error:',
                                error
                            );


                            speakCommentary(

                                "I'm sorry, I couldn't start the transfer. I'll make a note that you asked for a callback.",

                                'transfer-error'
                            );


                            scheduleAiTimers();
                        }
                    };


                // --------------------------------------
                // SILENCE
                // --------------------------------------

                const cancelSilenceCycle =
                    (
                        interruptAudio =
                            false
                    ) => {

                        const oldCycle =
                            activeSilenceCycle;


                        if (
                            oldCycle !==
                            null
                        ) {

                            clearHangupPurpose(
                                `silence-goodbye:${oldCycle}`
                            );
                        }


                        activeSilenceCycle =
                            null;

                        silencePhase =
                            'none';

                        silencePromptRequestedAt =
                            0;

                        silencePromptFinishedAt =
                            0;


                        if (
                            interruptAudio
                        ) {

                            clearTwilioAudio();
                        }
                    };


                const startSilenceCheck =
                    () => {

                        silenceCycle +=
                            1;


                        activeSilenceCycle =
                            silenceCycle;


                        silencePhase =
                            'waiting_playback';


                        silencePromptRequestedAt =
                            Date.now();


                        console.log(
                            '25 seconds of customer silence'
                        );


                        speakCommentary(

                            'Are you still there?',

                            `silence-check:${activeSilenceCycle}`
                        );
                    };


                const startSilenceHangup =
                    () => {

                        if (
                            activeSilenceCycle ===
                            null
                        ) {

                            return;
                        }


                        const purpose =
                            `silence-goodbye:${activeSilenceCycle}`;


                        silencePhase =
                            'ending';


                        console.log(
                            'Customer silent for another 10 seconds'
                        );


                        armHangupOnPurpose(

                            purpose,

                            'silence-timeout',

                            6000
                        );


                        speakCommentary(

                            "I'll go ahead and disconnect the call. Have a good day.",

                            purpose
                        );
                    };


                // --------------------------------------
                // TIMERS
                // --------------------------------------

                function scheduleAiTimers() {

                    clearAiTimers();


                    const elapsed =
                        Math.max(
                            0,
                            Date.now() -
                                callStartMs
                        );


                    const warningDelay =
                        MAX_CALL_WARNING_MS -
                        elapsed;


                    const closingDelay =
                        MAX_CALL_CLOSING_MS -
                        elapsed;


                    const hardDelay =
                        MAX_AI_CALL_MS -
                        elapsed;


                    if (
                        warningDelay >
                        0
                    ) {

                        maxWarningTimer =
                            setTimeout(
                                () => {

                                    if (
                                        transferInProgress ||
                                        shuttingDown ||
                                        twilioHangupRequested ||
                                        customerRequestedEnd
                                    ) {

                                        return;
                                    }


                                    speakCommentary(

                                        "We're almost at the end of the call. Is there anything else you need?",

                                        'five-minute-warning'
                                    );

                                },
                                warningDelay
                            );
                    }


                    if (
                        closingDelay >
                        0
                    ) {

                        maxClosingTimer =
                            setTimeout(
                                () => {

                                    if (
                                        transferInProgress ||
                                        shuttingDown ||
                                        twilioHangupRequested
                                    ) {

                                        return;
                                    }


                                    const purpose =
                                        `five-minute-goodbye:${Date.now()}`;


                                    armHangupOnPurpose(

                                        purpose,

                                        'five-minute-limit',

                                        5000
                                    );


                                    speakCommentary(

                                        "I'll go ahead and disconnect the call now. Have a good day.",

                                        purpose
                                    );

                                },
                                closingDelay
                            );
                    }


                    if (
                        hardDelay <=
                        0
                    ) {

                        void endTwilioCall(
                            'five-minute-hard-limit'
                        );

                        return;
                    }


                    maxHardTimer =
                        setTimeout(
                            () => {

                                void endTwilioCall(
                                    'five-minute-hard-limit'
                                );

                            },
                            hardDelay
                        );


                    silenceMonitor =
                        setInterval(
                            () => {

                                if (
                                    !sessionReady ||
                                    transferInProgress ||
                                    shuttingDown ||
                                    twilioHangupRequested ||
                                    customerRequestedEnd
                                ) {

                                    return;
                                }


                                const now =
                                    Date.now();


                                if (
                                    silencePhase ===
                                    'waiting_playback'
                                ) {

                                    if (
                                        now -
                                            silencePromptRequestedAt >=
                                        5000
                                    ) {

                                        silencePhase =
                                            'waiting_response';

                                        silencePromptFinishedAt =
                                            now;


                                        console.log(
                                            'Silence prompt playback mark timeout; starting 10-second wait'
                                        );
                                    }

                                    return;
                                }


                                if (
                                    silencePhase ===
                                    'waiting_response'
                                ) {

                                    if (
                                        now -
                                            silencePromptFinishedAt >=
                                        SILENCE_HANGUP_MS
                                    ) {

                                        startSilenceHangup();
                                    }

                                    return;
                                }


                                if (
                                    silencePhase ===
                                    'ending'
                                ) {

                                    return;
                                }


                                if (
                                    !hasAssistantPlaybackEnded ||
                                    assistantPlaybackBusy
                                ) {

                                    return;
                                }


                                const activityReference =
                                    Math.max(

                                        lastCustomerSpeechAt,

                                        lastAssistantPlaybackEndedAt
                                    );


                                if (
                                    now -
                                        activityReference >=
                                    SILENCE_CHECK_MS
                                ) {

                                    startSilenceCheck();
                                }

                            },
                            500
                        );
                }


                // --------------------------------------
                // START GPT LIVE
                // --------------------------------------

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


                // --------------------------------------
                // OPENAI OPEN
                // --------------------------------------

                openAiWs.on(
                    'open',

                    () => {

                        console.log(
                            'Connected to GPT-Live-1'
                        );


                        startSession();
                    }
                );


                // --------------------------------------
                // OPENAI EVENTS
                // --------------------------------------

                openAiWs.on(
                    'message',

                    (
                        data
                    ) => {

                        try {

                            const event =
                                JSON.parse(
                                    data
                                );


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


                                const firstLine =
                                    resumeReason ===
                                        'transfer-unavailable'

                                        ? "Thanks for waiting. I couldn't connect you right now, but I can keep helping you."

                                        : OPENING;


                                send({

                                    type:
                                        'session.instructions.append',

                                    delegation_id:
                                        null,

                                    content:
                                        `Your first spoken line on this call is exactly: "${firstLine}"`
                                });


                                speakCommentary(

                                    firstLine,

                                    'greeting'
                                );


                                scheduleAiTimers();


                            } else if (
                                event.type ===
                                    'session.output_audio.delta' &&

                                streamSid &&

                                connection.readyState ===
                                    WebSocket.OPEN
                            ) {

                                noteAssistantAudioChunk(
                                    event.delta
                                );


                            } else if (
                                event.type ===
                                    'session.input_transcript.delta'
                            ) {

                                console.log(
                                    'Customer:',
                                    event.delta
                                );


                                lastCustomerSpeechAt =
                                    Date.now();


                                if (
                                    silencePhase !==
                                    'none'
                                ) {

                                    const shouldInterrupt =
                                        silencePhase ===
                                            'waiting_playback' ||

                                        silencePhase ===
                                            'ending';


                                    cancelSilenceCycle(
                                        shouldInterrupt
                                    );


                                    console.log(
                                        'Customer returned after silence check'
                                    );
                                }


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


                                customerTextWindow =
                                    (
                                        `${customerTextWindow} ${event.delta}`
                                    ).slice(
                                        -500
                                    );


                                if (
                                    !customerRequestedEnd &&

                                    containsExplicitGoodbye(
                                        customerTextWindow
                                    )
                                ) {

                                    customerTextWindow =
                                        '';


                                    requestGracefulEnd();


                                    return;
                                }


                                if (
                                    !transferInProgress &&

                                    containsExplicitHumanRequest(
                                        customerTextWindow
                                    )
                                ) {

                                    customerTextWindow =
                                        '';


                                    void requestOwnerTransfer(
                                        'explicit-customer-request'
                                    );


                                    return;
                                }


                                if (
                                    !transferInProgress &&

                                    assistantOfferedTransferUntil >
                                        Date.now()
                                ) {

                                    if (
                                        containsAffirmative(
                                            customerTextWindow
                                        )
                                    ) {

                                        assistantOfferedTransferUntil =
                                            0;


                                        customerTextWindow =
                                            '';


                                        void requestOwnerTransfer(
                                            'accepted-mia-offer'
                                        );


                                        return;
                                    }


                                    if (
                                        containsNegative(
                                            customerTextWindow
                                        )
                                    ) {

                                        assistantOfferedTransferUntil =
                                            0;


                                        customerTextWindow =
                                            '';
                                    }
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


                                assistantTextWindow =
                                    (
                                        `${assistantTextWindow} ${event.delta}`
                                    ).slice(
                                        -500
                                    );


                                const assistantNormalized =
                                    normalizeForMatch(
                                        assistantTextWindow
                                    );


                                if (
                                    /would you like me to connect you (with|to) (someone|a person|the owner|a technician|a manager)/i.test(
                                        assistantNormalized
                                    ) ||

                                    /(хотите|хочешь).*(соединить|перевести).*(человек|менеджер|владелец|техник|мастер)/i.test(
                                        assistantNormalized
                                    )
                                ) {

                                    assistantOfferedTransferUntil =
                                        Date.now() +
                                        20_000;


                                    customerTextWindow =
                                        '';
                                }


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


                                if (
                                    delegationId
                                ) {

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


                        } catch (
                            error
                        ) {

                            console.error(
                                'OpenAI message error:',
                                error
                            );
                        }
                    }
                );


                // --------------------------------------
                // TWILIO EVENTS
                // --------------------------------------

                connection.on(
                    'message',

                    async (
                        message
                    ) => {

                        try {

                            const data =
                                JSON.parse(
                                    message
                                );


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
                                    'mark'
                            ) {

                                const markName =
                                    data.mark?.name;


                                const purpose =
                                    markName

                                        ? pendingPlaybackMarks.get(
                                            markName
                                        )

                                        : null;


                                if (
                                    markName &&
                                    purpose
                                ) {

                                    pendingPlaybackMarks.delete(
                                        markName
                                    );


                                    completedPlaybackPurposes.push(
                                        purpose
                                    );
                                }


                                if (
                                    pendingPlaybackMarks.size ===
                                        0 &&

                                    !assistantMarkDebounceTimer
                                ) {

                                    onAssistantPlaybackFullyDrained();
                                }


                            } else if (
                                data.event ===
                                    'start'
                            ) {

                                streamSid =
                                    data.start.streamSid;


                                currentCallSid =
                                    data.start.callSid ||
                                    null;


                                const custom =
                                    data.start
                                        .customParameters ||
                                    {};


                                currentCallerPhone =
                                    custom.From ||
                                    null;


                                currentPublicBaseUrl =
                                    custom.BaseUrl ||
                                    null;


                                const parsedStart =
                                    Number(
                                        custom.CallStartMs
                                    );


                                callStartMs =
                                    Number.isFinite(
                                        parsedStart
                                    ) &&
                                    parsedStart >
                                        0

                                        ? parsedStart

                                        : Date.now();


                                resumeReason =
                                    custom.ResumeReason ||
                                    null;


                                lastCustomerSpeechAt =
                                    Date.now();


                                console.log(
                                    'Incoming Twilio stream:',
                                    streamSid
                                );


                                console.log(
                                    'Twilio Call SID:',
                                    currentCallSid
                                );


                                if (
                                    resumeReason
                                ) {

                                    console.log(
                                        'Call resumed:',
                                        resumeReason
                                    );
                                }


                                startSession();


                                try {

                                    if (
                                        !currentCallerPhone
                                    ) {

                                        console.error(
                                            'Caller phone number was not received'
                                        );


                                    } else {

                                        const customer =
                                            await findOrCreateCustomer(
                                                currentCallerPhone
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
                                            `CRM job ready: #${job.job_number}`
                                        );


                                        await flushTranscript();
                                    }


                                } catch (
                                    error
                                ) {

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


                        } catch (
                            error
                        ) {

                            console.error(
                                'Twilio message error:',
                                error
                            );
                        }
                    }
                );


                // --------------------------------------
                // SOCKET CLOSE
                // --------------------------------------

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

                    (
                        error
                    ) => {

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
                        closeCode,
                        reason
                    ) => {

                        console.log(

                            'Disconnected from GPT-Live-1',

                            closeCode,

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

                    (
                        error
                    ) => {

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


// ======================================================
// START SERVER
// ======================================================

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


    } catch (
        error
    ) {

        console.error(
            'Startup error:',
            error
        );


        process.exit(
            1
        );
    }
}


start();
