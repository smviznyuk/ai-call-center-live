import {
    initDatabase,
    findOrCreateCustomer,
    createJobForCall
} from './db.js';
import Fastify from 'fastify';
import WebSocket from 'ws';
import dotenv from 'dotenv';
import fastifyFormBody from '@fastify/formbody';
import fastifyWs from '@fastify/websocket';

dotenv.config();

const { OPENAI_API_KEY } = process.env;

if (!OPENAI_API_KEY) {
    console.error('Missing OpenAI API key.');
    process.exit(1);
}

const MODEL = 'gpt-live-1';
const VOICE = 'marin';
const USER_AGENT = 'sv-ai-call-center/1.0';
const PORT = process.env.PORT || 5050;

const OPENING = "Hi, thanks for calling. How can I help you?";

const VOICE_PROMPT = `
You are the phone receptionist for an HVAC and plumbing service company
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

IDENTITY:
- Do not unnecessarily announce that you are an AI.
- Never falsely claim to be a human.
- If directly asked whether you are AI, say briefly:
  "I'm the company's virtual assistant."

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
If the customer asks about the service call price earlier, answer immediately
and briefly.

SCHEDULING:
Do not guarantee an appointment time unless availability has been confirmed.
For now, collect the customer's preferred time and say it will be confirmed.

SAFETY:
If there is a gas smell, fire, smoke, carbon monoxide alarm, or immediate danger,
tell the customer to leave the area and contact 911 or the appropriate utility.

ENDING:
Before ending the call, make sure you have the customer's name,
service issue, address, and preferred time.
Do not give a long recap.
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
            let sessionRequested = false;
            let sessionReady = false;

            const openAiWs = new WebSocket(
                'wss://api.openai.com/v1/live/sessions',
                {
                    headers: {
                        Authorization: `Bearer ${OPENAI_API_KEY}`,
                        'User-Agent': USER_AGENT
                    }
                }
            );

            const send = (event) => {
                if (openAiWs.readyState === WebSocket.OPEN) {
                    openAiWs.send(JSON.stringify(event));
                }
            };

            const close = () => {
                sessionReady = false;

                if (connection.readyState === WebSocket.OPEN) {
                    connection.close();
                }

                if (openAiWs.readyState === WebSocket.OPEN) {
                    openAiWs.close();
                }
            };

            const startSession = () => {

                if (
                    sessionRequested ||
                    !streamSid ||
                    openAiWs.readyState !== WebSocket.OPEN
                ) {
                    return;
                }

                sessionRequested = true;

                send({
                    type: 'session.start',
                    session: {
                        model: MODEL,
                        instructions: VOICE_PROMPT,
                        delegation: {
                            type: 'client'
                        },
                        audio: {
                            format: {
                                type: 'audio/pcmu',
                                rate: 8000
                            },
                            output: {
                                voice: VOICE
                            }
                        }
                    }
                });
            };

            openAiWs.on('open', () => {
                console.log('Connected to GPT-Live-1');
                startSession();
            });

            openAiWs.on('message', (data) => {

                try {

                    const event = JSON.parse(data);

                    if (event.type === 'session.started') {

                        sessionReady = true;

                        console.log(
                            'GPT-Live-1 session:',
                            event.session?.id
                        );

                        send({
                            type: 'session.instructions.append',
                            delegation_id: null,
                            content:
                                `Your first spoken line on this call is exactly: "${OPENING}"`
                        });

                        send({
                            type: 'session.commentary.append',
                            delegation_id: null,
                            content: OPENING
                        });

                    } else if (
                        event.type === 'session.output_audio.delta' &&
                        streamSid &&
                        connection.readyState === WebSocket.OPEN
                    ) {

                        connection.send(
                            JSON.stringify({
                                event: 'media',
                                streamSid,
                                media: {
                                    payload: event.delta
                                }
                            })
                        );

                    } else if (
                        event.type === 'session.output_transcript.delta'
                    ) {

                        console.log('Assistant:', event.delta);

                    } else if (event.type === 'error') {

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
            });

            connection.on('message', async (message) => {

                try {

                    const data = JSON.parse(message);

                    if (
                        data.event === 'media' &&
                        sessionReady &&
                        openAiWs.readyState === WebSocket.OPEN
                    ) {

                        send({
                            type: 'session.input_audio.append',
                            audio: data.media.payload
                        });

                   } else if (data.event === 'start') {

                        streamSid = data.start.streamSid;

                        const callSid = data.start.callSid;

                        const callerPhone =
                            data.start.customParameters?.From || null;

                        console.log(
                            'Incoming Twilio stream:',
                            streamSid
                        );

                        // Mia starts talking immediately.
                        // Database work happens without delaying the call.
                        startSession();

                        try {
                            if (!callerPhone) {
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
                                    customerId: customer.id,
                                    callSid
                                });

                            console.log(
                                `CRM job created: #${job.job_number}`
                            );
                    }

                } catch (error) {
                    console.error(
                        'CRM create job error:',
                        error
                    );
                }

                    } else if (data.event === 'stop') {

                        close();
                    }

                } catch (error) {

                    console.error(
                        'Twilio message error:',
                        error
                    );
                }
            });

            connection.on('close', () => {
                close();
                console.log('Caller disconnected');
            });

            connection.on('error', close);

            openAiWs.on('close', (code, reason) => {
                close();

                console.log(
                    'Disconnected from GPT-Live-1',
                    code,
                    reason.toString()
                );
            });

            openAiWs.on('error', (error) => {
                console.error(
                    'OpenAI WebSocket error:',
                    error
                );

                close();
            });
        }
    );
});

async function start() {
    try {
        await initDatabase();

        await fastify.listen({
            port: PORT,
            host: '0.0.0.0'
        });

        console.log(`AI Call Center listening on port ${PORT}`);

    } catch (error) {
        console.error('Startup error:', error);
        process.exit(1);
    }
}

start();
