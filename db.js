import pg from 'pg';

const { Pool } = pg;

if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is missing');
}

export const pool = new Pool({
    connectionString: process.env.DATABASE_URL
});


export async function initDatabase() {
    const client = await pool.connect();

    try {
        await client.query('BEGIN');

        // =========================================================
        // CUSTOMERS
        // =========================================================

        await client.query(`
            CREATE TABLE IF NOT EXISTS customers (
                id BIGSERIAL PRIMARY KEY,

                phone_e164 TEXT UNIQUE NOT NULL,

                name TEXT,
                email TEXT,

                default_address TEXT,
                default_zip TEXT,

                preferred_language TEXT DEFAULT 'en',

                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);


        // =========================================================
        // JOBS
        // =========================================================

        await client.query(`
            CREATE TABLE IF NOT EXISTS jobs (
                job_number BIGINT
                    GENERATED ALWAYS AS IDENTITY (START WITH 1001)
                    PRIMARY KEY,

                customer_id BIGINT NOT NULL
                    REFERENCES customers(id),

                source TEXT NOT NULL DEFAULT 'incoming_call',

                category TEXT,

                problem_summary TEXT,
                equipment_type TEXT,

                urgency TEXT NOT NULL DEFAULT 'normal'
                    CHECK (
                        urgency IN (
                            'normal',
                            'emergency'
                        )
                    ),

                status TEXT NOT NULL DEFAULT 'new'
                    CHECK (
                        status IN (
                            'new',
                            'waiting_for_customer',
                            'waiting_for_approval',
                            'scheduled',
                            'technician_en_route',
                            'in_progress',
                            'completed',
                            'cancelled'
                        )
                    ),

                service_address TEXT,
                service_zip TEXT,

                requested_start TIMESTAMPTZ,
                confirmed_start TIMESTAMPTZ,

                service_fee_cents INTEGER
                    NOT NULL
                    DEFAULT 9500,

                diagnosis TEXT,
                work_performed TEXT,

                total_amount_cents INTEGER,

                amount_paid_cents INTEGER
                    NOT NULL
                    DEFAULT 0,

                payment_status TEXT
                    NOT NULL
                    DEFAULT 'unpaid'
                    CHECK (
                        payment_status IN (
                            'unpaid',
                            'partial',
                            'paid'
                        )
                    ),

                payment_method TEXT,

                twilio_call_sid TEXT,

                created_at TIMESTAMPTZ
                    NOT NULL
                    DEFAULT NOW(),

                updated_at TIMESTAMPTZ
                    NOT NULL
                    DEFAULT NOW(),

                closed_at TIMESTAMPTZ
            );
        `);


        // =========================================================
        // APPOINTMENTS
        // =========================================================

        await client.query(`
            CREATE TABLE IF NOT EXISTS appointments (
                id BIGSERIAL PRIMARY KEY,

                job_number BIGINT NOT NULL
                    REFERENCES jobs(job_number)
                    ON DELETE CASCADE,

                requested_start TIMESTAMPTZ,
                confirmed_start TIMESTAMPTZ,

                status TEXT NOT NULL DEFAULT 'requested'
                    CHECK (
                        status IN (
                            'requested',
                            'pending_approval',
                            'confirmed',
                            'completed',
                            'cancelled'
                        )
                    ),

                notes TEXT,

                created_at TIMESTAMPTZ
                    NOT NULL
                    DEFAULT NOW(),

                updated_at TIMESTAMPTZ
                    NOT NULL
                    DEFAULT NOW()
            );
        `);


        // =========================================================
        // PAYMENTS
        // =========================================================

        await client.query(`
            CREATE TABLE IF NOT EXISTS payments (
                id BIGSERIAL PRIMARY KEY,

                job_number BIGINT NOT NULL
                    REFERENCES jobs(job_number)
                    ON DELETE CASCADE,

                amount_cents INTEGER NOT NULL,

                method TEXT,

                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (
                        status IN (
                            'pending',
                            'paid',
                            'refunded',
                            'failed'
                        )
                    ),

                notes TEXT,

                paid_at TIMESTAMPTZ,

                created_at TIMESTAMPTZ
                    NOT NULL
                    DEFAULT NOW()
            );
        `);


        // =========================================================
        // JOB EVENTS
        // =========================================================

        await client.query(`
            CREATE TABLE IF NOT EXISTS job_events (
                id BIGSERIAL PRIMARY KEY,

                job_number BIGINT NOT NULL
                    REFERENCES jobs(job_number)
                    ON DELETE CASCADE,

                event_type TEXT NOT NULL,

                details JSONB
                    NOT NULL
                    DEFAULT '{}'::jsonb,

                created_at TIMESTAMPTZ
                    NOT NULL
                    DEFAULT NOW()
            );
        `);


        // =========================================================
        // TRANSCRIPT
        // Raw transcript fragments from Customer + Mia
        // =========================================================

        await client.query(`
            CREATE TABLE IF NOT EXISTS transcript_chunks (
                id BIGSERIAL PRIMARY KEY,

                job_number BIGINT NOT NULL
                    REFERENCES jobs(job_number)
                    ON DELETE CASCADE,

                speaker TEXT NOT NULL
                    CHECK (
                        speaker IN (
                            'customer',
                            'assistant'
                        )
                    ),

                text TEXT NOT NULL,

                start_ms INTEGER,
                end_ms INTEGER,

                openai_event_id TEXT,
                openai_session_id TEXT,

                created_at TIMESTAMPTZ
                    NOT NULL
                    DEFAULT NOW()
            );
        `);


        // =========================================================
        // INDEXES
        // =========================================================

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_jobs_customer
            ON jobs(customer_id);
        `);

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_jobs_status
            ON jobs(status);
        `);

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_jobs_confirmed_start
            ON jobs(confirmed_start);
        `);

        await client.query(`
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_jobs_twilio_call_sid_unique
            ON jobs(twilio_call_sid)
            WHERE twilio_call_sid IS NOT NULL;
        `);

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_job_events_job
            ON job_events(job_number);
        `);

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_transcript_chunks_job
            ON transcript_chunks(job_number, id);
        `);


        await client.query('COMMIT');

        console.log(
            'Database initialized successfully'
        );

    } catch (error) {
        await client.query('ROLLBACK');

        console.error(
            'Database initialization failed:',
            error
        );

        throw error;

    } finally {
        client.release();
    }
}


// =========================================================
// FIND OR CREATE CUSTOMER
// Customer is identified by phone number
// =========================================================

export async function findOrCreateCustomer(
    phoneE164
) {
    if (!phoneE164) {
        throw new Error(
            'Customer phone number is missing'
        );
    }

    const result = await pool.query(
        `
        INSERT INTO customers (
            phone_e164,
            updated_at
        )
        VALUES (
            $1,
            NOW()
        )

        ON CONFLICT (phone_e164)
        DO UPDATE SET
            updated_at = NOW()

        RETURNING *;
        `,
        [
            phoneE164
        ]
    );

    return result.rows[0];
}


// =========================================================
// CREATE JOB FOR PHONE CALL
// Protects against duplicate jobs if WebSocket reconnects
// =========================================================

export async function createJobForCall({
    customerId,
    callSid
}) {

    if (callSid) {
        const existing =
            await pool.query(
                `
                SELECT *
                FROM jobs
                WHERE twilio_call_sid = $1
                LIMIT 1;
                `,
                [
                    callSid
                ]
            );

        if (
            existing.rows.length > 0
        ) {
            return existing.rows[0];
        }
    }


    let job;

    try {
        const result =
            await pool.query(
                `
                INSERT INTO jobs (
                    customer_id,
                    source,
                    status,
                    service_fee_cents,
                    twilio_call_sid
                )
                VALUES (
                    $1,
                    'incoming_call',
                    'new',
                    9500,
                    $2
                )
                RETURNING *;
                `,
                [
                    customerId,
                    callSid || null
                ]
            );

        job = result.rows[0];

    } catch (error) {

        // Another connection may have created
        // the same call at the same time.

        if (
            error.code === '23505' &&
            callSid
        ) {
            const existing =
                await pool.query(
                    `
                    SELECT *
                    FROM jobs
                    WHERE twilio_call_sid = $1
                    LIMIT 1;
                    `,
                    [
                        callSid
                    ]
                );

            if (
                existing.rows.length > 0
            ) {
                return existing.rows[0];
            }
        }

        throw error;
    }


    await pool.query(
        `
        INSERT INTO job_events (
            job_number,
            event_type,
            details
        )
        VALUES (
            $1,
            'incoming_call_started',
            $2::jsonb
        );
        `,
        [
            job.job_number,

            JSON.stringify({
                callSid:
                    callSid || null
            })
        ]
    );


    return job;
}


// =========================================================
// SAVE TRANSCRIPT CHUNKS
//
// GPT-Live sends transcript as small fragments.
// We save all fragments and later reconstruct
// the complete conversation in order.
// =========================================================

export async function saveTranscriptChunks(
    jobNumber,
    chunks
) {

    if (
        !jobNumber ||
        !Array.isArray(chunks) ||
        chunks.length === 0
    ) {
        return;
    }


    const cleanChunks =
        chunks.filter(
            (chunk) =>
                chunk &&
                typeof chunk.text === 'string' &&
                chunk.text.length > 0 &&
                (
                    chunk.speaker ===
                        'customer' ||
                    chunk.speaker ===
                        'assistant'
                )
        );


    if (
        cleanChunks.length === 0
    ) {
        return;
    }


    const values = [];
    const placeholders = [];


    cleanChunks.forEach(
        (chunk, index) => {

            const base =
                index * 7;

            placeholders.push(
                `(
                    $${base + 1},
                    $${base + 2},
                    $${base + 3},
                    $${base + 4},
                    $${base + 5},
                    $${base + 6},
                    $${base + 7}
                )`
            );


            values.push(
                jobNumber,

                chunk.speaker,

                chunk.text,

                chunk.startMs ?? null,

                chunk.endMs ?? null,

                chunk.eventId ?? null,

                chunk.sessionId ?? null
            );
        }
    );


    await pool.query(
        `
        INSERT INTO transcript_chunks (
            job_number,
            speaker,
            text,
            start_ms,
            end_ms,
            openai_event_id,
            openai_session_id
        )
        VALUES
            ${placeholders.join(',\n')}
        ;
        `,
        values
    );
}
