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
                    CHECK (urgency IN ('normal', 'emergency')),

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

                service_fee_cents INTEGER NOT NULL DEFAULT 9500,

                diagnosis TEXT,
                work_performed TEXT,

                total_amount_cents INTEGER,
                amount_paid_cents INTEGER NOT NULL DEFAULT 0,

                payment_status TEXT NOT NULL DEFAULT 'unpaid'
                    CHECK (
                        payment_status IN (
                            'unpaid',
                            'partial',
                            'paid'
                        )
                    ),

                payment_method TEXT,

                twilio_call_sid TEXT,

                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                closed_at TIMESTAMPTZ
            );
        `);

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

                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

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

                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        await client.query(`
            CREATE TABLE IF NOT EXISTS job_events (
                id BIGSERIAL PRIMARY KEY,

                job_number BIGINT NOT NULL
                    REFERENCES jobs(job_number)
                    ON DELETE CASCADE,

                event_type TEXT NOT NULL,
                details JSONB NOT NULL DEFAULT '{}'::jsonb,

                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

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
            CREATE INDEX IF NOT EXISTS idx_job_events_job
            ON job_events(job_number);
        `);

        await client.query('COMMIT');

        console.log('Database initialized successfully');

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Database initialization failed:', error);
        throw error;

    } finally {
        client.release();
    }
}
