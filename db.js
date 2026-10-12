import pg from 'pg';

const { Pool } = pg;

if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is missing');

export const pool = new Pool({ connectionString: process.env.DATABASE_URL });

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
        job_number BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1001) PRIMARY KEY,
        customer_id BIGINT NOT NULL REFERENCES customers(id),
        source TEXT NOT NULL DEFAULT 'incoming_call',
        category TEXT,
        problem_summary TEXT,
        equipment_type TEXT,
        urgency TEXT NOT NULL DEFAULT 'normal'
          CHECK (urgency IN ('normal', 'emergency')),
        status TEXT NOT NULL DEFAULT 'new'
          CHECK (status IN ('new','waiting_for_customer','waiting_for_approval','scheduled','technician_en_route','in_progress','completed','cancelled')),
        service_address TEXT,
        service_zip TEXT,
        requested_start TIMESTAMPTZ,
        requested_time_text TEXT,
        confirmed_start TIMESTAMPTZ,
        service_fee_cents INTEGER NOT NULL DEFAULT 9500,
        callback_at_caller_number BOOLEAN,
        callback_phone TEXT,
        diagnosis TEXT,
        work_performed TEXT,
        total_amount_cents INTEGER,
        amount_paid_cents INTEGER NOT NULL DEFAULT 0,
        payment_status TEXT NOT NULL DEFAULT 'unpaid'
          CHECK (payment_status IN ('unpaid', 'partial', 'paid')),
        payment_method TEXT,
        twilio_call_sid TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        closed_at TIMESTAMPTZ
      );
    `);

    // Existing databases need ALTERs because CREATE TABLE IF NOT EXISTS
    // does not add newly introduced columns.
    await client.query(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS requested_time_text TEXT;`);
    await client.query(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS callback_at_caller_number BOOLEAN;`);
    await client.query(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS callback_phone TEXT;`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS appointments (
        id BIGSERIAL PRIMARY KEY,
        job_number BIGINT NOT NULL REFERENCES jobs(job_number) ON DELETE CASCADE,
        requested_start TIMESTAMPTZ,
        confirmed_start TIMESTAMPTZ,
        status TEXT NOT NULL DEFAULT 'requested'
          CHECK (status IN ('requested','pending_approval','confirmed','completed','cancelled')),
        notes TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS payments (
        id BIGSERIAL PRIMARY KEY,
        job_number BIGINT NOT NULL REFERENCES jobs(job_number) ON DELETE CASCADE,
        amount_cents INTEGER NOT NULL,
        method TEXT,
        status TEXT NOT NULL DEFAULT 'pending'
          CHECK (status IN ('pending','paid','refunded','failed')),
        notes TEXT,
        paid_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS job_events (
        id BIGSERIAL PRIMARY KEY,
        job_number BIGINT NOT NULL REFERENCES jobs(job_number) ON DELETE CASCADE,
        event_type TEXT NOT NULL,
        details JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS transcript_chunks (
        id BIGSERIAL PRIMARY KEY,
        job_number BIGINT NOT NULL REFERENCES jobs(job_number) ON DELETE CASCADE,
        speaker TEXT NOT NULL CHECK (speaker IN ('customer','assistant')),
        text TEXT NOT NULL,
        start_ms INTEGER,
        end_ms INTEGER,
        openai_event_id TEXT,
        openai_session_id TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS transcript_turns (
        id BIGSERIAL PRIMARY KEY,
        job_number BIGINT NOT NULL REFERENCES jobs(job_number) ON DELETE CASCADE,
        speaker TEXT NOT NULL CHECK (speaker IN ('customer','assistant')),
        text TEXT NOT NULL,
        openai_session_id TEXT,
        turn_key TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS call_transcripts (
        job_number BIGINT PRIMARY KEY REFERENCES jobs(job_number) ON DELETE CASCADE,
        readable_text TEXT NOT NULL DEFAULT '',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finalized_at TIMESTAMPTZ
      );
    `);

    await client.query(`CREATE INDEX IF NOT EXISTS idx_jobs_customer ON jobs(customer_id);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_jobs_confirmed_start ON jobs(confirmed_start);`);
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_twilio_call_sid_unique ON jobs(twilio_call_sid) WHERE twilio_call_sid IS NOT NULL;`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_job_events_job ON job_events(job_number);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_transcript_chunks_job ON transcript_chunks(job_number, id);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_transcript_turns_job ON transcript_turns(job_number, id);`);
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_transcript_turns_key ON transcript_turns(job_number, turn_key) WHERE turn_key IS NOT NULL;`);

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

export async function findOrCreateCustomer(phoneE164) {
  if (!phoneE164) throw new Error('Customer phone number is missing');

  const result = await pool.query(
    `
      INSERT INTO customers (phone_e164, updated_at)
      VALUES ($1, NOW())
      ON CONFLICT (phone_e164)
      DO UPDATE SET updated_at = NOW()
      RETURNING *;
    `,
    [phoneE164]
  );

  return result.rows[0];
}

export async function createJobForCall({ customerId, callSid }) {
  if (callSid) {
    const existing = await pool.query(
      `SELECT * FROM jobs WHERE twilio_call_sid = $1 LIMIT 1;`,
      [callSid]
    );
    if (existing.rows.length) return existing.rows[0];
  }

  try {
    const result = await pool.query(
      `
        INSERT INTO jobs (customer_id, source, status, service_fee_cents, twilio_call_sid)
        VALUES ($1, 'incoming_call', 'new', 9500, $2)
        RETURNING *;
      `,
      [customerId, callSid || null]
    );

    const job = result.rows[0];
    await recordJobEvent(job.job_number, 'incoming_call_started', { callSid: callSid || null });
    return job;
  } catch (error) {
    if (error.code === '23505' && callSid) {
      const existing = await pool.query(
        `SELECT * FROM jobs WHERE twilio_call_sid = $1 LIMIT 1;`,
        [callSid]
      );
      if (existing.rows.length) return existing.rows[0];
    }
    throw error;
  }
}

export async function updateJobServiceFee({
  jobNumber,
  serviceFeeCents,
  requestedTimeText = null,
  requestedTimeMinutes = null,
  source = 'semantic_router',
}) {
  if (!jobNumber) throw new Error('Job number is required');
  if (![9500, 15000].includes(serviceFeeCents)) throw new Error('Unsupported service fee');

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const result = await client.query(
      `
        UPDATE jobs
        SET
          service_fee_cents = $2,
          requested_time_text = COALESCE(NULLIF($3, ''), requested_time_text),
          urgency = CASE WHEN $2 = 15000 THEN 'emergency' ELSE urgency END,
          updated_at = NOW()
        WHERE job_number = $1
        RETURNING *;
      `,
      [jobNumber, serviceFeeCents, requestedTimeText]
    );

    if (!result.rows.length) throw new Error(`Job #${jobNumber} not found`);

    await client.query(
      `INSERT INTO job_events (job_number, event_type, details) VALUES ($1, 'service_fee_updated', $2::jsonb);`,
      [jobNumber, JSON.stringify({ serviceFeeCents, requestedTimeText, requestedTimeMinutes, source })]
    );

    await client.query('COMMIT');
    return result.rows[0];
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export async function updateCrmFromConversation({
  jobNumber,
  customerId,
  customerName,
  preferredLanguage,
  category,
  problemSummary,
  equipmentType,
  urgency,
  serviceAddress,
  serviceZip,
  requestedTimeText,
  callbackAtCallerNumber,
  callbackPhone,
}) {
  if (!jobNumber || !customerId) return;

  const clean = (value) => {
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
  };

  const safeLanguage = ['en', 'ru', 'es', 'other'].includes(preferredLanguage)
    ? preferredLanguage
    : null;

  const safeCategory = ['hvac', 'plumbing', 'other'].includes(category)
    ? category
    : null;

  const safeUrgency = ['normal', 'emergency'].includes(urgency)
    ? urgency
    : null;

  const callbackValue =
    callbackAtCallerNumber === true || callbackAtCallerNumber === false
      ? callbackAtCallerNumber
      : null;

  const patch = {
    customerName: clean(customerName),
    preferredLanguage: safeLanguage,
    category: safeCategory,
    problemSummary: clean(problemSummary),
    equipmentType: clean(equipmentType),
    urgency: safeUrgency,
    serviceAddress: clean(serviceAddress),
    serviceZip: clean(serviceZip),
    requestedTimeText: clean(requestedTimeText),
    callbackAtCallerNumber: callbackValue,
    callbackPhone: clean(callbackPhone),
  };

  const hasAnyValue = Object.values(patch).some((value) => value !== null);
  if (!hasAnyValue) return;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    await client.query(
      `
        UPDATE customers
        SET
          name = COALESCE($2, name),
          preferred_language = COALESCE($3, preferred_language),
          default_address = COALESCE($4, default_address),
          default_zip = COALESCE($5, default_zip),
          updated_at = NOW()
        WHERE id = $1;
      `,
      [customerId, patch.customerName, patch.preferredLanguage, patch.serviceAddress, patch.serviceZip]
    );

    const result = await client.query(
      `
        UPDATE jobs
        SET
          category = COALESCE($2, category),
          problem_summary = COALESCE($3, problem_summary),
          equipment_type = COALESCE($4, equipment_type),
          urgency = COALESCE($5, urgency),
          service_address = COALESCE($6, service_address),
          service_zip = COALESCE($7, service_zip),
          requested_time_text = COALESCE($8, requested_time_text),
          callback_at_caller_number = COALESCE($9, callback_at_caller_number),
          callback_phone = COALESCE($10, callback_phone),
          updated_at = NOW()
        WHERE job_number = $1
        RETURNING *;
      `,
      [
        jobNumber,
        patch.category,
        patch.problemSummary,
        patch.equipmentType,
        patch.urgency,
        patch.serviceAddress,
        patch.serviceZip,
        patch.requestedTimeText,
        patch.callbackAtCallerNumber,
        patch.callbackPhone,
      ]
    );

    await client.query(
      `INSERT INTO job_events (job_number, event_type, details) VALUES ($1, 'crm_fields_updated', $2::jsonb);`,
      [jobNumber, JSON.stringify(patch)]
    );

    await client.query('COMMIT');
    return result.rows[0];
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export async function recordJobEvent(jobNumber, eventType, details = {}) {
  if (!jobNumber || !eventType) return;

  await pool.query(
    `INSERT INTO job_events (job_number, event_type, details) VALUES ($1, $2, $3::jsonb);`,
    [jobNumber, eventType, JSON.stringify(details || {})]
  );
}

export async function saveTranscriptChunks(jobNumber, chunks) {
  if (!jobNumber || !Array.isArray(chunks) || !chunks.length) return;

  const cleanChunks = chunks.filter(
    (chunk) =>
      chunk &&
      typeof chunk.text === 'string' &&
      chunk.text.length > 0 &&
      (chunk.speaker === 'customer' || chunk.speaker === 'assistant')
  );

  if (!cleanChunks.length) return;

  const values = [];
  const placeholders = [];

  cleanChunks.forEach((chunk, index) => {
    const base = index * 7;

    placeholders.push(
      `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7})`
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
  });

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
      VALUES ${placeholders.join(',\n')};
    `,
    values
  );
}

function normalizeReadableTranscriptText(value) {
  if (typeof value !== 'string') return '';

  return value
    .replace(/\s+/g, ' ')
    .replace(/\s+([,.;:!?])/g, '$1')
    .trim();
}

export async function rebuildReadableTranscript(
  jobNumber,
  { finalized = false } = {}
) {
  if (!jobNumber) return;

  await pool.query(
    `
      INSERT INTO call_transcripts (
        job_number,
        readable_text,
        updated_at,
        finalized_at
      )
      SELECT
        $1,
        COALESCE(
          string_agg(
            CASE
              WHEN speaker = 'customer'
                THEN 'Customer: ' || text
              ELSE
                'Mia: ' || text
            END,
            E'\\n\\n'
            ORDER BY id
          ),
          ''
        ),
        NOW(),
        CASE
          WHEN $2::boolean THEN NOW()
          ELSE NULL
        END
      FROM transcript_turns
      WHERE job_number = $1

      ON CONFLICT (job_number)
      DO UPDATE SET
        readable_text = EXCLUDED.readable_text,
        updated_at = NOW(),
        finalized_at = CASE
          WHEN $2::boolean THEN NOW()
          ELSE NULL
        END;
    `,
    [jobNumber, finalized]
  );
}

export async function saveReadableTranscriptTurn({
  jobNumber,
  speaker,
  text,
  sessionId = null,
  turnKey = null,
}) {
  if (!jobNumber) return;

  if (!['customer', 'assistant'].includes(speaker)) {
    return;
  }

  const cleaned = normalizeReadableTranscriptText(text);

  if (!cleaned) return;

  await pool.query(
    `
      INSERT INTO transcript_turns (
        job_number,
        speaker,
        text,
        openai_session_id,
        turn_key
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5
      )

      ON CONFLICT (job_number, turn_key)
      WHERE turn_key IS NOT NULL
      DO NOTHING;
    `,
    [
      jobNumber,
      speaker,
      cleaned,
      sessionId,
      turnKey,
    ]
  );

  await rebuildReadableTranscript(jobNumber);
}

export async function finalizeReadableTranscript(jobNumber) {
  if (!jobNumber) return;

  await rebuildReadableTranscript(
    jobNumber,
    {
      finalized: true,
    }
  );
}
