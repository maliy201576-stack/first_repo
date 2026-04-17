-- Initial schema for Glukhov Sales Engine

CREATE TYPE lead_status AS ENUM ('new', 'viewed', 'in_progress', 'rejected');

CREATE TABLE leads (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source          VARCHAR(50) NOT NULL,
    title           VARCHAR(500) NOT NULL,
    description     TEXT,
    url             VARCHAR(2048),
    budget          DECIMAL(15, 2),
    category        VARCHAR(100),
    matched_keywords JSONB NOT NULL DEFAULT '[]',
    tags            JSONB NOT NULL DEFAULT '[]',
    status          lead_status NOT NULL DEFAULT 'new',
    okpd2_codes     JSONB,
    max_contract_price DECIMAL(15, 2),
    submission_deadline TIMESTAMPTZ,
    discovered_at   TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_leads_source ON leads(source);
CREATE INDEX idx_leads_status ON leads(status);
CREATE INDEX idx_leads_created_at ON leads(created_at);
CREATE INDEX idx_leads_source_title ON leads(source, title);
CREATE INDEX idx_leads_tags ON leads USING GIN(tags);

CREATE TABLE lead_hashes (
    hash       VARCHAR(64) PRIMARY KEY,
    lead_id    UUID REFERENCES leads(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Composite index for fuzzy dedup queries (source + created_at window)
CREATE INDEX IF NOT EXISTS idx_leads_source_created ON leads(source, created_at);
