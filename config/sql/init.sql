-- init.sql — Inicialización de la base de datos PostgreSQL (capa CLEAN)
-- Se ejecuta automáticamente al levantar el contenedor de PostgreSQL.

-- ─────────────────────────────────────────────────────────────────────────────
-- teams
-- Fuente: RawAPIFootballPlayerStats.team_id/team_name + RawUnderstatPlayerSeason.team
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE teams (
    team_id               SERIAL PRIMARY KEY,
    api_football_id       INT UNIQUE,
    understat_name        TEXT,
    canonical_name        TEXT NOT NULL,
    -- Team identity
    country               TEXT,
    logo_url              TEXT,
    code                  TEXT,
    founded               SMALLINT,
    -- Home venue
    venue_name            TEXT,
    venue_address         TEXT,
    venue_city            TEXT,
    venue_capacity        INT,
    venue_surface         TEXT,
    venue_image_url       TEXT,
    -- Resolution metadata
    resolution_confidence DECIMAL(3,2),
    resolution_method     TEXT,
    resolved_at           TIMESTAMPTZ,
    created_at            TIMESTAMPTZ    DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- players
-- Fuente: RawAPIFootballPlayer + RawUnderstatPlayerSeason / RawUnderstatShot
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE players (
    player_id             SERIAL PRIMARY KEY,
    api_football_id       INT UNIQUE,    -- RawAPIFootballPlayer.player_id
    understat_id          INT UNIQUE,    -- RawUnderstatPlayerSeason.player_id
    canonical_name        TEXT NOT NULL, -- nombre canónico post-resolución
    known_name            TEXT,          -- nombre corto p.ej. "Pedri" vs "Pedro González López"
    birth_date            DATE,          -- RawAPIFootballPlayer.birth_date (parseado desde ISO string)
    nationality           TEXT,
    photo_url             TEXT,
    resolution_confidence DECIMAL(3,2),
    resolution_method     TEXT,
    resolved_at           TIMESTAMPTZ,
    created_at            TIMESTAMPTZ    DEFAULT NOW()
);

CREATE INDEX idx_players_api_football_id ON players(api_football_id);
CREATE INDEX idx_players_understat_id    ON players(understat_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- player_season_stats
-- Fuente: RawAPIFootballPlayerStats
-- Un jugador puede tener una fila por equipo en la misma temporada (fichajes a mitad de año).
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE player_season_stats (
    id                  SERIAL PRIMARY KEY,
    player_id           INT NOT NULL    REFERENCES players(player_id),
    team_id             INT NOT NULL    REFERENCES teams(team_id),
    season              TEXT NOT NULL,  -- formato "2024/2025"
    league_id           INT NOT NULL,   -- RawAPIFootballPlayerStats.league_id
    -- juegos
    appearances         INT,
    starts              INT,            -- API: games.lineups
    minutes             INT,
    shirt_number        INT,            -- API: games.number
    position            TEXT,           -- API: games.position (por equipo/temporada, no biográfico)
    rating              DECIMAL(5,3),   -- API: parseado desde string "7.342857"
    captain             BOOLEAN,
    -- tiros
    shots_total         INT,
    shots_on_target     INT,
    -- goles
    goals               INT,
    assists             INT,
    goals_conceded      INT,            -- porteros
    saves               INT,            -- porteros
    -- pases
    passes_total        INT,
    key_passes          INT,
    pass_accuracy       SMALLINT,       -- 0-100
    -- defensivo
    tackles             INT,
    blocks              INT,
    interceptions       INT,
    -- duelos
    duels_total         INT,
    duels_won           INT,
    -- regates
    dribbles_attempted  INT,
    dribbles_successful INT,
    dribbles_past       INT,            -- rivales que regatearon al jugador
    -- faltas
    fouls_drawn         INT,
    fouls_committed     INT,
    -- tarjetas
    cards_yellow        INT,
    cards_yellow_red    INT,
    cards_red           INT,
    -- penaltis
    penalties_won       INT,
    penalties_committed INT,
    penalties_scored    INT,
    penalties_missed    INT,
    penalties_saved     INT,
    created_at          TIMESTAMPTZ     DEFAULT NOW(),
    UNIQUE (player_id, team_id, season, league_id)
);

CREATE INDEX idx_pss_player_id ON player_season_stats(player_id);
CREATE INDEX idx_pss_team_id   ON player_season_stats(team_id);
CREATE INDEX idx_pss_season    ON player_season_stats(season);

-- ─────────────────────────────────────────────────────────────────────────────
-- player_season_advanced
-- Fuente: RawUnderstatPlayerSeason
-- Métricas avanzadas propias de Understat (no derivadas de shots individuales).
-- Los campos goals/assists/minutes se omiten: duplican player_season_stats.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE player_season_advanced (
    id          SERIAL PRIMARY KEY,
    player_id   INT NOT NULL    REFERENCES players(player_id),
    team_id     INT NOT NULL    REFERENCES teams(team_id),
    season      TEXT NOT NULL,  -- formato "2024/2025"
    -- xG family: valores de temporada completa, pueden superar 1.0
    xg          DECIMAL(8,4),
    xa          DECIMAL(8,4),
    npxg        DECIMAL(8,4),
    xg_chain    DECIMAL(8,4),
    xg_buildup  DECIMAL(8,4),
    -- volumen
    shots       INT,
    key_passes  INT,
    created_at  TIMESTAMPTZ     DEFAULT NOW(),
    UNIQUE (player_id, team_id, season)
);

CREATE INDEX idx_psa_player_id ON player_season_advanced(player_id);
CREATE INDEX idx_psa_team_id   ON player_season_advanced(team_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- player_shots
-- Fuente: RawUnderstatShot
-- Coordenadas (x, y) normalizadas 0-1 tal como las provee Understat.
-- team_id es nullable hasta que se complete entity resolution.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE player_shots (
    shot_id      SERIAL PRIMARY KEY,
    player_id    INT NOT NULL    REFERENCES players(player_id),
    team_id      INT             REFERENCES teams(team_id),
    season       TEXT NOT NULL,
    league_id    INT,            -- del config en el momento de ingesta
    -- evento
    understat_id INT,            -- RawUnderstatShot.id (ID interno de Understat)
    minute       INT,
    result       TEXT,           -- 'Goal' | 'MissedShots' | 'SavedShot' | ...
    x            DECIMAL(5,4),   -- coordenada normalizada 0-1
    y            DECIMAL(5,4),
    xg           DECIMAL(5,4),
    situation    TEXT,           -- 'OpenPlay' | 'FromCorner' | 'SetPiece' | ...
    body_part    TEXT,           -- 'Right Foot' | 'Left Foot' | 'Head' | ...
    created_at   TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX idx_ps_player_id ON player_shots(player_id);
CREATE INDEX idx_ps_team_id   ON player_shots(team_id);
CREATE INDEX idx_ps_season    ON player_shots(season);

-- ─────────────────────────────────────────────────────────────────────────────
-- player_profile
-- Fuente: RawAPIFootballPlayer (datos físicos biográficos)
-- preferred_foot y secondary_positions no existen en API-Football → omitidos.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE player_profile (
    player_id  INT PRIMARY KEY  REFERENCES players(player_id),
    height_cm  INT,             -- parseado desde "174 cm"
    weight_kg  INT,             -- parseado desde "60 kg"
    created_at TIMESTAMPTZ      DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- player_injuries
-- Fuente: RawAPIFootballInjury
-- end_date no existe en la API: solo hay fecha de reporte de la lesión.
-- fixture_id es null para lesiones producidas en entrenamiento.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE player_injuries (
    injury_id   SERIAL PRIMARY KEY,
    player_id   INT NOT NULL    REFERENCES players(player_id),
    team_id     INT             REFERENCES teams(team_id),
    league_id   INT,
    fixture_id  INT,            -- nullable: lesión en partido vs entrenamiento
    injury_date DATE,           -- RawAPIFootballInjury.date (parseado desde ISO string)
    type        TEXT,
    reason      TEXT,
    created_at  TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX idx_pi_player_id ON player_injuries(player_id);
CREATE INDEX idx_pi_team_id   ON player_injuries(team_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- player_transfers
-- Fuente: RawAPIFootballTransfer
-- El campo type de la API es ambiguo: puede ser "Loan"/"Free" o una cantidad "€ 222M".
-- Se parsea: si contiene símbolo de moneda → fee_text; si no → transfer_type.
-- from/to_team_name se desnormalizan para el caso en que el equipo no esté en nuestra DB.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE player_transfers (
    transfer_id     SERIAL PRIMARY KEY,
    player_id       INT NOT NULL    REFERENCES players(player_id),
    from_team_id    INT             REFERENCES teams(team_id),
    to_team_id      INT             REFERENCES teams(team_id),
    from_team_name  TEXT,
    to_team_name    TEXT,
    transfer_date   DATE,           -- RawAPIFootballTransfer.date (nullable en la API)
    transfer_type   TEXT,           -- 'Loan' | 'Free' | 'Transfer' | null
    fee_text        TEXT,           -- valor original cuando type contiene importe ("€ 222M")
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX idx_pt_player_id    ON player_transfers(player_id);
CREATE INDEX idx_pt_from_team_id ON player_transfers(from_team_id);
CREATE INDEX idx_pt_to_team_id   ON player_transfers(to_team_id);
