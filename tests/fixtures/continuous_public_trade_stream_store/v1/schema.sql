CREATE TABLE stream_store_metadata (
    singleton_key INTEGER NOT NULL PRIMARY KEY
        CHECK (typeof(singleton_key) = 'integer' AND singleton_key = 1),
    storage_marker BLOB NOT NULL
        CHECK (
            typeof(storage_marker) = 'blob'
            AND storage_marker = X'7765616c74682e636f6e74696e756f75735f7075626c69635f74726164652e73747265616d5f73746f72652f73716c6974652f7631'
        ),
    physical_format_version INTEGER NOT NULL
        CHECK (typeof(physical_format_version) = 'integer' AND physical_format_version = 1),
    schema_generation INTEGER NOT NULL
        CHECK (typeof(schema_generation) = 'integer' AND schema_generation = 1),
    natural_identity_key_version INTEGER NOT NULL
        CHECK (
            typeof(natural_identity_key_version) = 'integer'
            AND natural_identity_key_version = 1
        ),
    page_size INTEGER NOT NULL
        CHECK (typeof(page_size) = 'integer' AND page_size = 4096),
    schema_fingerprint BLOB NOT NULL
        CHECK (
            typeof(schema_fingerprint) = 'blob'
            AND length(schema_fingerprint) = 71
            AND substr(schema_fingerprint, 1, 7) = X'7368613235363a'
            AND length(CAST(substr(schema_fingerprint, 8, 64) AS TEXT)) = 64
            AND CAST(substr(schema_fingerprint, 8, 64) AS TEXT) NOT GLOB '*[^0-9a-f]*'
        )
) STRICT, WITHOUT ROWID;

CREATE TABLE continuous_public_trade_stream (
    stream_row_id INTEGER PRIMARY KEY,
    stream_uuid BLOB NOT NULL
        CHECK (typeof(stream_uuid) = 'blob' AND length(stream_uuid) = 16),
    natural_identity_key BLOB NOT NULL
        CHECK (
            typeof(natural_identity_key) = 'blob'
            AND length(natural_identity_key) BETWEEN 88 AND 1887
        ),
    stream_contract_version INTEGER NOT NULL
        CHECK (typeof(stream_contract_version) = 'integer' AND stream_contract_version = 1),
    creation_successor_version INTEGER NOT NULL
        CHECK (typeof(creation_successor_version) = 'integer' AND creation_successor_version = 1),
    creation_record_canonical_bytes BLOB NOT NULL
        CHECK (
            typeof(creation_record_canonical_bytes) = 'blob'
            AND length(creation_record_canonical_bytes) BETWEEN 1 AND 65536
        ),
    creation_record_digest BLOB NOT NULL
        CHECK (
            typeof(creation_record_digest) = 'blob'
            AND length(creation_record_digest) = 71
            AND substr(creation_record_digest, 1, 7) = X'7368613235363a'
            AND length(CAST(substr(creation_record_digest, 8, 64) AS TEXT)) = 64
            AND CAST(substr(creation_record_digest, 8, 64) AS TEXT) NOT GLOB '*[^0-9a-f]*'
        ),
    creation_history_root BLOB NOT NULL
        CHECK (
            typeof(creation_history_root) = 'blob'
            AND length(creation_history_root) = 71
            AND substr(creation_history_root, 1, 7) = X'7368613235363a'
            AND length(CAST(substr(creation_history_root, 8, 64) AS TEXT)) = 64
            AND CAST(substr(creation_history_root, 8, 64) AS TEXT) NOT GLOB '*[^0-9a-f]*'
        ),
    policy_schema_version BLOB NOT NULL
        CHECK (typeof(policy_schema_version) = 'blob' AND policy_schema_version = X'312e30'),
    policy_window_size_ms INTEGER NOT NULL
        CHECK (
            typeof(policy_window_size_ms) = 'integer'
            AND policy_window_size_ms BETWEEN 1 AND 9223372036854775807
        ),
    policy_settlement_lag_ms INTEGER NOT NULL
        CHECK (
            typeof(policy_settlement_lag_ms) = 'integer'
            AND policy_settlement_lag_ms BETWEEN 0 AND 9223372036854775807
        ),
    policy_max_catchup_span_ms INTEGER NOT NULL
        CHECK (
            typeof(policy_max_catchup_span_ms) = 'integer'
            AND policy_max_catchup_span_ms BETWEEN 1 AND 9223372036854775807
        ),
    policy_max_jobs_per_invocation INTEGER NOT NULL
        CHECK (
            typeof(policy_max_jobs_per_invocation) = 'integer'
            AND policy_max_jobs_per_invocation BETWEEN 1 AND 9223372036854775807
        ),
    policy_max_requests_per_job INTEGER NOT NULL
        CHECK (
            typeof(policy_max_requests_per_job) = 'integer'
            AND policy_max_requests_per_job BETWEEN 1 AND 9223372036854775807
        ),
    policy_max_records_per_job INTEGER NOT NULL
        CHECK (
            typeof(policy_max_records_per_job) = 'integer'
            AND policy_max_records_per_job BETWEEN 1 AND 9223372036854775807
        ),
    policy_fingerprint BLOB NOT NULL
        CHECK (
            typeof(policy_fingerprint) = 'blob'
            AND length(policy_fingerprint) = 71
            AND substr(policy_fingerprint, 1, 7) = X'7368613235363a'
            AND length(CAST(substr(policy_fingerprint, 8, 64) AS TEXT)) = 64
            AND CAST(substr(policy_fingerprint, 8, 64) AS TEXT) NOT GLOB '*[^0-9a-f]*'
        ),
    stream_start_epoch_ms INTEGER NOT NULL
        CHECK (
            typeof(stream_start_epoch_ms) = 'integer'
            AND stream_start_epoch_ms BETWEEN 0 AND 9223372036854775807
        ),
    current_version INTEGER NOT NULL
        CHECK (
            typeof(current_version) = 'integer'
            AND current_version BETWEEN 1 AND 9223372036854775807
        ),
    current_record_canonical_bytes BLOB NOT NULL
        CHECK (
            typeof(current_record_canonical_bytes) = 'blob'
            AND length(current_record_canonical_bytes) BETWEEN 1 AND 65536
        ),
    current_record_digest BLOB NOT NULL
        CHECK (
            typeof(current_record_digest) = 'blob'
            AND length(current_record_digest) = 71
            AND substr(current_record_digest, 1, 7) = X'7368613235363a'
            AND length(CAST(substr(current_record_digest, 8, 64) AS TEXT)) = 64
            AND CAST(substr(current_record_digest, 8, 64) AS TEXT) NOT GLOB '*[^0-9a-f]*'
        ),
    current_envelope_canonical_bytes BLOB NOT NULL
        CHECK (
            typeof(current_envelope_canonical_bytes) = 'blob'
            AND length(current_envelope_canonical_bytes) BETWEEN 1 AND 16384
        ),
    current_envelope_digest BLOB NOT NULL
        CHECK (
            typeof(current_envelope_digest) = 'blob'
            AND length(current_envelope_digest) = 71
            AND substr(current_envelope_digest, 1, 7) = X'7368613235363a'
            AND length(CAST(substr(current_envelope_digest, 8, 64) AS TEXT)) = 64
            AND CAST(substr(current_envelope_digest, 8, 64) AS TEXT) NOT GLOB '*[^0-9a-f]*'
        ),
    current_history_root BLOB NOT NULL
        CHECK (
            typeof(current_history_root) = 'blob'
            AND length(current_history_root) = 71
            AND substr(current_history_root, 1, 7) = X'7368613235363a'
            AND length(CAST(substr(current_history_root, 8, 64) AS TEXT)) = 64
            AND CAST(substr(current_history_root, 8, 64) AS TEXT) NOT GLOB '*[^0-9a-f]*'
        ),
    FOREIGN KEY (
        stream_row_id,
        creation_successor_version,
        creation_record_digest
    ) REFERENCES continuous_public_trade_history (
        stream_row_id,
        successor_version,
        record_digest
    ) ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        stream_row_id,
        current_version,
        current_record_digest,
        current_envelope_digest,
        current_history_root
    ) REFERENCES continuous_public_trade_history (
        stream_row_id,
        successor_version,
        record_digest,
        successor_envelope_digest,
        successor_history_root
    ) ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE continuous_public_trade_history (
    history_row_id INTEGER PRIMARY KEY,
    stream_row_id INTEGER NOT NULL,
    successor_version INTEGER NOT NULL
        CHECK (
            typeof(successor_version) = 'integer'
            AND successor_version BETWEEN 1 AND 9223372036854775807
        ),
    entry_kind BLOB NOT NULL
        CHECK (
            typeof(entry_kind) = 'blob'
            AND entry_kind IN (X'6372656174696f6e', X'7472616e736974696f6e')
        ),
    record_model_version BLOB NOT NULL
        CHECK (typeof(record_model_version) = 'blob' AND record_model_version = X'312e30'),
    serialization_version INTEGER NOT NULL
        CHECK (typeof(serialization_version) = 'integer' AND serialization_version = 1),
    record_canonical_bytes BLOB NOT NULL
        CHECK (
            typeof(record_canonical_bytes) = 'blob'
            AND length(record_canonical_bytes) BETWEEN 1 AND 65536
        ),
    record_digest BLOB NOT NULL
        CHECK (
            typeof(record_digest) = 'blob'
            AND length(record_digest) = 71
            AND substr(record_digest, 1, 7) = X'7368613235363a'
            AND length(CAST(substr(record_digest, 8, 64) AS TEXT)) = 64
            AND CAST(substr(record_digest, 8, 64) AS TEXT) NOT GLOB '*[^0-9a-f]*'
        ),
    successor_envelope_canonical_bytes BLOB NOT NULL
        CHECK (
            typeof(successor_envelope_canonical_bytes) = 'blob'
            AND length(successor_envelope_canonical_bytes) BETWEEN 1 AND 16384
        ),
    successor_envelope_digest BLOB NOT NULL
        CHECK (
            typeof(successor_envelope_digest) = 'blob'
            AND length(successor_envelope_digest) = 71
            AND substr(successor_envelope_digest, 1, 7) = X'7368613235363a'
            AND length(CAST(substr(successor_envelope_digest, 8, 64) AS TEXT)) = 64
            AND CAST(substr(successor_envelope_digest, 8, 64) AS TEXT) NOT GLOB '*[^0-9a-f]*'
        ),
    prior_version INTEGER
        CHECK (
            prior_version IS NULL
            OR (
                typeof(prior_version) = 'integer'
                AND prior_version BETWEEN 1 AND 9223372036854775807
            )
        ),
    prior_envelope_digest BLOB
        CHECK (
            prior_envelope_digest IS NULL
            OR (
                typeof(prior_envelope_digest) = 'blob'
                AND length(prior_envelope_digest) = 71
                AND substr(prior_envelope_digest, 1, 7) = X'7368613235363a'
                AND length(CAST(substr(prior_envelope_digest, 8, 64) AS TEXT)) = 64
                AND CAST(substr(prior_envelope_digest, 8, 64) AS TEXT)
                    NOT GLOB '*[^0-9a-f]*'
            )
        ),
    prior_history_root BLOB
        CHECK (
            prior_history_root IS NULL
            OR (
                typeof(prior_history_root) = 'blob'
                AND length(prior_history_root) = 71
                AND substr(prior_history_root, 1, 7) = X'7368613235363a'
                AND length(CAST(substr(prior_history_root, 8, 64) AS TEXT)) = 64
                AND CAST(substr(prior_history_root, 8, 64) AS TEXT)
                    NOT GLOB '*[^0-9a-f]*'
            )
        ),
    predecessor_record_canonical_bytes BLOB
        CHECK (
            predecessor_record_canonical_bytes IS NULL
            OR (
                typeof(predecessor_record_canonical_bytes) = 'blob'
                AND length(predecessor_record_canonical_bytes) BETWEEN 1 AND 65536
            )
        ),
    predecessor_record_digest BLOB
        CHECK (
            predecessor_record_digest IS NULL
            OR (
                typeof(predecessor_record_digest) = 'blob'
                AND length(predecessor_record_digest) = 71
                AND substr(predecessor_record_digest, 1, 7) = X'7368613235363a'
                AND length(CAST(substr(predecessor_record_digest, 8, 64) AS TEXT)) = 64
                AND CAST(substr(predecessor_record_digest, 8, 64) AS TEXT)
                    NOT GLOB '*[^0-9a-f]*'
            )
        ),
    successor_history_root BLOB NOT NULL
        CHECK (
            typeof(successor_history_root) = 'blob'
            AND length(successor_history_root) = 71
            AND substr(successor_history_root, 1, 7) = X'7368613235363a'
            AND length(CAST(substr(successor_history_root, 8, 64) AS TEXT)) = 64
            AND CAST(substr(successor_history_root, 8, 64) AS TEXT) NOT GLOB '*[^0-9a-f]*'
        ),
    CHECK (
        (
            entry_kind = X'6372656174696f6e'
            AND successor_version = 1
            AND prior_version IS NULL
            AND prior_envelope_digest IS NULL
            AND prior_history_root IS NULL
            AND predecessor_record_canonical_bytes IS NULL
            AND predecessor_record_digest IS NULL
        )
        OR (
            entry_kind = X'7472616e736974696f6e'
            AND successor_version > 1
            AND prior_version = successor_version - 1
            AND prior_envelope_digest IS NOT NULL
            AND prior_history_root IS NOT NULL
            AND predecessor_record_canonical_bytes IS NOT NULL
            AND predecessor_record_digest IS NOT NULL
        )
    ),
    FOREIGN KEY (stream_row_id)
        REFERENCES continuous_public_trade_stream (stream_row_id)
        ON UPDATE NO ACTION ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        stream_row_id,
        prior_version,
        predecessor_record_digest
    ) REFERENCES continuous_public_trade_history (
        stream_row_id,
        successor_version,
        record_digest
    ) ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE stream_tail_commit_guard (
    stream_row_id INTEGER NOT NULL,
    successor_version INTEGER NOT NULL
        CHECK (
            typeof(successor_version) = 'integer'
            AND successor_version BETWEEN 2 AND 9223372036854775807
        ),
    record_digest BLOB NOT NULL
        CHECK (
            typeof(record_digest) = 'blob'
            AND length(record_digest) = 71
            AND substr(record_digest, 1, 7) = X'7368613235363a'
            AND length(CAST(substr(record_digest, 8, 64) AS TEXT)) = 64
            AND CAST(substr(record_digest, 8, 64) AS TEXT) NOT GLOB '*[^0-9a-f]*'
        ),
    unresolved_singleton_key INTEGER NOT NULL
        CHECK (
            typeof(unresolved_singleton_key) = 'integer'
            AND unresolved_singleton_key = 0
        ),
    PRIMARY KEY (stream_row_id, successor_version),
    FOREIGN KEY (stream_row_id)
        REFERENCES continuous_public_trade_stream (stream_row_id)
        ON UPDATE NO ACTION ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (stream_row_id, successor_version, record_digest)
        REFERENCES continuous_public_trade_history (
            stream_row_id,
            successor_version,
            record_digest
        ) ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (unresolved_singleton_key)
        REFERENCES stream_store_metadata (singleton_key)
        ON UPDATE NO ACTION ON DELETE NO ACTION
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX ux_cpt_stream_uuid
    ON continuous_public_trade_stream (stream_uuid);

CREATE UNIQUE INDEX ux_cpt_stream_natural_key
    ON continuous_public_trade_stream (natural_identity_key);

CREATE UNIQUE INDEX ux_cpt_history_stream_version
    ON continuous_public_trade_history (stream_row_id, successor_version);

CREATE UNIQUE INDEX ux_cpt_history_record_binding
    ON continuous_public_trade_history (stream_row_id, successor_version, record_digest);

CREATE UNIQUE INDEX ux_cpt_history_tail_binding
    ON continuous_public_trade_history (
        stream_row_id,
        successor_version,
        record_digest,
        successor_envelope_digest,
        successor_history_root
    );

CREATE TRIGGER trg_metadata_no_update
BEFORE UPDATE ON stream_store_metadata
BEGIN
    SELECT RAISE(ABORT, 'stream_store_metadata is immutable');
END;

CREATE TRIGGER trg_metadata_no_delete
BEFORE DELETE ON stream_store_metadata
BEGIN
    SELECT RAISE(ABORT, 'stream_store_metadata cannot be deleted');
END;

CREATE TRIGGER trg_stream_insert_shape
BEFORE INSERT ON continuous_public_trade_stream
WHEN
    NEW.creation_successor_version != 1
    OR NEW.current_version != 1
    OR NEW.creation_record_canonical_bytes != NEW.current_record_canonical_bytes
    OR NEW.creation_record_digest != NEW.current_record_digest
    OR NEW.creation_history_root != NEW.current_history_root
BEGIN
    SELECT RAISE(ABORT, 'stream insertion does not bind creation and current state');
END;

CREATE TRIGGER trg_stream_current_update
BEFORE UPDATE ON continuous_public_trade_stream
WHEN
    NEW.stream_row_id IS NOT OLD.stream_row_id
    OR NEW.stream_uuid IS NOT OLD.stream_uuid
    OR NEW.natural_identity_key IS NOT OLD.natural_identity_key
    OR NEW.stream_contract_version IS NOT OLD.stream_contract_version
    OR NEW.creation_successor_version IS NOT OLD.creation_successor_version
    OR NEW.creation_record_canonical_bytes IS NOT OLD.creation_record_canonical_bytes
    OR NEW.creation_record_digest IS NOT OLD.creation_record_digest
    OR NEW.creation_history_root IS NOT OLD.creation_history_root
    OR NEW.policy_schema_version IS NOT OLD.policy_schema_version
    OR NEW.policy_window_size_ms IS NOT OLD.policy_window_size_ms
    OR NEW.policy_settlement_lag_ms IS NOT OLD.policy_settlement_lag_ms
    OR NEW.policy_max_catchup_span_ms IS NOT OLD.policy_max_catchup_span_ms
    OR NEW.policy_max_jobs_per_invocation IS NOT OLD.policy_max_jobs_per_invocation
    OR NEW.policy_max_requests_per_job IS NOT OLD.policy_max_requests_per_job
    OR NEW.policy_max_records_per_job IS NOT OLD.policy_max_records_per_job
    OR NEW.policy_fingerprint IS NOT OLD.policy_fingerprint
    OR NEW.stream_start_epoch_ms IS NOT OLD.stream_start_epoch_ms
    OR OLD.current_version = 9223372036854775807
    OR NEW.current_version != OLD.current_version + 1
    OR NOT EXISTS (
        SELECT 1
        FROM stream_tail_commit_guard AS pending
        WHERE pending.stream_row_id = OLD.stream_row_id
          AND pending.successor_version = NEW.current_version
          AND pending.record_digest = NEW.current_record_digest
    )
    OR NOT EXISTS (
        SELECT 1
        FROM continuous_public_trade_history AS history
        WHERE history.stream_row_id = OLD.stream_row_id
          AND history.successor_version = NEW.current_version
          AND history.record_canonical_bytes = NEW.current_record_canonical_bytes
          AND history.record_digest = NEW.current_record_digest
          AND history.successor_envelope_canonical_bytes =
              NEW.current_envelope_canonical_bytes
          AND history.successor_envelope_digest = NEW.current_envelope_digest
          AND history.successor_history_root = NEW.current_history_root
    )
BEGIN
    SELECT RAISE(ABORT, 'stream update is not one exact bound successor');
END;

CREATE TRIGGER trg_stream_transition_finalize
AFTER UPDATE ON continuous_public_trade_stream
BEGIN
    DELETE FROM stream_tail_commit_guard
    WHERE stream_row_id = NEW.stream_row_id
      AND successor_version = NEW.current_version
      AND record_digest = NEW.current_record_digest;
END;

CREATE TRIGGER trg_stream_no_delete
BEFORE DELETE ON continuous_public_trade_stream
BEGIN
    SELECT RAISE(ABORT, 'stream rows cannot be deleted');
END;

CREATE TRIGGER trg_history_insert_binding
BEFORE INSERT ON continuous_public_trade_history
WHEN NOT (
    (
        NEW.entry_kind = X'6372656174696f6e'
        AND EXISTS (
            SELECT 1
            FROM continuous_public_trade_stream AS stream
            WHERE stream.stream_row_id = NEW.stream_row_id
              AND stream.creation_successor_version = NEW.successor_version
              AND stream.current_version = NEW.successor_version
              AND stream.creation_record_canonical_bytes = NEW.record_canonical_bytes
              AND stream.creation_record_digest = NEW.record_digest
              AND stream.creation_history_root = NEW.successor_history_root
              AND stream.current_record_canonical_bytes = NEW.record_canonical_bytes
              AND stream.current_record_digest = NEW.record_digest
              AND stream.current_envelope_canonical_bytes =
                  NEW.successor_envelope_canonical_bytes
              AND stream.current_envelope_digest = NEW.successor_envelope_digest
              AND stream.current_history_root = NEW.successor_history_root
        )
    )
    OR (
        NEW.entry_kind = X'7472616e736974696f6e'
        AND EXISTS (
            SELECT 1
            FROM continuous_public_trade_stream AS stream
            WHERE stream.stream_row_id = NEW.stream_row_id
              AND stream.current_version = NEW.prior_version
              AND stream.current_record_canonical_bytes =
                  NEW.predecessor_record_canonical_bytes
              AND stream.current_record_digest = NEW.predecessor_record_digest
              AND stream.current_envelope_digest = NEW.prior_envelope_digest
              AND stream.current_history_root = NEW.prior_history_root
        )
        AND EXISTS (
            SELECT 1
            FROM continuous_public_trade_history AS predecessor
            WHERE predecessor.stream_row_id = NEW.stream_row_id
              AND predecessor.successor_version = NEW.prior_version
              AND predecessor.record_canonical_bytes =
                  NEW.predecessor_record_canonical_bytes
              AND predecessor.record_digest = NEW.predecessor_record_digest
              AND predecessor.successor_envelope_digest = NEW.prior_envelope_digest
              AND predecessor.successor_history_root = NEW.prior_history_root
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'history insertion is not bound to its stream and predecessor');
END;

CREATE TRIGGER trg_history_transition_pending
AFTER INSERT ON continuous_public_trade_history
WHEN NEW.entry_kind = X'7472616e736974696f6e'
BEGIN
    INSERT INTO stream_tail_commit_guard (
        stream_row_id,
        successor_version,
        record_digest,
        unresolved_singleton_key
    ) VALUES (
        NEW.stream_row_id,
        NEW.successor_version,
        NEW.record_digest,
        0
    );
END;

CREATE TRIGGER trg_history_no_update
BEFORE UPDATE ON continuous_public_trade_history
BEGIN
    SELECT RAISE(ABORT, 'history rows are immutable');
END;

CREATE TRIGGER trg_history_no_delete
BEFORE DELETE ON continuous_public_trade_history
BEGIN
    SELECT RAISE(ABORT, 'history rows cannot be deleted');
END;
