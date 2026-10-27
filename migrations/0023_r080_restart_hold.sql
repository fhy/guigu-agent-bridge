ALTER TABLE matrix_recovery_control ADD COLUMN restart_hold_state TEXT;
ALTER TABLE matrix_recovery_control ADD COLUMN restart_hold_event_id TEXT;
ALTER TABLE matrix_recovery_control ADD COLUMN restart_hold_reason TEXT;
