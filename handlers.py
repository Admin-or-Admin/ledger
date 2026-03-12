import logging
import json
from datetime import datetime, timezone
from shared.kafka_client import AuroraConsumer

logger = logging.getLogger(__name__)

class Handlers:
    @staticmethod
    def _get_log_id(message):
        """Robustly extract log_id from message key or value."""
        # 1. Try Kafka key
        if message.key:
            try:
                k = message.key.decode('utf-8').strip()
                if k: return k
            except:
                pass
        
        # 2. Try value fields
        val = message.value
        if not isinstance(val, dict):
            return None
            
        # Prioritize explicit IDs
        return (val.get('id') or 
                val.get('event_id') or 
                val.get('log_id') or 
                val.get('trace_id') or 
                val.get('trace.id') or
                AuroraConsumer.extract_id(val))

    @staticmethod
    def _ensure_log_exists(cur, log_id, timestamp=None, message="[Stub for enrichment]"):
        """
        Ensures a record exists in the logs table so foreign keys don't fail.
        This handles cases where enrichment arrives before the raw log.
        """
        if not log_id:
            return
            
        ts = timestamp or datetime.now(timezone.utc).isoformat()
        sql = """
        INSERT INTO logs (id, timestamp, message, processing_stage)
        VALUES (%s, %s, %s, 'stub')
        ON CONFLICT (id) DO NOTHING;
        """
        cur.execute(sql, (log_id, ts, message))

    @staticmethod
    def handle_correlated_events(cur, message):
        """Saves correlation events from the correlator."""
        val = message.value
        event_id = Handlers._get_log_id(message)

        if not event_id:
            logger.warning("Skipping correlated event: No event_id found.")
            return

        # 1. First, insert into core logs table to satisfy foreign keys for downstream agents
        logger.info(f"Persisting correlation as log: {event_id}")
        ts = val.get('timestamp') or datetime.now(timezone.utc).isoformat()
        sql_log = """
        INSERT INTO logs (id, timestamp, message, service_name, raw_severity, source, processing_stage)
        VALUES (%s, %s, %s, %s, %s, %s, 'correlated')
        ON CONFLICT (id) DO UPDATE SET 
            processing_stage = 'correlated',
            timestamp = COALESCE(logs.timestamp, EXCLUDED.timestamp),
            message = COALESCE(logs.message, EXCLUDED.message);
        """
        cur.execute(sql_log, (
            event_id, ts, val.get('description'), 
            val.get('rule_name'), val.get('severity'), 'correlator'
        ))

        # 2. Then save full correlation details
        logger.info(f"Persisting correlation event details: {event_id}")
        sql = """
        INSERT INTO correlations (
            event_id, rule_id, rule_name, strategy, severity, confidence, 
            timestamp, involved_entities, involved_sources, event_count, 
            mitre_tactics, mitre_techniques, attack_stage, description, 
            recommended_action, metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO UPDATE SET
            confidence = EXCLUDED.confidence,
            event_count = EXCLUDED.event_count,
            metadata = EXCLUDED.metadata;
        """
        cur.execute(sql, (
            event_id, val.get('rule_id'), val.get('rule_name'),
            val.get('strategy'), val.get('severity'), val.get('confidence'),
            ts, val.get('involved_entities', []),
            val.get('involved_sources', []), val.get('event_count'),
            val.get('mitre_tactics', []), val.get('mitre_techniques', []),
            val.get('attack_stage'), val.get('description'),
            val.get('recommended_action'), json.dumps(val.get('metadata', {}))
        ))

    @staticmethod
    def handle_unfiltered_logs(cur, message):
        """Saves core log data."""
        val = message.value
        log_id = Handlers._get_log_id(message)

        if not log_id:
            logger.warning("Skipping unfiltered log: No ID found.")
            return

        # Improved timestamp extraction for MockLogs and others
        ts = val.get('timestamp') or val.get('@timestamp')
        if not ts:
             ts = datetime.now(timezone.utc).isoformat()
             logger.warning(f"Log {log_id} missing timestamp, using current time: {ts}")

        logger.info(f"Persisting log: {log_id}")
        sql = """
        INSERT INTO logs (id, timestamp, message, service_name, raw_severity, user_id, http_status, process_pid, trace_id, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            timestamp = EXCLUDED.timestamp,
            message = EXCLUDED.message,
            service_name = EXCLUDED.service_name,
            raw_severity = EXCLUDED.raw_severity,
            user_id = EXCLUDED.user_id,
            http_status = EXCLUDED.http_status,
            process_pid = EXCLUDED.process_pid,
            trace_id = EXCLUDED.trace_id,
            source = EXCLUDED.source,
            processing_stage = CASE WHEN logs.processing_stage = 'stub' THEN 'unfiltered' ELSE logs.processing_stage END;
        """
        cur.execute(sql, (
            log_id, ts, val.get('message'),
            val.get('service.name'), val.get('log.level'),
            val.get('user.id'), val.get('http.response.status_code'),
            val.get('process.pid'), val.get('trace.id'), val.get('source')
        ))

    @staticmethod
    def handle_categories(cur, message):
        """Saves classification enrichment."""
        val = message.value
        log_id = Handlers._get_log_id(message)
        
        if not log_id:
            logger.warning("Skipping categorization: No log_id found.")
            return

        # Ensure parent exists
        Handlers._ensure_log_exists(cur, log_id)

        logger.info(f"Saving classification for: {log_id}")
        cur.execute("UPDATE logs SET processing_stage = 'categorized' WHERE id = %s AND processing_stage IN ('unfiltered', 'stub', 'correlated')", (log_id,))
        
        cblock = val.get('classification', {})
        sql = """
        INSERT INTO classifications (log_id, category, severity, is_cybersecurity, confidence, reasoning, tags)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (log_id) DO UPDATE SET
            category = EXCLUDED.category, severity = EXCLUDED.severity,
            is_cybersecurity = EXCLUDED.is_cybersecurity,
            confidence = EXCLUDED.confidence, reasoning = EXCLUDED.reasoning,
            tags = EXCLUDED.tags;
        """
        cur.execute(sql, (
            log_id, cblock.get('category'), cblock.get('severity'),
            cblock.get('isCybersecurity'), cblock.get('classificationConfidence'),
            cblock.get('reasoning'), cblock.get('tags', [])
        ))

    @staticmethod
    def handle_solver_plan(cur, message):
        """Saves threat assessment and individual steps."""
        val = message.value
        log_id = Handlers._get_log_id(message)

        if not log_id:
            logger.warning("Skipping solver plan: No log_id found.")
            return

        # Ensure parent exists
        Handlers._ensure_log_exists(cur, log_id)

        logger.info(f"Saving solver plan for: {log_id}")
        cur.execute("UPDATE logs SET processing_stage = 'investigated' WHERE id = %s AND processing_stage != 'resolved'", (log_id,))
        
        invest = val.get('investigation', {})
        sql_asmt = """
        INSERT INTO threat_assessments (
            log_id, attack_vector, ai_suggestion, complexity, 
            recurrence_rate, confidence, auto_fixable, requires_approval, priority, notify_teams
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (log_id) DO UPDATE SET 
            attack_vector = EXCLUDED.attack_vector,
            ai_suggestion = EXCLUDED.ai_suggestion,
            complexity = EXCLUDED.complexity,
            recurrence_rate = EXCLUDED.recurrence_rate,
            confidence = EXCLUDED.confidence,
            auto_fixable = EXCLUDED.auto_fixable,
            requires_approval = EXCLUDED.requires_approval,
            priority = EXCLUDED.priority,
            notify_teams = EXCLUDED.notify_teams;
        """
        cur.execute(sql_asmt, (
            log_id, invest.get('attackVector'), invest.get('aiSuggestion'),
            invest.get('complexity'), invest.get('recurrenceRate'),
            invest.get('confidence'), invest.get('autoFixable'),
            invest.get('requiresHumanApproval'), invest.get('priority'),
            invest.get('notifyTeams', [])
        ))
        
        # Save individual steps
        cur.execute("DELETE FROM remediation_steps WHERE log_id = %s", (log_id,))
        sql_step = """
        INSERT INTO remediation_steps (
            log_id, step_number, title, description, command, risk, 
            estimated_time, rollback, auto_execute, requires_approval
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        for step in invest.get('proposedSteps', []):
            cur.execute(sql_step, (
                log_id, step.get('id'), step.get('title'),
                step.get('description'), step.get('command'),
                step.get('risk'), step.get('estimatedTime'),
                step.get('rollback'), step.get('autoExecute'),
                step.get('requiresApproval')
            ))

    @staticmethod
    def handle_solution(cur, message):
        """Saves final incident resolution and updates step statuses."""
        val = message.value
        log_id = Handlers._get_log_id(message)

        if not log_id:
            logger.warning("Skipping solution: No log_id found.")
            return

        # Ensure parent exists
        Handlers._ensure_log_exists(cur, log_id)

        logger.info(f"Finalizing incident for: {log_id}")
        cur.execute("UPDATE logs SET processing_stage = 'resolved' WHERE id = %s", (log_id,))
        
        res = val.get('resolution', {})
        summary = res.get('postIncidentSummary', {})
        incident_id = val.get('incident_id', f"INC-{log_id[:8]}")
        
        sql_inc = """
        INSERT INTO incidents (
            incident_id, log_id, resolution_mode, executive_summary, 
            outcome, resolved_by, what_happened, impact_assessment, 
            root_cause, lessons_learned, resolved_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (incident_id) DO UPDATE SET 
            outcome = EXCLUDED.outcome,
            resolved_at = EXCLUDED.resolved_at;
        """
        cur.execute(sql_inc, (
            incident_id, log_id, res.get('resolutionMode'),
            res.get('executiveSummary'), val.get('resolution_outcome', 'resolved'),
            val.get('resolved_by', 'AI-Responder'),
            summary.get('whatHappened'), summary.get('impactAssessment'),
            summary.get('rootCause'), summary.get('lessonsLearned'),
            val.get('resolved_at') or datetime.now(timezone.utc).isoformat()
        ))
        
        # Handle Follow-up Actions
        cur.execute("DELETE FROM follow_up_actions WHERE incident_id = %s", (incident_id,))
        sql_follow = """
        INSERT INTO follow_up_actions (incident_id, title, description, owner, deadline)
        VALUES (%s, %s, %s, %s, %s);
        """
        for action in res.get('followUpActions', []):
            cur.execute(sql_follow, (
                incident_id, action.get('title'), action.get('description'),
                action.get('owner'), action.get('deadline')
            ))

    @staticmethod
    def handle_analytics(cur, message):
        """Saves agent analytics/heartbeats."""
        val = message.value
        if not isinstance(val, dict): return
        
        logger.info(f"Saving analytics from {val.get('agent')} (event: {val.get('event')})")
        sql = """
        INSERT INTO analytics (agent, event, timestamp, data)
        VALUES (%s, %s, %s, %s);
        """
        cur.execute(sql, (
            val.get('agent'),
            val.get('event'),
            val.get('timestamp') or datetime.now(timezone.utc).isoformat(),
            json.dumps(val)
        ))

    @staticmethod
    def handle_actions(cur, message, producer):
        """Handles internal service actions/requests."""
        val = message.value
        action = val.get("action")
        requester = val.get("requester")
        
        # Ignore responses (usually sent by Ledger itself)
        if action and action.startswith("response_"):
            return
        
        logger.info(f"Incoming action request: {action} from {requester}")

        if action == "get_last_unfiltered_timestamp":
            logger.info(f"Handling timestamp request from {requester}")
            cur.execute("SELECT MAX(timestamp) FROM logs")
            row = cur.fetchone()
            
            ts_val = row[0] if row else None
            
            # Handle string vs datetime
            if isinstance(ts_val, str):
                timestamp = ts_val
            elif ts_val:
                timestamp = ts_val.isoformat().replace('+00:00', 'Z')
            else:
                timestamp = "1970-01-01T00:00:00.000Z"
            
            payload = {
                "action": "response_last_unfiltered_timestamp",
                "requester": requester,
                "timestamp": timestamp
            }
            logger.info(f"Publishing response to 'actions' topic: {payload}")
            producer.send_log("actions", payload, key=f"response-{requester}")
            producer.flush()
            logger.info(f"Sent last timestamp {timestamp} to {requester}")

        elif action == "get_correlated_event":
            event_id = val.get("id")
            logger.info(f"Handling correlation detail request for {event_id} from {requester}")
            
            cur.execute("SELECT * FROM correlations WHERE event_id = %s", (event_id,))
            row = cur.fetchone()
            
            correlation_data = None
            if row:
                # Map columns from schema to dict
                # event_id, rule_id, rule_name, strategy, severity, confidence, timestamp, 
                # involved_entities, involved_sources, event_count, mitre_tactics, 
                # mitre_techniques, attack_stage, description, recommended_action, metadata, created_at
                correlation_data = {
                    "event_id": row[0],
                    "rule_id": row[1],
                    "rule_name": row[2],
                    "strategy": row[3],
                    "severity": row[4],
                    "confidence": row[5],
                    "timestamp": row[6].isoformat() if row[6] else None,
                    "involved_entities": row[7],
                    "involved_sources": row[8],
                    "event_count": row[9],
                    "mitre_tactics": row[10],
                    "mitre_techniques": row[11],
                    "attack_stage": row[12],
                    "description": row[13],
                    "recommended_action": row[14],
                    "metadata": row[15]
                }
            
            payload = {
                "action": "response_get_correlated_event",
                "requester": requester,
                "id": event_id,
                "correlation": correlation_data
            }
            producer.send_log("actions", payload, key=f"response-{requester}")
            producer.flush()
            logger.info(f"Sent correlation detail response for {event_id} to {requester}")
