"""Only fixed non-secret diagnostic predicates for this run's private database."""
import argparse
import json
import sqlite3

CATEGORIES = ("authentication", "timed out", "malformed", "exited", "framing",
              "JSON-RPC", "transport", "structured", "response", "refusal",
              "nonterminal", "unsupported", "api-key", "terminal", "end_turn")


def diagnose(database):
    with sqlite3.connect(database) as db:
        rows = db.execute("SELECT seq,status,payload FROM task_events ORDER BY seq").fetchall()
    result = []
    for seq, status, payload in rows:
        parsed = json.loads(payload)
        text = json.dumps(parsed)
        categories = [category for category in CATEGORIES if category in text]
        result.append({"seq": seq, "status": status, "categories": categories})
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("database")
    print(json.dumps(diagnose(parser.parse_args().database)))
