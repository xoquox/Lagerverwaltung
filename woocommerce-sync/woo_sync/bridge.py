"""The sole remote transport. SQL only, never HTTP or WooCommerce REST."""

import json
import re
from . import PROTOCOL
from .protocol import json_text


class Bridge:
    def __init__(self, connection, prefix, max_age=300):
        if not re.fullmatch(r"[A-Za-z0-9_]+", prefix):
            raise ValueError("Ungueltiger WordPress-Tabellenpraefix.")
        self.con = connection
        self.prefix = prefix
        self.max_age = max_age

    def table(self, name):
        # Config validates prefix; the suffix is a fixed, internal table name.
        return f"`{self.prefix}lager_mc_{name}`"

    def info(self):
        with self.con.cursor() as cur:
            cur.execute(
                f"SELECT *,TIMESTAMPDIFF(SECOND,updated_at,UTC_TIMESTAMP()) AS age_seconds FROM {self.table('info')} WHERE id=1"
            )
            row = cur.fetchone()
        if not row or int(row["protocol_version"]) != PROTOCOL:
            raise RuntimeError(
                "WooCommerce-Datenbankbruecke fehlt oder Protokollversion passt nicht."
            )
        if int(row["age_seconds"]) > self.max_age:
            raise RuntimeError(
                "WordPress-Bruecke meldet keinen aktuellen Heartbeat. lager_mc_tick / WP-Cron pruefen."
            )
        row["capabilities"] = json.loads(row["payload"])
        if row["capabilities"].get("currency") != "EUR":
            raise RuntimeError("Lager-MC ist derzeit fuer EUR ausgelegt.")
        return row

    def bind(self, client_id):
        with self.con.cursor() as cur:
            cur.execute(
                f"UPDATE {self.table('info')} SET client_id=%s WHERE id=1 AND (client_id IS NULL OR client_id=%s)",
                (client_id, client_id),
            )
            cur.execute(f"SELECT client_id FROM {self.table('info')} WHERE id=1")
            if cur.fetchone()["client_id"] != client_id:
                raise RuntimeError(
                    "Diese WooCommerce-Bruecke ist bereits an eine andere Lagerdatenbank gebunden."
                )

    def events(self, limit):
        with self.con.cursor() as cur:
            cur.execute(
                f"SELECT seq,kind,entity_id,payload,deleted FROM {self.table('events')} WHERE acknowledged=0 ORDER BY seq LIMIT %s",
                (limit,),
            )
            return cur.fetchall()

    def backlog(self):
        with self.con.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) AS count FROM {self.table('events')} WHERE acknowledged=0"
            )
            return int(cur.fetchone()["count"])

    def acknowledge(self, seqs):
        if seqs:
            with self.con.cursor() as cur:
                cur.execute(
                    f"UPDATE {self.table('events')} SET acknowledged=1 WHERE seq IN ({','.join(['%s'] * len(seqs))})",
                    tuple(seqs),
                )

    def submit(self, command):
        with self.con.cursor() as cur:
            cur.execute(
                f"INSERT IGNORE INTO {self.table('commands')} (command_id,kind,payload,status,created_at) VALUES (%s,%s,%s,'pending',UTC_TIMESTAMP())",
                (command["command_id"], command["kind"], json_text(command["payload"])),
            )
            cur.execute(
                f"SELECT kind,payload FROM {self.table('commands')} WHERE command_id=%s",
                (command["command_id"],),
            )
            row = cur.fetchone()
            if (
                row["kind"] != command["kind"]
                or json.loads(row["payload"]) != command["payload"]
            ):
                raise RuntimeError(
                    "Remote-Auftrags-ID existiert mit abweichendem Inhalt."
                )

    def result(self, command_id):
        with self.con.cursor() as cur:
            cur.execute(
                f"SELECT status,result,error FROM {self.table('commands')} WHERE command_id=%s",
                (command_id,),
            )
            row = cur.fetchone()
        if row and row.get("result"):
            row["result"] = json.loads(row["result"])
        return row
