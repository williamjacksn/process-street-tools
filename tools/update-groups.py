import collections
import logging
import os
import signal
import sys
import time
import types
import typing

import datime
import httpx
import notch
import psycopg2.extras
from apscheduler.schedulers.blocking import BlockingScheduler

log = logging.getLogger(__name__)

GroupDef = collections.namedtuple("GroupDef", "group_name sql_filter")
UserDef = collections.namedtuple("UserDef", "id display_name")

PRST_API_KEY = os.getenv("PRST_API_KEY", "")


def pg_get_connection() -> psycopg2._psycopg.connection:
    cnx = psycopg2.connect(
        os.getenv("DB"), cursor_factory=psycopg2.extras.RealDictCursor
    )
    return cnx


def pg_get_group_id(cnx: psycopg2._psycopg.connection, display_name: str) -> str:
    sql = """
        select id
        from prst_groups
        where display_name = %(display_name)s
    """
    params = {"display_name": display_name}
    with cnx:
        cur: psycopg2.extras.RealDictCursor
        with cnx.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row.get("id")


def pg_get_groups_to_sync(cnx: psycopg2._psycopg.connection) -> list[GroupDef]:
    sql = """
        select group_name, sql_filter
        from prst_group_sync_definitions
        order by group_name
    """
    with cnx:
        cur: psycopg2.extras.RealDictCursor
        with cnx.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return [GroupDef(row.get("group_name"), row.get("sql_filter")) for row in rows]


def pg_get_users_for_group(
    cnx: psycopg2._psycopg.connection, group_def: GroupDef
) -> list[UserDef]:
    sql = f"""
        select p.id, p.display_name
        from prst_users p
        left join lu_employee_id l on l.alias = p.user_name
        left join v_wd_current_dedup w on w.employee_id = l.employee_id
        where worker_status <> 'Terminated' and {group_def.sql_filter}
    """  # noqa: S608 (group_def.sql_filter is not user input)
    with cnx:
        cur: psycopg2.extras.RealDictCursor
        with cnx.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    plural = "s"
    if len(rows) == 1:
        plural = ""
    log.debug(f"Found {len(rows)} user{plural} for group {group_def.group_name}")
    return [UserDef(u.get("id"), u.get("display_name")) for u in rows]


def pg_upload_groups(cnx: psycopg2._psycopg.connection, batch: list[dict]) -> None:
    sql = """
        insert into prst_groups (id, display_name) values (%(id)s, %(display_name)s)
        on conflict (id) do update set display_name = excluded.display_name
    """
    with cnx, cnx.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, batch)


def pg_upload_users(cnx: psycopg2._psycopg.connection, batch: list[dict]) -> None:
    sql = """
        insert into prst_users (
            id, user_name, display_name
        ) values (
            %(id)s, %(user_name)s, %(display_name)s
        ) on conflict (id) do update set
            user_name = excluded.user_name, display_name = excluded.display_name
    """
    with cnx, cnx.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, batch)


def process_group(cnx: psycopg2._psycopg.connection, group_def: GroupDef) -> None:
    log.info(f"Processing {group_def.group_name}")
    group_id = pg_get_group_id(cnx, group_def.group_name)
    users = pg_get_users_for_group(cnx, group_def)
    for user in users:
        log.debug(f"Adding user {user.display_name} to group {group_def.group_name}")
        prst_add_group_member(group_id, user.id)


def prst_add_group_member(group_id: str, user_id: str) -> None:
    url = f"https://public-api.process.st/api/scim/Groups/{group_id}"
    payload = {
        "Operations": [{"op": "Add", "path": "members", "value": [{"value": user_id}]}],
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
    }
    with httpx.Client(
        headers={"Content-Type": "application/scim+json", "X-API-Key": PRST_API_KEY}
    ) as client:
        resp = client.patch(url, json=payload)
        resp.raise_for_status()


def prst_yield_groups() -> typing.Iterator[list]:
    page_size = 100
    url = "https://public-api.process.st/api/scim/Groups"
    params = {"count": page_size, "startIndex": 1}
    with httpx.Client(headers={"X-API-Key": PRST_API_KEY}) as client:
        has_more = True
        while has_more:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            payload = resp.json()
            group_list = payload.get("Resources", [])
            if group_list:
                yield [
                    {"id": g.get("id"), "display_name": g.get("displayName")}
                    for g in group_list
                ]
                params["startIndex"] = params["startIndex"] + page_size
            else:
                has_more = False


def prst_yield_users() -> typing.Iterable[list]:
    page_size = 100
    url = "https://public-api.process.st/api/scim/Users"
    params = {"count": page_size, "startIndex": 1}
    with httpx.Client(headers={"X-Api-Key": PRST_API_KEY}) as client:
        has_more = True
        while has_more:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            payload = resp.json()
            user_list = payload.get("Resources", [])
            if user_list:
                yield [
                    {
                        "id": u.get("id"),
                        "user_name": u.get("userName"),
                        "display_name": u.get("displayName"),
                    }
                    for u in user_list
                ]
                params["startIndex"] = params["startIndex"] + page_size
            else:
                has_more = False


def main_job(repeat_interval_hours: int | None = None) -> None:
    start = time.monotonic()
    log.info("sync-prst-user-groups starting")

    cnx = pg_get_connection()
    for batch in prst_yield_users():
        pg_upload_users(cnx, batch)
    for batch in prst_yield_groups():
        pg_upload_groups(cnx, batch)
    for group in pg_get_groups_to_sync(cnx):
        process_group(cnx, group)

    if repeat_interval_hours:
        plural = "" if repeat_interval_hours == 1 else "s"
        repeat_message = f"see you again in {repeat_interval_hours} hour{plural}"
    else:
        repeat_message = "quitting"
    duration = datime.pretty_duration_short(int(time.monotonic() - start))
    log.info(f"sync-prst-user-groups completed in {duration}, {repeat_message}")


def main() -> None:
    notch.configure()
    repeat = os.getenv("REPEAT", "false").lower() in ("1", "on", "true", "yes")
    if repeat:
        repeat_interval_hours = int(os.getenv("REPEAT_INTERVAL_HOURS", "24"))
        plural = "" if repeat_interval_hours == 1 else "s"
        log.info(f"This job will repeat every {repeat_interval_hours} hour{plural}")
        log.info(
            "Change this value by setting the "
            "REPEAT_INTERVAL_HOURS environment variable"
        )
        scheduler = BlockingScheduler()
        scheduler.add_job(
            main_job,
            "interval",
            args=[repeat_interval_hours],
            hours=repeat_interval_hours,
        )
        scheduler.add_job(main_job, args=[repeat_interval_hours])
        scheduler.start()
    else:
        main_job()


def handle_sigterm(_signal: int, _frame: types.FrameType | None) -> None:
    sys.exit()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
