import argparse
import asyncio
import getpass
import json
import sys
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import (
    ChatAdminRequiredError,
    FloodWaitError,
    SessionPasswordNeededError,
    UserAdminInvalidError,
)
from telethon.tl.functions.channels import EditBannedRequest
from telethon.tl.types import ChannelParticipantsAdmins, ChatBannedRights


PENDING_LOGIN_PATH = Path(".telegram_login_pending.json")


BAN_RIGHTS = ChatBannedRights(
    until_date=None,
    view_messages=True,
    send_messages=True,
    send_media=True,
    send_stickers=True,
    send_gifs=True,
    send_games=True,
    send_inline=True,
    embed_links=True,
    send_polls=True,
    change_info=True,
    invite_users=True,
    pin_messages=True,
)


def display_name(user):
    name = " ".join(
        part
        for part in [getattr(user, "first_name", ""), getattr(user, "last_name", "")]
        if part
    ).strip()
    username = f"@{user.username}" if getattr(user, "username", None) else ""
    return (name or f"id={user.id}", username)


def safe_print(message):
    try:
        print(message)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(message.encode("utf-8", errors="replace") + b"\n")
        sys.stdout.buffer.flush()


def load_pending_login():
    if not PENDING_LOGIN_PATH.exists():
        return None

    try:
        return json.loads(PENDING_LOGIN_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def save_pending_login(phone, phone_code_hash):
    payload = {
        "phone": phone,
        "phone_code_hash": phone_code_hash,
    }
    PENDING_LOGIN_PATH.write_text(json.dumps(payload), encoding="utf-8")


def clear_pending_login():
    try:
        PENDING_LOGIN_PATH.unlink()
    except FileNotFoundError:
        pass


async def collect_admin_ids(client, group):
    admin_ids = set()

    async for admin in client.iter_participants(group, filter=ChannelParticipantsAdmins):
        admin_ids.add(admin.id)

    return admin_ids


async def collect_targets(client, group, include_admins=False, include_bots=False, limit=None):
    me = await client.get_me()
    admin_ids = set()

    if not include_admins:
        admin_ids = await collect_admin_ids(client, group)

    targets = []
    scanned = 0

    async for user in client.iter_participants(group):
        scanned += 1

        if limit and scanned > limit:
            break

        if not user:
            continue

        if user.id == me.id:
            continue

        if not include_bots and getattr(user, "bot", False):
            continue

        if not include_admins and user.id in admin_ids:
            continue

        targets.append(user)

    return targets, scanned, admin_ids


async def remove_members(client, group, users, delay_seconds=0.75):
    removed = 0
    failed = 0

    for user in users:
        name, username = display_name(user)

        try:
            await client(EditBannedRequest(group, user, BAN_RIGHTS))
            removed += 1
            safe_print(f"[OK] removed: {name} {username}".strip())
            await asyncio.sleep(delay_seconds)
        except FloodWaitError as exc:
            wait_seconds = int(getattr(exc, "seconds", 0) or 0)
            safe_print(f"[WAIT] FloodWait {wait_seconds}s while removing {name} {username}".strip())
            await asyncio.sleep(wait_seconds + 1)

            try:
                await client(EditBannedRequest(group, user, BAN_RIGHTS))
                removed += 1
                safe_print(f"[OK] removed after wait: {name} {username}".strip())
                await asyncio.sleep(delay_seconds)
            except Exception as retry_exc:
                failed += 1
                safe_print(f"[FAIL] {name} {username} -> {retry_exc}".strip())
        except (UserAdminInvalidError, ChatAdminRequiredError) as exc:
            failed += 1
            safe_print(f"[FAIL] missing rights for {name} {username}: {exc}".strip())
        except Exception as exc:
            failed += 1
            safe_print(f"[FAIL] {name} {username} -> {exc}".strip())

    return removed, failed


async def main():
    parser = argparse.ArgumentParser(
        description="Remove Telegram group members by banning them from the group."
    )
    parser.add_argument("--api-id", type=int, required=True)
    parser.add_argument("--api-hash", required=True)
    parser.add_argument("--group", required=True, help="Example: @mygroup or https://t.me/mygroup")
    parser.add_argument("--session", default="member_cleanup")
    parser.add_argument("--phone", help="Phone number for Telegram login, e.g. +15551234567")
    parser.add_argument("--code", help="Telegram login code")
    parser.add_argument("--password", help="Telegram 2FA password if enabled")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on participants scanned")
    parser.add_argument("--include-admins", action="store_true", help="Also target admins")
    parser.add_argument("--include-bots", action="store_true", help="Also target bot accounts")
    parser.add_argument("--delay", type=float, default=0.75, help="Delay between moderation actions")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually remove users. Without this flag, the script only previews matches.",
    )
    args = parser.parse_args()

    client = TelegramClient(args.session, args.api_id, args.api_hash)
    await client.connect()

    try:
        if not await client.is_user_authorized():
            pending_login = load_pending_login()

            if args.code:
                if not pending_login:
                    raise SystemExit(
                        "Login code provided, but no pending login state was found. "
                        "Re-run with --phone first to send a fresh code."
                    )

                try:
                    await client.sign_in(
                        phone=pending_login["phone"],
                        code=args.code,
                        phone_code_hash=pending_login["phone_code_hash"],
                    )
                    clear_pending_login()
                except SessionPasswordNeededError:
                    password = args.password or getpass.getpass("Telegram 2FA password: ")
                    await client.sign_in(password=password)
                    clear_pending_login()
            else:
                if not args.phone:
                    raise SystemExit("Login required. Re-run with --phone to send a Telegram code.")

                sent = await client.send_code_request(args.phone)
                save_pending_login(args.phone, sent.phone_code_hash)
                print("[*] login code sent to Telegram.")
                print("[*] Re-run with --code to continue.")
                return

        group = await client.get_entity(args.group)

        safe_print("[*] collecting members...")
        targets, scanned, admin_ids = await collect_targets(
            client,
            group,
            include_admins=args.include_admins,
            include_bots=args.include_bots,
            limit=args.limit,
        )

        safe_print(f"[*] scanned participants: {scanned}")
        safe_print(f"[*] admin accounts detected: {len(admin_ids)}")
        safe_print(f"[*] matched removal targets: {len(targets)}")

        if not targets:
            safe_print("[!] no members matched the current filters")
            return

        safe_print("[*] preview:")
        for user in targets[:20]:
            name, username = display_name(user)
            safe_print(f"    - {name} {username}".strip())

        if len(targets) > 20:
            safe_print(f"    ... and {len(targets) - 20} more")

        if not args.apply:
            safe_print("\n[DRY RUN ONLY]")
            safe_print("Re-run with --apply to actually remove the matched members.")
            return

        removed, failed = await remove_members(
            client,
            group,
            targets,
            delay_seconds=args.delay,
        )
        safe_print(f"\n[*] done. removed={removed} failed={failed}")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
