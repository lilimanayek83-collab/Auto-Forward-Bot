#!/usr/bin/env python3
"""
Telegram Channel Auto-Forwarder Bot v7.6  (Manual Userbot Login + Toggle)
--------------------------------------------------------------------------
Start:   python3 forward_bot_11.py
Login:   Use the "🔐 Userbot Login" button or /setupuserbot

Changes in v7.6 (this revision):
  - FIX: live auto-forward could silently do nothing for a source the bot
    account was never actually added to (e.g. it was set via "forward a
    message from it", which only resolves cached peer metadata — not real
    membership). In that case Telegram never delivers NewMessage/
    MessageEdited updates to the bot session for that chat at all, so
    on_channel_post simply never fired — no error, just nothing happening.
    The userbot's live-capture handler previously only picked up messages
    from OTHER BOTS, assuming bot_client always handled human senders
    itself; that assumption broke for sources the bot can't actually see.
    Each batch's source now has its real bot-account access checked
    (check_bot_access / bot_has_access, cached on the batch) — when the bot
    lacks access, the userbot captures EVERY message from that source
    (human or bot-authored) instead of just bot-authored ones, since it's
    the only session that will ever see them. Existing batches created
    before this fix are re-checked automatically at startup
    (refresh_all_batch_access) so you don't need to recreate them.
    REQUIREMENT (same as bot-authored capture): when the userbot is doing
    this on a bot-inaccessible source, it needs to be a member (ideally
    admin) of every TARGET in that batch too, since it has to send what it
    fetches — file references don't transfer between sessions.

Changes in v7.5 (carried forward):
  - FIX: sign_in() only caught PhoneCodeInvalidError, so an *expired* code
    (PhoneCodeExpiredError — "The confirmation code has expired") fell
    through to the generic except-block, killed the login session outright,
    and forced you to restart /setupuserbot from scratch. It's now caught
    explicitly: on expiry the bot automatically calls send_code_request()
    again and asks you to enter the *new* code, without losing your place
    in the flow.
  - FIX: do_bulk_forward() always fetched (and sent) via bot_client, even
    when the userbot was logged in. If the bot account itself isn't
    admin/member of the source channel — even though the userbot is — bulk
    forward failed outright with "channel is private / no permission",
    despite delete-sync (which already uses the userbot) working fine on
    the same channel. It now falls back to user_client for both the fetch
    AND the send when bot_client can't reach the source (file references
    are session-bound, so fetch and send must use the same client), and
    tags each forwarded entry's `via` correctly so later edit/delete-sync
    picks the right client automatically.

Changes in v7.4 (carried forward):
  - FIX: userbot "logged in?" detection was checking user_client.is_connected(),
    which is True the instant the process starts (main() connects the userbot
    client just to probe it) — long before anyone has actually logged in. That
    made the menu falsely show "🟢 Userbot Active" from boot, and tapping the
    login button then crashed (get_me() on an unauthorized session returns
    None, and the old code did me.first_name on it). Login-state is now
    tracked with a cached `userbot_authorized` flag that is only set True
    after a real, successful is_user_authorized() check / login.
  - NEW: the userbot is now a genuine ON/OFF toggle, independent of whether
    it's logged in. `userbot_enabled` is persisted in Mongo (meta collection)
    so it survives restarts. Buttons: "Userbot: ON/OFF" toggles it, "Logout
    Userbot" fully logs the session out. All userbot-driven behavior (delete
    sync AND the bot-message capture below) checks this flag first and
    does nothing while it's off — no data is deleted/forwarded via the
    userbot unless you've explicitly turned it on.
  - NEW / IMPORTANT FIX: messages authored by OTHER BOTS in a source group
    were never reaching this bot at all. This is a Telegram-side restriction,
    not a filter bug: Telegram does not deliver messages sent by other bots
    to a bot-token session in a group, under any privacy-mode / admin
    setting. The only way to see them is a real user session. The userbot
    (once logged in AND toggled on) now also listens for NewMessage/
    MessageEdited on source chats, but ONLY processes messages whose sender
    is a bot — everything else continues to be handled by bot_client exactly
    as before, so nothing is double-forwarded.
    REQUIREMENT: because Telegram file references are bound to whichever
    session fetched them, a message captured by the userbot must also be
    *sent* by the userbot (not handed off to bot_client) — so the userbot
    account needs to be a member (ideally admin, so sends don't fail) of any
    target chat that a bot-authored source message needs to land in. Each
    forwarded copy now remembers which client (bot or user) sent it, so
    later edit-sync edits it with the matching client.
  - Background tasks (login's run_until_disconnected, keepalive, bulk
    forward) are now kept via a module-level task-registry helper instead of
    bare asyncio.create_task(), since asyncio only holds a weak reference to
    unreferenced tasks and can garbage-collect them mid-flight.

Changes from v7.0 (carried forward):
  - on_channel_post / on_channel_edit accept GROUPS as a source, not just
    broadcast channels.
  - Resolver for private-chat links t.me/c/<internal_id>/<msg_id>.

Changes in v7.2 (carried forward):
  - Per-batch MEMBER FILTER for group sources via /set_target_member
    (alias /add_target_member) and /remove_target_member.

Changes in v7.3 (carried forward):
  - Per-batch KEYWORD FILTER via /add_filter / /remove_filter. A batch with
    both a member filter and a keyword filter needs both to match.
  - Note: a group admin posting "as the channel" (anonymous admin) hides
    their real user ID from everyone, including the userbot — a member
    filter can never match those, but the keyword filter still can since it
    only inspects text.
"""

import asyncio, logging, os, re
from typing import Dict, List, Optional, Tuple

import motor.motor_asyncio
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.custom import Button
from telethon.tl.types import (
    MessageMediaWebPage, MessageService, UpdateDeleteChannelMessages,
    ChatInviteAlready, ChatInvitePeek, PeerUser,
)
from telethon.tl.functions.channels import GetChannelsRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.errors import (
    ChatAdminRequiredError, FloodWaitError, MessageDeleteForbiddenError,
    MessageIdInvalidError, MessageNotModifiedError,
    SessionPasswordNeededError, PhoneNumberInvalidError,
    PhoneCodeInvalidError, PhoneCodeExpiredError, PasswordHashInvalidError,
    InviteHashExpiredError, InviteHashInvalidError, UserAlreadyParticipantError,
    UsernameNotOccupiedError, UsernameInvalidError, ChannelPrivateError,
)

# --- Config ---
load_dotenv()
def _env(key, required=True, default=""):
    val = os.environ.get(key, default).strip().strip('"').strip("'")
    if not val and required:
        raise RuntimeError(f"Missing .env key: {key}")
    return val

BOT_TOKEN  = _env("BOT_TOKEN")
API_ID     = int(_env("API_ID"))
API_HASH   = _env("API_HASH")
PHONE      = _env("PHONE",     required=False)
MONGO_URL  = _env("MONGO_URL")
MONGO_DB   = _env("MONGO_DB",  required=False, default="forwarder")
ADMIN_IDS  = [int(x.strip()) for x in _env("ADMIN").split(",") if x.strip().lstrip("-").isdigit()]
KEEPALIVE  = 180
DEVICE_MODEL, SYS_VER, APP_VER = "Samsung Galaxy S21 Ultra", "Android 13", "9.6.1 (3882)"

logging.basicConfig(format="%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# --- MongoDB ---
_mongo = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URL)
_db = _mongo[MONGO_DB]
col_batches, col_msgmap, col_meta = _db["batches"], _db["message_map"], _db["meta"]

async def db_init():
    await col_batches.create_index("source_id")
    await col_msgmap.create_index([("source_chat_id", 1), ("source_msg_id", 1)], unique=True, name="src_lookup")
    log.info("MongoDB indexes ensured.")

async def db_next_id():
    r = await col_meta.find_one_and_update({"_id":"batch_counter"}, {"$inc":{"value":1}}, upsert=True, return_document=True)
    return r["value"]

async def db_save_batch(bid, batch): await col_batches.replace_one({"_id":bid}, {"_id":bid, **batch}, upsert=True)
async def db_del_batch(bid): await col_batches.delete_one({"_id":bid})
async def db_load_batches():
    batches = {}
    async for d in col_batches.find():
        bid = d.pop("_id"); batches[bid] = d
    return batches

def _mmid(sc, sm): return f"{sc}:{sm}"

async def db_save_msgmap(sc, sm, targets):
    """targets: list of (chat_id, msg_id, via) where via is 'bot' or 'user'."""
    doc = {
        "_id": _mmid(sc, sm), "source_chat_id": sc, "source_msg_id": sm,
        "targets": [{"chat_id": c, "msg_id": m, "via": v} for (c, m, v) in targets],
    }
    await col_msgmap.replace_one({"_id": doc["_id"]}, doc, upsert=True)

async def db_del_msgmap(sc, sm): await col_msgmap.delete_one({"_id":_mmid(sc,sm)})

async def db_load_msgmap():
    mm = {}
    async for d in col_msgmap.find():
        mm[(d["source_chat_id"], d["source_msg_id"])] = [
            (t["chat_id"], t["msg_id"], t.get("via", "bot")) for t in d["targets"]
        ]
    return mm

async def db_load_userbot_enabled():
    doc = await col_meta.find_one({"_id": "userbot_state"})
    return bool(doc.get("enabled", False)) if doc else False

async def db_save_userbot_enabled(val: bool):
    await col_meta.update_one({"_id": "userbot_state"}, {"$set": {"enabled": bool(val)}}, upsert=True)

# --- Clients ---
bot_client = TelegramClient("forwarder_bot_session", API_ID, API_HASH, flood_sleep_threshold=60)

USER_SESSION_NAME = "forwarder_user_session"

def _new_user_client():
    """A fresh TelegramClient instance. Needed because Telethon raises
    'TelegramClient instance cannot be reused after logging out' if you try
    to connect()/login again on the same object post log_out() — so logout
    always has to be followed by building a brand new client, not reusing
    the old one, whether it's the same phone number or a different one."""
    return TelegramClient(USER_SESSION_NAME, API_ID, API_HASH,
                          device_model=DEVICE_MODEL, system_version=SYS_VER,
                          app_version=APP_VER, flood_sleep_threshold=60)

user_client = _new_user_client() if PHONE else None

# --- Caches ---
batches_cache = {}
message_map = {}
setup_sessions = {}
login_sessions = {}

# Cached userbot state — never infer "logged in" from is_connected()! See v7.4 notes.
userbot_authorized = False   # True only after a real is_user_authorized()/login success
userbot_enabled = False      # optional runtime toggle, persisted in Mongo
_userbot_handlers_registered = False  # handlers are bound to a specific client object — reset on logout
_keepalive_running = False   # only one keepalive_loop() should ever be alive at a time

# Keep strong references to background tasks so asyncio can't GC them mid-flight.
_bg_tasks = set()
def _spawn(coro):
    t = asyncio.create_task(coro)
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)
    return t

AWAIT_SOURCE, AWAIT_TARGET, AWAIT_BULK_FIRST, AWAIT_BULK_LAST, AWAIT_TARGET_MEMBER, AWAIT_FILTER_KEYWORD = range(6)
LOGIN_AWAIT_PHONE, LOGIN_AWAIT_CODE, LOGIN_AWAIT_PASSWORD = range(10,13)

# --- Utilities ---
def is_admin(uid): return uid in ADMIN_IDS
def is_human(user): return not getattr(user, "bot", False)
def canonical_id(raw):
    s = str(raw)
    if s.startswith("-100"): return raw
    if raw < 0: return raw
    return int(f"-100{raw}")
def same_channel(a,b): return canonical_id(a) == canonical_id(b)
def batches_for_source(sid):
    cid = canonical_id(sid)
    return [b for b in batches_cache.values() if b.get("source_id") == cid]
def all_source_ids(): return list({b["source_id"] for b in batches_cache.values() if "source_id" in b})
def batch_summary(bid, batch):
    targets = batch.get("targets",[])
    tlist = "\n".join(f"  └ {t['name']}" for t in targets) or "  (none yet)"
    members = batch.get("target_members", [])
    if members:
        mlist = "\n".join(f"  └ {m['name']} (`{m['id']}`)" for m in members)
        mtext = f"\n👤 Member filter (only these senders forwarded):\n{mlist}"
    else:
        mtext = "\n👤 Member filter: none (all senders forwarded)"
    keywords = batch.get("filter_keywords", [])
    if keywords:
        klist = ", ".join(f"`{k}`" for k in keywords)
        ktext = f"\n🔎 Keyword filter (message must contain one of): {klist}"
    else:
        ktext = "\n🔎 Keyword filter: none (any message content forwarded)"
    return f"**Batch #{bid}**\n🔵 Source : `{batch['source_name']}`\n🎯 Targets:\n{tlist}{mtext}{ktext}"

async def resolve_name(eid):
    try:
        ent = await bot_client.get_entity(eid)
        return getattr(ent,"title",None) or getattr(ent,"first_name",None) or getattr(ent,"username",None) or str(eid)
    except: return str(eid)

async def safe_reply(ev, text, buttons=None):
    await ev.respond(text, buttons=buttons) if buttons else await ev.respond(text)

async def _get_sender_safe(event):
    try:
        return await event.get_sender()
    except Exception:
        return None

# --- Build dynamic main menu (uses cached flags only — never is_connected()) ---
def build_main_menu():
    if not userbot_authorized:
        ub_row = [Button.inline("🔐 Userbot Login", b"m:userbot_login")]
    else:
        state_btn = (Button.inline("🟢 Userbot: ON (tap to turn OFF)", b"m:userbot_toggle")
                     if userbot_enabled else
                     Button.inline("⚪ Userbot: OFF (tap to turn ON)", b"m:userbot_toggle"))
        ub_row = [state_btn, Button.inline("🚪 Logout Userbot", b"m:userbot_logout")]
    return [
        [Button.inline("📦 New Batch",     b"m:newbatch"),  Button.inline("📋 List Batches",  b"m:listbatches")],
        [Button.inline("🗑 Delete Batch",  b"m:deletebatch"), Button.inline("📊 Status",        b"m:status")],
        [Button.inline("➕ Add Target",    b"m:addtarget"),  Button.inline("➖ Remove Target", b"m:removetarget")],
        [Button.inline("📥 Bulk Forward",  b"m:bulkforward"), Button.inline("❓ Help",          b"m:help")],
        [Button.inline("👤 Set Target Member", b"m:set_target_member"), Button.inline("🚫 Remove Target Member", b"m:remove_target_member")],
        [Button.inline("🔎 Add Filter",    b"m:add_filter"),  Button.inline("🚫 Remove Filter", b"m:remove_filter")],
        ub_row,
    ]

HELP_TEXT = (
    "🤖 **Channel Auto-Forwarder Bot v7.6**\n\n"
    "**Commands:**\n"
    "/newbatch /done /cancelbatch /listbatches /deletebatch /addtarget /removetarget /bulkforward /status\n"
    "/setupuserbot /toggleuserbot /logoutuserbot\n"
    "/set_target_member (alias: /add_target_member) /done_members /remove_target_member\n"
    "/add_filter /done_filters /remove_filter\n\n"
    "**Member filter (forward only specific sender(s) from a source group):**\n"
    "1️⃣ /add_target_member (same as /set_target_member) — pick a batch, then send the member's "
    "numeric ID, @username, or forward a message from them (repeat to add more, then /done_members)\n"
    "2️⃣ /remove_target_member — pick a batch, then pick which member to remove\n\n"
    "**Keyword filter (forward only messages containing a word/phrase):**\n"
    "1️⃣ /add_filter — pick a batch, then send a keyword (repeat to add more, then /done_filters)\n"
    "2️⃣ /remove_filter — pick a batch, then pick which keyword to remove\n\n"
    "A batch with BOTH a member filter and a keyword filter only forwards messages that match "
    "BOTH — e.g. member @example_bot AND text containing `Dual-Audio`.\n"
    "With no filters set, a batch forwards every message from the source group (default, unchanged behavior).\n\n"
    "**Setting a source/target channel or group** — use any of:\n"
    "1️⃣ Forward any message from it\n"
    "2️⃣ Send its invite link (t.me/joinchat/... or t.me/+...)\n"
    "3️⃣ Send its numeric ID (e.g. -1001234567890)\n"
    "4️⃣ Send its private link (t.me/c/xxxxxxxxxx/msgid) — works once the bot is already a member/admin there\n\n"
    "**🔐 Userbot Login / Toggle / Logout:**\n"
    "Use the button or /setupuserbot to log in your phone number (number, OTP, and password if 2FA).\n"
    "Once logged in, the userbot's actual behavior is gated by a separate ON/OFF toggle "
    "(/toggleuserbot or the button) that's saved and survives restarts — logging in does NOT "
    "automatically mean it's actively deleting/forwarding anything until you switch it ON.\n"
    "/logoutuserbot fully logs the session out (you'll need to log in again to re-enable it).\n\n"
    "**Why some source messages don't show up without the userbot:**\n"
    "Telegram never delivers messages authored by *other bots* in a group to a bot-token session — "
    "this is a platform restriction, not a filter setting. Once the userbot is logged in AND toggled "
    "ON, it captures those bot-authored messages itself (everything else keeps going through the bot "
    "as usual — nothing is double-forwarded). For this to work, the userbot account also needs to be "
    "a member (ideally admin) of any target chat those messages get forwarded into, since a message "
    "it captures has to be sent by it too (Telegram file references don't transfer between sessions).\n\n"
    "**When the bot account isn't actually in the source:** a source added via 'forward a message from "
    "it' only needs cached info to resolve a name — it doesn't prove the bot is really a member/admin "
    "there. If it isn't, Telegram never delivers live updates to the bot for that chat at all, and bulk "
    "forward fails with a private/permission error. This is detected automatically: when it happens, "
    "BOTH live forwarding and bulk forward for that source run entirely through the userbot instead "
    "(provided it's logged in and toggled ON) — make sure the userbot account is a member/admin of the "
    "source AND every target in that batch, since it has to send what it fetches too.\n\n"
    "_⚠️ Bot must be admin in all channels/groups for forwarding + delete-sync to work reliably."
    " Phone account must be member of source channels and of any target that needs bot-authored"
    " messages relayed, or that bulk forward needs to fall back to it for."
    " Group privacy mode must be OFF (or bot must be admin) for it to see group messages at all."
    " Forwarding a message from a group only reveals the sender, not the group, so use a link"
    " or numeric ID for groups when setting the source/target._"
)

# --- Delete Sync Core ---
async def _do_delete_sync(channel_id_raw, msg_ids):
    cid = canonical_id(channel_id_raw)
    if not any(b.get("source_id")==cid for b in batches_cache.values()): return
    for mid in msg_ids:
        key = (cid, mid)
        if key not in message_map: continue
        targets = message_map.pop(key)
        asyncio.create_task(db_del_msgmap(cid, mid))
        for tid, tmid, via in targets:
            try:
                await bot_client.delete_messages(tid, [tmid])
                log.info(f"🗑  delete-synced msg {mid} → {tid}:{tmid}")
            except (MessageDeleteForbiddenError, ChatAdminRequiredError):
                log.warning(f"No delete permission in {tid}")
            except MessageIdInvalidError: pass
            except Exception as e:
                log.error(f"Delete-sync {tid}:{tmid}: {e}")

@bot_client.on(events.Raw(UpdateDeleteChannelMessages))
async def on_delete_bot(event): await _do_delete_sync(event.channel_id, list(event.messages))

def _register_userbot_handlers():
    """Registers all userbot-side listeners exactly once. Every one of them
    checks `userbot_enabled` first — logging in does not, by itself, turn any
    of this on; see the ON/OFF toggle."""

    @user_client.on(events.Raw(UpdateDeleteChannelMessages))
    async def on_delete_userbot(event):
        if not userbot_enabled: return
        await _do_delete_sync(event.channel_id, list(event.messages))

    @user_client.on(events.NewMessage(func=lambda e: not e.is_private))
    async def on_channel_post_userbot(event):
        if not userbot_enabled: return
        source_id = event.chat_id
        if not batches_for_source(source_id): return
        if source_has_bot_access(source_id):
            # Normal case: bot_client is actually in this chat and handles
            # human-authored messages itself. The userbot only needs to
            # pick up messages from OTHER BOTS, which Telegram never
            # delivers to a bot-token session.
            sender = await _get_sender_safe(event)
            if not sender or not getattr(sender, "bot", False):
                return  # non-bot senders are already captured by bot_client
        # else: bot_client was never actually added to this source, so it
        # never receives updates for it at all — the userbot must capture
        # every message here, human or bot-authored, or live forwarding
        # for this source silently does nothing.
        await process_source_message(event.message, source_id, user_client)

    @user_client.on(events.MessageEdited(func=lambda e: not e.is_private))
    async def on_channel_edit_userbot(event):
        if not userbot_enabled: return
        msg = event.message; key = (canonical_id(event.chat_id), msg.id)
        if key not in message_map: return
        await _apply_edit_sync(msg, key)

async def activate_delete_sync(source_ids=None):
    if not user_client or not user_client.is_connected(): return
    ids = source_ids if source_ids is not None else all_source_ids()
    if not ids: return
    for cid in ids:
        try:
            await user_client(GetChannelsRequest([cid]))
            await user_client.get_messages(cid, limit=1)
            log.info(f"🔔 delete-sync stream activated for {cid}")
        except Exception as e: log.warning(f"Could not activate delete-sync for {cid}: {e}")
        await asyncio.sleep(0.5)

async def keepalive_loop():
    while True:
        await asyncio.sleep(KEEPALIVE)
        if userbot_enabled:
            await activate_delete_sync()
            log.info("🔄 Keep-alive completed.")

# --- Bot-access detection (v7.6) ---
# A source can be added to a batch via "forward a message from it", which
# only needs cached peer metadata to resolve a display name — it does NOT
# prove the bot account is actually a member/admin there. If it isn't,
# Telegram never delivers NewMessage/MessageEdited updates for that chat to
# the bot session at all (silently — no error, the handler just never
# fires), and GetMessagesRequest for bulk forward fails outright. Each
# batch's source is checked once here and the result cached as
# `bot_has_access` so the userbot knows whether it needs to capture
# EVERYTHING from that source (not just other bots' messages) or just its
# usual bot-authored-message duty.
async def check_bot_access(source_id):
    try:
        await bot_client(GetChannelsRequest([source_id]))
        await bot_client.get_messages(source_id, limit=1)
        return True
    except Exception as e:
        log.warning(f"Bot account lacks access to {source_id}: {e}")
        return False

def source_has_bot_access(source_id):
    """True if bot_client can see this source at all — i.e. the userbot only
    needs to pick up bot-authored messages here (its normal job). False means
    the bot was never actually added to this chat, so the userbot must
    capture every message from it, human or bot-authored, since bot_client
    will never receive updates for it."""
    matched = batches_for_source(source_id)
    if not matched:
        return True
    return any(b.get("bot_has_access", True) for b in matched)

async def refresh_all_batch_access():
    """Re-checks bot_has_access for every existing batch. Run once at
    startup so batches created before v7.6 (or where the bot was later
    removed from a source) self-correct without needing to be recreated."""
    for bid, batch in list(batches_cache.items()):
        if "source_id" not in batch: continue
        access = await check_bot_access(batch["source_id"])
        if batch.get("bot_has_access") != access:
            batch["bot_has_access"] = access
            await db_save_batch(bid, batch)
            log.info(f"Batch #{bid} ('{batch.get('source_name')}') bot access: "
                     f"{'yes' if access else 'NO — userbot will handle all capture for this source'}")
        await asyncio.sleep(0.3)

# --- Message copy ---
async def copy_msg(msg, target_id, client=None):
    """client defaults to bot_client. When relaying a message captured by the
    userbot, pass client=user_client — Telegram file references are bound to
    whichever session fetched them, so the same client must send it."""
    client = client or bot_client
    if msg is None or isinstance(msg, MessageService): return None
    markup = msg.reply_markup
    try:
        if msg.media and not isinstance(msg.media, MessageMediaWebPage):
            sent = await client.send_file(target_id, file=msg.media, caption=msg.message or "",
                                          formatting_entities=msg.entities or [], buttons=markup, supports_streaming=True)
        else:
            sent = await client.send_message(target_id, message=msg.message or "",
                                             formatting_entities=msg.entities or [], buttons=markup,
                                             link_preview=isinstance(msg.media, MessageMediaWebPage))
        if isinstance(sent, list): return sent[0].id if sent else None
        return sent.id
    except FloodWaitError as e:
        log.warning(f"FloodWait {e.seconds}s"); await asyncio.sleep(e.seconds+2); return None
    except Exception as e:
        log.error(f"copy_msg → {target_id}: {e}"); return None

# --- Shared forwarding pipeline (used by both bot_client and userbot capture) ---
async def process_source_message(msg, source_id, client):
    via = "bot" if client is bot_client else "user"
    matched = batches_for_source(source_id)
    if not matched: return
    sent = []
    msg_text_lower = (msg.message or "").lower()
    for batch in matched:
        members = batch.get("target_members")
        if members:
            allowed_ids = {m["id"] for m in members}
            if msg.sender_id not in allowed_ids:
                continue  # this batch only forwards specific member(s); sender isn't one of them
        keywords = batch.get("filter_keywords")
        if keywords:
            if not any(kw.lower() in msg_text_lower for kw in keywords):
                continue  # this batch only forwards messages containing one of its keywords
        for target in batch["targets"]:
            new_id = await copy_msg(msg, target["id"], client=client)
            if new_id is not None:
                sent.append((target["id"], new_id, via))
                log.info(f"✉️  msg {msg.id} '{batch['source_name']}' → '{target['name']}' (via {via})")
    if sent:
        key = (canonical_id(source_id), msg.id)
        message_map[key] = sent
        await db_save_msgmap(key[0], key[1], sent)

async def _apply_edit_sync(msg, key):
    entries = message_map.get(key)
    if not entries: return
    markup = msg.reply_markup
    has_media = msg.media and not isinstance(msg.media, MessageMediaWebPage)
    is_webpage = isinstance(msg.media, MessageMediaWebPage)
    updated = []
    for (tid, tmid, via) in list(entries):
        editor = user_client if (via == "user" and user_client) else bot_client
        try:
            if has_media:
                await editor.edit_message(tid, tmid, text=msg.message or "", file=msg.media,
                                          formatting_entities=msg.entities or [], buttons=markup)
            else:
                await editor.edit_message(tid, tmid, text=msg.message or "",
                                          formatting_entities=msg.entities or [], buttons=markup, link_preview=is_webpage)
            updated.append((tid, tmid, via))
            log.info(f"✏️  edit-synced {msg.id} → {tid}:{tmid} (via {via})")
        except MessageNotModifiedError:
            updated.append((tid, tmid, via))
        except Exception as e:
            log.warning(f"edit fallback {tid}:{tmid} ({e})")
            try:
                await bot_client.delete_messages(tid, [tmid])
                new_id = await copy_msg(msg, tid, client=editor)
                if new_id is not None:
                    updated.append((tid, new_id, via))
            except Exception as e2: log.error(f"Edit fallback failed: {e2}")
    message_map[key] = updated
    await db_save_msgmap(key[0], key[1], updated)

# --- Bot events: new post / edit ---
# NOTE: previously `not e.is_private and not e.is_group` silently excluded
# every GROUP source, even with the bot admin and privacy mode off. Groups are
# allowed through — only private 1:1 chats with the bot are excluded.
# The bot-sender skip below is defensive: Telegram doesn't deliver other
# bots' messages to a bot-token session anyway, so this branch should rarely
# trigger, but it keeps bot_client and the userbot's capture cleanly
# non-overlapping if that ever changes.
@bot_client.on(events.NewMessage(func=lambda e: not e.is_private))
async def on_channel_post(event):
    msg = event.message; source_id = event.chat_id
    if not batches_for_source(source_id): return
    sender = await _get_sender_safe(event)
    if sender is not None and getattr(sender, "bot", False):
        return  # bot-authored — handled by the userbot capture instead, if enabled
    await process_source_message(msg, source_id, bot_client)

@bot_client.on(events.MessageEdited(func=lambda e: not e.is_private))
async def on_channel_edit(event):
    msg = event.message; key = (canonical_id(event.chat_id), msg.id)
    if key not in message_map: return
    await _apply_edit_sync(msg, key)

# --- Bulk forward ---
async def do_bulk_forward(report_chat_id, batch, source_id, first_id, last_id):
    """
    Fetches (and sends) via bot_client by default. If bot_client can't reach
    the source at all (e.g. "channel is private and you lack permission" —
    the bot isn't a member/admin there even though the userbot is), this
    falls back to fetching AND sending through user_client instead. Fetch
    and send must use the same client because Telegram file references are
    bound to whichever session originally fetched them — mixing clients
    would break media on the forwarded copies.
    """
    log.info(f"📥 bulk_forward src={source_id} {first_id}→{last_id}")
    id_list = list(range(first_id, last_id+1))
    fetch_client = bot_client
    try:
        raw_msgs = await bot_client.get_messages(source_id, ids=id_list)
    except Exception as e:
        if user_client and userbot_authorized and userbot_enabled and user_client.is_connected():
            log.warning(f"bot_client couldn't fetch {source_id} ({e}); retrying via userbot")
            try:
                raw_msgs = await user_client.get_messages(source_id, ids=id_list)
                fetch_client = user_client
            except Exception as e2:
                await bot_client.send_message(report_chat_id, f"❌ **Bulk Forward Failed**: {e2}"); return
        else:
            hint = ""
            if user_client and not (userbot_authorized and userbot_enabled):
                hint = "\n_Tip: the userbot isn't logged in / turned ON — enabling it may let bulk forward fall back to it for sources the bot itself can't reach._"
            await bot_client.send_message(report_chat_id, f"❌ **Bulk Forward Failed**: {e}{hint}"); return

    msgs = [m for m in raw_msgs if m and not isinstance(m, MessageService)]
    copied = skipped = errors = flood = 0
    for idx, msg in enumerate(msgs):
        any_ok = False
        for target in batch["targets"]:
            try:
                new_id = await copy_msg(msg, target["id"], client=fetch_client)
                if new_id is not None:
                    any_ok = True
                    key = (canonical_id(source_id), msg.id)
                    via = "user" if fetch_client is user_client else "bot"
                    message_map.setdefault(key, []).append((target["id"], new_id, via))
                    await db_save_msgmap(key[0], key[1], message_map[key])
                else: errors += 1
            except FloodWaitError as e: flood += e.seconds; await asyncio.sleep(e.seconds+1)
            except Exception as e: errors += 1; log.error(f"bulk copy {msg.id} → {target['name']}: {e}")
        if any_ok: copied += 1
        else: skipped += 1
        await asyncio.sleep(0.05)
        if (idx+1)%25==0: await asyncio.sleep(1.0)
    flood_note = f"\n⏱ Flood-wait total: **{flood}s**" if flood else ""
    via_note = "\n_Sent via userbot (bot account lacks access to the source)._" if fetch_client is user_client else ""
    await bot_client.send_message(report_chat_id,
        f"✅ **Bulk Forward Complete!**\nRange: `{first_id}` → `{last_id}`\nCopied: {copied}, Skipped: {skipped}, Errors: {errors}{flood_note}{via_note}",
        buttons=build_main_menu())

# --- Human + admin check decorator ---
def human_admin(func):
    async def wrapper(event):
        sender = await event.get_sender()
        if not is_admin(event.sender_id) or not is_human(sender): return
        return await func(event)
    return wrapper

# --- Command handlers ---
@bot_client.on(events.NewMessage(pattern=r"^/start(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_start(event):
    await safe_reply(event, "👋 **Channel Auto-Forwarder Bot v7.6**\n\nUse the buttons below.", buttons=build_main_menu())

@bot_client.on(events.NewMessage(pattern=r"^/help(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_help(event): await safe_reply(event, HELP_TEXT, buttons=build_main_menu())

@bot_client.on(events.NewMessage(pattern=r"^/setupuserbot(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_setupuserbot(event):
    await handle_userbot_login_start(event)

@bot_client.on(events.NewMessage(pattern=r"^/toggleuserbot(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_toggleuserbot(event):
    await handle_userbot_toggle(event)

@bot_client.on(events.NewMessage(pattern=r"^/logoutuserbot(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_logoutuserbot(event):
    await handle_userbot_logout(event)

@bot_client.on(events.NewMessage(pattern=r"^/newbatch(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_newbatch(event):
    uid = event.sender_id; bid = await db_next_id()
    setup_sessions[uid] = {"state": AWAIT_SOURCE, "batch_id": bid}
    await safe_reply(event, f"📦 **New Batch #{bid} — Step 1**\n"
                             f"Set the source channel/group:\n"
                             f"1️⃣ Forward any message from it\n"
                             f"2️⃣ Send its invite link\n"
                             f"3️⃣ Send its numeric ID\n"
                             f"4️⃣ Send its private link (t.me/c/xxxxxxxxxx/msgid)")

@bot_client.on(events.NewMessage(pattern=r"^/done(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_done(event):
    uid = event.sender_id; session = setup_sessions.get(uid)
    if not session or session.get("state") not in (AWAIT_SOURCE, AWAIT_TARGET):
        await safe_reply(event, "ℹ️ No active setup."); return
    bid = session["batch_id"]; batch = batches_cache.get(bid)
    if not batch or not batch.get("targets"):
        await safe_reply(event, "❌ Source or targets missing."); return
    del setup_sessions[uid]
    _spawn(activate_delete_sync([batch["source_id"]]))
    await safe_reply(event, f"✅ **Batch #{bid} is live!**\n\n{batch_summary(bid, batch)}", buttons=build_main_menu())

@bot_client.on(events.NewMessage(pattern=r"^/cancelbatch(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_cancelbatch(event):
    uid = event.sender_id; session = setup_sessions.pop(uid, None)
    if not session: await safe_reply(event, "ℹ️ No active setup."); return
    batches_cache.pop(session["batch_id"], None); await db_del_batch(session["batch_id"])
    await safe_reply(event, f"❌ Batch #{session['batch_id']} cancelled.")

@bot_client.on(events.NewMessage(pattern=r"^/listbatches(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_listbatches(event):
    complete = {bid:b for bid,b in batches_cache.items() if "source_id" in b}
    if not complete: await safe_reply(event, "📭 No batches."); return
    parts = ["📋 **All Forwarding Batches**\n"] + [batch_summary(bid,b) for bid,b in complete.items()]
    await safe_reply(event, "\n\n".join(parts), buttons=build_main_menu())

@bot_client.on(events.NewMessage(pattern=r"^/deletebatch(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_deletebatch(event):
    complete = {bid:b for bid,b in batches_cache.items() if "source_id" in b}
    if not complete: await safe_reply(event, "📭 No batches."); return
    btns = [[Button.inline(f"🗑 Batch #{bid} — {b['source_name']}", f"del:{bid}".encode())] for bid,b in complete.items()]
    btns.append([Button.inline("❌ Cancel", b"del:cancel")])
    await safe_reply(event, "🗑 **Select batch to delete:**", buttons=btns)

@bot_client.on(events.NewMessage(pattern=r"^/addtarget(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_addtarget(event):
    complete = {bid:b for bid,b in batches_cache.items() if "source_id" in b}
    if not complete: await safe_reply(event, "📭 No batches."); return
    btns = [[Button.inline(f"Batch #{bid} — {b['source_name']}", f"addtgt:{bid}".encode())] for bid,b in complete.items()]
    btns.append([Button.inline("❌ Cancel", b"addtgt:cancel")])
    await safe_reply(event, "➕ **Add target to which batch?**", buttons=btns)

@bot_client.on(events.NewMessage(pattern=r"^/removetarget(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_removetarget(event):
    complete = {bid:b for bid,b in batches_cache.items() if "source_id" in b and b.get("targets")}
    if not complete: await safe_reply(event, "📭 No batches with targets."); return
    btns = [[Button.inline(f"Batch #{bid} — {b['source_name']}", f"rmtgt_b:{bid}".encode())] for bid,b in complete.items()]
    btns.append([Button.inline("❌ Cancel", b"rmtgt_b:cancel")])
    await safe_reply(event, "➖ **Remove target from which batch?**", buttons=btns)

@bot_client.on(events.NewMessage(pattern=r"^/status(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_status(event):
    complete = {bid:b for bid,b in batches_cache.items() if "source_id" in b}
    total_targets = sum(len(b.get("targets",[])) for b in complete.values())
    dsync = _delete_sync_status_text()
    await safe_reply(event,
        f"📊 **Bot Status**\n\nActive Batches: {len(complete)}\nTotal Targets: {total_targets}\nTracked Messages: {len(message_map)}\nUserbot: {dsync}",
        buttons=build_main_menu())

def _delete_sync_status_text():
    if not userbot_authorized:
        return "❌ Not logged in"
    if not userbot_enabled:
        return "⚪ Logged in, but OFF (tap 'Userbot: OFF' to enable)"
    if user_client and user_client.is_connected():
        return "🟢 ON — delete-sync + bot-message capture active"
    return "🟡 ON, but connection dropped (will retry)"

@bot_client.on(events.NewMessage(pattern=r"^/bulkforward(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_bulkforward(event):
    complete = {bid:b for bid,b in batches_cache.items() if "source_id" in b}
    if not complete: await safe_reply(event, "📭 No batches."); return
    btns = [[Button.inline(f"Batch #{bid} — {b['source_name']}", f"bulk:{bid}".encode())] for bid,b in complete.items()]
    btns.append([Button.inline("❌ Cancel", b"bulk:cancel")])
    await safe_reply(event, "📥 **Bulk Forward — Select Batch**", buttons=btns)

# --- Member-filter menus (shared by command + callback entry points) ---
def _batches_with_source():
    return {bid:b for bid,b in batches_cache.items() if "source_id" in b}

def _batches_with_members():
    return {bid:b for bid,b in batches_cache.items() if "source_id" in b and b.get("target_members")}

def render_set_target_member_menu():
    complete = _batches_with_source()
    if not complete: return "📭 No batches. Create one with /newbatch first.", build_main_menu()
    btns = [[Button.inline(f"Batch #{bid} — {b['source_name']}", f"setmem:{bid}".encode())] for bid,b in complete.items()]
    btns.append([Button.inline("❌ Cancel", b"setmem:cancel")])
    return "👤 **Set Target Member — select the batch to filter:**", btns

def render_remove_target_member_menu():
    complete = _batches_with_members()
    if not complete: return "📭 No batches currently have a member filter set.", build_main_menu()
    btns = [[Button.inline(f"Batch #{bid} — {b['source_name']}", f"rmmem_b:{bid}".encode())] for bid,b in complete.items()]
    btns.append([Button.inline("❌ Cancel", b"rmmem_b:cancel")])
    return "🚫 **Remove Target Member — select the batch:**", btns

@bot_client.on(events.NewMessage(pattern=r"^/set_target_member(@\w+)?$", func=lambda e: e.is_private))
@bot_client.on(events.NewMessage(pattern=r"^/add_target_member(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_set_target_member(event):
    text, btns = render_set_target_member_menu()
    await safe_reply(event, text, buttons=btns)

@bot_client.on(events.NewMessage(pattern=r"^/done_members(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_done_members(event):
    uid = event.sender_id; session = setup_sessions.get(uid)
    if not session or session.get("state") != AWAIT_TARGET_MEMBER:
        await safe_reply(event, "ℹ️ No active member-filter setup."); return
    bid = session["batch_id"]; batch = batches_cache.get(bid)
    del setup_sessions[uid]
    if not batch:
        await safe_reply(event, "❌ Session error.", buttons=build_main_menu()); return
    await safe_reply(event, f"✅ Member filter saved.\n\n{batch_summary(bid, batch)}", buttons=build_main_menu())

@bot_client.on(events.NewMessage(pattern=r"^/remove_target_member(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_remove_target_member(event):
    text, btns = render_remove_target_member_menu()
    await safe_reply(event, text, buttons=btns)

# --- Keyword-filter menus ---
def _batches_with_keywords():
    return {bid:b for bid,b in batches_cache.items() if "source_id" in b and b.get("filter_keywords")}

def render_add_filter_menu():
    complete = _batches_with_source()
    if not complete: return "📭 No batches. Create one with /newbatch first.", build_main_menu()
    btns = [[Button.inline(f"Batch #{bid} — {b['source_name']}", f"addfilter:{bid}".encode())] for bid,b in complete.items()]
    btns.append([Button.inline("❌ Cancel", b"addfilter:cancel")])
    return "🔎 **Add Filter Keyword — select the batch:**", btns

def render_remove_filter_menu():
    complete = _batches_with_keywords()
    if not complete: return "📭 No batches currently have a keyword filter set.", build_main_menu()
    btns = [[Button.inline(f"Batch #{bid} — {b['source_name']}", f"rmfilter_b:{bid}".encode())] for bid,b in complete.items()]
    btns.append([Button.inline("❌ Cancel", b"rmfilter_b:cancel")])
    return "🚫 **Remove Filter Keyword — select the batch:**", btns

@bot_client.on(events.NewMessage(pattern=r"^/add_filter(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_add_filter(event):
    text, btns = render_add_filter_menu()
    await safe_reply(event, text, buttons=btns)

@bot_client.on(events.NewMessage(pattern=r"^/done_filters(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_done_filters(event):
    uid = event.sender_id; session = setup_sessions.get(uid)
    if not session or session.get("state") != AWAIT_FILTER_KEYWORD:
        await safe_reply(event, "ℹ️ No active filter setup."); return
    bid = session["batch_id"]; batch = batches_cache.get(bid)
    del setup_sessions[uid]
    if not batch:
        await safe_reply(event, "❌ Session error.", buttons=build_main_menu()); return
    await safe_reply(event, f"✅ Keyword filter saved.\n\n{batch_summary(bid, batch)}", buttons=build_main_menu())

@bot_client.on(events.NewMessage(pattern=r"^/remove_filter(@\w+)?$", func=lambda e: e.is_private))
@human_admin
async def cmd_remove_filter(event):
    text, btns = render_remove_filter_menu()
    await safe_reply(event, text, buttons=btns)

# --- Callback Query Handler ---
@bot_client.on(events.CallbackQuery())
@human_admin
async def on_callback(event):
    await event.answer(); data = event.data.decode("utf-8"); uid = event.sender_id
    async def edit(text, buttons=None): await event.edit(text, buttons=buttons)
    if data == "m:userbot_login": await handle_userbot_login_start(event); return
    if data == "m:userbot_toggle": await handle_userbot_toggle(event); return
    if data == "m:userbot_logout": await handle_userbot_logout(event); return
    if data.startswith("m:"):
        cmd = data[2:]
        if cmd == "newbatch":
            bid = await db_next_id(); setup_sessions[uid] = {"state":AWAIT_SOURCE, "batch_id":bid}
            await edit(f"📦 **New Batch #{bid} — Step 1**\n"
                       f"Set the source channel/group:\n"
                       f"1️⃣ Forward any message from it\n"
                       f"2️⃣ Send its invite link\n"
                       f"3️⃣ Send its numeric ID\n"
                       f"4️⃣ Send its private link (t.me/c/xxxxxxxxxx/msgid)")
        elif cmd == "listbatches":
            complete = {bid:b for bid,b in batches_cache.items() if "source_id" in b}
            if not complete: await edit("📭 No batches.", buttons=build_main_menu()); return
            parts = ["📋 **All Batches**\n"] + [batch_summary(bid,b) for bid,b in complete.items()]
            await edit("\n\n".join(parts), buttons=build_main_menu())
        elif cmd == "deletebatch":
            complete = {bid:b for bid,b in batches_cache.items() if "source_id" in b}
            if not complete: await edit("📭 No batches.", buttons=build_main_menu()); return
            btns = [[Button.inline(f"🗑 #{bid} — {b['source_name']}", f"del:{bid}".encode())] for bid,b in complete.items()]
            btns.append([Button.inline("❌ Cancel", b"del:cancel")])
            await edit("🗑 **Select batch to delete:**", buttons=btns)
        elif cmd == "status":
            complete = {bid:b for bid,b in batches_cache.items() if "source_id" in b}
            total_targets = sum(len(b.get("targets",[])) for b in complete.values())
            dsync = _delete_sync_status_text()
            await edit(f"📊 **Bot Status**\n\nBatches: {len(complete)} | Targets: {total_targets} | Tracked: {len(message_map)}\nUserbot: {dsync}", buttons=build_main_menu())
        elif cmd == "addtarget":
            complete = {bid:b for bid,b in batches_cache.items() if "source_id" in b}
            if not complete: await edit("📭 No batches.", buttons=build_main_menu()); return
            btns = [[Button.inline(f"#{bid} — {b['source_name']}", f"addtgt:{bid}".encode())] for bid,b in complete.items()]
            btns.append([Button.inline("❌ Cancel", b"addtgt:cancel")])
            await edit("➕ **Add target to which batch?**", buttons=btns)
        elif cmd == "removetarget":
            complete = {bid:b for bid,b in batches_cache.items() if "source_id" in b and b.get("targets")}
            if not complete: await edit("📭 No batches with targets.", buttons=build_main_menu()); return
            btns = [[Button.inline(f"#{bid} — {b['source_name']}", f"rmtgt_b:{bid}".encode())] for bid,b in complete.items()]
            btns.append([Button.inline("❌ Cancel", b"rmtgt_b:cancel")])
            await edit("➖ **Remove target from which batch?**", buttons=btns)
        elif cmd == "bulkforward":
            complete = {bid:b for bid,b in batches_cache.items() if "source_id" in b}
            if not complete: await edit("📭 No batches.", buttons=build_main_menu()); return
            btns = [[Button.inline(f"#{bid} — {b['source_name']}", f"bulk:{bid}".encode())] for bid,b in complete.items()]
            btns.append([Button.inline("❌ Cancel", b"bulk:cancel")])
            await edit("📥 **Bulk Forward — Select Batch**", buttons=btns)
        elif cmd == "help": await edit(HELP_TEXT, buttons=build_main_menu())
        elif cmd == "set_target_member":
            text, btns = render_set_target_member_menu(); await edit(text, buttons=btns)
        elif cmd == "remove_target_member":
            text, btns = render_remove_target_member_menu(); await edit(text, buttons=btns)
        elif cmd == "add_filter":
            text, btns = render_add_filter_menu(); await edit(text, buttons=btns)
        elif cmd == "remove_filter":
            text, btns = render_remove_filter_menu(); await edit(text, buttons=btns)
        return
    if data.startswith("del:"):
        part = data[4:]
        if part == "cancel": await edit("Cancelled.", buttons=build_main_menu()); return
        bid = int(part); batch = batches_cache.pop(bid, None); await db_del_batch(bid)
        await edit(f"✅ Batch #{bid} deleted." if batch else "❌ Not found.", buttons=build_main_menu())
    elif data.startswith("addtgt:"):
        part = data[7:]
        if part == "cancel": await edit("Cancelled.", buttons=build_main_menu()); return
        bid = int(part)
        if bid not in batches_cache: await edit("❌ Batch not found.", buttons=build_main_menu()); return
        setup_sessions[uid] = {"state":AWAIT_TARGET, "batch_id":bid}
        await edit(f"📦 **Batch #{bid}** — set the target channel/group:\n"
                   f"1️⃣ Forward any message from it\n"
                   f"2️⃣ Send its invite link\n"
                   f"3️⃣ Send its numeric ID\n"
                   f"4️⃣ Send its private link (t.me/c/xxxxxxxxxx/msgid)")
    elif data.startswith("rmtgt_b:"):
        part = data[8:]
        if part == "cancel": await edit("Cancelled.", buttons=build_main_menu()); return
        bid = int(part); batch = batches_cache.get(bid)
        if not batch or not batch.get("targets"): await edit("❌ Not found.", buttons=build_main_menu()); return
        btns = [[Button.inline(f"🎯 {t['name']}", f"rmtgt_do:{bid}:{t['id']}".encode())] for t in batch["targets"]]
        btns.append([Button.inline("❌ Cancel", b"rmtgt_do:cancel")])
        await edit(f"➖ Select target to remove from Batch #{bid}:", buttons=btns)
    elif data.startswith("rmtgt_do:"):
        parts = data.split(":")
        if parts[1] == "cancel": await edit("Cancelled.", buttons=build_main_menu()); return
        bid, target_id = int(parts[1]), int(parts[2])
        batch = batches_cache.get(bid)
        if not batch: await edit("❌ Batch not found.", buttons=build_main_menu()); return
        before = len(batch["targets"])
        batch["targets"] = [t for t in batch["targets"] if t["id"] != target_id]
        if len(batch["targets"]) < before:
            await db_save_batch(bid, batch); await edit(f"✅ Target removed from Batch #{bid}.", buttons=build_main_menu())
        else: await edit("❌ Target not found.", buttons=build_main_menu())
    elif data.startswith("bulk:"):
        part = data[5:]
        if part == "cancel": await edit("Cancelled.", buttons=build_main_menu()); return
        bid = int(part); batch = batches_cache.get(bid)
        if not batch or not batch.get("targets"): await edit("❌ Batch not found or no targets.", buttons=build_main_menu()); return
        setup_sessions[uid] = {"state":AWAIT_BULK_FIRST, "batch_id":bid, "source_id":batch["source_id"], "report_chat":event.chat_id}
        await edit(f"📥 **Bulk Forward — Batch #{bid}**\nSource: `{batch['source_name']}`\n\nStep 1: Forward the **FIRST** message.")
    elif data.startswith("setmem:"):
        part = data[7:]
        if part == "cancel": await edit("Cancelled.", buttons=build_main_menu()); return
        bid = int(part)
        if bid not in batches_cache: await edit("❌ Batch not found.", buttons=build_main_menu()); return
        setup_sessions[uid] = {"state": AWAIT_TARGET_MEMBER, "batch_id": bid}
        b = batches_cache[bid]
        await edit(f"👤 **Batch #{bid}** — source: `{b['source_name']}`\n\n"
                   f"Send the member to filter on:\n"
                   f"1️⃣ Their numeric user ID\n"
                   f"2️⃣ Their @username\n"
                   f"3️⃣ Forward a message from them (only works if they haven't hidden forward attribution)\n\n"
                   f"Send one at a time — repeat to add more, then /done_members to finish.")
    elif data.startswith("rmmem_b:"):
        part = data[8:]
        if part == "cancel": await edit("Cancelled.", buttons=build_main_menu()); return
        bid = int(part); batch = batches_cache.get(bid)
        if not batch or not batch.get("target_members"):
            await edit("❌ That batch has no member filter.", buttons=build_main_menu()); return
        btns = [[Button.inline(f"👤 {m['name']}", f"rmmem_do:{bid}:{m['id']}".encode())] for m in batch["target_members"]]
        btns.append([Button.inline("❌ Cancel", b"rmmem_do:cancel")])
        await edit(f"🚫 Select member to remove from Batch #{bid}'s filter:", buttons=btns)
    elif data.startswith("rmmem_do:"):
        parts = data.split(":")
        if parts[1] == "cancel": await edit("Cancelled.", buttons=build_main_menu()); return
        bid, member_id = int(parts[1]), int(parts[2])
        batch = batches_cache.get(bid)
        if not batch: await edit("❌ Batch not found.", buttons=build_main_menu()); return
        before = len(batch.get("target_members", []))
        batch["target_members"] = [m for m in batch.get("target_members", []) if m["id"] != member_id]
        if len(batch["target_members"]) < before:
            await db_save_batch(bid, batch)
            note = "all senders will now be forwarded (no filter left)" if not batch["target_members"] else "remaining filter still applies"
            await edit(f"✅ Member removed from Batch #{bid}'s filter — {note}.", buttons=build_main_menu())
        else:
            await edit("❌ Member not found in that batch's filter.", buttons=build_main_menu())
    elif data.startswith("addfilter:"):
        part = data[10:]
        if part == "cancel": await edit("Cancelled.", buttons=build_main_menu()); return
        bid = int(part)
        if bid not in batches_cache: await edit("❌ Batch not found.", buttons=build_main_menu()); return
        setup_sessions[uid] = {"state": AWAIT_FILTER_KEYWORD, "batch_id": bid}
        b = batches_cache[bid]
        await edit(f"🔎 **Batch #{bid}** — source: `{b['source_name']}`\n\n"
                   f"Send a word or keyword to filter on (case-insensitive substring match "
                   f"against the message text).\n\n"
                   f"Send one at a time — repeat to add more, then /done_filters to finish.")
    elif data.startswith("rmfilter_b:"):
        part = data[11:]
        if part == "cancel": await edit("Cancelled.", buttons=build_main_menu()); return
        bid = int(part); batch = batches_cache.get(bid)
        if not batch or not batch.get("filter_keywords"):
            await edit("❌ That batch has no keyword filter.", buttons=build_main_menu()); return
        btns = [[Button.inline(f"🔎 {kw}", f"rmfilter_do:{bid}:{i}".encode())] for i, kw in enumerate(batch["filter_keywords"])]
        btns.append([Button.inline("❌ Cancel", b"rmfilter_do:cancel")])
        await edit(f"🚫 Select keyword to remove from Batch #{bid}'s filter:", buttons=btns)
    elif data.startswith("rmfilter_do:"):
        parts = data.split(":")
        if parts[1] == "cancel": await edit("Cancelled.", buttons=build_main_menu()); return
        bid, idx = int(parts[1]), int(parts[2])
        batch = batches_cache.get(bid)
        if not batch or not batch.get("filter_keywords") or idx >= len(batch["filter_keywords"]):
            await edit("❌ Keyword not found.", buttons=build_main_menu()); return
        removed = batch["filter_keywords"].pop(idx)
        await db_save_batch(bid, batch)
        note = "all message content will now be forwarded (no keyword filter left)" if not batch["filter_keywords"] else "remaining keywords still apply"
        await edit(f"✅ Keyword `{removed}` removed from Batch #{bid}'s filter — {note}.", buttons=build_main_menu())

# --- Login flow ---
async def handle_userbot_login_start(event):
    global userbot_authorized
    if userbot_authorized:
        me = await user_client.get_me()
        name = me.first_name if me else "unknown"
        await safe_reply(event, f"🟢 Already logged in as {name}.\nUse the ON/OFF button to toggle behavior, or 'Logout Userbot' to log out.", buttons=build_main_menu()); return
    if user_client is None:
        await safe_reply(event, "❌ PHONE isn't set in .env, so no userbot client was created. Set PHONE and restart to enable this."); return
    uid = event.sender_id; login_sessions.pop(uid, None)
    login_sessions[uid] = {"state": LOGIN_AWAIT_PHONE}
    await safe_reply(event, "📱 **Step 1** – Send your phone number (e.g. +919876543210).\n/cancel to abort.",
                     buttons=[[Button.inline("❌ Cancel", b"login_cancel")]])

async def handle_userbot_toggle(event):
    global userbot_enabled
    if not userbot_authorized:
        await safe_reply(event, "❌ Not logged in yet — use 'Userbot Login' first.", buttons=build_main_menu()); return
    userbot_enabled = not userbot_enabled
    await db_save_userbot_enabled(userbot_enabled)
    if userbot_enabled:
        _spawn(activate_delete_sync())
        msg = "🟢 Userbot turned **ON** — delete-sync and bot-authored-message capture are now active."
    else:
        msg = "⚪ Userbot turned **OFF** — delete-sync and bot-authored-message capture are now paused (still logged in)."
    await safe_reply(event, msg, buttons=build_main_menu())

async def handle_userbot_logout(event):
    global userbot_authorized, userbot_enabled, user_client, _userbot_handlers_registered
    if not userbot_authorized:
        await safe_reply(event, "ℹ️ Userbot isn't logged in.", buttons=build_main_menu()); return
    try:
        await user_client.log_out()
    except Exception as e:
        log.error(f"logout: {e}")
    finally:
        try:
            await user_client.disconnect()
        except Exception:
            pass
    # Telethon will not let you connect()/sign_in() again on a client object
    # that has been logged out — not with the same number, not with a
    # different one. Drop the old session file and build a brand new client
    # so the next login (whatever phone number) starts clean.
    try:
        session_file = f"{USER_SESSION_NAME}.session"
        if os.path.exists(session_file):
            os.remove(session_file)
    except Exception as e:
        log.warning(f"Could not remove old userbot session file: {e}")
    user_client = _new_user_client()
    _userbot_handlers_registered = False  # handlers were bound to the old client object
    userbot_authorized = False
    userbot_enabled = False
    await db_save_userbot_enabled(False)
    await safe_reply(event, "🚪 Userbot logged out. Delete-sync and bot-message capture are off until you log in again.", buttons=build_main_menu())

async def process_login_step(event):
    uid = event.sender_id; session = login_sessions.get(uid)
    if not session: return
    text = event.message.text.strip()
    if text.lower() == "/cancel":
        login_sessions.pop(uid, None); await safe_reply(event, "❌ Cancelled.", buttons=build_main_menu()); return
    state = session["state"]
    if state == LOGIN_AWAIT_PHONE:
        phone = text
        if not phone.startswith("+"): await safe_reply(event, "❌ Invalid format."); return
        session["phone"] = phone; session["state"] = LOGIN_AWAIT_CODE
        try:
            if not user_client.is_connected(): await user_client.connect()
            await user_client.send_code_request(phone)
        except PhoneNumberInvalidError:
            await safe_reply(event, "❌ Invalid phone number."); login_sessions.pop(uid); return
        except Exception as e:
            log.error(f"send_code_request: {e}"); await safe_reply(event, f"❌ Error: {e}"); login_sessions.pop(uid); return
        await safe_reply(event, "📩 **Step 2** – OTP sent. Reply with the code.", buttons=[[Button.inline("❌ Cancel", b"login_cancel")]])
    elif state == LOGIN_AWAIT_CODE:
        code = text; phone = session["phone"]
        try:
            await user_client.sign_in(phone, code)
        except SessionPasswordNeededError:
            session["state"] = LOGIN_AWAIT_PASSWORD
            await safe_reply(event, "🔐 **2FA** – Reply with your password.", buttons=[[Button.inline("❌ Cancel", b"login_cancel")]])
            return
        except PhoneCodeInvalidError:
            await safe_reply(event, "❌ Invalid code. Try again."); return
        except PhoneCodeExpiredError:
            # Request a fresh code instead of just failing outright — keeps
            # the user in the same login flow instead of forcing a restart
            # via /setupuserbot. Note: if the number is currently being
            # rate-limited/flagged by Telegram for repeated login attempts,
            # the new code can fail too — in that case wait a while before
            # retrying that specific number.
            try:
                await user_client.send_code_request(phone)
                await safe_reply(event, "⌛ That code expired (often means an older code was requested/used, "
                                         "or repeated login attempts on this number are being throttled by Telegram). "
                                         "I've sent a **new** code — reply with it now.",
                                 buttons=[[Button.inline("❌ Cancel", b"login_cancel")]])
            except Exception as e:
                log.error(f"resend after expiry: {e}")
                await safe_reply(event, f"❌ Code expired, and resend failed too: {e}\n"
                                         f"This usually means Telegram is throttling repeated login attempts on this "
                                         f"number — wait 10-15+ minutes before trying again, then use /setupuserbot.")
                login_sessions.pop(uid, None)
            return
        except Exception as e:
            log.error(f"sign_in: {e}"); await safe_reply(event, f"❌ Login failed: {e}"); login_sessions.pop(uid); return
        await _finalize_login(uid, event)
    elif state == LOGIN_AWAIT_PASSWORD:
        password = text
        try:
            await user_client.sign_in(password=password)
        except PasswordHashInvalidError:
            await safe_reply(event, "❌ Incorrect password. Try again."); return
        except Exception as e:
            log.error(f"sign_in (password): {e}"); await safe_reply(event, f"❌ Login failed: {e}"); login_sessions.pop(uid); return
        await _finalize_login(uid, event)

async def _finalize_login(uid, event):
    global userbot_authorized, userbot_enabled, _userbot_handlers_registered, _keepalive_running
    login_sessions.pop(uid, None); me = await user_client.get_me()
    log.info(f"👤 Userbot logged in as {me.first_name} (@{me.username})")
    userbot_authorized = True
    if not _userbot_handlers_registered:
        _register_userbot_handlers(); _userbot_handlers_registered = True
    userbot_enabled = True
    await db_save_userbot_enabled(True)
    await activate_delete_sync()
    if not _keepalive_running:
        _keepalive_running = True
        _spawn(keepalive_loop())
    _spawn(user_client.run_until_disconnected())
    await safe_reply(event, f"✅ **Userbot logged in and turned ON** – {me.first_name}\nDelete-sync and bot-message capture are now active. Toggle it off any time from the menu.", buttons=build_main_menu())

# --- Private message handler (login + batch setup) ---
def _extract_fwd(msg):
    fwd = getattr(msg, "fwd_from", None)
    if not fwd: return None, None
    from_id = getattr(fwd, "from_id", None)
    channel_id = getattr(from_id, "channel_id", None) if from_id else None
    msg_id = getattr(fwd, "channel_post", None)
    return channel_id, msg_id

# --- Channel/group resolution: forward / invite link / username link / c-link / numeric id ---
INVITE_RE = re.compile(r"(?:https?://)?t\.me/(?:joinchat/|\+)([\w-]+)", re.I)
USERNAME_RE = re.compile(r"(?:https?://)?t\.me/([A-Za-z]\w{3,31})/?$", re.I)
# t.me/c/<internal_id>/<msg_id> — private-chat "copy link" format. Not an
# invite hash and not a public username, so it needs its own resolver.
CLINK_RE = re.compile(r"(?:https?://)?t\.me/c/(\d+)(?:/\d+)?", re.I)

def extract_invite_hash(text):
    m = INVITE_RE.search(text)
    return m.group(1) if m else None

def extract_public_username(text):
    text = text.strip()
    if text.startswith("@") and len(text) > 1:
        return text[1:]
    m = USERNAME_RE.search(text)
    return m.group(1) if m else None

def extract_clink_id(text):
    m = CLINK_RE.search(text)
    return int(m.group(1)) if m else None

def looks_numeric_id(text):
    return bool(re.fullmatch(r"-?\d+", text.strip()))

async def resolve_by_clink(text):
    raw = extract_clink_id(text)
    if raw is None: return None
    cid = canonical_id(raw)
    try:
        ent = await bot_client.get_entity(cid)
    except Exception as e:
        raise ValueError(
            f"Bot doesn't recognize that group/channel yet ({e}). Make sure the bot is "
            f"already an admin/member there (and, for groups, that privacy mode is off or "
            f"the bot is admin), then try again."
        )
    name = getattr(ent, "title", None) or getattr(ent, "username", None) or str(cid)
    return cid, name

async def resolve_by_invite_link(text):
    h = extract_invite_hash(text)
    if not h: return None
    try:
        result = await bot_client(CheckChatInviteRequest(h))
    except (InviteHashExpiredError, InviteHashInvalidError):
        raise ValueError("That invite link is invalid or expired.")
    except Exception as e:
        raise ValueError(f"Could not check invite link: {e}")
    if isinstance(result, ChatInviteAlready):
        chat = result.chat
        return canonical_id(chat.id), (getattr(chat, "title", None) or str(chat.id))
    if isinstance(result, ChatInvitePeek) and getattr(result, "chat", None):
        chat = result.chat
        return canonical_id(chat.id), (getattr(chat, "title", None) or str(chat.id))
    # Not yet a member — try to join so we get the chat object with its id
    try:
        updates = await bot_client(ImportChatInviteRequest(h))
        chats = getattr(updates, "chats", [])
        if chats:
            chat = chats[0]
            return canonical_id(chat.id), (getattr(chat, "title", None) or str(chat.id))
    except UserAlreadyParticipantError:
        pass
    except Exception as e:
        raise ValueError(
            f"Could not join via invite link ({e}). Add the bot as admin manually, "
            f"then use its numeric ID instead."
        )
    raise ValueError(
        "Could not resolve that invite link. Add the bot as admin manually and use "
        "its numeric ID instead."
    )

async def resolve_by_username(text):
    uname = extract_public_username(text)
    if not uname: return None
    try:
        ent = await bot_client.get_entity(uname)
    except (UsernameNotOccupiedError, UsernameInvalidError):
        raise ValueError("That username/link doesn't exist.")
    except Exception as e:
        raise ValueError(f"Could not resolve @{uname}: {e}")
    cid = canonical_id(ent.id)
    name = getattr(ent, "title", None) or getattr(ent, "username", None) or str(cid)
    return cid, name

async def resolve_by_numeric_id(text):
    if not looks_numeric_id(text): return None
    raw = int(text.strip())
    cid = canonical_id(raw)
    try:
        ent = await bot_client.get_entity(cid)
    except Exception as e:
        raise ValueError(
            f"Bot doesn't recognize that ID yet ({e}). Make sure the bot is already an "
            f"admin/member there and has received at least one update from it, then try again — "
            f"or use the invite link / forward-a-message method instead."
        )
    name = getattr(ent, "title", None) or getattr(ent, "username", None) or str(cid)
    return cid, name

async def resolve_member_input(msg):
    """
    Resolve a group MEMBER (not a channel/group) to filter forwarding by, from:
    1) a forwarded message authored by them (if not attribution-hidden),
    2) their @username, 3) their numeric user ID.
    Returns (user_id, display_name) or raises ValueError with a user-facing message.
    """
    fwd = getattr(msg, "fwd_from", None)
    if fwd:
        from_id = getattr(fwd, "from_id", None)
        if isinstance(from_id, PeerUser):
            uid = from_id.user_id
            name = await resolve_name(uid)
            return uid, name
        if getattr(fwd, "from_name", None):
            raise ValueError(
                "That user hides who forwarded messages come from — send their numeric "
                "user ID or @username instead."
            )

    text = (msg.text or "").strip()
    if not text:
        raise ValueError("Send the member's numeric user ID, @username, or forward a message from them.")

    if text.startswith("@") and len(text) > 1:
        uname = text[1:]
        try:
            ent = await bot_client.get_entity(uname)
        except (UsernameNotOccupiedError, UsernameInvalidError):
            raise ValueError("That username doesn't exist.")
        except Exception as e:
            raise ValueError(f"Could not resolve @{uname}: {e}")
        name = getattr(ent, "first_name", None) or getattr(ent, "username", None) or str(ent.id)
        return ent.id, name

    if looks_numeric_id(text):
        uid = int(text)
        name = await resolve_name(uid)
        return uid, name

    raise ValueError(
        "Couldn't recognize that. Send the member's numeric user ID, @username, "
        "or forward a message from them."
    )

async def resolve_channel_input(msg):
    """
    Resolve a source/target channel or group from any of:
    1) a forwarded message, 2) an invite link, 3) a public t.me/username link,
    4) a t.me/c/<id>/<msg> private link, 5) a numeric ID.
    Returns (canonical_id, name) or raises ValueError with a user-facing message.
    """
    fwd_channel_id, _ = _extract_fwd(msg)
    if fwd_channel_id:
        full_id = canonical_id(fwd_channel_id)
        name = await resolve_name(fwd_channel_id)
        return full_id, name

    text = (msg.text or "").strip()
    if not text:
        raise ValueError(
            "Send one of: forward a message from it, its invite link, its t.me/c/ link, or its numeric ID."
        )

    for resolver in (resolve_by_clink, resolve_by_invite_link, resolve_by_username, resolve_by_numeric_id):
        result = await resolver(text)
        if result:
            return result

    raise ValueError(
        "Couldn't recognize that. Send one of:\n"
        "1️⃣ Forward any message from the group/channel\n"
        "2️⃣ Its invite link (t.me/joinchat/... or t.me/+...)\n"
        "3️⃣ Its private link (t.me/c/xxxxxxxxxx/msgid)\n"
        "4️⃣ Its numeric ID (e.g. -1001234567890)\n\n"
        "Note: forwarding a message from a normal group only reveals the sender, "
        "not the group — use the invite link, t.me/c/ link, or numeric ID for groups."
    )

@bot_client.on(events.NewMessage(func=lambda e: e.is_private))
@human_admin
async def handle_private(event):
    uid = event.sender_id; msg = event.message
    if uid in login_sessions:
        await process_login_step(event); return
    if msg.text and msg.text.lstrip().startswith("/"): return
    session = setup_sessions.get(uid)
    if not session:
        await safe_reply(event, "ℹ️ No active setup. Use /newbatch.", buttons=build_main_menu()); return
    state = session["state"]

    if state in (AWAIT_BULK_FIRST, AWAIT_BULK_LAST):
        fwd_channel_id, fwd_msg_id = _extract_fwd(msg)
        if not fwd_channel_id or not fwd_msg_id:
            await safe_reply(event, "❌ Please forward a message."); return
        if not same_channel(fwd_channel_id, session["source_id"]):
            await safe_reply(event, "❌ Message not from source channel."); return
        bid = session["batch_id"]; batch = batches_cache.get(bid)
        if not batch: await safe_reply(event, "❌ Session error."); setup_sessions.pop(uid); return
        if state == AWAIT_BULK_FIRST:
            session["first_msg_id"] = fwd_msg_id; session["state"] = AWAIT_BULK_LAST
            await safe_reply(event, f"✅ First message set (ID `{fwd_msg_id}`). Now forward the LAST message.")
        else:
            first_id, last_id = session["first_msg_id"], fwd_msg_id
            if last_id < first_id: first_id, last_id = last_id, first_id
            del setup_sessions[uid]
            await safe_reply(event, f"✅ Range: `{first_id}` → `{last_id}`. Bulk forward started…")
            _spawn(do_bulk_forward(session.get("report_chat", uid), batch, session["source_id"], first_id, last_id))
        return

    if state == AWAIT_FILTER_KEYWORD:
        text = (msg.text or "").strip()
        if not text:
            await safe_reply(event, "❌ Send a word or keyword (plain text)."); return
        bid = session["batch_id"]; batch = batches_cache.get(bid)
        if not batch:
            await safe_reply(event, "❌ Session error."); setup_sessions.pop(uid); return
        keywords = batch.setdefault("filter_keywords", [])
        if any(k.lower() == text.lower() for k in keywords):
            await safe_reply(event, "⚠️ Already in the filter for this batch."); return
        keywords.append(text)
        await db_save_batch(bid, batch)
        await safe_reply(event, f"✅ Keyword added: `{text}`\n"
                                 f"Send another, or /done_filters to finish.")
        return

    if state == AWAIT_TARGET_MEMBER:
        try:
            member_id, member_name = await resolve_member_input(msg)
        except ValueError as e:
            await safe_reply(event, f"❌ {e}"); return
        bid = session["batch_id"]; batch = batches_cache.get(bid)
        if not batch:
            await safe_reply(event, "❌ Session error."); setup_sessions.pop(uid); return
        members = batch.setdefault("target_members", [])
        if any(m["id"] == member_id for m in members):
            await safe_reply(event, "⚠️ Already in the filter for this batch."); return
        members.append({"id": member_id, "name": member_name})
        await db_save_batch(bid, batch)
        await safe_reply(event, f"✅ Member added: `{member_name}` (`{member_id}`)\n"
                                 f"Send another, or /done_members to finish.")
        return

    if state in (AWAIT_SOURCE, AWAIT_TARGET):
        try:
            full_id, channel_name = await resolve_channel_input(msg)
        except ValueError as e:
            await safe_reply(event, f"❌ {e}"); return
        bid = session["batch_id"]
        if state == AWAIT_SOURCE:
            bot_access = await check_bot_access(full_id)
            batches_cache[bid] = {"source_id": full_id, "source_name": channel_name, "targets": [],
                                   "bot_has_access": bot_access}
            await db_save_batch(bid, batches_cache[bid]); session["state"] = AWAIT_TARGET
            warn = ""
            if not bot_access:
                warn = ("\n\n⚠️ **The bot account doesn't actually have access to this source** "
                        "(it can resolve the name but can't fetch messages — usually means it was "
                        "never added as admin/member there, just seen via a forward). Live forwarding "
                        "and bulk forward for this source will run entirely through the **userbot** "
                        "instead. Make sure the userbot account is a member/admin of this source AND "
                        "of every target you add below.")
            await safe_reply(event, f"✅ Source set: `{channel_name}`{warn}\n"
                                     f"Now add target channels/groups (forward a message, send an invite "
                                     f"link, a t.me/c/ link, or a numeric ID), then /done.")
        else:  # AWAIT_TARGET
            batch = batches_cache.get(bid)
            if not batch: await safe_reply(event, "❌ Session error."); setup_sessions.pop(uid); return
            if same_channel(full_id, batch["source_id"]): await safe_reply(event, "⚠️ Same as source."); return
            if any(t["id"] == full_id for t in batch["targets"]): await safe_reply(event, "⚠️ Already a target."); return
            batch["targets"].append({"id": full_id, "name": channel_name})
            await db_save_batch(bid, batch)
            await safe_reply(event, f"✅ Target #{len(batch['targets'])} added: `{channel_name}`\nMore targets or /done.")
        return

# --- Main ---
async def main():
    global batches_cache, message_map, userbot_authorized, userbot_enabled, _userbot_handlers_registered, _keepalive_running
    log.info("Starting Channel Forwarder Bot v7.6…")
    await db_init()
    batches_cache = await db_load_batches()
    message_map   = await db_load_msgmap()
    log.info(f"Loaded {len(batches_cache)} batches, {len(message_map)} message maps.")
    await bot_client.start(bot_token=BOT_TOKEN)
    me = await bot_client.get_me()
    log.info(f"🤖 Bot: @{me.username}")
    if batches_cache:
        log.info("Checking bot access for existing batch sources…")
        await refresh_all_batch_access()
    tasks = [bot_client.run_until_disconnected()]
    if user_client is not None:
        await user_client.connect()
        userbot_authorized = await user_client.is_user_authorized()
        if userbot_authorized:
            if not _userbot_handlers_registered:
                _register_userbot_handlers(); _userbot_handlers_registered = True
            userbot_enabled = await db_load_userbot_enabled()
            me_user = await user_client.get_me()
            log.info(f"👤 Userbot: {me_user.first_name} (@{me_user.username}) — "
                      f"{'ENABLED' if userbot_enabled else 'logged in but currently OFF'}")
            if userbot_enabled:
                await activate_delete_sync()
            if not _keepalive_running:
                _keepalive_running = True
                _spawn(keepalive_loop())
            tasks.append(user_client.run_until_disconnected())
        else:
            userbot_enabled = False
            log.info("👤 Userbot not logged in. Use the button or /setupuserbot.")
            try: await bot_client.send_message(ADMIN_IDS[0], "ℹ️ **Userbot not logged in.**\nUse the button below.", buttons=build_main_menu())
            except: pass
    else:
        log.warning("⚠️  PHONE not set – userbot features (delete-sync, bot-message capture) disabled.")
    log.info("Running…")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
