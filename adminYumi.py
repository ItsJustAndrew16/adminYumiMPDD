# adminYumi.py — aiogram v3.7+
"""
Админ-бот (без сторонних серверов/Pyrogram).
— Кнопки: «Добавить видео-ватермарку в конец» / «Заменить ватермарку» / «Главное меню»
— Принимает: обычные видео + видео-документы (mp4/mov/webm), одиночно и альбомами
— 9×16 (1080×1920) с полосами, фикс FPS, без B-frames, сброс PTS (чтобы не было артефактов в начале)
— Прогресс: одиночное — свой %, пачка — суммарный %
— Быстро: h264_amf (AMD RX 580) при наличии, иначе libx264
— Отправка ТОЛЬКО как ВИДЕО (supports_streaming=True)
— Автосжатие ≤ 49 MB; если не влезает — повторные попытки и финальный даунскейл 720×1280
— Альбомы: корректный дебаунс — один финализатор на группу, тишина ALBUM_SETTLE_SEC или жёсткий таймаут
"""

import asyncio, contextlib, html, shutil, tempfile, time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Set, Tuple, List, Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import FSInputFile, KeyboardButton, Message, ReplyKeyboardMarkup

# --------------------- НАСТРОЙКИ ---------------------
BOT_TOKEN = "7757214060:AAF9AN7goysowEAco1miwwsyjjKjPGPNXcM"   # <-- вставь токен @BotFather
OUT_W, OUT_H = 1080, 1920
MAX_TG_VIDEO_MB = 49.0
AUDIO_KBPS = 64
SIZE_MARGIN_MB = 2
MIN_VIDEO_KBPS = 120
TARGET_FPS = 30
X264_PRESET = "superfast"

# Альбомы: пауза без новых элементов → закрываем альбом; но не дольше таймаута
ALBUM_SETTLE_SEC = 2.5
ALBUM_HARD_TIMEOUT_SEC = 20.0

# ---------------------- СТАТУСЫ ----------------------
watermark_by_chat: Dict[int, str] = {}
awaiting_wm: Set[int] = set()
processing_lock: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

# Альбомы: key=(chat_id, media_group_id)
album_buf: Dict[Tuple[int, str], Dict[int, Message]] = {}
album_timer: Dict[Tuple[int, str], asyncio.Task] = {}
album_notice: Dict[Tuple[int, str], Message] = {}
album_first_ts: Dict[Tuple[int, str], float] = {}

# ---------------------- КНОПКИ -----------------------
BTN_ADD_WM = "➕ Добавить видео-ватермарку в конец"
BTN_REPLACE_WM = "♻️ Заменить ватермарку"
BTN_MAIN_MENU = "⬅️ Главное меню"

def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=BTN_ADD_WM)]], resize_keyboard=True)

def kb_sub() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_REPLACE_WM)], [KeyboardButton(text=BTN_MAIN_MENU)]],
        resize_keyboard=True,
    )

# -------------------- FFmpeg helpers --------------------
async def run(cmd: list[str]):
    p = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, err = await p.communicate()
    if p.returncode:
        raise RuntimeError(err.decode(errors="ignore") or "ffmpeg error")

async def probe_has_encoder(name: str) -> bool:
    try:
        await run([
            "ffmpeg","-hide_banner","-v","error",
            "-f","lavfi","-i",f"color=c=black:s=16x16:r={TARGET_FPS}:d=1",
            "-frames:v","1","-c:v",name,"-f","null","-"
        ])
        return True
    except Exception:
        return False

async def ffprobe_duration(path: str) -> float:
    p = await asyncio.create_subprocess_exec(
        "ffprobe","-v","error","-show_entries","format=duration","-of","csv=p=0", path,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, _ = await p.communicate()
    try: return float(out.decode().strip())
    except Exception: return 0.0

async def ffprobe_has_audio(path: str) -> bool:
    p = await asyncio.create_subprocess_exec(
        "ffprobe","-v","error","-select_streams","a","-show_entries","stream=index","-of","csv=p=0", path,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, _ = await p.communicate()
    return bool(out.strip())

def scale_pad_filter(w: int, h: int) -> str:
    pad = f"pad={w}:{h}:({w}-iw)/2:({h}-ih)/2:black"
    return f"scale=w={w}:h={h}:force_original_aspect_ratio=decrease,format=yuv420p,{pad},setsar=1,fps={TARGET_FPS}"

def calc_target_v_kbps(duration_sec: float,
                       max_mb: float = MAX_TG_VIDEO_MB,
                       audio_kbps: int = AUDIO_KBPS,
                       margin_mb: int = SIZE_MARGIN_MB) -> int:
    if duration_sec <= 0: return 800
    target_bits = int((max_mb - margin_mb) * 1024 * 1024 * 8)
    kbps = int(target_bits / (duration_sec * 1000) - audio_kbps)
    return max(MIN_VIDEO_KBPS, kbps)

ENCODER_NAME: Optional[str] = None
ENCODER_ARGS_CBR: List[str] = []

async def init_encoder():
    global ENCODER_NAME, ENCODER_ARGS_CBR
    if await probe_has_encoder("h264_amf"):
        ENCODER_NAME = "h264_amf"  # AMD RX 580
        ENCODER_ARGS_CBR = ["-c:v","h264_amf","-rc","cbr","-profile:v","main","-quality","speed","-g","120","-bf","0"]
    else:
        ENCODER_NAME = "libx264"
        ENCODER_ARGS_CBR = ["-c:v","libx264","-preset",X264_PRESET,"-tune","fastdecode","-g","120","-bf","0"]

def encoder_cbr_args(v_kbps: int) -> list[str]:
    return [
        *ENCODER_ARGS_CBR,
        "-b:v", f"{v_kbps}k",
        "-maxrate", f"{v_kbps}k",
        "-bufsize", f"{max(v_kbps*2, 400)}k",
        "-pix_fmt","yuv420p",
        "-movflags","+faststart",
        "-threads","0",
        "-vsync","cfr",
        "-r", str(TARGET_FPS),
        "-force_key_frames", "expr:gte(t,0)"
    ]

async def ffmpeg_progress(cmd: list[str], total_seconds: float, msg: Message, label: str):
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    last = -5; tail: List[str] = []
    while True:
        line = await proc.stdout.readline()
        if not line: break
        s = line.decode(errors="ignore").strip()
        if len(tail) > 120: tail.pop(0)
        tail.append(s)
        if s.startswith("out_time_ms=") and total_seconds:
            ms_s = s.split("=",1)[1]
            if ms_s.isdigit():
                pct = int(int(ms_s)/1_000_000/max(0.001,total_seconds)*100)
                if pct - last >= 3:
                    last = pct
                    with contextlib.suppress(Exception):
                        await msg.edit_text(f"⏳ {pct}%… ({label})")
    await proc.wait()
    if proc.returncode:
        raise RuntimeError("ffmpeg failed:\n" + "\n".join(tail[-25:]))

async def fix_h264(src: str, dst: str, keep_audio: bool):
    cmd = ["ffmpeg","-hide_banner","-y","-i",src]
    cmd += (["-map","0","-c","copy"] if keep_audio else ["-map","0:v:0","-c:v","copy","-an"])
    cmd += ["-bsf:v","h264_metadata=video_full_range_flag=0:colour_primaries=1:transfer_characteristics=1:matrix_coefficients=1", dst]
    await run(cmd)

# -------------------- СКЛЕЙКА → сразу под лимит --------------------
async def concat_to_target(main: str, wm: str, out_: str, total_sec: float, msg: Message, label: str):
    sp = scale_pad_filter(OUT_W, OUT_H)
    main_has_a = await ffprobe_has_audio(main)
    wm_has_a = await ffprobe_has_audio(wm)
    audio_concat = main_has_a and wm_has_a

    if audio_concat:
        fc = (
            f"[0:v]{sp},setpts=PTS-STARTPTS[v0];"
            f"[1:v]{sp},setpts=PTS-STARTPTS[v1];"
            f"[0:a]aresample=async=1:first_pts=0,asetpts=PTS-STARTPTS[a0];"
            f"[1:a]aresample=async=1:first_pts=0,asetpts=PTS-STARTPTS[a1];"
            f"[v0][v1]concat=n=2:v=1:a=0[v];"
            f"[a0][a1]concat=n=2:v=0:a=1[a]"
        )
        maps = ["-map","[v]","-map","[a]"]
    else:
        fc = (
            f"[0:v]{sp},setpts=PTS-STARTPTS[v0];"
            f"[1:v]{sp},setpts=PTS-STARTPTS[v1];"
            f"[v0][v1]concat=n=2:v=1:a=0[v]"
        )
        maps = ["-map","[v]","-map","0:a?"]

    v_kbps = calc_target_v_kbps(total_sec)
    cmd = [
        "ffmpeg","-hide_banner","-y",
        "-i",main,"-i",wm,
        "-filter_complex", fc, *maps,
        *encoder_cbr_args(v_kbps),
        "-c:a","aac","-b:a",f"{AUDIO_KBPS}k",
        "-progress","pipe:1", out_
    ]
    await ffmpeg_progress(cmd, total_sec, msg, label)

# -------------------- ДОСЖАТИЕ / ДАУНСКЕЙЛ --------------------
def _calc_v_for_size(duration_sec: float, target_mb: float, audio_kbps: int) -> int:
    if duration_sec <= 0: return 600
    bits = int(target_mb * 1024 * 1024 * 8)
    kbps = int(bits / (duration_sec * 1000) - audio_kbps)
    return max(80, kbps)

async def shrink_under_limit(src: Path, dst: Path, msg: Message, label: str) -> Path:
    size_mb = src.stat().st_size / 1_048_576
    target_mb = MAX_TG_VIDEO_MB - 0.5
    if size_mb <= target_mb:
        return src

    dur = await ffprobe_duration(str(src))
    if dur <= 0:
        return src

    v_kbps = _calc_v_for_size(dur, target_mb, AUDIO_KBPS)
    a_kbps = AUDIO_KBPS
    tmp_in = str(src)
    out_path = str(dst)

    for attempt in range(1, 6+1):
        with contextlib.suppress(Exception):
            await msg.edit_text(f"⏳ 0%… ({label}, попытка {attempt})")
        cmd = [
            "ffmpeg","-hide_banner","-y",
            "-i", tmp_in,
            "-map","0:v:0","-map","0:a?",
            *encoder_cbr_args(v_kbps),
            "-c:a","aac","-b:a",f"{a_kbps}k",
            "-vf", f"fps={TARGET_FPS}",
            "-progress","pipe:1", out_path
        ]
        await ffmpeg_progress(cmd, dur, msg, f"{label} попытка {attempt}")
        out_mb = Path(out_path).stat().st_size / 1_048_576
        if out_mb <= target_mb:
            return Path(out_path)
        factor = (target_mb / max(1e-3, out_mb)) * 0.96
        v_kbps = max(80, int(v_kbps * factor))
        if v_kbps <= 80 and a_kbps > 48:
            a_kbps = 48
        tmp_in = out_path

    # финальный фолбэк — даунскейл 720×1280
    w, h = 720, 1280
    sp720 = scale_pad_filter(w, h)
    v_kbps = _calc_v_for_size(dur, target_mb, a_kbps)
    final_out = Path(dst).with_name(Path(dst).stem + "_720p.mp4")
    with contextlib.suppress(Exception):
        await msg.edit_text("⏳ 0%… (даунскейл 720p)")
    cmd = [
        "ffmpeg","-hide_banner","-y",
        "-i", str(src),
        "-filter_complex", f"[0:v]{sp720},setpts=PTS-STARTPTS[v];[0:a]aresample=async=1:first_pts=0,asetpts=PTS-STARTPTS[a]",
        "-map","[v]","-map","[a]?","-dn",
        *encoder_cbr_args(v_kbps),
        "-c:a","aac","-b:a",f"{a_kbps}k",
        "-progress","pipe:1", str(final_out)
    ]
    await ffmpeg_progress(cmd, dur, msg, "даунскейл 720p")
    return final_out

# -------------------- Вспомог.: извлечь video file_id --------------------
def get_video_file_id(m: Message) -> Optional[str]:
    if m.video:
        return m.video.file_id
    if m.document:
        mt = (m.document.mime_type or "").lower()
        name = (m.document.file_name or "").lower()
        if mt.startswith("video/") or name.endswith((".mp4", ".mov", ".mkv", ".webm")):
            return m.document.file_id
    return None

# -------------------- ERROR helper --------------------
async def send_err(msg: Message, exc: Exception):
    text = html.escape(str(exc))
    if len(text) > 3500: text = text[-3500:]
    await msg.answer(f"<pre>{text}</pre>", parse_mode=ParseMode.HTML)

# -------------------- ROUTES --------------------
router = Router()

@router.message(CommandStart())
async def cmd_start(m: Message):
    await m.answer(
        "Привет! 👋\n"
        "1) Нажми «Добавить видео-ватермарку в конец» ниже.\n"
        "2) Сначала пришли ВИДЕО-ватермарку (хвост).\n"
        "3) Потом шли мемы — по одному или альбомом (можно видео как документы). Я приклею хвост в конец.",
        reply_markup=kb_main()
    )

@router.message(F.text == BTN_ADD_WM)
async def cmd_add(m: Message):
    awaiting_wm.add(m.chat.id)
    await m.answer("Жду ВИДЕО-ватермарку. После сохранения просто шли мемы (видео/видео-документы), можно альбомом.",
                   reply_markup=kb_sub())

@router.message(F.text == BTN_REPLACE_WM)
async def cmd_replace(m: Message):
    awaiting_wm.add(m.chat.id)
    await m.answer("Ок, пришли новую ВИДЕО-ватермарку.")

@router.message(F.text == BTN_MAIN_MENU)
async def cmd_menu(m: Message):
    awaiting_wm.discard(m.chat.id)
    await m.answer("Главное меню.", reply_markup=kb_main())

# ---- Приём обычных видео ----
@router.message(F.video)
async def handle_video(m: Message, bot: Bot):
    await handle_media_message(m, bot)

# ---- Приём видео-документов ----
@router.message(F.document)
async def handle_document(m: Message, bot: Bot):
    if not get_video_file_id(m):
        return
    await handle_media_message(m, bot)

# ---- Общий обработчик ----
async def handle_media_message(m: Message, bot: Bot):
    cid = m.chat.id
    try:
        # 1) Сохраняем ватермарку
        if cid in awaiting_wm:
            fid = get_video_file_id(m)
            if not fid:
                await m.answer("Пришли именно видео (mp4/mov/webm).")
                return
            watermark_by_chat[cid] = fid
            awaiting_wm.discard(cid)
            await m.answer("✅ Ватермарка сохранена. Шли мемы (по одному или альбомом).", reply_markup=kb_sub())
            return

        if cid not in watermark_by_chat:
            await m.answer("Сначала отправь ватермарку: «Добавить видео-ватермарку в конец».")
            return

        # 2) Альбом — накапливаем, перезапускаем таймер, один финализатор
        if m.media_group_id:
            key = (cid, str(m.media_group_id))
            fid = get_video_file_id(m)
            if not fid:
                return

            bucket = album_buf.setdefault(key, {})
            bucket[m.message_id] = m

            # уведомление (одно на альбом) + счётчик
            if key not in album_notice:
                album_first_ts[key] = time.monotonic()
                album_notice[key] = await m.answer("📥 Принял альбом, собираю элементы… (1)")
            else:
                with contextlib.suppress(Exception):
                    await album_notice[key].edit_text(f"📥 Принял альбом, собираю элементы… ({len(bucket)})")

            # перезапускаем таймер «устаканивания»
            old = album_timer.get(key)
            if old and not old.done():
                old.cancel()

            async def _finalize_album(key=key, chat_id=cid):
                last_len = -1
                start = album_first_ts.get(key, time.monotonic())
                try:
                    while True:
                        await asyncio.sleep(ALBUM_SETTLE_SEC)
                        cur_len = len(album_buf.get(key, {}))
                        timed_out = (time.monotonic() - start) >= ALBUM_HARD_TIMEOUT_SEC
                        if cur_len == last_len or timed_out:
                            break
                        last_len = cur_len
                        with contextlib.suppress(Exception):
                            await album_notice[key].edit_text(f"📥 Принял альбом, собираю элементы… ({cur_len})")

                    msgs = sorted(album_buf.pop(key, {}).values(), key=lambda x: x.message_id)
                    if msgs:
                        with contextlib.suppress(Exception):
                            await album_notice[key].edit_text("⏳ Начинаю обработку альбома…")
                        async with processing_lock[chat_id]:
                            await process_batch(msgs, bot, watermark_by_chat[chat_id])
                finally:
                    with contextlib.suppress(Exception):
                        note = album_notice.pop(key, None)
                        if note: await note.delete()
                    album_timer.pop(key, None)
                    album_first_ts.pop(key, None)

            album_timer[key] = asyncio.create_task(_finalize_album())
            return

        # 3) Одиночные видео — сразу
        async with processing_lock[cid]:
            await process_single(m, bot, watermark_by_chat[cid])

    except Exception as e:
        await send_err(m, e)

# -------------------- ОБРАБОТКА --------------------
async def concat_to_target_wrap(main_fix: str, wm_fix: str, out_p: Path, prog: Message, label: str):
    total_sec = (await ffprobe_duration(main_fix)) + (await ffprobe_duration(wm_fix))
    await concat_to_target(main_fix, wm_fix, str(out_p), total_sec, prog, label)

async def process_single(m: Message, bot: Bot, wm_fid: str):
    tmp = Path(tempfile.mkdtemp(prefix="yumi_"))
    main_p, wm_p = tmp / "main.mp4", tmp / "wm.mp4"
    main_fix, wm_fix = tmp / "main_fix.mp4", tmp / "wm_fix.mp4"
    out_p = tmp / "out.mp4"
    out_small = tmp / "out_small.mp4"

    prog = await m.answer("⏳ 0%…")
    try:
        in_fid = get_video_file_id(m)
        if not in_fid:
            await prog.delete()
            await m.answer("Пришли видео-файл (mp4/mov/webm).")
            return

        await asyncio.gather(
            bot.download(wm_fid, destination=str(wm_p)),
            bot.download(in_fid,  destination=str(main_p)),
        )
        await asyncio.gather(
            fix_h264(str(main_p), str(main_fix), keep_audio=True),
            fix_h264(str(wm_p),   str(wm_fix),   keep_audio=True),
        )
        await concat_to_target_wrap(str(main_fix), str(wm_fix), out_p, prog, "склейка")

        final_p = out_p
        if out_p.stat().st_size / 1_048_576 > MAX_TG_VIDEO_MB - 0.1:
            final_p = await shrink_under_limit(out_p, out_small, prog, "сжатие")

        with contextlib.suppress(Exception):
            await prog.edit_text("✅ Готово! Отправляю…")
        await m.answer_video(FSInputFile(final_p), caption="Готово ✅", supports_streaming=True)

    except Exception as e:
        await send_err(m, e)
    finally:
        with contextlib.suppress(Exception):
            await prog.delete()
        shutil.rmtree(tmp, ignore_errors=True)

async def process_batch(msgs: List[Message], bot: Bot, wm_fid: str):
    chat = msgs[0].chat
    tmp = Path(tempfile.mkdtemp(prefix="yumi_batch_"))
    prog: Optional[Message] = None
    try:
        prog = await chat.send_message("⏳ 0%… (пачка)")

        wm_p = tmp / "wm.mp4"
        wm_fix = tmp / "wm_fix.mp4"
        await bot.download(wm_fid, destination=str(wm_p))
        await fix_h264(str(wm_p), str(wm_fix), keep_audio=True)
        wm_dur = await ffprobe_duration(str(wm_fix))

        in_fids: List[str] = []
        for msg in msgs:
            fid = get_video_file_id(msg)
            if fid: in_fids.append(fid)
        if not in_fids:
            with contextlib.suppress(Exception):
                await prog.delete()
            await chat.send_message("В альбоме нет видео (mp4/mov/webm).")
            return

        main_files = [tmp / f"main_{i}.mp4" for i in range(len(in_fids))]
        await asyncio.gather(*[
            bot.download(in_fids[i], destination=str(main_files[i])) for i in range(len(in_fids))
        ])
        main_fix_files = [tmp / f"main_fix_{i}.mp4" for i in range(len(in_fids))]
        await asyncio.gather(*[
            fix_h264(str(main_files[i]), str(main_fix_files[i]), keep_audio=True) for i in range(len(in_fids))
        ])

        durations = await asyncio.gather(*[ffprobe_duration(str(p)) for p in main_fix_files])
        grand_ms = int(sum(d + wm_dur for d in durations) * 1000)

        out_files: List[Path] = []
        done_ms = 0
        for i, main_fix in enumerate(main_fix_files):
            out_p = tmp / f"out_{i}.mp4"
            await concat_to_target_wrap(str(main_fix), str(wm_fix), out_p, prog, f"{i+1}/{len(in_fids)} склейка")

            final_p = out_p
            if out_p.stat().st_size / 1_048_576 > MAX_TG_VIDEO_MB - 0.1:
                final_p = await shrink_under_limit(out_p, tmp / f"out_small_{i}.mp4", prog, "сжатие")

            out_files.append(final_p)
            total_sec = durations[i] + wm_dur
            done_ms += int(total_sec * 1000)
            pct = int(done_ms / max(1, grand_ms) * 100)
            with contextlib.suppress(Exception):
                await prog.edit_text(f"⏳ {pct}%… ({i+1}/{len(in_fids)})")

        with contextlib.suppress(Exception):
            await prog.edit_text("✅ Пачка готова! Отправляю…")
        for fp in out_files:
            await chat.send_video(FSInputFile(fp), caption="Готово ✅", supports_streaming=True)

    except Exception as e:
        try:
            await chat.send_message(f"<pre>{html.escape(str(e))}</pre>", parse_mode=ParseMode.HTML)
        except Exception:
            pass
    finally:
        with contextlib.suppress(Exception):
            if prog: await prog.delete()
        shutil.rmtree(tmp, ignore_errors=True)

# -------------------- MAIN --------------------
async def main():
    await init_encoder()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    print(f"Polling… (encoder: {ENCODER_NAME})")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
