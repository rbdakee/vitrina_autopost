"""
Уникализация видео — библиотека для создания уникальных версий одного видео.
Использует ffmpeg для изменения скорости, цвета, резкости, аудио и т.д.
"""
import random
import subprocess
import tempfile
from pathlib import Path
from typing import Optional, Tuple


# ==================== ПАРАМЕТРЫ ====================
SKIP_IF_EXISTS = True

SPEED_RANGE = (1.03, 1.07)
BRIGHTNESS_RANGE = (0.02, 0.06)
CONTRAST_RANGE = (1.06, 1.12)
SATURATION_RANGE = (1.03, 1.10)

SHARPEN_PROB = 0.8
UNSHARP_RANGE = (0.8, 1.15)

LOGO_HEIGHT_RANGE = (120, 170)
LOGO_MARGIN_RANGE = (8, 30)
LOGO_POSITIONS = ("tl", "tr", "bl", "br")

TEXT_FONTSIZE_RANGE = (22, 30)
TEXT_MARGIN_RANGE = (10, 28)

METADATA_MODES = ("remove", "set")

AUDIO_ENABLE_DEFAULT = True
AUDIO_VOLUME_RANGE = (0.97, 1.03)
AUDIO_HIGHPASS_RANGE = (40, 90)
AUDIO_LOWPASS_RANGE = (14500, 18000)
AUDIO_EQ_FREQ_RANGE = (800, 3000)
AUDIO_EQ_GAIN_RANGE = (-1.5, 1.5)
AUDIO_PITCH_RANGE = (0.985, 1.015)

USE_RUBBERBAND = True


# ==================== ВСПОМОГАТЕЛЬНЫЕ ====================

def _overlay_expr(pos: str, margin: int) -> str:
    return {
        "tl": f"{margin}:{margin}",
        "tr": f"W-w-{margin}:{margin}",
        "bl": f"{margin}:H-h-{margin}",
        "br": f"W-w-{margin}:H-h-{margin}",
    }.get(pos, f"{margin}:{margin}")


def _ff_escape_path(p: Path) -> str:
    # Достаточно для типичных путей; при необходимости можно расширить экранирование
    return str(p.resolve().as_posix()).replace(":", r"\:")


def _has_audio_stream(src: Path) -> bool:
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "a:0",
        "-show_entries", "stream=codec_type",
        "-of", "csv=p=0", str(src)
    ]
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return p.returncode == 0 and "audio" in (p.stdout or "").lower()


def _run(cmd_list: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd_list, capture_output=True, text=True, encoding="utf-8", errors="replace")


def _stderr_has_rubberband_issue(stderr: str) -> bool:
    s = (stderr or "").lower()
    # типовые сообщения ffmpeg
    return ("rubberband" in s) and (
        ("no such filter" in s) or
        ("filter not found" in s) or
        ("invalid argument" in s) or
        ("error initializing filter" in s)
    )


def _pick_params(rng: random.Random) -> dict:
    speed = round(rng.uniform(*SPEED_RANGE), 3)
    brightness = round(rng.uniform(*BRIGHTNESS_RANGE), 3)
    contrast = round(rng.uniform(*CONTRAST_RANGE), 3)
    saturation = round(rng.uniform(*SATURATION_RANGE), 3)

    apply_sharpen = rng.random() < SHARPEN_PROB
    sharpen_strength = round(rng.uniform(*UNSHARP_RANGE), 2)

    logo_height = rng.randint(*LOGO_HEIGHT_RANGE)
    logo_margin = rng.randint(*LOGO_MARGIN_RANGE)
    logo_pos = rng.choice(LOGO_POSITIONS)

    text_size = rng.randint(*TEXT_FONTSIZE_RANGE)
    text_margin = rng.randint(*TEXT_MARGIN_RANGE)

    meta_mode = rng.choice(METADATA_MODES)

    # аудио параметры (на всякий, используем если аудио есть и включено)
    vol = round(rng.uniform(*AUDIO_VOLUME_RANGE), 3)
    hp = rng.randint(*AUDIO_HIGHPASS_RANGE)
    lp = rng.randint(*AUDIO_LOWPASS_RANGE)
    eqf = rng.randint(*AUDIO_EQ_FREQ_RANGE)
    eqg = round(rng.uniform(*AUDIO_EQ_GAIN_RANGE), 2)
    pitch = round(rng.uniform(*AUDIO_PITCH_RANGE), 4)

    return {
        "speed": speed,
        "brightness": brightness,
        "contrast": contrast,
        "saturation": saturation,
        "apply_sharpen": apply_sharpen,
        "sharpen_strength": sharpen_strength,
        "logo_height": logo_height,
        "logo_margin": logo_margin,
        "logo_pos": logo_pos,
        "text_size": text_size,
        "text_margin": text_margin,
        "meta_mode": meta_mode,
        "vol": vol,
        "hp": hp,
        "lp": lp,
        "eqf": eqf,
        "eqg": eqg,
        "pitch": pitch,
    }


def _build_cmd(
    *,
    inp_path: Path,
    out_path: Path,
    params: dict,
    logo_path: Optional[Path],
    overlay_text: Optional[str],
    font_path: Optional[Path],
    audio_enabled: bool,
    audio_present: bool,
    use_rubberband: bool,
    td_path: Path,  # для text.txt
) -> Tuple[list[str], list[str], str]:
    """
    Возвращает (cmd, fc_parts, filter_complex)
    fc_parts возвращаем, чтобы при надобности можно было дебажить.
    """
    speed = params["speed"]
    brightness = params["brightness"]
    contrast = params["contrast"]
    saturation = params["saturation"]

    vf_chain = [
        f"setpts=PTS/{speed}",
        "scale=iw*1.01:ih*1.01,crop=iw/1.01:ih/1.01:(iw-iw/1.01)/2:(ih-ih/1.01)/2",
        f"eq=brightness={brightness}:contrast={contrast}:saturation={saturation}",
    ]

    if params["apply_sharpen"]:
        vf_chain.append(f"unsharp=5:5:{params['sharpen_strength']}:5:5:0.0")
    base_vf = ",".join(vf_chain)

    inputs = ["-i", str(inp_path)]
    fc_parts = [f"[0:v]{base_vf}[v0]"]
    vcur = "v0"

    if logo_path and logo_path.is_file():
        inputs += ["-i", str(logo_path)]
        fc_parts.append(f"[1:v]scale=-1:{params['logo_height']}[logo]")
        fc_parts.append(f"[{vcur}][logo]overlay={_overlay_expr(params['logo_pos'], params['logo_margin'])}[v1]")
        vcur = "v1"

    # текст
    if overlay_text and font_path and font_path.is_file():
        textfile_path = td_path / "text.txt"
        textfile_path.write_text(overlay_text, encoding="utf-8")
        fontfile = _ff_escape_path(font_path)
        textfile = _ff_escape_path(textfile_path)
        fc_parts.append(
            f"[{vcur}]drawtext=fontfile='{fontfile}':textfile='{textfile}':reload=0:"
            f"fontsize={params['text_size']}:fontcolor=white:x=w-tw-{params['text_margin']}:y=h-th-{params['text_margin']}[vout]"
        )
        vcur = "vout"

    # аудио
    if audio_enabled and audio_present:
        af_chain = [
            f"atempo={speed}",
            f"volume={params['vol']}",
            f"highpass=f={params['hp']}",
            f"lowpass=f={params['lp']}",
            f"equalizer=f={params['eqf']}:t=q:w=1:g={params['eqg']}",
        ]
        if use_rubberband:
            af_chain.append(f"rubberband=pitch={params['pitch']}")
        fc_parts.append(f"[0:a]{','.join(af_chain)}[aout]")

    filter_complex = ";".join(fc_parts)

    meta_args: list[str] = []
    if params["meta_mode"] == "remove":
        meta_args = ["-map_metadata", "-1"]
    else:
        meta_args = ["-map_metadata", "-1", "-metadata", f"comment=proc_{random.randint(100000, 999999)}"]

    cmd: list[str] = [
        "ffmpeg", "-y",
        *inputs,
        "-filter_complex", filter_complex,
        "-map", f"[{vcur}]",
    ]
    if audio_enabled and audio_present:
        cmd += ["-map", "[aout]"]
    else:
        cmd += ["-map", "0:a?"]

    cmd += [
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
        "-c:a", "aac", "-b:a", "160k",
        *meta_args,
        str(out_path),
    ]
    return cmd, fc_parts, filter_complex


# ==================== ПУБЛИЧНЫЙ API ====================

def uniquify_video_file(
    input_path: Path,
    output_path: Path,
    *,
    seed: Optional[int] = None,
    skip_if_exists: bool = SKIP_IF_EXISTS,
    logo_path: Optional[Path] = None,
    overlay_text: Optional[str] = None,
    font_path: Optional[Path] = None,
    audio_enabled: bool = AUDIO_ENABLE_DEFAULT,
) -> Path:
    """
    File-to-file уникализация: читает input_path, пишет output_path, возвращает output_path.
    Это основной серверный режим: меньше RAM, меньше копирований.
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    if skip_if_exists and output_path.is_file():
        return output_path

    if not font_path:
        fp = Path(__file__).parent / "Arial.ttf"
        font_path = fp if fp.is_file() else None

    output_path.parent.mkdir(parents=True, exist_ok=True)

    rng = random.Random(seed) if seed is not None else random
    params = _pick_params(rng)

    audio_present = _has_audio_stream(input_path)

    # Временная директория только под text.txt (если надо).
    with tempfile.TemporaryDirectory(prefix="vitrina_unique_") as td:
        td_path = Path(td)

        # 1) пробуем с rubberband (если включено)
        use_rb = bool(USE_RUBBERBAND)
        cmd, _, _ = _build_cmd(
            inp_path=input_path,
            out_path=output_path,
            params=params,
            logo_path=Path(logo_path) if logo_path else None,
            overlay_text=overlay_text,
            font_path=Path(font_path) if font_path else None,
            audio_enabled=audio_enabled,
            audio_present=audio_present,
            use_rubberband=use_rb,
            td_path=td_path,
        )
        p = _run(cmd)

        # 2) если rubberband недоступен — повторяем без него
        if p.returncode != 0 and use_rb and audio_enabled and audio_present and _stderr_has_rubberband_issue(p.stderr):
            cmd2, _, _ = _build_cmd(
                inp_path=input_path,
                out_path=output_path,
                params=params,
                logo_path=Path(logo_path) if logo_path else None,
                overlay_text=overlay_text,
                font_path=Path(font_path) if font_path else None,
                audio_enabled=audio_enabled,
                audio_present=audio_present,
                use_rubberband=False,
                td_path=td_path,
            )
            p = _run(cmd2)

        if p.returncode != 0:
            raise RuntimeError(f"ffmpeg error: {p.stderr or p.stdout or 'unknown'}")

    return output_path


def uniquify_video(
    input_bytes: bytes,
    *,
    seed: Optional[int] = None,
    output_path: Optional[Path] = None,
    skip_if_exists: bool = SKIP_IF_EXISTS,
    logo_path: Optional[Path] = None,
    overlay_text: Optional[str] = None,
    font_path: Optional[Path] = None,
    audio_enabled: bool = AUDIO_ENABLE_DEFAULT,
) -> bytes:
    """
    Bytes->bytes обёртка поверх file-to-file.
    Удобно для API/старого кода, но расходует больше RAM/IO.
    """
    with tempfile.TemporaryDirectory(prefix="vitrina_unique_bytes_") as td:
        td_path = Path(td)
        inp_path = td_path / "in.mp4"
        inp_path.write_bytes(input_bytes)

        if output_path:
            out_path = Path(output_path)
        else:
            out_path = td_path / "out.mp4"

        uniquify_video_file(
            inp_path,
            out_path,
            seed=seed,
            skip_if_exists=skip_if_exists,
            logo_path=logo_path,
            overlay_text=overlay_text,
            font_path=font_path,
            audio_enabled=audio_enabled,
        )
        return out_path.read_bytes()
