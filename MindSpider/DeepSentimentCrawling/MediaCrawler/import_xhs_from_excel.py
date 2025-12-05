"""Import Xiaohongshu data from an .xlsx file into xhs_creator/xhs_note/xhs_note_comment.

Usage / Assumptions
-------------------
1. 推荐方式：单 Sheet Excel（与你目前的格式一致）

   - Excel 只有第一个 Sheet，其中每一行是一条笔记，列名类似：
     - 笔记分类、笔记内容标签、提及品类、种草品牌、商业合作品牌、
       发布时间、发文属地、账号昵称、作者类别、账号小红书号、账号属性、
       当前粉丝数、账号主页链接、互动量、收藏、评论、分享。
   - 脚本会：
     - 用这一张 Sheet 同时写入 `xhs_note` 与 `xhs_creator` 两张表；
     - 如果你以后在同一张 Sheet 里增加 `笔记id` / `笔记链接` / `笔记标题` / `笔记内容` 等列，
       也会被自动识别并写入对应字段；
     - 如果没有 `笔记id` 列，会自动用 `row_行号` 生成一个 note_id，保证唯一即可。

2. 兼容方式：老的 3 Sheet Excel

   - 仍然兼容最初的设计：Excel 可以包含 3 个 Sheet（名字可通过命令行指定，默认为
     `xhs_note` / `xhs_creator` / `xhs_note_comment`）。
   - 每个 Sheet 的列名可以使用英文字段名或常见中文别名，例如：
     - 笔记：note_id/笔记id, title/笔记标题, desc/笔记内容, time/发布时间,
       tag_list/话题/笔记内容标签, source_keyword/命中关键词,
       liked_count/点赞/赞/互动量, collected_count/收藏,
       comment_count/评论, share_count/分享, user_id/账号小红书号, nickname/账号昵称,
       ip_location/发文属地, note_url/笔记链接。
     - 创作者：user_id/账号小红书号, nickname/账号昵称, fans/当前粉丝数,
       interaction/互动量 等。
     - 评论：comment_id/评论id, note_id/笔记id, content/评论内容, create_time/评论时间,
       like_count/点赞数 等。

如果你的 Excel 列名跟这里不完全一样，可以在本脚本里扩展别名字典，或者告诉我帮你改。
"""

from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path

import pandas as pd
from loguru import logger

from database.db_session import create_tables_without_creating_database, get_session
from database.models import XhsCreator, XhsNote, XhsNoteComment


NOTE_ALIASES: dict[str, list[str]] = {
    "note_id": ["note_id", "笔记id", "笔记ID"],
    "note_url": ["note_url", "笔记链接"],
    "title": ["title", "笔记标题"],
    "desc": ["desc", "笔记内容"],
    "time": ["time", "发布时间"],
    # 笔记内容标签：你的 Excel 里就是这一列
    "tag_list": ["tag_list", "话题", "笔记内容标签"],
    "source_keyword": ["source_keyword", "命中关键词"],
    # 很多表里会把“互动量”当作点赞数使用，这里也作为 liked_count 的一个别名
    "liked_count": ["liked_count", "点赞", "赞", "互动量"],
    "collected_count": ["collected_count", "收藏"],
    "comment_count": ["comment_count", "评论"],
    "share_count": ["share_count", "分享"],
    "user_id": ["user_id", "账号小红书号", "作者ID"],
    "nickname": ["nickname", "账号昵称"],
    "avatar": ["avatar", "头像", "账号头像"],
    "ip_location": ["ip_location", "发文属地", "IP属地"],
}

CREATOR_ALIASES: dict[str, list[str]] = {
    "user_id": ["user_id", "账号小红书号", "作者ID"],
    "nickname": ["nickname", "账号昵称"],
    "avatar": ["avatar", "头像", "账号头像"],
    "ip_location": ["ip_location", "IP属地", "常驻地"],
    "desc": ["desc", "账号简介", "作者简介"],
    "gender": ["gender", "性别"],
    "follows": ["follows", "关注数"],
    "fans": ["fans", "当前粉丝数", "粉丝数"],
    "interaction": ["interaction", "互动量"],
    "tag_list": ["tag_list", "账号标签"],
}

COMMENT_ALIASES: dict[str, list[str]] = {
    "comment_id": ["comment_id", "评论id", "评论ID"],
    "note_id": ["note_id", "笔记id", "笔记ID"],
    "user_id": ["user_id", "账号小红书号", "作者ID"],
    "nickname": ["nickname", "账号昵称"],
    "avatar": ["avatar", "头像", "账号头像"],
    "ip_location": ["ip_location", "IP属地"],
    "content": ["content", "评论内容"],
    "create_time": ["create_time", "评论时间"],
    "sub_comment_count": ["sub_comment_count", "回复数"],
    "pictures": ["pictures", "图片"],
    "parent_comment_id": ["parent_comment_id", "父评论id"],
    "like_count": ["like_count", "点赞数", "赞"],
}


def _build_col_map(df: pd.DataFrame, aliases: dict[str, list[str]]) -> dict[str, str]:
    """根据别名字典，从 DataFrame 列名中构建字段到真实列名的映射。"""

    lower_cols = {c.lower(): c for c in df.columns}
    col_map: dict[str, str] = {}
    for field, names in aliases.items():
        for name in names:
            col = lower_cols.get(name.lower())
            if col:
                col_map[field] = col
                break
    return col_map


def _get(row: pd.Series, col_map: dict[str, str], field: str, default=None):
    """安全地从一行中取值，带 NaN 处理。"""

    col = col_map.get(field)
    if not col:
        return default
    val = row.get(col, default)
    try:
        if pd.isna(val):
            return default
    except Exception:
        pass
    return val


def _to_int(val, default=None):
    """尽量把各种类型（int/float/str）转成 int。"""

    try:
        if pd.isna(val):
            return default
    except Exception:
        pass
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str) and val.strip():
        try:
            return int(float(val))
        except ValueError:
            return default
    return default


def _to_ts_ms(val, default=None):
    """把 Excel 里时间字段转换成毫秒时间戳。"""

    num = _to_int(val, None)
    if num is not None:
        # 已经是时间戳（秒或毫秒）
        return num
    if val is None:
        return default
    try:
        dt = pd.to_datetime(val)
        return int(dt.timestamp() * 1000)
    except Exception:
        return default


def _read_sheet(path: Path, sheet_name: str) -> pd.DataFrame | None:
    """读取指定 Sheet，不存在则返回 None 并打日志。"""

    try:
        return pd.read_excel(path, sheet_name=sheet_name)
    except ValueError:
        logger.warning(f"Sheet '{sheet_name}' not found, skip importing.")
        return None


async def import_from_excel(
    path: Path,
    notes_sheet: str = "xhs_note",
    creators_sheet: str = "xhs_creator",
    comments_sheet: str = "xhs_note_comment",
) -> None:
    """导入 Excel 数据到 xhs_creator / xhs_note / xhs_note_comment。"""

    path = path.resolve()
    if not path.exists():
        raise FileNotFoundError(path)

    now_ms = int(time.time() * 1000)

    # 兼容两种情况：
    # 1) Excel 只有一个 Sheet：用这一个 Sheet 同时导入 note + creator；
    # 2) Excel 有多个 Sheet：按老逻辑分别按名字读取。
    try:
        excel_file = pd.ExcelFile(path)
        sheet_names = excel_file.sheet_names
    except Exception:
        excel_file = None
        sheet_names = []

    if len(sheet_names) == 1 and excel_file is not None:
        df_single = pd.read_excel(excel_file, sheet_name=sheet_names[0])
        logger.info(
            f"Excel 仅包含一个 Sheet ('{sheet_names[0]}')，"
            "将使用该 Sheet 同时导入 xhs_note 和 xhs_creator。"
        )
        df_notes = df_single
        df_creators = df_single
        df_comments = None
    else:
        df_notes = _read_sheet(path, notes_sheet)
        df_creators = _read_sheet(path, creators_sheet)
        df_comments = _read_sheet(path, comments_sheet)

    async with get_session() as session:
        if session is None:
            logger.error("数据库未配置为 SQL 类型（当前 SAVE_DATA_OPTION 不是 mysql/sqlite/postgresql），无法导入。")
            return

        # ---------- xhs_creator ----------
        if df_creators is not None and not df_creators.empty:
            col_map = _build_col_map(df_creators, CREATOR_ALIASES)
            if "user_id" not in col_map:
                logger.warning("Creator sheet 缺少 user_id 列，跳过导入 xhs_creator。")
            else:
                objs: list[XhsCreator] = []
                seen_user_ids: set[str] = set()
                for _, row in df_creators.iterrows():
                    user_id = _get(row, col_map, "user_id")
                    if not user_id:
                        continue
                    user_id_str = str(user_id)
                    # 去重：同一个账号只插入一条 creator 记录
                    if user_id_str in seen_user_ids:
                        continue
                    seen_user_ids.add(user_id_str)
                    obj = XhsCreator(
                        user_id=user_id_str,
                        nickname=_get(row, col_map, "nickname"),
                        avatar=_get(row, col_map, "avatar"),
                        ip_location=_get(row, col_map, "ip_location"),
                        desc=_get(row, col_map, "desc"),
                        gender=_get(row, col_map, "gender"),
                        follows=str(_to_int(_get(row, col_map, "follows"), 0)),
                        fans=str(_to_int(_get(row, col_map, "fans"), 0)),
                        interaction=str(_to_int(_get(row, col_map, "interaction"), 0)),
                        tag_list=_get(row, col_map, "tag_list"),
                        add_ts=now_ms,
                        last_modify_ts=now_ms,
                    )
                    objs.append(obj)
                if objs:
                    session.add_all(objs)
                logger.info(f"Inserted {len(objs)} creators into xhs_creator.")

        # ---------- xhs_note ----------
        if df_notes is not None and not df_notes.empty:
            col_map = _build_col_map(df_notes, NOTE_ALIASES)
            objs: list[XhsNote] = []
            if "note_id" not in col_map:
                logger.warning(
                    "Note sheet 缺少 note_id 列，将使用自动生成的 note_id（row_行号）。"
                )
            for idx, row in df_notes.iterrows():
                if "note_id" in col_map:
                    note_id = _get(row, col_map, "note_id")
                    if not note_id:
                        # 显式提供了 note_id 列，但这一行为空，则跳过
                        continue
                else:
                    # 没有 note_id 列时，使用行号生成一个稳定但无业务含义的 ID
                    note_id = f"row_{idx + 1}"

                t_ms = _to_ts_ms(_get(row, col_map, "time"), now_ms)

                # 处理标签：优先使用笔记内容标签，如果有“笔记分类 / 提及品类 / 种草品牌 / 商业合作品牌”
                # 会一并拼接进去，便于后续分析。
                raw_tag_list = _get(row, col_map, "tag_list")
                extra_tags: list[str] = []
                for col_name in ["笔记分类", "提及品类", "种草品牌", "商业合作品牌"]:
                    if col_name in df_notes.columns:
                        val = row.get(col_name)
                        if isinstance(val, str) and val.strip():
                            extra_tags.append(val.strip())
                if raw_tag_list is not None and str(raw_tag_list).strip():
                    extra_tags.insert(0, str(raw_tag_list).strip())
                merged_tag_list = " | ".join(extra_tags) if extra_tags else None

                # 如果 user_id 为空，使用占位符 "unknown" 避免 NOT NULL 约束报错
                raw_user_id = _get(row, col_map, "user_id")
                user_id_str = str(raw_user_id) if raw_user_id else "unknown"

                obj = XhsNote(
                    user_id=user_id_str,
                    nickname=_get(row, col_map, "nickname"),
                    avatar=_get(row, col_map, "avatar"),
                    ip_location=_get(row, col_map, "ip_location"),
                    add_ts=now_ms,
                    last_modify_ts=now_ms,
                    note_id=str(note_id),
                    type=None,
                    title=_get(row, col_map, "title"),
                    desc=_get(row, col_map, "desc"),
                    video_url=None,
                    time=t_ms,
                    last_update_time=now_ms,
                    liked_count=str(_to_int(_get(row, col_map, "liked_count"), 0)),
                    collected_count=str(_to_int(_get(row, col_map, "collected_count"), 0)),
                    comment_count=str(_to_int(_get(row, col_map, "comment_count"), 0)),
                    share_count=str(_to_int(_get(row, col_map, "share_count"), 0)),
                    image_list=None,
                    tag_list=merged_tag_list,
                    note_url=_get(row, col_map, "note_url"),
                    source_keyword=_get(row, col_map, "source_keyword"),
                    xsec_token=None,
                )
                objs.append(obj)
            if objs:
                session.add_all(objs)
            logger.info(f"Inserted {len(objs)} notes into xhs_note.")

        # ---------- xhs_note_comment ----------
        if df_comments is not None and not df_comments.empty:
            col_map = _build_col_map(df_comments, COMMENT_ALIASES)
            if "comment_id" not in col_map or "note_id" not in col_map:
                logger.warning(
                    "Comment sheet 需要至少包含 comment_id 与 note_id 列，跳过导入 xhs_note_comment。"
                )
            else:
                objs = []
                for _, row in df_comments.iterrows():
                    comment_id = _get(row, col_map, "comment_id")
                    note_id = _get(row, col_map, "note_id")
                    if not comment_id or not note_id:
                        continue
                    ct_ms = _to_ts_ms(_get(row, col_map, "create_time"), now_ms)
                    obj = XhsNoteComment(
                        user_id=str(_get(row, col_map, "user_id")) if _get(row, col_map, "user_id") else None,
                        nickname=_get(row, col_map, "nickname"),
                        avatar=_get(row, col_map, "avatar"),
                        ip_location=_get(row, col_map, "ip_location"),
                        add_ts=now_ms,
                        last_modify_ts=now_ms,
                        comment_id=str(comment_id),
                        create_time=ct_ms,
                        note_id=str(note_id),
                        content=_get(row, col_map, "content"),
                        sub_comment_count=_to_int(_get(row, col_map, "sub_comment_count"), 0) or 0,
                        pictures=_get(row, col_map, "pictures"),
                        parent_comment_id=_get(row, col_map, "parent_comment_id"),
                        like_count=str(_to_int(_get(row, col_map, "like_count"), 0)),
                    )
                    objs.append(obj)
                if objs:
                    session.add_all(objs)
                logger.info(f"Inserted {len(objs)} comments into xhs_note_comment.")


async def _main_async(args: argparse.Namespace) -> None:
    # 只创建表，不再尝试 CREATE DATABASE（数据库已存在时更安全）
    await create_tables_without_creating_database()
    await import_from_excel(
        Path(args.file),
        notes_sheet=args.notes_sheet,
        creators_sheet=args.creators_sheet,
        comments_sheet=args.comments_sheet,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import Xiaohongshu data from Excel into database."
    )
    parser.add_argument("--file", "-f", required=True, help="路径：Excel .xlsx 文件")
    parser.add_argument("--notes-sheet", default="xhs_note", help="笔记 Sheet 名称，默认 xhs_note")
    parser.add_argument(
        "--creators-sheet", default="xhs_creator", help="创作者 Sheet 名称，默认 xhs_creator"
    )
    parser.add_argument(
        "--comments-sheet",
        default="xhs_note_comment",
        help="评论 Sheet 名称，默认 xhs_note_comment",
    )
    args = parser.parse_args()

    asyncio.run(_main_async(args))


if __name__ == "__main__":
    main()

