# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from aops_crawler.items import CategoryItem, PostItem
import re
from datetime import datetime, timedelta
try:
    import dateparser  # optional
except Exception:  # pragma: no cover
    dateparser = None
from aops_crawler.db.sqlite_store import SqliteStore
import json
import os
import logging
from aops_crawler.single_page import TAGS_XPATH


logger = logging.getLogger(__name__)


def transform_cmty_post_html(html: str) -> str:
    # Replace <img ... alt="..."> with its alt text (LaTeX)
    html = re.sub(r'<img\b[^>]*\balt="([^"]*)"[^>]*>', lambda m: m.group(1), html)
    # Convert <br> to newlines
    html = re.sub(r'<br\s*/?>', '\n', html, flags=re.I)
    # Convert <i>..</i> to [i]..[/i]
    html = re.sub(r'<i\b[^>]*>', '[i]', html, flags=re.I)
    html = re.sub(r'</i>', '[/i]', html, flags=re.I)
    # Unwrap any spans (AoPS adds white-space:pre wrappers)
    html = re.sub(r'</?span\b[^>]*>', '', html, flags=re.I)
    # Drop div wrappers
    html = re.sub(r'</?div\b[^>]*>', '', html, flags=re.I)
    # Remove remaining tags, keep text
    html = re.sub(r'<[^>]+>', '', html)
    # Normalize whitespace
    html = re.sub(r'[\t\r\f]+', ' ', html)
    html = re.sub(r'\s*\n\s*', '\n', html).strip()
    return html


def normalize_backslashes(text: str | None) -> str | None:
    if text is None:
        return None
    # Ensure single backslashes for latex sequences like \\alpha → \\alpha
    return text.replace('\\\\', '\\')


def parse_aops_time(s: str | None) -> float | None:
    if not s:
        return None
    # Determine local timezone from system
    local_tz = datetime.now().astimezone().tzinfo
    if dateparser:
        dt = dateparser.parse(
            s,
            languages=["en"],
            settings={
                "RELATIVE_BASE": datetime.now(local_tz),
                "PREFER_DATES_FROM": "past",
                "DATE_ORDER": "MDY",
                "RETURN_AS_TIMEZONE_AWARE": True,
            },
        )
        if not dt:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=local_tz)
        else:
            dt = dt.astimezone(local_tz)
        return dt.timestamp()

    # Fallback simple parser
    try:
        dt = datetime.strptime(s, "%b %d, %Y, %I:%M %p").replace(tzinfo=local_tz)
        return dt.timestamp()
    except Exception:
        pass

    m = re.match(r'^(Yesterday|Today)\s+at\s+(\d{1,2}:\d{2}\s?[AP]M)$', s, re.I)
    if m:
        day_word, time_part = m.groups()
        base = datetime.now(local_tz)
        if day_word.lower() == "yesterday":
            base = base - timedelta(days=1)
        try:
            t = datetime.strptime(time_part.upper(), "%I:%M %p").time()
            dt = datetime.combine(base.date(), t, tzinfo=local_tz)
            return dt.timestamp()
        except Exception:
            return None

    m = re.match(r'^(\d+)\s+(second|minute|hour|day|week)s?\s+ago$', s, re.I)
    if m:
        n, unit = m.groups()
        n = int(n)
        kw = {"seconds": n} if unit.lower() == "second" else \
             {"minutes": n} if unit.lower() == "minute" else \
             {"hours": n}   if unit.lower() == "hour"   else \
             {"days": n}    if unit.lower() == "day"    else \
             {"weeks": n}
        return (datetime.now(local_tz) - timedelta(**kw)).timestamp()

    if s.strip().lower() == "just now":
        return datetime.now(local_tz).timestamp()
    return None


class AopsCrawlerPipeline:
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        pipeline._sqlite_path = crawler.settings.get("AOPS_SQLITE_PATH")
        pipeline._store = None
        if pipeline._sqlite_path:
            try:
                pipeline._store = SqliteStore(pipeline._sqlite_path)
                pipeline._store.open()
                logger.info(f"[PIPELINE] Opened SqliteStore at {pipeline._sqlite_path}")
            except Exception as e:
                logger.warning(f"[PIPELINE] Failed to open SqliteStore: {e}")
        else:
            logger.info("[PIPELINE] No AOPS_SQLITE_PATH configured; DB writes disabled")
        return pipeline

    def open_spider(self, spider):
        logger.info("[PIPELINE] Pipeline opened")

    def close_spider(self, spider):
        try:
            if getattr(self, "_store", None) is not None:
                self._store.commit()
                self._store.close()
                logger.info("[PIPELINE] SqliteStore closed")
        except Exception as e:
            logger.warning(f"[PIPELINE] Error closing SqliteStore: {e}")
        logger.info("[PIPELINE] Pipeline closed")

    def process_item(self, item, spider):
        if isinstance(item, CategoryItem):
            category_id = item.get("category_id")
            name = item.get("raw").get("item_text")
            parent_id = item.get("parent_id")

            logger.info(f"[PIPELINE] CategoryItem: id={category_id}, parent_id={parent_id}, name={name}")
            # Persist category and connection
            try:
                if getattr(self, "_store", None) is not None:
                    raw_json = None
                    try:
                        raw_json = json.dumps(item.get("raw")) if item.get("raw") is not None else None
                    except Exception:
                        raw_json = None
                    self._store.upsert_category(
                        category_id=category_id,
                        name=name,
                        subtitle=item.get("raw").get("item_subtitle"),
                        url=item.get("url"),
                        raw_json=raw_json,
                    )
                    self._store.link(parent_id=parent_id, child_id=category_id, type_of_child="category")
                    self._store.commit()
            except Exception as e:
                logger.warning(f"[PIPELINE] Failed to persist CategoryItem {category_id}: {e}")
            return item

        if isinstance(item, PostItem):
            post_id = item.get("post_id")
            parent_id = item.get("parent_id")
            source = item.get("url")
            response = item.get("response")

            # Record category -> thread link only (no thread table)
            try:
                if getattr(self, "_store", None) is not None:
                    self._store.link(parent_id=parent_id, child_id=post_id, type_of_child="post")
                    self._store.commit()
            except Exception as e:
                logger.warning(f"[PIPELINE] Failed to link thread {post_id} to category {parent_id}: {e}")

            # Capture tags from the page and store as (thread_id, tag)
            try:
                if getattr(self, "_store", None) is not None and response is not None:
                    tag_texts = [
                        t.strip()
                        for t in response.xpath(f'{TAGS_XPATH}//a/div[contains(@class,"cmty-item-tag")]/text()').getall()
                        if t and t.strip()
                    ]
                    for tag in tag_texts:
                        self._store.add_tag(thread_id=post_id, tag=tag)
                    self._store.commit()
            except Exception as e:
                logger.warning(f"[PIPELINE] Failed to persist tags for thread {post_id}: {e}")

            if response:
                # Iterate posts in the right container
                posts = response.xpath('/html/body/div[1]/div[3]/div/div/div[3]/div/div[4]/div/div[2]/div').xpath('./div[contains(@class, "cmty-post")]')
                is_first = True
                for post in posts:
                    mid = post.xpath('.//div[contains(@class,"cmty-post-middle")]')
                    created_text = mid.xpath('normalize-space(.//span[contains(@class,"cmty-post-date")])').get()
                    created_ts = parse_aops_time(created_text)
                    thanks_text = mid.xpath('normalize-space(.//span[contains(@class,"cmty-post-thank-count")])').get() or ''
                    m_thanks = re.search(r'\d+', thanks_text)
                    thanks_count = int(m_thanks.group(0)) if m_thanks else 0

                    nothanks_text = mid.xpath('normalize-space(.//span[contains(@class,"cmty-post-nothank-count")])').get() or ''
                    m_nothanks = re.search(r'\d+', nothanks_text)
                    nothanks_count = int(m_nothanks.group(0)) if m_nothanks else 0
                    user_id = post.xpath('normalize-space(substring-after((.//a[starts-with(@href,"/community/user/")]/@href)[1], "/community/user/"))').get()
                    post_html = ''.join(post.xpath('.//div[contains(@class,"cmty-post-html")]/node()').getall()).strip()
                    post_text = transform_cmty_post_html(post_html)

                    # Append to test log
                    try:
                        os.makedirs("test", exist_ok=True)
                    except Exception:
                        pass
                    with open("test/post_log.txt", "a", encoding="utf-8") as f:
                        f.write(f"{post_id}\n")
                        f.write(f"{parent_id}\n")
                        f.write(f"{source}\n")
                        f.write(f"{user_id}\n")
                        f.write(f"{created_text}\n")
                        f.write(f"{created_ts}\n")
                        f.write(f"{thanks_count}\n")
                        f.write(f"{nothanks_count}\n")
                        f.write(f"{post_html}\n")
                        f.write(f"{post_text}\n")

                    # Persist each message
                    try:
                        if getattr(self, "_store", None) is not None:
                            user_int = None
                            try:
                                user_int = int(user_id) if user_id and str(user_id).isdigit() else None
                            except Exception:
                                user_int = None
                            self._store.insert_post_message(
                                thread_id=post_id,
                                user_id=user_int,
                                created_at=created_ts,
                                thanks_count=thanks_count,
                                nothanks_count=nothanks_count,
                                raw_html=post_html,
                                processed_html=post_text,
                                is_first_post=is_first,
                                source=source,
                            )
                            is_first = False
                    except Exception as e:
                        logger.warning(f"[PIPELINE] Failed to persist message in thread {post_id}: {e}")
                try:
                    if getattr(self, "_store", None) is not None:
                        self._store.commit()
                except Exception:
                    pass

            logger.info(f"[PIPELINE] PostItem: id={post_id}, parent_id={parent_id}, source={source}")
            return item

        # default passthrough
        return item
