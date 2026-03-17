import asyncio
import datetime
from urllib.parse import urlparse

import feedparser
import httpx
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message
from aiogram.filters import Command

from sqlalchemy import (
    Column, Integer, String, ForeignKey, DateTime,
    select, UniqueConstraint
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import (
    create_async_engine, async_sessionmaker
)

# ================= CONFIG =================
BOT_TOKEN = "YOUR_BOT_TOKEN"
DATABASE_URL = "postgresql+asyncpg://user:pass@localhost:5432/news"

# ================= DB =================
Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    telegram_id = Column(String, unique=True)


class Source(Base):
    __tablename__ = "sources"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class News(Base):
    __tablename__ = "news"

    id = Column(Integer, primary_key=True)
    title = Column(String)
    url = Column(String, unique=True)
    source_id = Column(Integer, ForeignKey("sources.id"))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class Subscription(Base):
    __tablename__ = "subscriptions"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    source_id = Column(Integer, ForeignKey("sources.id"))

    __table_args__ = (UniqueConstraint("user_id", "source_id"),)


engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

# ================= REPO =================


class NewsRepo:
    def __init__(self, session):
        self.session = session

    async def add_news(self, news_list):
        for n in news_list:
            res = await self.session.execute(
                select(News).where(News.url == n["url"])
            )
            if not res.scalar():
                self.session.add(News(**n))

        await self.session.commit()

    async def get_news(self, limit=5):
        res = await self.session.execute(
            select(News).order_by(News.created_at.desc()).limit(limit)
        )
        return res.scalars().all()


class UserRepo:
    def __init__(self, session):
        self.session = session

    async def get_or_create(self, telegram_id: str):
        res = await self.session.execute(
            select(User).where(User.telegram_id == telegram_id)
        )
        user = res.scalar()

        if not user:
            user = User(telegram_id=telegram_id)
            self.session.add(user)
            await self.session.commit()

        return user


class SubscriptionRepo:
    def __init__(self, session):
        self.session = session

    async def subscribe(self, user_id, source_id):
        self.session.add(Subscription(user_id=user_id, source_id=source_id))
        await self.session.commit()

    async def get_users_by_source(self, source_id):
        res = await self.session.execute(
            select(Subscription).where(Subscription.source_id == source_id)
        )
        return res.scalars().all()


# ================= PARSERS =================


class ReutersParser:
    URL = "https://www.reutersagency.com/feed/?best-topics=business-finance"

    async def fetch(self):
        feed = feedparser.parse(self.URL)
        return [
            {
                "title": entry.title,
                "url": entry.link,
                "source_id": 1
            }
            for entry in feed.entries[:10]
        ]


class KommersantParser:
    URL = "https://www.kommersant.ru"

    async def fetch(self):
        async with httpx.AsyncClient() as client:
            r = await client.get(self.URL)

        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select(".uho__link")

        news = []
        for item in items[:10]:
            news.append({
                "title": item.text.strip(),
                "url": self.URL + item["href"],
                "source_id": 2
            })

        return news


# ================= UTILS =================


def format_news(news):
    domain = urlparse(news.url).netloc
    return f"{news.title}\n{domain}"


# ================= BOT =================

router = Router()


@router.message(Command("start"))
async def start(msg: Message):
    async with SessionLocal() as session:
        repo = UserRepo(session)
        await repo.get_or_create(str(msg.from_user.id))

    await msg.answer(
        "Привет! Я агрегатор новостей 📰\n"
        "/news — получить новости\n"
        "/sub — подписаться"
    )


@router.message(Command("news"))
async def get_news(msg: Message):
    async with SessionLocal() as session:
        repo = NewsRepo(session)
        news_list = await repo.get_news()

    if not news_list:
        await msg.answer("Нет новостей")
        return

    text = "\n\n".join(format_news(n) for n in news_list)
    await msg.answer(text)


@router.message(Command("sub"))
async def subscribe(msg: Message):
    async with SessionLocal() as session:
        user_repo = UserRepo(session)
        sub_repo = SubscriptionRepo(session)

        user = await user_repo.get_or_create(str(msg.from_user.id))
        await sub_repo.subscribe(user.id, 1)

    await msg.answer("Подписка оформлена (Reuters)")


# ================= SCHEDULER =================


async def fetch_news():
    parsers = [ReutersParser(), KommersantParser()]

    async with SessionLocal() as session:
        repo = NewsRepo(session)

        for parser in parsers:
            try:
                news = await parser.fetch()
                await repo.add_news(news)
            except Exception as e:
                print("Parser error:", e)


async def scheduler(bot: Bot):
    while True:
        await fetch_news()

        async with SessionLocal() as session:
            repo = NewsRepo(session)
            news_list = await repo.get_news(limit=3)

            # примитивная рассылка
            res = await session.execute(select(User))
            users = res.scalars().all()

            for user in users:
                try:
                    text = "\n\n".join(format_news(n) for n in news_list)
                    await bot.send_message(user.telegram_id, text)
                except:
                    pass

        await asyncio.sleep(300)


# ================= MAIN =================


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    dp.include_router(router)

    asyncio.create_task(scheduler(bot))

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())