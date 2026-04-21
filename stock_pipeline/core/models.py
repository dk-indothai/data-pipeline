"""Read-only SQLAlchemy mapping of the shared trading-system tables.

Source of truth is the Rust service's Diesel schema at
mono-p2/rust-utils/src/db/model.rs. Keep this file in sync manually if the
schema drifts — a mismatch only shows up at query time.
"""
from datetime import date
from decimal import Decimal

from sqlalchemy import Date, Integer, Numeric, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Instrument(Base):
    __tablename__ = "instruments"

    instrument_token: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange_token: Mapped[int | None] = mapped_column(Integer)
    tradingsymbol: Mapped[str | None] = mapped_column(String)
    name: Mapped[str | None] = mapped_column(String)
    last_price: Mapped[Decimal | None] = mapped_column(Numeric)
    expiry: Mapped[date | None] = mapped_column(Date)
    strike: Mapped[Decimal | None] = mapped_column(Numeric)
    tick_size: Mapped[Decimal | None] = mapped_column(Numeric)
    lot_size: Mapped[int | None] = mapped_column(Integer)
    instrument_type: Mapped[str | None] = mapped_column(String)
    segment: Mapped[str | None] = mapped_column(String)
    exchange: Mapped[str | None] = mapped_column(String)
