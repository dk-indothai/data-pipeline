"""Read-only SQLAlchemy mapping of the shared trading-system tables.

Source of truth is the Rust service's Diesel schema at
mono-p2/rust-utils/src/db/model.rs. Keep this file in sync manually if the
schema drifts — a mismatch only shows up at query time.
"""
from datetime import date
from decimal import Decimal

from sqlalchemy import Date, Float, Integer, Numeric, String
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


class SymphonyInstruments(Base):
    __tablename__ = "symphony_instruments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange_segment: Mapped[str] = mapped_column(String)
    scrip_code: Mapped[int] = mapped_column(Integer)
    instrument_type: Mapped[str] = mapped_column(String)
    name: Mapped[str] = mapped_column(String)
    trading_symbol: Mapped[str] = mapped_column(String)
    series: Mapped[str] = mapped_column(String)
    instrument_id: Mapped[str] = mapped_column(String)
    freeze_qty: Mapped[int] = mapped_column(Integer)
    upper_circuit: Mapped[float | None] = mapped_column(Float)
    lower_circuit: Mapped[float | None] = mapped_column(Float)
    tick_size: Mapped[Decimal] = mapped_column(Numeric)
    lot_size: Mapped[int] = mapped_column(Integer)
    multiplier: Mapped[float] = mapped_column(Float)
    underlying_instrument_id: Mapped[str | None] = mapped_column(String)
    underlying_symbol_name: Mapped[str | None] = mapped_column(String)
    expiry_date: Mapped[date | None] = mapped_column(Date)
    strike_price: Mapped[float | None] = mapped_column(Float)
    option_type: Mapped[str | None] = mapped_column(String)
