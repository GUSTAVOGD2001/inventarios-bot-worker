from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# --- Pagination ---

class PaginatedResponse(BaseModel):
    total: int
    page: int
    per_page: int
    items: list[Any]


# --- Pricing Rules ---

class PricingRuleCreate(BaseModel):
    name: str = Field(..., max_length=100)
    rule_type: str = Field(..., pattern=r"^(percentage|fixed_amount)$")
    value: float
    priority: int = 0
    is_active: bool = True


class PricingRuleUpdate(BaseModel):
    name: str | None = Field(None, max_length=100)
    rule_type: str | None = Field(None, pattern=r"^(percentage|fixed_amount)$")
    value: float | None = None
    priority: int | None = None
    is_active: bool | None = None


class PricingRuleOut(BaseModel):
    id: int
    name: str
    rule_type: str
    value: float
    priority: int
    is_active: bool
    created_at: datetime | None = None
    updated_at: datetime | None = None


# --- SKU Overrides ---

class SkuOverrideCreate(BaseModel):
    sku: str = Field(..., max_length=100)
    override_type: str = Field(..., pattern=r"^(fixed_price|percentage|fixed_amount)$")
    value: float
    is_active: bool = True
    notes: str | None = None


class SkuOverrideUpdate(BaseModel):
    sku: str | None = Field(None, max_length=100)
    override_type: str | None = Field(None, pattern=r"^(fixed_price|percentage|fixed_amount)$")
    value: float | None = None
    is_active: bool | None = None
    notes: str | None = None


class SkuOverrideOut(BaseModel):
    id: int
    sku: str
    override_type: str
    value: float
    is_active: bool
    notes: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


# --- Settings ---

class SettingsPatch(BaseModel):
    model_config = {"extra": "allow"}


# --- Worker ---

class WorkerTrigger(BaseModel):
    dry_run: bool = True


class WorkerStatusOut(BaseModel):
    status: str
    last_sync: dict | None = None
    next_sync_estimated: str | None = None
    dry_run: bool | None = None


# --- Pricing Simulate ---

class SimulateRequest(BaseModel):
    skus: list[str]
    preview_rules: dict | None = None


class PriceCalculation(BaseModel):
    sku: str
    ddvc_price: float | None
    override_applied: str | None = None
    global_rule_applied: str | None = None
    after_rules: float | None = None
    rounding_applied: bool = False
    final_price: float | None = None
    margin_amount: float | None = None
    margin_percent: float | None = None
    steps: list[str] = []


# --- SKU Analysis ---

class SkuAnalysis(BaseModel):
    sku: str
    shopify: dict | None = None
    ddvc: dict | None = None
    pricing: PriceCalculation | None = None
    history: list[dict] = []


# --- Stats ---

class StatsSummary(BaseModel):
    shopify_variants_count: int = 0
    sku_state_count: int = 0
    matched: int = 0
    ddvc_only: int = 0
    shopify_only: int = 0
    in_stock: int = 0
    out_of_stock: int = 0
    changes_today: int = 0
    settings: dict = {}
