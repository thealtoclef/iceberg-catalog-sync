"""Sync reporting models — SyncAction, SyncError, and SyncResult."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class ActionType(str, Enum):
    REGISTER = "register"
    UPDATE = "update"
    DROP = "drop"
    SKIP = "skip"
    CREATE_NAMESPACE = "create_namespace"
    UPDATE_NAMESPACE_PROPERTIES = "update_namespace_properties"


class SyncAction(BaseModel):
    action: ActionType
    namespace: str
    table: str | None = None
    metadata_location: str | None = None
    dry_run: bool = False


class SyncError(BaseModel):
    namespace: str
    table: str | None = None
    error: str


class SyncResult(BaseModel):
    actions: list[SyncAction] = Field(default_factory=list)
    errors: list[SyncError] = Field(default_factory=list)
    dry_run: bool = False

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    @property
    def summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for action in self.actions:
            key = action.action.value
            counts[key] = counts.get(key, 0) + 1
        counts["errors"] = len(self.errors)
        return counts
