"""Enumerations for lead status, source, and tags."""

from enum import Enum


class LeadStatus(str, Enum):
    """Lifecycle status of a lead."""

    NEW = "new"
    VIEWED = "viewed"
    IN_PROGRESS = "in_progress"
    REJECTED = "rejected"


class LeadSource(str, Enum):
    """Known lead sources."""

    TELEGRAM = "telegram"
    FL_RU = "fl.ru"
    KWORK = "kwork.ru"
    WEBLANCER = "weblancer.net"
    ZAKUPKI_GOV = "zakupki_gov"
    PROFI_RU = "profi.ru"


class LeadTag(str, Enum):
    """Priority tags for leads."""

    URGENT = "urgent"
    NORMAL = "normal"
