"""Enumerations for lead status, source, and tags."""

from enum import Enum


class LeadStatus(str, Enum):
    NEW = "new"
    VIEWED = "viewed"
    IN_PROGRESS = "in_progress"
    REJECTED = "rejected"


class LeadSource(str, Enum):
    TELEGRAM = "telegram"
    FL_RU = "fl.ru"
    HABR_FREELANCE = "habr_freelance"
    ZAKUPKI_GOV = "zakupki_gov"


class LeadTag(str, Enum):
    URGENT = "urgent"
    NORMAL = "normal"
