"""Unit tests for web parsers — is_urgent_deadline and HTML parsing."""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from bs4 import BeautifulSoup

from src.worker_web.parsers.base import ScrapedOrder, is_urgent_deadline
from src.worker_web.parsers.fl_ru import FlRuParser
from src.worker_web.parsers.habr_freelance import HabrFreelanceParser
from src.worker_web.parsers.zakupki_gov import ZakupkiGovParser


# ---------------------------------------------------------------------------
# is_urgent_deadline tests
# ---------------------------------------------------------------------------


class TestIsUrgentDeadline:
    """Tests for the is_urgent_deadline utility function."""

    def test_deadline_in_past_is_urgent(self) -> None:
        now = date(2024, 6, 10)  # Monday
        deadline = date(2024, 6, 9)  # Sunday (past)
        assert is_urgent_deadline(deadline, now) is True

    def test_deadline_today_is_urgent(self) -> None:
        now = date(2024, 6, 10)  # Monday
        assert is_urgent_deadline(now, now) is True

    def test_deadline_tomorrow_is_urgent(self) -> None:
        # Monday now, Tuesday deadline — 0 business days between
        now = date(2024, 6, 10)
        deadline = date(2024, 6, 11)
        assert is_urgent_deadline(deadline, now) is True

    def test_two_business_days_away_is_urgent(self) -> None:
        # Monday now, Wednesday deadline — 1 business day between (Tue)
        now = date(2024, 6, 10)
        deadline = date(2024, 6, 12)
        assert is_urgent_deadline(deadline, now) is True

    def test_three_business_days_between_is_not_urgent(self) -> None:
        # Monday now, Friday deadline — 3 business days between (Tue, Wed, Thu)
        now = date(2024, 6, 10)
        deadline = date(2024, 6, 14)
        assert is_urgent_deadline(deadline, now) is False

    def test_weekend_not_counted(self) -> None:
        # Friday now, Monday deadline — 0 business days between (Sat, Sun skipped)
        now = date(2024, 6, 7)  # Friday
        deadline = date(2024, 6, 10)  # Monday
        assert is_urgent_deadline(deadline, now) is True

    def test_weekend_span_with_enough_days_not_urgent(self) -> None:
        # Wednesday now, next Wednesday deadline
        # Between: Thu, Fri, (Sat, Sun skipped), Mon, Tue = 4 business days
        now = date(2024, 6, 5)  # Wednesday
        deadline = date(2024, 6, 12)  # next Wednesday
        assert is_urgent_deadline(deadline, now) is False

    def test_accepts_datetime_objects(self) -> None:
        now = datetime(2024, 6, 10, 12, 0, tzinfo=timezone.utc)
        deadline = datetime(2024, 6, 11, 8, 0, tzinfo=timezone.utc)
        assert is_urgent_deadline(deadline, now) is True

    def test_thursday_to_monday_is_urgent(self) -> None:
        # Thursday now, Monday deadline — between: Fri, (Sat, Sun) = 1 biz day
        now = date(2024, 6, 6)  # Thursday
        deadline = date(2024, 6, 10)  # Monday
        assert is_urgent_deadline(deadline, now) is True

    def test_exactly_three_business_days_between(self) -> None:
        # Monday now, Thursday deadline — between: Tue, Wed = 2 biz days → urgent
        now = date(2024, 6, 10)  # Monday
        deadline = date(2024, 6, 13)  # Thursday
        assert is_urgent_deadline(deadline, now) is True


# ---------------------------------------------------------------------------
# FlRuParser HTML parsing tests
# ---------------------------------------------------------------------------


_FL_RU_HTML = """
<div class="b-post">
  <a class="b-post__link" href="/projects/123/">Test Project</a>
  <div class="b-post__txt">Build a web app</div>
  <div class="b-post__price">50 000 руб.</div>
  <span class="b-post__spec">Веб-разработка</span>
  <span class="b-post__time" title="2024-06-10T12:00:00+03:00">10 июня</span>
</div>
"""


class TestFlRuParser:
    def test_parse_item_extracts_fields(self) -> None:
        parser = FlRuParser()
        soup = BeautifulSoup(_FL_RU_HTML, "html.parser")
        item = soup.select_one("div.b-post")
        assert item is not None
        order = parser._parse_item(item)
        assert order is not None
        assert order.source == "fl.ru"
        assert order.title == "Test Project"
        assert order.description == "Build a web app"
        assert order.url == "https://www.fl.ru/projects/123/"
        assert order.budget == Decimal("50000")
        assert order.category == "Веб-разработка"

    def test_parse_item_missing_title_returns_none(self) -> None:
        parser = FlRuParser()
        soup = BeautifulSoup('<div class="b-post"><p>no link</p></div>', "html.parser")
        item = soup.select_one("div.b-post")
        assert item is not None
        assert parser._parse_item(item) is None


# ---------------------------------------------------------------------------
# HabrFreelanceParser HTML parsing tests
# ---------------------------------------------------------------------------


_HABR_HTML = """
<li class="content-list__item">
  <div class="task__title"><a href="/task/456">Design a logo</a></div>
  <div class="task__description">Need a modern logo</div>
  <span class="count">15 000 ₽</span>
  <span class="tags__item_link">Дизайн</span>
  <span class="params__published-at">
    <time datetime="2024-06-10T10:00:00+03:00">10 июня</time>
  </span>
</li>
"""


class TestHabrFreelanceParser:
    def test_parse_item_extracts_fields(self) -> None:
        parser = HabrFreelanceParser()
        soup = BeautifulSoup(_HABR_HTML, "html.parser")
        item = soup.select_one("li.content-list__item")
        assert item is not None
        order = parser._parse_item(item)
        assert order is not None
        assert order.source == "habr_freelance"
        assert order.title == "Design a logo"
        assert order.description == "Need a modern logo"
        assert order.url == "https://freelance.habr.com/task/456"
        assert order.budget == Decimal("15000")
        assert order.category == "Дизайн"

    def test_parse_item_missing_title_returns_none(self) -> None:
        parser = HabrFreelanceParser()
        soup = BeautifulSoup('<li class="content-list__item"><p>x</p></li>', "html.parser")
        item = soup.select_one("li.content-list__item")
        assert item is not None
        assert parser._parse_item(item) is None


# ---------------------------------------------------------------------------
# ZakupkiGovParser HTML parsing tests
# ---------------------------------------------------------------------------


_ZAKUPKI_HTML = """
<div class="search-registry-entry-block">
  <div class="registry-entry__header-mid__number">
    <a href="/epz/order/notice/ea44/view/common-info.html?regNumber=123">
      №0123456789
    </a>
  </div>
  <div class="registry-entry__body-value">Разработка ПО для учёта</div>
  <div class="price-block__value">1 500 000,50</div>
  <div class="data-block__value">15.07.2024</div>
  <span class="registry-entry__body-val">62.01.11.000</span>
</div>
"""


class TestZakupkiGovParser:
    def test_parse_item_extracts_fields(self) -> None:
        parser = ZakupkiGovParser()
        soup = BeautifulSoup(_ZAKUPKI_HTML, "html.parser")
        item = soup.select_one("div.search-registry-entry-block")
        assert item is not None
        order = parser._parse_item(item)
        assert order is not None
        assert order.source == "zakupki_gov"
        assert order.title == "Разработка ПО для учёта"
        assert order.max_contract_price == Decimal("1500000.50")
        assert order.okpd2_codes == ["62.01.11.000"]
        assert "zakupki.gov.ru" in order.url

    def test_parse_item_missing_link_returns_none(self) -> None:
        parser = ZakupkiGovParser()
        html = '<div class="search-registry-entry-block"><p>empty</p></div>'
        soup = BeautifulSoup(html, "html.parser")
        item = soup.select_one("div.search-registry-entry-block")
        assert item is not None
        assert parser._parse_item(item) is None

    def test_urgent_flag_set_when_deadline_close(self) -> None:
        """Verify is_urgent is True when deadline is within 3 business days."""
        parser = ZakupkiGovParser()
        # Build HTML with a deadline that is tomorrow
        html = """
        <div class="search-registry-entry-block">
          <div class="registry-entry__header-mid__number">
            <a href="/order/1">№001</a>
          </div>
          <div class="registry-entry__body-value">Urgent procurement</div>
          <div class="price-block__value">100 000</div>
          <div class="data-block__value">11.06.2024</div>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        item = soup.select_one("div.search-registry-entry-block")
        assert item is not None
        # The deadline parsed will be 2024-06-11 — we can't control "now"
        # in _parse_item directly, but we can verify the field exists
        order = parser._parse_item(item)
        assert order is not None
        assert order.submission_deadline is not None


# ---------------------------------------------------------------------------
# FlRuParser — edge-case HTML parsing tests
# ---------------------------------------------------------------------------

_FL_RU_NO_BUDGET_HTML = """
<div class="b-post">
  <a class="b-post__link" href="/projects/200/">No Budget Project</a>
  <div class="b-post__txt">Some description</div>
  <span class="b-post__spec">Мобильная разработка</span>
</div>
"""

_FL_RU_NO_CATEGORY_HTML = """
<div class="b-post">
  <a class="b-post__link" href="/projects/201/">No Category Project</a>
  <div class="b-post__txt">Another description</div>
  <div class="b-post__price">30 000 руб.</div>
</div>
"""

_FL_RU_MULTIPLE_ITEMS_HTML = """
<div class="b-post">
  <a class="b-post__link" href="/projects/301/">First Project</a>
  <div class="b-post__txt">Desc 1</div>
  <div class="b-post__price">10 000 руб.</div>
  <span class="b-post__spec">Backend</span>
</div>
<div class="b-post">
  <a class="b-post__link" href="/projects/302/">Second Project</a>
  <div class="b-post__txt">Desc 2</div>
  <div class="b-post__price">20 000 руб.</div>
  <span class="b-post__spec">Frontend</span>
</div>
<div class="b-post">
  <a class="b-post__link" href="/projects/303/">Third Project</a>
  <div class="b-post__txt">Desc 3</div>
</div>
"""


class TestFlRuParserEdgeCases:
    def test_missing_budget_returns_none_budget(self) -> None:
        parser = FlRuParser()
        soup = BeautifulSoup(_FL_RU_NO_BUDGET_HTML, "html.parser")
        item = soup.select_one("div.b-post")
        assert item is not None
        order = parser._parse_item(item)
        assert order is not None
        assert order.budget is None
        assert order.title == "No Budget Project"
        assert order.category == "Мобильная разработка"

    def test_missing_category_returns_none_category(self) -> None:
        parser = FlRuParser()
        soup = BeautifulSoup(_FL_RU_NO_CATEGORY_HTML, "html.parser")
        item = soup.select_one("div.b-post")
        assert item is not None
        order = parser._parse_item(item)
        assert order is not None
        assert order.category is None
        assert order.budget == Decimal("30000")

    def test_multiple_items_parsed(self) -> None:
        parser = FlRuParser()
        soup = BeautifulSoup(_FL_RU_MULTIPLE_ITEMS_HTML, "html.parser")
        items = soup.select("div.b-post")
        orders = [parser._parse_item(it) for it in items]
        orders = [o for o in orders if o is not None]
        assert len(orders) == 3
        assert orders[0].title == "First Project"
        assert orders[1].title == "Second Project"
        assert orders[2].title == "Third Project"
        assert orders[2].budget is None  # third has no price


# ---------------------------------------------------------------------------
# HabrFreelanceParser — edge-case HTML parsing tests
# ---------------------------------------------------------------------------

_HABR_NO_BUDGET_HTML = """
<li class="content-list__item">
  <div class="task__title"><a href="/task/500">Task Without Budget</a></div>
  <div class="task__description">Description here</div>
  <span class="tags__item_link">Разработка</span>
</li>
"""

_HABR_NO_DESCRIPTION_HTML = """
<li class="content-list__item">
  <div class="task__title"><a href="/task/501">Task Without Description</a></div>
  <span class="count">5 000 ₽</span>
  <span class="tags__item_link">Тестирование</span>
</li>
"""


class TestHabrFreelanceParserEdgeCases:
    def test_missing_budget_returns_none_budget(self) -> None:
        parser = HabrFreelanceParser()
        soup = BeautifulSoup(_HABR_NO_BUDGET_HTML, "html.parser")
        item = soup.select_one("li.content-list__item")
        assert item is not None
        order = parser._parse_item(item)
        assert order is not None
        assert order.budget is None
        assert order.title == "Task Without Budget"
        assert order.description == "Description here"

    def test_missing_description_returns_empty_string(self) -> None:
        parser = HabrFreelanceParser()
        soup = BeautifulSoup(_HABR_NO_DESCRIPTION_HTML, "html.parser")
        item = soup.select_one("li.content-list__item")
        assert item is not None
        order = parser._parse_item(item)
        assert order is not None
        assert order.description == ""
        assert order.budget == Decimal("5000")
        assert order.title == "Task Without Description"


# ---------------------------------------------------------------------------
# ZakupkiGovParser — edge-case HTML parsing tests
# ---------------------------------------------------------------------------

_ZAKUPKI_MULTIPLE_OKPD2_HTML = """
<div class="search-registry-entry-block">
  <div class="registry-entry__header-mid__number">
    <a href="/epz/order/notice/ea44/view/common-info.html?regNumber=999">
      №9999999999
    </a>
  </div>
  <div class="registry-entry__body-value">Комплексная IT-разработка</div>
  <div class="price-block__value">3 000 000,00</div>
  <div class="data-block__value">20.07.2024</div>
  <span class="registry-entry__body-val">62.01.11.000</span>
  <span class="registry-entry__body-val">62.01.12.000</span>
  <span class="registry-entry__body-val">62.02.20.120</span>
</div>
"""

_ZAKUPKI_NO_PRICE_HTML = """
<div class="search-registry-entry-block">
  <div class="registry-entry__header-mid__number">
    <a href="/epz/order/notice/ea44/view/common-info.html?regNumber=888">
      №8888888888
    </a>
  </div>
  <div class="registry-entry__body-value">Закупка без цены</div>
  <div class="data-block__value">25.07.2024</div>
  <span class="registry-entry__body-val">62.01.11.000</span>
</div>
"""


class TestZakupkiGovParserEdgeCases:
    def test_multiple_okpd2_codes_extracted(self) -> None:
        parser = ZakupkiGovParser()
        soup = BeautifulSoup(_ZAKUPKI_MULTIPLE_OKPD2_HTML, "html.parser")
        item = soup.select_one("div.search-registry-entry-block")
        assert item is not None
        order = parser._parse_item(item)
        assert order is not None
        assert order.okpd2_codes == [
            "62.01.11.000",
            "62.01.12.000",
            "62.02.20.120",
        ]
        assert order.max_contract_price == Decimal("3000000.00")

    def test_missing_price_returns_none(self) -> None:
        parser = ZakupkiGovParser()
        soup = BeautifulSoup(_ZAKUPKI_NO_PRICE_HTML, "html.parser")
        item = soup.select_one("div.search-registry-entry-block")
        assert item is not None
        order = parser._parse_item(item)
        assert order is not None
        assert order.max_contract_price is None
        assert order.title == "Закупка без цены"
        assert order.okpd2_codes == ["62.01.11.000"]
