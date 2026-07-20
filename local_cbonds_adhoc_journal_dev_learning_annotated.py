"""Локальный автономный runner журнальной загрузки Cbonds AdHoc (DEV).

Этот файл предназначен только для разработки и ручного тестирования в PyCharm.
Он не используется Airflow: боевой/тестовый контур в репозитории —
`airflow-dags/cbonds/ADHOC_DEV/dag_cbonds_adhoc_journal_dev.py`.

Что делает скрипт
-----------------
1. Берёт из MS SQL Server записи журнала `BCS_BIZ.BBG.AdHocRequest_DEV`,
   созданные SQL-планировщиком/чекером для источника Cbonds (Source_code=12).
2. По каждой записи определяет метод Cbonds, тип запроса, фильтры, поля и сортировку.
3. Отправляет запросы в JSON API Cbonds с пагинацией по 1000 записей.
4. Первую страницу ответа кладёт в исходную запись журнала.
5. Если страниц больше одной — создаёт дочерние записи через
   `BCS_STG.BBG.AdHocResponse_auto_CBonds_DEV` и пишет туда JSON.
6. Успехи и ошибки дополнительно пишет в `BCS_BIZ.BBG.spNSD_CBonds_ExLog_DEV`.

Где настраивать подключения
----------------------------
Все настройки лежат **внутри этого .py файла** в секции CONFIG (ниже).
Отдельного config.yaml / .env для runner нет.

Креды можно задать двумя способами (см. resolve_runtime_config):
  1. Environment variables в PyCharm Run Configuration — рекомендуется;
  2. Константы в секции CONFIG этого файла — если env не задан.

Быстрый старт (PyCharm, без Airflow)
------------------------------------
  1. pip install requests pyodbc
  2. Установить ODBC Driver 17 for SQL Server (или указать свой в CONFIG).
  3. Run Configuration → Environment variables:
       CBONDS_LOGIN=...
       CBONDS_PASSWORD=...
       MSSQL_HOST=...
       MSSQL_PORT=1433
       MSSQL_DATABASE=BCS_STG
       MSSQL_USER=...
       MSSQL_PASSWORD=...
       MSSQL_ODBC_DRIVER=ODBC Driver 17 for SQL Server
  4. Script path: этот файл.
  5. Working directory: любая (скрипт самодостаточен).

Важно: скрипт пишет в DEV SQL-объекты (*_DEV). View
`DWH_BCS.OHD.vRequestTypesCbonds` общий с PROD.
"""

# =============================================================================
# УЧЕБНАЯ КАРТА ФАЙЛА
# =============================================================================
#
# Этот файл выполняется сверху вниз:
#
#   1. Python импортирует модули, создаёт константы, классы и функции.
#      Тела функций на этом этапе НЕ запускаются — Python только связывает
#      имя функции с созданным объектом функции.
#   2. В самом низу условие `if __name__ == "__main__"` вызывает main(),
#      но только при прямом запуске файла. При импорте main() не вызывается.
#   3. main() собирает конфигурацию и запускает run_cbonds_adhoc_journal().
#   4. Runner читает pending-записи из SQL Server и обрабатывает их по одной.
#   5. Для каждой записи выполняется HTTP-запрос Cbonds; ответ сохраняется
#      обратно в журнал SQL Server. Дополнительные страницы получают дочерние
#      строки журнала.
#
# Главная цепочка вызовов:
#
#   main
#     -> run_cbonds_adhoc_journal
#          -> fetch_pending_requests
#          -> process_one_request
#               -> process_json_method ИЛИ process_schema_method
#                    -> HTTP Cbonds
#                    -> mark_request_completed / save_cbonds_page
#
# Сквозной пример пагинации
# -------------------------
# Допустим, из SQL выбрана строка request_id=42, а первый ответ Cbonds содержит
# total=2500. При PAGE_LIMIT=1000 цикл range(0, 2500, 1000) даст три offset:
#
#   offset=0    -> ответ уже получен первым POST и сохраняется в request_id=42;
#   offset=1000 -> выполняется новый POST, создаётся первая дочерняя строка;
#   offset=2000 -> выполняется ещё один POST, создаётся вторая дочерняя строка.
#
# Функция process_json_method() возвращает 2500. process_one_request() пишет
# этот total в технический success-log и возвращает True. Внешний цикл видит
# True и увеличивает stats["success"] на единицу: считается успешно обработанный
# метод/запрос журнала, а не количество строк Cbonds.
#
# Комментарии здесь объясняют причины решений и движение данных. Они стоят
# СНАРУЖИ строковых литералов. Особенно важно не помещать Python-комментарии
# внутрь тройных кавычек с SQL: там символы `# ...` становятся частью SQL.

from __future__ import annotations

# Благодаря future-import аннотации типов вычисляются отложенно. Например,
# `pyodbc.Connection` ниже не разыменовывается во время импорта модуля.

import ast  # Безопасно разбирает старые Python-like литералы через literal_eval; код из строки не выполняется.
import json  # Преобразует JSON-текст в Python-объекты и обратно.
import logging  # Пишет ход загрузки, номера запросов, попытки и ошибки.
import os  # Читает переменные окружения, заданные в PyCharm или Windows.
import time  # Даёт time.sleep() для пауз между страницами и повторными попытками.
from dataclasses import dataclass  # Генерирует конструкторы классов-контейнеров RuntimeConfig и JournalRequest.
from datetime import datetime, timedelta  # datetime хранит момент времени, timedelta — длительность или смещение.
from typing import Any, Iterable  # Any допускает разные типы значений; Iterable означает «можно перебрать в for».
from urllib.parse import urlencode  # Безопасно кодирует логин, пароль и lang как параметры URL.
from xml.sax.saxutils import escape  # Экранирует символы &, < и > для хранения текста в XML-поле.

import requests  # Выполняет HTTP POST/GET к Cbonds и предоставляет переиспользуемую Session.

try:
    import pyodbc
except ImportError:  # pragma: no cover
    # Импорт файла остаётся возможным без pyodbc. Понятная ошибка появится
    # позже, только если код действительно попробует открыть SQL-соединение.
    pyodbc = None


log = logging.getLogger(__name__)  # Имя логгера совпадает с именем модуля и отображается в каждой строке лога.


# =============================================================================
# CONFIG — настройки внутри ЭТОГО файла (отдельного конфиг-файла нет)
# =============================================================================
#
# Приоритет при запуске:
#   1) переменные окружения (PyCharm Run Configuration);
#   2) константы ниже, если env пустой.
#
# Cbonds API:
#   CBONDS_LOGIN, CBONDS_PASSWORD
#
# MS SQL Server:
#   MSSQL_HOST, MSSQL_PORT, MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD
#   MSSQL_ODBC_DRIVER
#   MSSQL_TRUSTED_CONNECTION=yes — Windows Integrated Security без пароля
#
# HTTP proxy (опционально):
#   HTTP_PROXY, HTTPS_PROXY
# =============================================================================

# --- Cbonds API ---
CBONDS_LOGIN = ""  # Резервный логин; предпочтительно задавать через Environment Variables.
CBONDS_PASSWORD = ""  # Резервный пароль; реальный секрет не следует сохранять в файле или Git.

# --- MS SQL Server (журнал AdHocRequest_DEV) ---
MSSQL_HOST = ""  # DNS-имя или IP SQL Server без порта.
MSSQL_PORT = 1433  # TCP-порт хранится как int; env позднее преобразуется из строки через int().
MSSQL_DATABASE = "BCS_STG"  # База по умолчанию, с которой начинается SQL-сессия.
MSSQL_USER = ""  # SQL-login, когда Trusted Connection отключён.
MSSQL_PASSWORD = ""  # Пароль SQL-login; в лог он не выводится.
MSSQL_ODBC_DRIVER = "ODBC Driver 17 for SQL Server"  # Имя должно совпадать с установленным ODBC-драйвером.
# True = Trusted_Connection=yes (доменная учётка процесса Windows)
MSSQL_TRUSTED_CONNECTION = False  # True переключает подключение на Windows Integrated Security.

# --- HTTP proxy (None = без прокси) ---
PROXIES: dict[str, str] | None = None  # Словарь вида {"https": "http://proxy:port"} либо None.

# --- Поведение процесса (как в Windows-скрипте / Airflow DAG) ---
SOURCE_CODE_CBONDS = 12  # Фильтр строк журнала: обрабатываются только запросы источника Cbonds.
EXLOG_GROUP_CBONDS = 12  # Группа процесса для технической процедуры логирования.
PAGE_LIMIT = 1000  # Максимальное число записей одной страницы Cbonds.
CBONDS_TIMEOUT_SECONDS = 600  # Максимальное ожидание одного HTTP-ответа: 10 минут.
MAX_METHOD_RETRIES = 25  # Число попыток обработки одной журнальной записи.
RETRY_DELAY_SECONDS = 120  # Пауза между попытками одной записи: 2 минуты.
PAGE_DELAY_SECONDS = 2.1  # Пауза между страницами снижает риск превышения rate limit.
FATAL_CBONDS_ERR_NOS = frozenset({500100})  # Неизменяемое множество API-кодов, для которых retry не помогает.

# False = как старый скрипт: ошибка одной записи не валит весь прогон.
# True = в конце main() будет ненулевой exit code при failed > 0.
FAIL_ON_METHOD_ERROR = False  # False разрешает завершить весь процесс успешно, даже если отдельные методы failed.


@dataclass(frozen=True)
class RuntimeConfig:
    """Итоговые параметры подключения после слияния env и CONFIG."""

    # dataclass генерирует __init__, __repr__ и сравнение объектов. frozen=True
    # запрещает случайно менять уже собранную конфигурацию во время запуска.
    cbonds_login: str  # Итоговый логин после выбора между env и константой.
    cbonds_password: str  # Итоговый пароль Cbonds.
    mssql_host: str  # Итоговый адрес SQL Server.
    mssql_port: int  # Итоговый числовой TCP-порт.
    mssql_database: str  # Имя стартовой базы данных.
    mssql_user: str  # Пользователь SQL-аутентификации; при Trusted Connection может быть пустым.
    mssql_password: str  # Пароль SQL-аутентификации.
    mssql_odbc_driver: str  # Имя ODBC-драйвера для connection string.
    mssql_trusted_connection: bool  # Выбран ли вход от имени Windows-учётной записи процесса.
    proxies: dict[str, str] | None  # Прокси, передаваемые requests, либо None.


def _env_first(*names: str, default: str = "") -> str:
    """Вернуть первое непустое значение из списка переменных окружения."""
    # `*names` собирает все позиционные аргументы в tuple. Вызов
    # _env_first("MSSQL_HOST", "MSSQL_SERVER") последовательно проверит
    # два допустимых имени одной настройки.
    for name in names:
        value = os.environ.get(name, "").strip()  # Отсутствующая env превращается в ""; пробелы по краям удаляются.
        if value:  # Непустая строка считается найденным значением.
            return value  # return сразу завершает функцию: остальные имена уже не проверяются.
    return default  # Используется только когда ни одна из перечисленных env не заполнена.


def _env_bool(*names: str, default: bool = False) -> bool:
    """Прочитать bool из env: 1/true/yes/on."""
    raw = _env_first(*names, default="")  # Получаем текст, потому что переменные окружения всегда строки.
    if not raw:  # Пустое значение означает «настройка не задана».
        return default  # Возвращаем bool по умолчанию, а не строку.
    return raw.lower() in {"1", "true", "yes", "on"}  # Оператор `in` проверяет принадлежность множеству допустимых true-значений.


def resolve_runtime_config() -> RuntimeConfig:
    """Собрать итоговый конфиг: env (PyCharm) имеет приоритет над константами CONFIG."""
    http_proxy = _env_first("HTTP_PROXY", "http_proxy")  # Поддерживаем написание env и в верхнем, и в нижнем регистре.
    https_proxy = _env_first("HTTPS_PROXY", "https_proxy")  # HTTPS-прокси проверяется независимо от HTTP.
    if https_proxy or http_proxy:
        proxies: dict[str, str] | None = {}  # Создаётся новый изменяемый словарь только при наличии хотя бы одного proxy URL.
        if http_proxy:
            proxies["http"] = http_proxy  # Ключ "http" — соглашение библиотеки requests.
        if https_proxy:
            proxies["https"] = https_proxy  # HTTPS-запросы будут направлены через этот адрес.
    else:
        proxies = PROXIES  # Если env отсутствуют, используем значение из CONFIG.

    port_raw = _env_first("MSSQL_PORT", default=str(MSSQL_PORT))  # str() нужен, чтобы оба источника имели один строковый тип.

    # Именованные аргументы делают явным соответствие источника значения полю
    # конфигурации. Результат функции — один RuntimeConfig, а не набор globals.
    return RuntimeConfig(
        cbonds_login=_env_first("CBONDS_LOGIN", default=CBONDS_LOGIN),
        cbonds_password=_env_first("CBONDS_PASSWORD", default=CBONDS_PASSWORD),
        mssql_host=_env_first("MSSQL_HOST", "MSSQL_SERVER", default=MSSQL_HOST),
        mssql_port=int(port_raw),  # Например, строка "1433" превращается в целое число 1433.
        mssql_database=_env_first("MSSQL_DATABASE", "MSSQL_DB", default=MSSQL_DATABASE),
        mssql_user=_env_first("MSSQL_USER", "MSSQL_LOGIN", default=MSSQL_USER),
        mssql_password=_env_first("MSSQL_PASSWORD", "MSSQL_PWD", default=MSSQL_PASSWORD),
        mssql_odbc_driver=_env_first("MSSQL_ODBC_DRIVER", default=MSSQL_ODBC_DRIVER),
        mssql_trusted_connection=_env_bool(
            "MSSQL_TRUSTED_CONNECTION",
            default=MSSQL_TRUSTED_CONNECTION,
        ),
        proxies=proxies,
    )


def validate_runtime_config(cfg: RuntimeConfig) -> None:
    """Проверить обязательные поля до обращения к API/БД."""
    missing: list[str] = []  # Накапливаем сразу все проблемы, чтобы пользователь исправил их за один запуск.
    if not cfg.cbonds_login:
        missing.append("CBONDS_LOGIN (env или константа в CONFIG)")  # append изменяет существующий список на месте.
    if not cfg.cbonds_password:
        missing.append("CBONDS_PASSWORD (env или константа в CONFIG)")
    if not cfg.mssql_host:
        missing.append("MSSQL_HOST (env или константа в CONFIG)")
    if not cfg.mssql_trusted_connection:  # При Windows-аутентификации отдельные SQL user/password не обязательны.
        if not cfg.mssql_user:
            missing.append("MSSQL_USER (env или константа в CONFIG)")
        if not cfg.mssql_password:
            missing.append("MSSQL_PASSWORD (env или константа в CONFIG)")
    if missing:  # Непустой список в условии имеет значение True.
        raise ValueError(
            "Не заданы обязательные параметры:\n  - "
            + "\n  - ".join(missing)  # join превращает список сообщений в одну многострочную строку.
            + "\n\nЗадайте их в PyCharm Run Configuration (env) "
            "или в секции CONFIG внутри этого .py файла."
        )


def log_runtime_config(cfg: RuntimeConfig) -> None:
    """Залогировать параметры подключения без паролей."""
    auth_mode = "Trusted_Connection" if cfg.mssql_trusted_connection else f"UID={cfg.mssql_user}"  # Тернарное выражение выбирает одно из двух представлений.
    log.info("Runtime config:")
    log.info("  Cbonds login: %s", cfg.cbonds_login)
    log.info(
        "  MSSQL: %s@%s:%s/%s (driver=%s)",
        auth_mode,
        cfg.mssql_host,
        cfg.mssql_port,
        cfg.mssql_database,
        cfg.mssql_odbc_driver,
    )
    log.info("  Journal: BCS_BIZ.BBG.AdHocRequest_DEV")
    log.info("  HTTP proxy: %s", "enabled" if cfg.proxies else "disabled")


# Активный конфиг на время одного запуска (заполняется в main).
_CFG: RuntimeConfig | None = None  # Модульное состояние: до main() активной конфигурации ещё нет.


def _require_cfg() -> RuntimeConfig:
    # `_CFG` имеет тип union: либо RuntimeConfig, либо None. После проверки
    # Python-код может безопасно вернуть именно RuntimeConfig.
    if _CFG is None:
        raise RuntimeError("RuntimeConfig не инициализирован. Запускайте через main().")
    return _CFG

# Группы методов из старого `CBonds_1.0.py`. Они нужны не для красоты, а для
# выбора разных тел запроса и разных правил сохранения страниц в журнал.
STATIC_METHODS_CBONDS = {
    # set выбран потому, что дальше нужна быстрая проверка `code in ...`,
    # а порядок перечисления методов для алгоритма не имеет значения.
    "cbonds_get_emissions",
    "cbonds_get_stocks_full",
    "cbonds_get_stocks_trading_grounds",
    "cbonds_get_index_types",
    "cbonds_get_emitents",
    "cbonds_get_emission_guarantors",
    "cbonds_get_offert",
    "cbonds_get_funds",
    "cbonds_get_etf_funds",
    "cbonds_get_etf_share_classes_trading_grounds",
    "cbonds_get_funds_companies",
    "cbonds_get_etf_share_classes_nav",
    "cbonds_get_funds_dividends",
    "cbonds_get_emission_default",
    "cbonds_get_emission_ratings_new",
    "cbonds_get_emitent_ratings_new",
    "cbonds_get_rating_scale_points",
    "cbonds_get_index_groups",
    "cbonds_get_stocks_placements",
    "cbonds_get_derivatives_contracts",
    "cbonds_get_derivatives_assets",
    "cbonds_get_finreporting_multiples_annual",
    "cbonds_get_finreporting_multiples_quarterly",
    "cbonds_get_finreporting_multiples_ttm",
    "cbonds_get_report_msfo_global_quarter",
    "cbonds_get_report_msfo_global_annual",
}

PRICE_METHODS_CBONDS = {
    # Методы цен относятся к той же группе данных, но выделены отдельно,
    # чтобы сохранить классификацию исторического Windows-скрипта.
    "cbonds_get_tradings_new",
    "cbonds_get_index_value_new",
    "cbonds_get_tradings_stocks_full_new",
    "cbonds_get_flow_new",
    "cbonds_get_stocks_dividends_v2",
    "cbonds_get_stocks_beta_coefficient",
    "cbonds_get_derivatives_tradings",
    "cbonds_get_etf_share_classes_quotes",
    "cbonds_get_stocks_full_splits",
}

DICT_METHODS_CBONDS = {
    # Справочники формируют другой HTTP-body: без filters и fields.
    "cbonds_get_auction_types",
    "cbonds_get_boards",
    "cbonds_get_branches",
    "cbonds_get_calendar_weekends_countries",
    "cbonds_get_calendar_weekends_currencies",
    "cbonds_get_calendar_weekends_trading_grounds",
    "cbonds_get_countries",
    "cbonds_get_coupon_types",
    "cbonds_get_currencies",
    "cbonds_get_day_count_conventions",
    "cbonds_get_default_types",
    "cbonds_get_emission_kinds",
    "cbonds_get_emission_statuses",
    "cbonds_get_emission_subkinds",
    "cbonds_get_emitent_types",
    "cbonds_get_offert_statuses",
    "cbonds_get_offert_types",
    "cbonds_get_participation_status",
    "cbonds_get_placing_types",
    "cbonds_get_private_offerings",
    "cbonds_get_rates",
    "cbonds_get_reg_forms",
    "cbonds_get_regions",
    "cbonds_get_stocks_dividends_periods",
    "cbonds_get_stocks_full_kinds",
    "cbonds_get_stocks_kot_micex",
    "cbonds_get_stocks_kot_rts",
    "cbonds_get_subregions",
    "cbonds_get_trading_ground_quotelists",
    "cbonds_get_trading_grounds",
    "cbonds_get_variable_rate_types",
    "cbonds_get_index_categories",
    "cbonds_get_index_groups2",
    "cbonds_get_index_content",
    "cbonds_get_emitent_categories",
    "cbonds_get_emission_issue_forms",
    "cbonds_get_stocks",
    "cbonds_get_entity_codes",
    "cbonds_get_rating_scales",
    "cbonds_get_rating_forecasts",
    "cbonds_get_rating_agencies",
    "cbonds_get_derivatives_contract_types",
    "cbonds_get_derivatives_futures_settlement_types",
    "cbonds_get_derivatives_offert_option_types",
    "cbonds_get_derivatives_options_payment_types",
    "cbonds_get_derivatives_quotation_measures",
    "cbonds_get_derivatives_sections",
    "cbonds_get_derivatives_futures",
}

SCHEMA_METHODS_CBONDS = {
    # Эти методы вызываются через GET, тогда как данные и справочники — через POST.
    "cbonds_info_xml",
    "cbonds_info_json",
    "cbonds_info_json_rus",
}

# Эти статические методы в старом коде при инициализации сохранялись без
# Updates_from/Updates_to, даже если такие поля есть в записи журнала.
INIT_WITHOUT_UPDATES_METHODS = STATIC_METHODS_CBONDS | {  # Оператор `|` создаёт объединение двух множеств.
    "cbonds_get_stocks_dividends_v2",
    "cbonds_get_flow_new",
    "cbonds_get_stocks_beta_coefficient",
    "cbonds_get_etf_share_classes_quotes",
    "cbonds_get_stocks_full_splits",
}


class CbondsJournalError(Exception):  # Базовый прикладной тип: по нему можно отличить ожидаемую ошибку загрузчика.
    """Ошибка обработки одной записи журнала Cbonds."""


class CbondsApiError(CbondsJournalError):  # Наследование сохраняет общий тип и уточняет источник ошибки — API.
    """Cbonds вернул JSON-блок error или некорректный ответ."""


class CbondsApiFatalError(CbondsApiError):  # Самый узкий тип: его process_one_request ловит раньше общего Exception.
    """Фатальная ошибка Cbonds API: повторы не помогут (например, err_no=500100)."""

    def __init__(self, err_no: int, err_str: str) -> None:
        # Сначала сохраняем структурированные поля, затем вызываем конструктор
        # базового Exception через super(), чтобы у исключения был текст.
        self.err_no = err_no
        self.err_str = err_str
        super().__init__(f"Cbonds API error {err_no}: {err_str}")


@dataclass(frozen=True)
class JournalRequest:
    """Одна строка из `BCS_BIZ.BBG.AdHocRequest_DEV` с метаданными метода Cbonds."""

    group_name: str | None  # Человекочитаемая группа метода из справочника; LEFT JOIN допускает NULL.
    name: str | None  # Отображаемое имя метода; тоже может отсутствовать при незаполненном справочнике.
    code: str  # Машинный код, например cbonds_get_etf_share_classes_quotes.
    method_id: int  # Id метода из vRequestTypesCbonds, передаваемый в SQL-процедуру.
    data_url: str  # Полный endpoint Cbonds для HTTP-запроса.
    request_id: int  # Первичный идентификатор конкретной строки журнала.
    request_time: datetime | None  # Время создания/отправки запроса по данным SQL.
    request_xml: str | None  # Историческое поле с filters/fields, иногда обёрнутыми в XML-комментарий.
    response_time: datetime | None  # NULL означает, что строка ещё не завершена.
    response_xml: str | None  # Здесь фактически хранится экранированный JSON или текст схемы.
    source_code: int  # Код источника; для данного runner ожидается 12.
    request_code: int  # Код типа запроса, используемый также для особенностей schema endpoint.
    date_request: datetime | None  # Бизнес-дата строки журнала.
    schedule_source_id: int | None  # Ссылка на породившее запрос расписание, если она есть.
    his_from: datetime | None  # Начало исторического временного окна.
    his_to: datetime | None  # Конец исторического временного окна.
    his_master: int | None  # Для дочерней страницы содержит request_id родительской строки.
    fix_master: int | None  # Дополнительная историческая связь журнала.
    is_err: int | None  # 1 означает ошибку; NULL/0 — неошибочное состояние.
    err_desc: str | None  # Текст последней зафиксированной ошибки.
    comment: str | None  # Здесь ожидается тип операции: «инициализация» или «обновление».
    updates_from: datetime | None  # Нижняя граница окна обновлений.
    updates_to: datetime | None  # Верхняя граница окна обновлений.
    chars: int | None  # Служебная статистика размера ответа в символах.
    lines: int | None  # Служебное число строк.
    kbytes: int | None  # Служебный размер ответа в килобайтах.

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "JournalRequest":
        """Преобразовать dict из pyodbc в типизированную запись.

        Имена некоторых полей в SQL начинаются с заглавной буквы или совпадают
        с зарезервированными словами. Сразу приводим их к нормальным Python-
        именам, чтобы дальше код читался без постоянных `row["Request_id"]`.
        """
        # classmethod получает класс в аргументе cls. Поэтому конструктор
        # вызывается как cls(...), а не жёстко как JournalRequest(...).
        return cls(
            group_name=row.get("Group"),  # get возвращает None вместо KeyError, если необязательной колонки нет.
            name=row.get("Name"),  # LEFT JOIN способен вернуть SQL NULL, который pyodbc превращает в None.
            code=row["Code"],  # Квадратные скобки требуют наличие обязательного ключа; иначе будет KeyError.
            method_id=int(row["Id"]),  # Явно нормализуем числовое значение к int.
            data_url=row["DataUrl"],  # URL обязателен: без него запрос выполнить невозможно.
            request_id=int(row["Request_id"]),  # Id также приводится к обычному Python int.
            request_time=row.get("Request_time"),
            request_xml=row.get("Request_xml"),
            response_time=row.get("Response_time"),
            response_xml=row.get("Response_xml"),
            source_code=int(row["Source_code"]),
            request_code=int(row["Request_code"]),
            date_request=row.get("Date_request"),
            schedule_source_id=row.get("Schedule_source_id"),
            his_from=row.get("His_from"),
            his_to=row.get("His_to"),
            his_master=row.get("His_master"),
            fix_master=row.get("Fix_master"),
            is_err=row.get("isErr"),
            err_desc=row.get("ErrDesc"),
            comment=row.get("Comment"),
            updates_from=row.get("Updates_from"),
            updates_to=row.get("Updates_to"),
            chars=row.get("chars"),
            lines=row.get("lines"),
            kbytes=row.get("KBytes"),
        )

    @property
    def process_name(self) -> str:
        """Название процесса в SQL-логе, как в старом скрипте."""
        # property позволяет читать request.process_name как атрибут, хотя
        # значение вычисляется функцией при каждом обращении.
        return f"{self.code}: {self.comment} Request_id={self.request_id}"



def connect_mssql():
    """Открыть pyodbc-соединение к SQL Server из RuntimeConfig (без Airflow Connection)."""
    if pyodbc is None:  # Проверяется результат опционального импорта в начале файла.
        raise CbondsJournalError(
            "Python package pyodbc is not installed. "
            "Install pyodbc and a SQL Server ODBC Driver before running this script."
        )

    cfg = _require_cfg()  # Получаем уже проверенный RuntimeConfig, установленный main().
    driver = cfg.mssql_odbc_driver  # Локальное имя сокращает следующую f-строку.
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={cfg.mssql_host},{cfg.mssql_port};"
        f"DATABASE={cfg.mssql_database};"
    )
    if cfg.mssql_trusted_connection:  # Ветка Windows Integrated Security.
        if cfg.mssql_user:
            conn_str += f"UID={cfg.mssql_user};"  # Необязательное явное имя Windows-пользователя.
        conn_str += "Trusted_Connection=yes;"  # Просим ODBC использовать учётную запись процесса Windows.
    else:
        conn_str += f"UID={cfg.mssql_user};PWD={cfg.mssql_password};"  # SQL-аутентификация требует обе части пары.

    log.info(
        "Opening MSSQL connection host=%s database=%s",
        cfg.mssql_host,
        cfg.mssql_database,
    )
    # autocommit=True означает: каждая SQL-команда фиксируется отдельно.
    # Это важно для понимания сбоев: несколько обновлений не образуют одну
    # общую транзакцию и уже выполненная команда сама не откатится.
    return pyodbc.connect(conn_str, autocommit=True)


def cbonds_auth() -> dict[str, str]:
    """Вернуть auth-блок Cbonds из RuntimeConfig."""
    cfg = _require_cfg()  # Функция не читает env повторно: весь запуск использует один снимок конфигурации.
    return {"login": cfg.cbonds_login, "password": cfg.cbonds_password}  # Формат ключей задаётся контрактом Cbonds API.

def fetch_pending_requests(conn: pyodbc.Connection) -> list[JournalRequest]:
    """Выбрать записи журнала, которые ещё нужно отправить в Cbonds.

    Условия WHERE повторяют активный запрос старого скрипта:
    - Source_code = 12;
    - Date_request = текущая дата SQL Server;
    - His_master is null, чтобы не брать дочерние страницы;
    - Response_time is null, то есть ответа ещё нет;
    - IsErr не равен 1.
    """
    sql = """
        SELECT
            rt.[Group],
            rt.[Name],
            rt.Code,
            rt.Id,
            rt.DataUrl,
            ad.Request_id,
            ad.Request_time,
            ad.Request_xml,
            ad.Response_time,
            ad.Response_xml,
            ad.Source_code,
            ad.Request_code,
            ad.Date_request,
            ad.Schedule_source_id,
            ad.His_from,
            ad.His_to,
            ad.His_master,
            ad.Fix_master,
            ad.isErr,
            ad.ErrDesc,
            ad.Comment,
            ad.Updates_from,
            ad.Updates_to,
            ad.chars,
            ad.lines,
            ad.KBytes
        FROM BCS_BIZ.BBG.AdHocRequest_DEV AS ad WITH (NOLOCK)
        LEFT JOIN DWH_BCS.OHD.vRequestTypesCbonds AS rt WITH (NOLOCK)
            ON ad.Request_code = rt.Id
        WHERE ad.Source_code = ?
          AND ad.Date_request = CAST(GETDATE() AS date)
          AND ad.His_master IS NULL
          AND ad.Response_time IS NULL
          AND ISNULL(ad.isErr, 0) <> 1
        ORDER BY rt.Id
    """
    # Знак `?` в SQL — placeholder pyodbc. Значение передаётся отдельно,
    # поэтому оно не склеивается со строкой SQL и корректно экранируется.
    rows = execute_fetch_dicts(conn, sql, (SOURCE_CODE_CBONDS,))
    # List comprehension преобразует каждую словарную строку результата SQL
    # в типизированный JournalRequest и возвращает новый список.
    requests_to_process = [JournalRequest.from_row(row) for row in rows]  # Выражение выполняет from_row отдельно для каждой строки.
    log.info("Found %s pending Cbonds journal request(s)", len(requests_to_process))
    return requests_to_process


def execute_fetch_dicts(
    conn: pyodbc.Connection,
    sql: str,
    params: Iterable[Any] = (),
) -> list[dict[str, Any]]:
    """Выполнить SQL и вернуть первый result set как list[dict].

    Stored procedure в SQL Server может сначала вернуть служебные rowcount-
    resultset-ы. Поэтому идём по `nextset()`, пока не найдём набор с колонками.
    """
    # Контекстный менеджер вызывает закрытие cursor и при успехе, и если внутри
    # блока возникнет исключение. Само соединение здесь не закрывается.
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))  # tuple материализует Iterable один раз и передаёт значения для `?` placeholders.
        while True:
            if cur.description:  # Наличие description означает result set с колонками.
                columns = [column[0] for column in cur.description]  # Берём только имя из метаданных каждой колонки.
                return [dict(zip(columns, row)) for row in cur.fetchall()]  # zip сопоставляет имена колонок значениям одной строки.
            if not cur.nextset():  # nextset переключается на следующий результат stored procedure.
                return []  # Ни один result set не содержал табличных данных.


def execute_no_result(
    conn: pyodbc.Connection,
    sql: str,
    params: Iterable[Any] = (),
) -> None:
    """Выполнить SQL-команду, от которой не ждём result set."""
    with conn.cursor() as cur:  # Курсор будет закрыт при выходе из блока.
        cur.execute(sql, tuple(params))  # Результат намеренно не читается; autocommit фиксирует команду.


def write_success_to_sql(conn: pyodbc.Connection, process_name: str, total: int) -> None:
    """Записать успешное завершение метода в `spNSD_CBonds_ExLog_DEV`."""
    message = f"{process_name}, Total: {total}"  # f-строка подставляет имя процесса и число записей в текст.
    sql = "EXEC BCS_BIZ.BBG.spNSD_CBonds_ExLog_DEV ?, ?, ?"
    execute_no_result(conn, sql, (EXLOG_GROUP_CBONDS, message, datetime.now()))  # Скобки с запятыми создают tuple параметров.


def write_error_to_sql(conn: pyodbc.Connection, process_name: str, error_text: str) -> None:
    """Записать ошибку метода в `spNSD_CBonds_ExLog_DEV`."""
    sql = "EXEC BCS_BIZ.BBG.spNSD_CBonds_ExLog_DEV ?, ?, ?, ?"  # Четыре `?` требуют ровно четыре значения в том же порядке.
    execute_no_result(conn, sql, (EXLOG_GROUP_CBONDS, process_name, datetime.now(), error_text))  # Ошибка сохраняется отдельно от Python-лога.


def mark_request_failed(conn: pyodbc.Connection, request_id: int, error_text: str) -> None:
    """Пометить исходную запись журнала ошибочной после исчерпания повторов."""
    # Первая команда закрывает строку журнала временем ответа и флагом ошибки.
    execute_no_result(
        conn,
        """
        UPDATE BCS_BIZ.BBG.AdHocRequest_DEV
        SET Response_time = ?,
            IsErr = 1
        WHERE Request_id = ?
        """,
        (datetime.now(), request_id),  # Первый элемент заменяет первый `?`, второй — второй `?`.
    )
    # Вторая команда отдельно записывает подробное описание ошибки.
    execute_no_result(
        conn,
        """
        UPDATE BCS_BIZ.BBG.AdHocRequest_DEV
        SET ErrDesc = CONCAT(?, ':', ' Ошибка, выполнено максимальное количество попыток обращения к API CBonds')
        WHERE Request_id = ?
        """,
        (error_text, request_id),  # Текст не склеивается с SQL, поэтому кавычки внутри безопасны для синтаксиса.
    )


def mark_request_completed(
    conn: pyodbc.Connection,
    request_id: int,
    comment: str,
    response_xml: str | None = None,
) -> None:
    """Обновить запись журнала успешным ответом или отметкой `total=0`."""
    if response_xml is None:  # Ветка total=0: сохранять тело ответа не требуется.
        execute_no_result(
            conn,
            """
            UPDATE BCS_BIZ.BBG.AdHocRequest_DEV
            SET Response_time = ?,
                Comment = ?,
                IsErr = NULL,
                ErrDesc = NULL
            WHERE Request_id = ?
            """,
            (datetime.now(), comment, request_id),  # Три значения соответствуют трём placeholders.
        )
    else:  # Ветка успешной страницы с фактическим JSON/XML-текстом.
        execute_no_result(
            conn,
            """
            UPDATE BCS_BIZ.BBG.AdHocRequest_DEV
            SET Response_time = ?,
                Comment = ?,
                Response_xml = ?,
                IsErr = NULL,
                ErrDesc = NULL
            WHERE Request_id = ?
            """,
            (datetime.now(), comment, response_xml, request_id),  # Response_xml передаётся как параметр, а не часть SQL.
        )


def create_child_request(
    conn: pyodbc.Connection,
    request: JournalRequest,
    type_request: int,
    his_from: str | None,
    his_to: str | None,
    updates_from: str | None,
    updates_to: str | None,
) -> int:
    """Создать дочернюю запись журнала для очередной страницы Cbonds.

    Старый скрипт делал это через `AdHocResponse_auto_CBonds_DEV @InsUpdType=1`,
    а затем отдельным вызовом `@InsUpdType=2` записывал в неё ответ. Здесь
    сохраняем тот же контракт, только параметры передаём безопасно.
    """
    # Процедура сначала создаёт строку, затем SELECT возвращает её новый id.
    rows = execute_fetch_dicts(
        conn,
        """
        DECLARE @Rec_Id_Output int;

        EXEC BCS_STG.BBG.AdHocResponse_auto_CBonds_DEV
            @InsUpdType = 1,
            @Request_Id = NULL,
            @i_Xml = NULL,
            @Type = ?,
            @TypeRequest = ?,
            @HisMaster = ?,
            @Comment = NULL,
            @His_from = ?,
            @His_to = ?,
            @Updates_from = ?,
            @Updates_to = ?,
            @Request_Id_inserted = @Rec_Id_Output OUTPUT;

        SELECT @Rec_Id_Output AS Request_Id_inserted;
        """,
        (
            request.method_id,  # @Type: идентификатор Cbonds-метода.
            type_request,  # @TypeRequest: 3 для инициализации или 1 для обновления.
            request.request_id,  # @HisMaster: связь дочерней строки с родителем.
            his_from,  # @His_from: начало исторического окна либо None → SQL NULL.
            his_to,  # @His_to: конец исторического окна либо None.
            updates_from,  # @Updates_from: начало окна обновлений.
            updates_to,  # @Updates_to: конец окна обновлений.
        ),
    )
    if not rows:  # Пустой список означает, что процедура не вернула ожидаемый SELECT.
        raise CbondsJournalError("Procedure AdHocResponse_auto_CBonds_DEV did not return child Request_id")

    # pyodbc для SELECT @var может вернуть колонку как `Request_Id_inserted`.
    # На всякий случай берём первое значение, чтобы пережить отличие alias-а.
    child_request_id = next(iter(rows[0].values()))  # iter создаёт итератор значений, next берёт первое из единственной колонки.
    if child_request_id is None:  # SQL NULL преобразуется драйвером pyodbc в Python None.
        raise CbondsJournalError("Procedure AdHocResponse_auto_CBonds_DEV returned NULL child Request_id")
    return int(child_request_id)  # Нормализуем возможный Decimal/драйверный тип к обычному int.


def update_child_request(
    conn: pyodbc.Connection,
    child_request_id: int,
    request: JournalRequest,
    type_request: int,
    response_xml: str,
    updates_from: str | None,
    updates_to: str | None,
) -> None:
    """Записать страницу Cbonds в уже созданную дочернюю запись журнала."""
    # В отличие от create_child_request, здесь процедура обновляет уже созданный id и ничего не возвращает.
    execute_no_result(
        conn,
        """
        EXEC BCS_STG.BBG.AdHocResponse_auto_CBonds_DEV
            @InsUpdType = 2,
            @Request_Id = ?,
            @i_Xml = ?,
            @Type = ?,
            @TypeRequest = ?,
            @HisMaster = ?,
            @Comment = NULL,
            @Updates_from = ?,
            @Updates_to = ?
        """,
        (
            child_request_id,  # @Request_Id: какую дочернюю строку обновить.
            response_xml,  # @i_Xml: экранированный JSON страницы.
            request.method_id,  # @Type: метод Cbonds.
            type_request,  # @TypeRequest: режим инициализации/обновления.
            request.request_id,  # @HisMaster: родительский request_id.
            updates_from,  # Временные границы передаются только для группы данных.
            updates_to,
        ),
    )


def request_type_to_code(request_type: str | None) -> int:
    """Преобразовать текст из Comment в код процедуры.

    В старом скрипте:
    - `инициализация` -> 3;
    - `обновление` -> 1.
    """
    if request_type == "инициализация":  # Строка сравнивается точно, включая регистр и пробелы.
        return 3  # Это код @TypeRequest SQL-процедуры, а не exit code программы.
    if request_type == "обновление":
        return 1  # Исторический контракт процедуры для инкрементального обновления.
    raise CbondsJournalError(f"Unknown request type in Comment: {request_type!r}")


def method_group(code: str) -> int:
    """Вернуть тип Cbonds-метода: 1 = данные, 2 = справочник, 3 = схема."""
    if code in STATIC_METHODS_CBONDS or code in PRICE_METHODS_CBONDS:  # `in set` проверяет принадлежность в среднем за O(1).
        return 1  # Основные данные: POST с filters и fields.
    if code in DICT_METHODS_CBONDS:
        return 2  # Справочник: POST без filters/fields.
    if code in SCHEMA_METHODS_CBONDS:
        return 3  # Схема: GET с query parameters.
    raise CbondsJournalError(f"Unknown Cbonds method code: {code!r}")


def format_sql_datetime(value: datetime | None) -> str | None:
    """Формат дат старого скрипта: `YYYY-mm-dd HH:MM:SS` плюс timezone, если есть."""
    if value is None:  # Отсутствующую SQL-дату не пытаемся форматировать.
        return None  # None позднее станет SQL NULL при параметризованном вызове.
    return value.strftime("%Y-%m-%d %H:%M:%S%z")  # `%z` добавит смещение timezone, если datetime timezone-aware.


def trim_empty_values(value: Any) -> Any:
    """Рекурсивно удалить из ответа Cbonds пустые значения.

    Старый скрипт делал это для основных методов, чтобы уменьшить объём JSON,
    сохраняемого в `Response_xml`.
    """
    # Рекурсия: для вложенного dict/list функция вызывает саму себя. Базовый
    # случай — скалярное значение, которое возвращается без изменений.
    if isinstance(value, dict):  # isinstance учитывает и сам dict, и его подклассы.
        return {k: trim_empty_values(v) for k, v in value.items() if v is not None and v != ""}  # Dict comprehension отбрасывает None/"" среди значений словаря.
    if isinstance(value, list):
        return [trim_empty_values(item) for item in value]  # List comprehension сохраняет длину и порядок списка.
    return value  # Числа, bool и строки возвращаются как базовый случай рекурсии.


def response_to_sql_xml_text(response: dict[str, Any], trim_empty: bool) -> str:
    """Подготовить JSON-ответ Cbonds к записи в поле `Response_xml`.

    Название поля историческое: старый контур сохраняет JSON как текст,
    предварительно экранируя символы, которые ломали XML/SQL-обвязку.
    Порядок замен оставлен максимально близким к исходному скрипту.
    """
    payload = trim_empty_values(response) if trim_empty else response  # Тернарное выражение выбирает очищенный или исходный объект.
    text = json.dumps(payload)  # Сериализация создаёт JSON-строку; исходный dict не изменяется.
    text = text.replace("&", "\\u0026")  # Амперсанд заменяется Unicode escape-последовательностью JSON.
    text = text.replace('\\"', "\\u0022")  # Экранированные кавычки внутри JSON-строк заменяются кодом символа.
    text = text.replace("'", "&apos;")  # Историческая совместимость с XML/SQL-обвязкой.
    return escape(text)  # Последний шаг экранирует XML-значимые &, < и >.


def parse_request_xml(request_xml: str | None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Разобрать `Request_xml` из журнала в `(filters, fields)`.

    Старый скрипт ожидал два формата:
    - `<!-- [{"field":"..."}] -->`, то есть только список фильтров;
    - `<!-- "filters":[...], "fields":[...] -->`, то есть фрагмент JSON-объекта.

    Здесь сначала пробуем строгий JSON, затем `ast.literal_eval` для обратной
    совместимости с Python-like строками. `eval()` намеренно не используется.
    """
    if not request_xml:  # Сюда попадают None и пустая строка.
        return [], []

    cleaned = request_xml.strip()  # Создаётся новая строка без пробелов и переводов строк по краям.
    if cleaned.startswith("<!--"):  # Старый формат маскировал JSON как XML-комментарий.
        cleaned = cleaned.removeprefix("<!--").removesuffix("-->").strip()  # removeprefix/removesuffix удаляют только точные края.

    if not cleaned or cleaned.lower() == "no xml":  # lower делает проверку нечувствительной к регистру.
        return [], []

    if not cleaned.startswith(("[", "{")) and ('"fields"' in cleaned or '"filters"' in cleaned):
        cleaned = "{" + cleaned + "}"  # Исторический фрагмент превращается в полный JSON-объект.

    parsed: Any
    # Сначала используем строгий JSON. literal_eval — ограниченный парсер
    # Python-литералов для совместимости со старыми строками; в отличие от
    # eval(), он не выполняет произвольный код.
    try:
        parsed = json.loads(cleaned)  # Строгий JSON: двойные кавычки, true/false/null по правилам JSON.
    except json.JSONDecodeError:  # Только ошибка синтаксиса JSON разрешает перейти к fallback-парсеру.
        try:
            parsed = ast.literal_eval(cleaned)  # Поддерживает Python-литералы с одинарными кавычками, None, True/False.
        except (SyntaxError, ValueError) as exc:
            raise CbondsJournalError(f"Cannot parse Request_xml safely: {request_xml!r}") from exc

    if isinstance(parsed, list):  # Старый короткий формат трактует весь список как filters.
        return parsed, []  # Функция всегда возвращает tuple из двух списков.
    if isinstance(parsed, dict):
        filters = parsed.get("filters") or []  # Отсутствующий ключ и None нормализуются к пустому списку.
        fields = parsed.get("fields") or []
        if not isinstance(filters, list) or not isinstance(fields, list):
            raise CbondsJournalError(f"Request_xml filters/fields must be lists: {request_xml!r}")
        return filters, fields

    raise CbondsJournalError(f"Unsupported Request_xml structure: {type(parsed).__name__}")


def sorting_for_method(code: str) -> list[dict[str, str]]:
    """Сортировка Cbonds из старого скрипта.

    Offset-пагинация должна идти по стабильному порядку. Для большинства
    методов достаточно `id asc`, но у отдельных методов в старом контуре были
    особые поля сортировки.
    """
    if code == "cbonds_get_tradings_new":  # Некоторые endpoints не поддерживают универсальную сортировку по id.
        return [{"field": "created_at", "order": "asc"}]  # API ожидает список объектов field/order.
    if code == "cbonds_get_index_value_new":
        return [{"field": "date", "order": "asc"}, {"field": "updated_at", "order": "asc"}]  # Второе поле уточняет порядок при одинаковой date.
    if code == "cbonds_get_stocks_dividends_v2":
        return [{"field": "stock_id", "order": "asc"}, {"field": "created_at", "order": "asc"}]
    if code == "cbonds_get_flow_new":
        return [{"field": "emission_id", "order": "asc"}, {"field": "date", "order": "asc"}]
    if code in {"cbonds_get_emissions", "cbonds_get_emission_guarantors"}:
        return [{"field": "id", "order": "asc"}]
    if code == "cbonds_get_funds":
        return [{"field": "isin_code", "order": "asc"}]
    if code == "cbonds_get_etf_funds":
        return [{"field": "isin", "order": "asc"}]
    if code == "cbonds_get_emission_ratings_new":
        return [{"field": "emission_id", "order": "asc"}]
    return [{"field": "id", "order": "asc"}]  # Резервная сортировка для методов без специального правила.


def build_json_body(
    auth: dict[str, str],  # Пара login/password в формате Cbonds.
    request: JournalRequest,  # Метаданные записи; сейчас параметр сохраняет единый интерфейс, но в теле не используется.
    group: int,  # 1 = данные, 2 = справочник.
    limit: int,  # Максимальное число элементов страницы.
    offset: int,  # Сколько элементов от начала результата пропустить.
    filters: list[dict[str, Any]],  # Ограничения выборки Cbonds.
    fields: list[dict[str, Any]],  # Явно запрошенные поля ответа.
    sorting: list[dict[str, str]],  # Последовательность правил стабильного порядка.
) -> dict[str, Any]:
    """Собрать POST-body Cbonds для одной страницы.

    Для справочников старый скрипт не передавал фильтры и fields, только auth,
    quantity и сортировку по id. Для основных методов передавались фильтры,
    fields и метод-специфичная сортировка.
    """
    # `body` — обычный Python-словарь. requests ниже сериализует его в JSON,
    # потому что он будет передан именованным аргументом json=body.
    body: dict[str, Any] = {
        "auth": auth,  # Вложенный словарь будет JSON-объектом auth.
        "quantity": {"limit": limit, "offset": offset},  # Вложенный объект управляет пагинацией.
        "sorting": sorting,  # Список сохраняет порядок приоритетов сортировки.
    }
    if group == 1:  # Только основные методы получают фильтры и перечень полей.
        body["filters"] = filters  # Присваивание по ключу изменяет ранее созданный словарь body.
        body["fields"] = fields
    return body  # Возвращается dict; сериализацию в JSON выполнит requests.


def post_cbonds_page(
    session: requests.Session,  # Открытая Session переиспользует TCP/TLS-соединения между страницами.
    request: JournalRequest,  # Из неё берутся URL, code и request_id для запроса и логов.
    body: dict[str, Any],  # Python-словарь одной страницы, подготовленный build_json_body.
) -> dict[str, Any]:
    """Отправить один POST в Cbonds и проверить JSON-ответ."""
    log.info(
        "POST Cbonds method=%s request_id=%s url=%s quantity=%s",
        request.code,
        request.request_id,
        request.data_url,
        body.get("quantity"),
    )
    response = session.post(  # Сетевой вызов блокирует текущий поток до ответа или timeout.
        request.data_url,
        json=body,  # requests сам вызывает JSON-сериализацию и ставит Content-Type: application/json.
        proxies=_require_cfg().proxies,  # Прокси берутся из активного RuntimeConfig.
        timeout=CBONDS_TIMEOUT_SECONDS,  # Без timeout зависший сервер мог бы удерживать процесс неограниченно долго.
    )
    # raise_for_status() превращает HTTP 4xx/5xx в исключение. Только после
    # этой проверки разбираем тело ответа как JSON.
    response.raise_for_status()
    data = response.json()  # Разбирает тело ответа в Python dict/list; невалидный JSON вызовет исключение.
    if not isinstance(data, dict):  # Верхний уровень успешного ответа по контракту должен быть JSON-object.
        raise CbondsApiError(f"Cbonds returned non-object JSON: {type(data).__name__}")
    if "error" in data:  # API может вернуть HTTP 200, но сообщить прикладную ошибку внутри JSON.
        err = data.get("error") or {}  # None нормализуется к пустому dict для безопасных последующих get().
        err_no = err.get("err_no")  # Машинный номер ошибки.
        err_str = err.get("err_str") or str(err)  # При отсутствии текста сохраняем хотя бы строковое представление блока.
        if err_no in FATAL_CBONDS_ERR_NOS:  # Фатальный код прекращает retry этой записи.
            raise CbondsApiFatalError(int(err_no), err_str)
        raise CbondsApiError(f"Cbonds API error {err_no}: {err_str}")
    if "total" not in data:  # total управляет числом итераций offset-пагинации.
        raise CbondsApiError(f"Cbonds response has no total field: keys={list(data.keys())}")
    return data


def get_total(response: dict[str, Any]) -> int:
    """Достать `total` из ответа Cbonds как int."""
    try:
        return int(response.get("total") or 0)  # Строка "2500" станет int 2500; None станет 0.
    except (TypeError, ValueError) as exc:
        raise CbondsApiError(f"Invalid Cbonds total value: {response.get('total')!r}") from exc


def get_cbonds_schema_text(session: requests.Session, auth: dict[str, str], request: JournalRequest) -> str:
    """Получить XML/JSON-схему метода Cbonds через GET.

    Старый скрипт ходил в schema endpoints через query parameters. Для методов
    230 и 233 добавлялся `lang=rus`; это сохранено.
    """
    params = {"login": auth["login"], "password": auth["password"]}  # Схемный GET принимает auth в query string.
    if request.request_code in (230, 233):
        params["lang"] = "rus"  # Для двух исторических request_code запрашивается русская схема.
    separator = "&" if "?" in request.data_url else "?"  # Не создаём второй `?`, если URL уже содержит параметры.
    url = request.data_url + separator + urlencode(params)  # urlencode экранирует специальные символы значений.
    log.info("GET Cbonds schema method=%s request_id=%s url=%s", request.code, request.request_id, request.data_url)
    response = session.get(url, proxies=_require_cfg().proxies, timeout=CBONDS_TIMEOUT_SECONDS)  # GET возвращает текст схемы.
    response.raise_for_status()
    text = escape(response.text)  # response.text декодирован requests в Python str.
    return text.replace("'", "&apos;")  # Дополнительно кодируем апостроф для исторического XML-поля.


def request_windows(
    request: JournalRequest,
    request_type_code: int,
) -> tuple[str | None, str | None, str | None, str | None]:
    """Рассчитать His/Updates интервалы для передачи в SQL-процедуру.

    Эта функция повторяет ветвление старого скрипта, но делает его явным и
    защищённым от `None`.
    """
    his_from = format_sql_datetime(request.his_from)  # Результат — str или None, готовые для SQL-процедуры.
    his_to = format_sql_datetime(request.his_to)

    if request_type_code == 3 and request.code in INIT_WITHOUT_UPDATES_METHODS:  # Инициализация части методов игнорирует Updates-интервал.
        updates_from = None  # None драйвер передаст в SQL как NULL.
        updates_to = None
    else:
        updates_from = format_sql_datetime(request.updates_from)
        updates_to = format_sql_datetime(request.updates_to)

    return his_from, his_to, updates_from, updates_to  # Позиционный tuple распаковывается вызывающей функцией в четыре имени.


def save_cbonds_page(
    conn: pyodbc.Connection,  # Открытое соединение SQL Server.
    request: JournalRequest,  # Родительская запись журнала.
    type_request: int,  # Режим процедуры: инициализация или обновление.
    group: int,  # Определяет, передавать ли временные окна дочерней строке.
    total: int,  # Общее число записей во всём ответе, не размер текущей страницы.
    offset: int,  # Смещение текущей страницы: 0, 1000, 2000 и т.д.
    response_xml: str,  # Подготовленный текст текущей страницы.
    his_from: str | None,  # Форматированная нижняя граница истории.
    his_to: str | None,  # Форматированная верхняя граница истории.
    updates_from: str | None,  # Форматированная нижняя граница обновлений.
    updates_to: str | None,  # Форматированная верхняя граница обновлений.
) -> None:
    """Сохранить одну страницу ответа Cbonds в журнал.

    Логика такая же, как в Windows-скрипте:
    - total <= 1000: всегда обновляем исходную запись;
    - total > 1000 и offset == 0: первую страницу кладём в исходную запись;
    - total > 1000 и offset > 0: создаём дочернюю запись и пишем ответ туда.
    """
    comment = f"{request.comment}:  пп 0 из {total}" if total > 0 else f"{request.comment}: пп 0 из {total}"  # Исторический формат текста журнала сохранён дословно.

    if total <= PAGE_LIMIT or offset == 0:  # Единственная либо первая страница записывается в исходную строку.
        log.info(
            "Saving Cbonds page to master journal row request_id=%s total=%s offset=%s",
            request.request_id,
            total,
            offset,
        )
        mark_request_completed(conn, request.request_id, comment, response_xml)  # Ставит Response_time и сохраняет тело.
        return  # Ранний выход не позволяет одновременно создать child для первой страницы.

    child_request_id = create_child_request(
        conn=conn,
        request=request,
        type_request=type_request,
        his_from=his_from if group == 1 else None,  # Для справочника временные интервалы не передаются.
        his_to=his_to if group == 1 else None,
        updates_from=updates_from if group == 1 else None,
        updates_to=updates_to if group == 1 else None,
    )
    log.info(
        "Created child journal row request_id=%s for master_request_id=%s total=%s offset=%s",
        child_request_id,
        request.request_id,
        total,
        offset,
    )
    update_child_request(
        conn=conn,
        child_request_id=child_request_id,
        request=request,
        type_request=type_request,
        response_xml=response_xml,
        updates_from=updates_from if group == 1 else None,
        updates_to=updates_to if group == 1 else None,
    )


def process_json_method(
    conn: pyodbc.Connection,  # Используется для сохранения каждой полученной страницы.
    session: requests.Session,  # Одна HTTP Session переиспользуется между методами и страницами.
    auth: dict[str, str],  # Auth-блок Cbonds.
    request: JournalRequest,  # Текущая строка журнала.
    group: int,  # 1 = данные, 2 = справочник; schema сюда не попадает.
    type_request: int,  # Код режима для дочерних SQL-записей.
) -> int:
    """Обработать основной или справочный JSON-метод Cbonds."""
    filters: list[dict[str, Any]] = []  # Новые списки создаются отдельно для каждого вызова функции.
    fields: list[dict[str, Any]] = []
    if group == 1:
        filters, fields = parse_request_xml(request.request_xml)  # Распаковка tuple присваивает два результата одновременно.

    sorting = sorting_for_method(request.code)  # Стабильный порядок нужен, чтобы offset-страницы не перемешивались.
    his_from, his_to, updates_from, updates_to = request_windows(request, type_request)  # Готовим SQL-метаданные один раз на метод.

    first_body = build_json_body(
        auth=auth,
        request=request,
        group=group,
        limit=PAGE_LIMIT,
        offset=0,
        filters=filters,
        fields=fields,
        sorting=sorting,
    )
    first_response = post_cbonds_page(session, request, first_body)  # Первый запрос одновременно даёт данные offset=0 и total.
    total = get_total(first_response)  # total определяет, сколько страниц потребуется.
    log.info(
        "Cbonds total=%s limit=%s request_id=%s method=%s filters=%s fields=%s sorting=%s",
        total,
        PAGE_LIMIT,
        request.request_id,
        request.code,
        filters,
        fields,
        sorting,
    )

    if total <= 0:  # При пустом результате дочерние страницы и Response_xml не создаются.
        mark_request_completed(conn, request.request_id, f"{request.comment}: пп 0 из {total}")
        log.info("Cbonds returned total=0 for request_id=%s method=%s", request.request_id, request.code)
        return total  # Обычно 0; значение попадёт в success-log вызывающей функции.

    # Улучшение относительно Windows-скрипта: первую страницу не запрашиваем
    # второй раз. Старый код сначала делал POST для total, а потом повторял
    # offset=0 в цикле. Для результата в журнале это то же самое, но меньше
    # риск упереться в лимит Cbonds 30 запросов в минуту.
    # range(start, stop, step) не включает stop. При total=2500 получим
    # offset 0, 1000, 2000 — три страницы по limit=1000.
    for offset in range(0, total, PAGE_LIMIT):
        if offset == 0:
            page_response = first_response  # Переиспользуем уже загруженную страницу и не расходуем API-вызов повторно.
        else:
            time.sleep(PAGE_DELAY_SECONDS)  # Пауза выполняется только перед второй и последующими страницами.
            page_body = build_json_body(
                auth=auth,
                request=request,
                group=group,
                limit=PAGE_LIMIT,
                offset=offset,
                filters=filters,
                fields=fields,
                sorting=sorting,
            )
            page_response = post_cbonds_page(session, request, page_body)  # Получаем страницу для текущего offset.

        response_xml = response_to_sql_xml_text(page_response, trim_empty=(group == 1))  # Данные очищаются, справочники сохраняются целиком.
        save_cbonds_page(
            conn=conn,
            request=request,
            type_request=type_request,
            group=group,
            total=total,
            offset=offset,
            response_xml=response_xml,
            his_from=his_from,
            his_to=his_to,
            updates_from=updates_from,
            updates_to=updates_to,
        )
        log.info(
            "Saved Cbonds page request_id=%s method=%s total=%s offset=%s",
            request.request_id,
            request.code,
            total,
            offset,
        )

    return total  # Возвращается число записей API, а не число созданных журнальных строк.


def process_schema_method(
    conn: pyodbc.Connection,  # SQL-соединение для сохранения текста схемы.
    session: requests.Session,  # HTTP Session для GET.
    auth: dict[str, str],  # Логин и пароль Cbonds.
    request: JournalRequest,  # Содержит endpoint и request_code.
) -> int:
    """Обработать метод схемы Cbonds (`cbonds_info_*`)."""
    schema_text = get_cbonds_schema_text(session, auth, request)  # В отличие от JSON-метода здесь нет пагинации.
    mark_request_completed(conn, request.request_id, "схемы методов сервисов CBonds", schema_text)
    log.info("Saved Cbonds schema response request_id=%s method=%s", request.request_id, request.code)
    return 1  # Успешный schema-запрос считается одной сохранённой сущностью; это не код ошибки процесса.


def process_one_request(
    conn: pyodbc.Connection,  # Общее SQL-соединение текущего прогона.
    session: requests.Session,  # Общая HTTP Session текущего прогона.
    auth: dict[str, str],  # Неизменяемые в рамках прогона реквизиты API.
    request: JournalRequest,  # Ровно одна pending-строка журнала.
) -> bool:
    """Обработать одну запись журнала с внутренними повторами.

    Возвращает True при успехе и False, если по записи исчерпаны все попытки.
    Такой контракт нужен, чтобы процесс мог продолжить следующие записи журнала
    как старый Windows-процесс.
    """
    request_type_code = request_type_to_code(request.comment)  # Текст Comment переводится в код SQL-процедуры.
    group = method_group(request.code)  # Код метода определяет JSON/справочник/schema ветку.
    last_error = ""  # После неудачи здесь хранится диагностический текст последней попытки.

    log.info(
        "Start Cbonds request request_id=%s request_code=%s method_id=%s code=%s group=%s name=%s type=%s",
        request.request_id,
        request.request_code,
        request.method_id,
        request.code,
        request.group_name,
        request.name,
        request.comment,
    )
    log.info("Journal request payload: %s", request)

    # `+ 1` нужен потому, что правая граница range не включается. При лимите
    # 25 значения attempt будут от 1 до 25 включительно.
    for attempt in range(1, MAX_METHOD_RETRIES + 1):
        try:
            if group == 3:  # Schema endpoint использует GET и не имеет offset-пагинации.
                total = process_schema_method(conn, session, auth, request)
            else:
                total = process_json_method(conn, session, auth, request, group, request_type_code)  # Группы 1 и 2 используют POST JSON.

            write_success_to_sql(conn, request.process_name, total)  # Success-log пишется только после полного завершения метода.
            log.info(
                "Finished Cbonds request request_id=%s method=%s total=%s attempt=%s",
                request.request_id,
                request.code,
                total,
                attempt,
            )
            return True  # Булев результат позволяет внешнему циклу увеличить stats["success"].
        # Порядок except важен: сначала ловим узкий тип fatal-ошибки, затем
        # общий Exception. Иначе специальная обработка никогда не выполнится.
        except CbondsApiFatalError as exc:
            error_text = f"{exc.err_no}: {exc.err_str}"  # Сохраняем одновременно машинный код и сообщение API.
            log.error(
                "Fatal Cbonds API error request_id=%s method=%s err_no=%s: %s",
                request.request_id,
                request.code,
                exc.err_no,
                exc.err_str,
            )
            write_error_to_sql(conn, request.process_name, error_text)
            mark_request_failed(conn, request.request_id, error_text)
            return False  # Fatal-ошибка прекращает цикл попыток немедленно.
        except Exception as exc:  # Ловит остальные ошибки HTTP, JSON, SQL и Python внутри try-блока.
            last_error = (
                f"{attempt} попытка, Request_id={request.request_id}, "
                f"method={request.code}. Error occurred: {type(exc).__name__}: {exc}, "
                f"Time: {datetime.now()}"
            )
            log.exception(last_error)  # В отличие от log.error, добавляет текущий traceback.
            write_error_to_sql(conn, request.process_name, last_error)

            if attempt < MAX_METHOD_RETRIES:  # После последней попытки ждать уже не нужно.
                log.info(
                    "Retrying Cbonds request_id=%s method=%s in %s seconds (%s/%s)",
                    request.request_id,
                    request.code,
                    RETRY_DELAY_SECONDS,
                    attempt,
                    MAX_METHOD_RETRIES,
                )
                time.sleep(RETRY_DELAY_SECONDS)  # Текущий поток блокируется на заданное число секунд.
            else:
                log.error(
                    "Retry limit exceeded for Cbonds request_id=%s method=%s",
                    request.request_id,
                    request.code,
                )
                mark_request_failed(conn, request.request_id, last_error)
                return False  # Лимит исчерпан: внешняя статистика отметит запись failed.

    return False  # Защитный return для полноты контракта; при положительном лимите цикл завершает функцию раньше.


def run_cbonds_adhoc_journal() -> dict[str, int]:
    """Точка входа локального runner-а (и совместимая с Airflow task-ом по смыслу)."""
    begin = datetime.now()  # Один снимок времени используется для расчёта общей длительности прогона.
    date_request = datetime.now() - timedelta(hours=5)  # Историческое диагностическое значение; SELECT использует GETDATE() SQL Server.
    log.info("Начало загрузки Cbonds AdHoc: %s", begin)
    log.info("date_request=%s date_request_str=%s", date_request, date_request.strftime("%Y-%m-%d"))

    stats = {"found": 0, "success": 0, "failed": 0}  # Изменяемый dict накапливает три независимых счётчика.
    auth = cbonds_auth()  # Auth создаётся один раз и переиспользуется для всех записей.

    # Вложенные context manager отвечают за разные ресурсы: внешний закрывает
    # SQL-соединение, внутренний — HTTP Session и её пул TCP-соединений.
    with connect_mssql() as conn:
        pending_requests = fetch_pending_requests(conn)  # SQL выполняется один раз; новые строки во время прогона в список не попадут.
        stats["found"] = len(pending_requests)  # len возвращает число объектов JournalRequest в памяти.

        with requests.Session() as session:  # Session закрывается после обработки всего snapshot pending-записей.
            for request in pending_requests:  # Последовательная обработка: следующая запись ждёт завершения текущей.
                ok = process_one_request(conn, session, auth, request)  # ok имеет тип bool.
                if ok:  # В Python условие проверяет само булево значение, сравнение `ok == True` не требуется.
                    stats["success"] += 1  # `+= 1` читает текущее значение, прибавляет единицу и записывает обратно.
                else:
                    stats["failed"] += 1  # Ошибка одной записи учитывается отдельно при штатном возврате False.

    end = datetime.now()  # Фиксируем конец после закрытия HTTP и SQL context managers.
    log.info("Конец загрузки Cbonds AdHoc: %s, duration=%s, stats=%s", end, end - begin, stats)

    if FAIL_ON_METHOD_ERROR and stats["failed"]:  # Число failed > 0 трактуется как True.
        raise CbondsJournalError(f"Cbonds AdHoc finished with failed methods: {stats}")
    return stats  # Вызывающий код получает тот же dict со значениями found/success/failed.




def configure_console_logging() -> None:
    """Включить понятный вывод логов в консоль PyCharm."""
    logging.basicConfig(  # Настраивает корневой логгер, если приложение ещё не настроило его раньше.
        level=logging.INFO,  # Показываются INFO, WARNING, ERROR и CRITICAL; DEBUG скрыт.
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",  # Формат: время, уровень, имя модуля, сообщение.
    )


def main() -> int:
    """Собрать конфиг, прогнать журнальную загрузку, вернуть exit code."""
    # Без global присваивание создало бы локальную переменную _CFG только
    # внутри main(). Здесь намеренно меняется переменная уровня модуля.
    global _CFG

    configure_console_logging()  # Логи включаются до валидации, чтобы ранние ошибки были видны в консоли.
    cfg = resolve_runtime_config()  # Читаем env и CONFIG в один frozen-объект.
    validate_runtime_config(cfg)  # При недостающих обязательных значениях ValueError остановит запуск до сети/БД.
    log_runtime_config(cfg)  # Выводит адреса и режимы без паролей.
    _CFG = cfg  # После этого _require_cfg() внутри нижележащих функций начнёт возвращать активный конфиг.

    # finally выполняется и при успехе, и при исключении. Поэтому временная
    # глобальная конфигурация будет сброшена даже после аварийного завершения.
    try:
        stats = run_cbonds_adhoc_journal()  # Основная работа запускается только после успешной подготовки конфигурации.
    finally:
        _CFG = None  # Удаляем ссылку на конфиг, включая секреты, из модульного состояния после прогона.

    log.info("Local DEV AdHoc run finished with stats=%s", stats)
    if FAIL_ON_METHOD_ERROR and stats.get("failed"):  # dict.get вернёт None, если ключ неожиданно отсутствует.
        return 1  # Ненулевой exit code позволяет PyCharm/планировщику распознать ошибочный прогон.
    return 0  # Ноль означает штатное завершение процесса.


if __name__ == "__main__":
    # main() возвращает целое число. SystemExit передаёт его операционной
    # системе как exit code: 0 означает успех, ненулевое значение — ошибку.
    raise SystemExit(main())
