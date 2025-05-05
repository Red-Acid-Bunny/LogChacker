from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from collections import deque
import argparse
import re
import os
import sys
import tempfile
import shutil

class CustomDateTime(datetime):
    """Расширенный datetime с поддержкой %[N]f для микросекунд"""
    
    def custom_strftime(self, format_str: str) -> str:
        """
        Расширенный strftime с поддержкой:
        %f  - полные микросекунды (6 цифр)
        %3f - 3 цифры микросекунд (миллисекунды)
        %4f - 4 цифры и т.д.
        """
        def replace_micro(match):
            if match.group(1):  # Если есть число перед f (%3f, %4f и т.д.)
                digits = int(match.group(1))
                micro = str(self.microsecond).zfill(6)[:digits]
            else:  # Просто %f
                micro = str(self.microsecond).zfill(6)
            return micro
        
        # Заменяем все %[N]f в формате
        pattern = r'%(\d*)f'
        format_str = re.sub(pattern, replace_micro, format_str)
        
        # Стандартное форматирование для остальных директив
        return super().strftime(format_str)

    @classmethod
    def custom_strptime(cls, date_string: str, format_str: str) -> 'CustomDateTime':
        """
        Расширенная версия strptime с поддержкой формата %[N]f для обработки микросекунд.
        Убирает числовое значение перед символом 'f' и передает дальше стандартной обработке.
        """
        # Регулярка для замены всех вариантов %[N]f на %f
        cleaned_format = re.sub(r"%\d*f", "%f", format_str)
        
        # Используем стандартную обработку datetime.strptime
        dt = datetime.strptime(date_string, cleaned_format)
        
        # Возвращаем объект нашего класса
        return cls(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond)

DEFAULT_CACHE_PATH="/tmp/log_checker/"
DEFAULT_LASTTIME_NAME="lasttime.csv"
DEFAULT_FORMAT_CSVTIME="%s.%f"
DEFAULT_FORMAT_LOGTIME="%Y-%m-%d %H:%M:%S.%3f"
DEFAULT_PATTERN="err T"
DEFAULT_LIMIT_LINES=20
DEFAULT_CSV_DELIMITER=","

FORMAT_LOGTIME=""
PATTERN=""
LASTTIME_PATH=Path(".")
LASTTIME_PREFIX=""
LOCAL_CACHE_PATH=Path(".")
PATH_TO_LOGFILE=Path(".")
CACHE_PATH=Path(".")
ADDITIONAL_NAME=""
LOCAL_BUFFER_FILE=Path(".")
ENCODING="utf-8"
LAST_LOG_TIME=CustomDateTime.min
PATH_TO_BODY=Path(".")
LIMIT_LINES=20
CSV_DELIMITER = ","

def main():
    args = parse_arguments()

    init_global(args.lc_path_to_logfile, 
                args.additional_name, 
                args.csv_delimiter, 
                args.format_logtime, 
                args.pattern, 
                args.path_to_body, 
                args.full_path_to_body, 
                args.limit_lines
                )
    init_lasttime()
    last_time_unix=get_lasttime()
    print(f"{last_time_unix.custom_strftime(DEFAULT_FORMAT_CSVTIME)}", file=sys.stderr)
    print(f"{last_time_unix.custom_strftime(DEFAULT_FORMAT_LOGTIME)}", file=sys.stderr)
    grep_to_buffer()
    check_buffer_and_exit()
    last_logline = get_last_line(LOCAL_BUFFER_FILE)
    last_time_log = parse_log_time(last_logline)
    global LAST_LOG_TIME
    LAST_LOG_TIME = last_time_log
    print(f"{last_time_log.custom_strftime(DEFAULT_FORMAT_LOGTIME)}", file=sys.stderr)
    print(f"{last_time_unix.custom_strftime(DEFAULT_FORMAT_LOGTIME)}", file=sys.stderr)
    if last_time_log <= last_time_unix:
        print("INFO: Нет новый логов", file=sys.stderr)
        sys.exit(0)
    test = del_old_log_in_buffer_file()
    print(f"{test}", file=sys.stderr)

    # Обработка результата
    process_files()
    update_lasttime()


def init_global(path_to_logfile: str, 
                additional_name: str, 
                csv_delimiter: str, 
                format_logtime: str, 
                pattern: str, 
                path_to_body: str, 
                full_path_to_body: bool, 
                limit_lines: int
                ) -> None:
    """
    Инициализация глобальных переменных
    :param path_to_logfile: Путь к лог файлу
    :param additional_name: Суфикс для локального каталога
    :param csv_delimiter: Разделитель CSV-файла
    """
    global LIMIT_LINES
    LIMIT_LINES = limit_lines

    global FORMAT_LOGTIME
    FORMAT_LOGTIME = format_logtime

    global PATTERN
    PATTERN = pattern

    global PATH_TO_LOGFILE
    PATH_TO_LOGFILE = Path(path_to_logfile).resolve()
    
    global ADDITIONAL_NAME
    ADDITIONAL_NAME = additional_name

    global CSV_DELIMITER
    CSV_DELIMITER = csv_delimiter
    
    global LASTTIME_PREFIX
    LASTTIME_PREFIX = f"{PATH_TO_LOGFILE}{ADDITIONAL_NAME}{CSV_DELIMITER}"

    global CACHE_PATH
    CACHE_PATH = Path(DEFAULT_CACHE_PATH).resolve()
    Path(CACHE_PATH).mkdir(parents=True, exist_ok=True)

    global PATH_TO_BODY
    if full_path_to_body:
        PATH_TO_BODY = Path(path_to_body).resolve()
    else:
        PATH_TO_BODY = CACHE_PATH / Path(path_to_body)

    global LASTTIME_PATH
    LASTTIME_PATH = CACHE_PATH / Path(DEFAULT_LASTTIME_NAME)
    Path(LASTTIME_PATH).touch(exist_ok=True)

    global LOCAL_CACHE_PATH
    LOCAL_CACHE_PATH = CACHE_PATH / Path(PurePosixPath(PATH_TO_LOGFILE).name + f"{ADDITIONAL_NAME}")
    Path(LOCAL_CACHE_PATH).mkdir(parents=True, exist_ok=True)

    global LOCAL_BUFFER_FILE
    LOCAL_BUFFER_FILE = Path(LOCAL_CACHE_PATH) / Path("buffer")
    Path(LOCAL_BUFFER_FILE).touch(exist_ok=True)

    print(f"DEFAULT_CACHE_PATH: {DEFAULT_CACHE_PATH}", file=sys.stderr)
    print(f"DEFAULT_LASTTIME_NAME: {DEFAULT_LASTTIME_NAME}", file=sys.stderr)
    print(f"DEFAULT_FORMAT_CSVTIME: {DEFAULT_FORMAT_CSVTIME}", file=sys.stderr)
    print(f"DEFAULT_FORMAT_LOGTIME: {DEFAULT_FORMAT_LOGTIME}", file=sys.stderr)
    print(f"DEFAULT_PATTERN: {DEFAULT_PATTERN}", file=sys.stderr)
    print(f"PATTERN: {PATTERN}", file=sys.stderr)
    print(f"FORMAT_LOGTIME: {FORMAT_LOGTIME}", file=sys.stderr)
    print(f"LASTTIME_PATH: {LASTTIME_PATH}", file=sys.stderr)
    print(f"LASTTIME_PREFIX: {LASTTIME_PREFIX}", file=sys.stderr)
    print(f"LOCAL_CACHE_PATH: {LOCAL_CACHE_PATH}", file=sys.stderr)
    print(f"PATH_TO_LOGFILE: {PATH_TO_LOGFILE}", file=sys.stderr)
    print(f"CACHE_PATH: {CACHE_PATH}", file=sys.stderr)
    print(f"ADDITIONAL_NAME: {ADDITIONAL_NAME}", file=sys.stderr)
    print(f"LOCAL_BUFFER_FILE: {LOCAL_BUFFER_FILE}", file=sys.stderr)
    print(f"PATH_TO_BODY: {PATH_TO_BODY}", file=sys.stderr)
    

def init_lasttime() -> None:
    """
    Инициализация файла lasttime.csv
    """
    # Проверить наличие записи для текущего лог-файла
    entry_exists = False
    with open(LASTTIME_PATH, 'r+', encoding=ENCODING) as f:
        expected_prefix = f"{LASTTIME_PREFIX}"
        
        # Поиск существующей записи
        for line in f:
            if line.startswith(expected_prefix):
                entry_exists = True
                break
        
        # Добавить запись, если не найдена
        if not entry_exists:
            f.write(f"{expected_prefix}0.000001\n")

def parse_time_string(time_str) -> CustomDateTime:
    seconds_float = float(time_str)
    return CustomDateTime.fromtimestamp(seconds_float)

def update_lasttime() -> None:
    """
    Обновление значения времени в файле lasttime.csv
    """
    time_value = LAST_LOG_TIME.custom_strftime(DEFAULT_FORMAT_CSVTIME)
    lines = []
    updated = False

    # Чтение существующего содержимого и обновление строки
    with LASTTIME_PATH.open(mode="r+", encoding=ENCODING) as file:
        for line in file:
            if line.strip().startswith(LASTTIME_PREFIX):
                lines.append(f"{LASTTIME_PREFIX}{time_value}\n")
                updated = True
            else:
                lines.append(line)

        # Если строка не была обновлена — добавляем новую строку
        if not updated:
            lines.append(f"{LASTTIME_PREFIX}{time_value}\n")

    # Запись обратно в файл
    with LASTTIME_PATH.open(mode="w", encoding=ENCODING) as file:
        file.writelines(lines)
    
def process_files() -> None:
    current = LOCAL_BUFFER_FILE
    path_to_output = PATH_TO_BODY
    log_file_name = PATH_TO_LOGFILE
    limit_lines = LIMIT_LINES
    try:
        # Проверяем наличие исходных файлов
        if not current.exists():
            print(f"Ошибка: Файл '{current}' не найден.")
            exit(1)
        
        if not log_file_name.exists():
            print(f"Ошибка: Файл '{log_file_name}' не найден.")
            exit(1)

        # Создаем директорию для выходного файла, если её ещё нет
        path_to_output.parent.mkdir(parents=True, exist_ok=True)

        # Чтение нужного количества строк с конца файла
        with open(current, 'r', encoding=ENCODING) as input_file:
            # Подсчет общего количества строк
            total_lines = sum(1 for _ in input_file)
            input_file.seek(0)  # Возвращаемся в начало файла
            # Чтение последних limit_lines строк
            lines = deque(input_file, maxlen=limit_lines or None)

        # Вычисляем количество пропущенных строк
        if limit_lines is not None and limit_lines > 0:
            skipped_lines = max(0, total_lines - limit_lines)
        else:
            skipped_lines = 0
            
        # Запись результата в выходной файл
        with open(path_to_output, 'a', encoding=ENCODING) as output_file:
            # Заголовочная секция
            output_file.write(f"---\n")
            output_file.write(f"File name: {log_file_name}\n")
            output_file.write(f"Skipped lines: {skipped_lines}\n")
            output_file.write(f"ADDITIONAL_NAME: {ADDITIONAL_NAME}\n")
            output_file.write(f"PATTERN: {PATTERN}\n")

            output_file.write(f"\n")
        
            # Кладём строки в файл
            for line in lines:
                output_file.write(line)
                
            # Завершающая секция
            output_file.write("---\n\n")
    
        print("Файлы успешно обработаны!")
    
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        exit(1)

def get_lasttime() -> CustomDateTime:
    """
    Получение последней сохраненной метки времени из CSV-файла
    :return: Значение временной метки
    """
    time_str='0.0'
    try:
        with open(LASTTIME_PATH, 'r', encoding=ENCODING) as f:
            search_prefix = f"{LASTTIME_PREFIX}"
            
            for line in f:
                if line.startswith(search_prefix):
                    # Разделить строку и взять второе значение
                    time_str = line.strip().split(CSV_DELIMITER)[1]
                    
    except FileNotFoundError:
        pass
    return parse_time_string(time_str)

def del_old_log_in_buffer_file() -> int:
    """
    Определяет стартовую позицию для обработки новых записей в лог-файле.
    
    Args:
        last_csv_time: Последняя обработанная временная метка в CSV-формате
    
    Returns:
        Номер строки (1-based), с которой нужно начать обработку
    
    Raises:
        FileNotFoundError: Если файл не существует
        ValueError: Если время не может быть конвертировано
    """
    # Конвертация CSV-времени в формат лога
    str_last_log_time = LAST_LOG_TIME.custom_strftime(DEFAULT_FORMAT_LOGTIME)
    print(f"str_last_log_time: {str_last_log_time}", file=sys.stderr)
    found = False
    start_line = 1
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)
            
            with LOCAL_BUFFER_FILE.open('r', encoding=ENCODING) as src:
                for line_num, line in enumerate(src, start=1):
                    if not found and str_last_log_time in line:
                        found = True
                        start_line = line_num + 1
                        continue
                    
                    if found or not found:
                        tmp_file.write(line)
            
            # Заменяем исходный файл временным
            tmp_path.replace(LOCAL_BUFFER_FILE)
            
    except Exception as e:
        if 'tmp_path' in locals() and tmp_path.exists():
            tmp_path.unlink()
        raise
    
    return start_line

def grep_to_buffer() -> None:
    """
    Аналог grep -P с сохранением результатов в файл
    """
    try:
        # Проверка существования файла
        if not Path(PATH_TO_LOGFILE).is_file():
            raise FileNotFoundError(f"Файл '{PATH_TO_LOGFILE}' не найден")

        # Компиляция регулярного выражения
        regex = re.compile(pattern=PATTERN)

        # Чтение и фильтрация строк
        with open(PATH_TO_LOGFILE, "r", encoding=ENCODING) as src:
            matched_lines = [line for line in src if regex.search(line)]

        # Запись результатов
        with open(LOCAL_BUFFER_FILE, "w", encoding=ENCODING) as dst:
            #print(f"{matched_lines}", file=sys.stderr)
            dst.writelines(matched_lines)

    except re.error as e:
        raise ValueError(f"Ошибка в регулярном выражении: {e}") from e
    except IOError as e:
        raise RuntimeError(f"Ошибка ввода-вывода: {e}") from e

def parse_time(input_time: str) -> CustomDateTime:
    """Парсит строку времени в объект CustomDateTime"""
    try:
        return CustomDateTime.custom_strptime(input_time, FORMAT_LOGTIME)
    except ValueError as e:
        print(f"ERROR: Неверный формат времени '{input_time}'. Ожидается: {FORMAT_LOGTIME}", file=sys.stderr)
        print(f"{e}", file=sys.stderr)
        sys.exit(1)

def parse_log_time(log_line: str) -> CustomDateTime:
    """
    Извлекает временную метку из строки лога согласно формату.
    
    Args:
        log_line: Строка лога для анализа
    
    Returns:
        Объект CustomDateTime с временной меткой
    
    В случае ошибки выводит сообщение и завершает программу с кодом 1
    """
    if not log_line:
        print("ERROR: Пустая строка лога", file=sys.stderr)
        sys.exit(1)

    try:
        # Нормализация формата (удаление лишних пробелов)
        normalized_format = ' '.join(FORMAT_LOGTIME.split())
        segments = len(normalized_format.split())
        
        # Извлечение временной метки из строки лога
        parts = log_line.split(maxsplit=segments)
        if len(parts) < segments:
            raise ValueError(f"Недостаточно сегментов в строке лога. Ожидается {segments}, получено {len(parts)}")
        
        time_stamp = ' '.join(parts[:segments])
        return parse_time(time_stamp)
        
    except Exception as e:
        print(f"ERROR: Не удалось извлечь время из лога: {str(e)}", file=sys.stderr)
        print(f"Строка лога: '{log_line[:100]}...'", file=sys.stderr)  # Выводим первые 100 символов для отладки
        sys.exit(1)


def get_last_line(file_path: Path) -> str:
    """Возвращает последнюю строку файла (аналог tail -n1)"""
    try:
        with file_path.open('r', encoding=ENCODING) as f:
            # Читаем файл с конца для эффективности
            for line in f:
                pass  # Пропускаем все строки до последней
            return line.strip() if line else ""
    except FileNotFoundError:
        return ""

def check_buffer_and_exit() -> None:
    """
    Проверяет, что файл существует и не пуст.
    Если условие не выполняется — выводит сообщение и завершает программу.
    """
    is_empty = not LOCAL_BUFFER_FILE.exists() or LOCAL_BUFFER_FILE.stat().st_size == 0
    if is_empty:
        print(f"Файл LOCAL_BUFFER_FILE: {LOCAL_BUFFER_FILE} пустой", file=sys.stderr)
        sys.exit(0)

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Анализатор логов (Python-версия)",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Обязательные параметры
    parser.add_argument(
        '-l',"--lc-path-to-logfile",
        type=validate_file,
        required=True,
        help="Путь к анализируемому лог-файлу (ОБЯЗАТЕЛЬНЫЙ ПАРАМЕТР).\n"
             "Можно использовать относительный путь, он будет преобразован в полный."
    )

    parser.add_argument(
        "--additional-name",
        default="",
        help="Суфикс для записи в lasttime. Позволяет обработать один лог файл несколько раз.\n"
             "Для каждой отдельной обработки задавать новый суфикс.\n"
             "На основе этого сификса будет создан каталог, по этому можно использовать только цифры, буквы и символы: '_', '-', '.'."
    )

    # Параметры с дефолтными значениями
    parser.add_argument(
        "--pattern",
        type=validate_regex,
        default=DEFAULT_PATTERN,
        help='Регулярное выражение для поиска (default: "err T")\n'
             "Может быть задано несколько шаблонов разделенных символом '|'\n"
             "Синтаксис регулярных выражений смотреть в документации к Python библиотеке re\n"
             'Пример: --lc-trigger-pattern "inf T|err T|wrn T"'
    )

    parser.add_argument(
        "--full-path-to-body",
        action="store_true",
        help='Если флаг задан, то путь к выходному файлу будет интерпретирован как полный путь к файлу.\n'
             'По умолчание путь задается относительно каталога с кешем.'
    )

    parser.add_argument(
        "--path-to-body",
        default=f"body",
        help="Путь к выходному файлу. В него будет записан результат выполнения.\n" 
             "Если не задан флаг '--relative-path-to-body'"
    )

    # TODO: описание
    parser.add_argument(
        "--cache-path",
        default=f"{DEFAULT_CACHE_PATH}",
        help=f"Путь к папке с кешем. Default: '{DEFAULT_CACHE_PATH}'\n"
              "В этой папке хранятся прошлые последние метки времени,\n" 
              "буферные файлы для кождого лога,\n"
              "выходной файл\n"
    )

    # TODO: описание
    parser.add_argument(
        "--csv-delimiter",
        default=DEFAULT_CSV_DELIMITER,
        help=f"Разделитель ячеек в csv. Default: '{DEFAULT_CSV_DELIMITER}'"
    )

    # TODO: описание
    parser.add_argument(
        "--format-logtime",
        default=f"{DEFAULT_FORMAT_LOGTIME}",
        help='Формат времени в лог-файле.\n'
             'Формат смотреть в документации к Python библиотеке datetime.\n'
        "Directive\t|Meaning                                                                  \t|Example\n"
        "\n"
        "`%%a`    \t|Weekday as locale’s abbreviated name.                                    \t|Sun\n"
        "`%%A`    \t|Weekday as locale’s full name.                                           \t|Sunday\n"
        "`%%w`    \t|Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.        \t|0, 1, …, 6\n"
        "`%%d`    \t|Day of the month as a zero-padded decimal number.                        \t|01, 02, …, 31\n"
        "`%%b`    \t|Month as locale’s abbreviated name.                                      \t|Jan\n"
        "`%%B`    \t|Month as locale’s full name.                                             \t|January\n"
        "`%%m`    \t|Month as a zero-padded decimal number.                                   \t|01, 02, …, 12\n"
        "`%%y`    \t|Year without century as a zero-padded decimal number.                    \t|00, 01, …, 99\n"
        "`%%Y`    \t|Year with century as a decimal number.                                   \t|0001\n"
        "`%%H`    \t|Hour (24-hour clock) as a zero-padded decimal number.                    \t|00, 01, …, 23\n"
        "`%%I`    \t|Hour (12-hour clock) as a zero-padded decimal number.                    \t|01, 02, …, 12\n"
        "`%%p`    \t|Locale’s equivalent of either AM or PM.                                  \t|AM, PM (en\_US);  am, pm (de\_DE)\n"
        "`%%M`    \t|Minute as a zero-padded decimal number.                                  \t|00, 01, …, 59\n"
        "`%%S`    \t|Second as a zero-padded decimal number.                                  \t|00, 01, …, 59\n"
        "`%%f`    \t|Microsecond as a decimal number, zero-padded to 6 digits.                \t|000000, 000001, …, 999999\n"
        "`%%[N]f` \t|Тоже самое что и `%%f` но с возмжностью ограничивать количество символов.\t|%%f = 123456, %%3f = 123\n"
        "`%%z`    \t|Смотреть в документации datetime                                         \t|(empty), +0000, -0400\n"
        "`%%Z`    \t|Time zone name (empty string if the object is naive).                    \t|(empty), UTC, GMT\n"
        "`%%j`    \t|Day of the year as a zero-padded decimal number.                         \t|001, 002, …, 366\n"
        "`%%U`    \t|Смотреть в документации datetime                                         \t|00, 01, …, 53\n"
        "`%%W`    \t|Смотреть в документации datetime                                         \t|00, 01, …, 53\n"
        "`%%c`    \t|Locale’s appropriate date and time representation.                       \t|Tue Aug 16 21:30:00 1988 (en\_US)\n"
        "`%%x`    \t|Locale’s appropriate date representation.                                \t|08/16/88 (None)\n"
        "`%%X`    \t|Locale’s appropriate time representation.                                \t|21:30:00 (en\_US)\n"
        "`%%%%` \t\t|A literal `'%%'` character.\n"
    )

    # Числовые параметры
    parser.add_argument(
        "--limit-lines",
        type=int,
        default=DEFAULT_LIMIT_LINES,
        help="Лимит строк в отчете (0 = без ограничений).\n"
        f"Default: {DEFAULT_LIMIT_LINES}"
    )

    return parser.parse_args()

def validate_file(path):
    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"Файл '{path}' не существует!")
    return path

def validate_regex(pattern):
    try:
        re.compile(pattern)  # Проверка корректности регулярки
        return pattern
    except re.error:
        raise argparse.ArgumentTypeError(f"Некорректное регулярное выражение: '{pattern}'")

if __name__ == "__main__":
    main()
