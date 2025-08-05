import heapq
import os
import tempfile
import urllib.request
import zipfile
import csv
import gzip

def external_merge_sort(input_file, output_file, M, B):
    """
    Сортировка слиянием во внешней памяти.
    :param input_file: Входной файл с числами (по одному на строке).
    :param output_file: Выходной файл.
    :param M: Размер внутренней памяти (в элементах).
    :param B: Размер блока (в элементах).
    """
    # Шаг 1: Создание отсортированных runs
    runs = []
    with open(input_file, 'r') as fin:
        while True:
            chunk = []
            for _ in range(M):
                line = fin.readline().strip()
                if not line:
                    break
                chunk.append(int(line))
            if not chunk:
                break
            chunk.sort()
            with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp:
                for num in chunk:
                    temp.write(f"{num}\n")
                runs.append(temp.name)

    # Шаг 2: Многопутевое слияние
    while len(runs) > 1:
        new_runs = []
        for i in range(0, len(runs), M // B):
            merge_group = runs[i:i + M // B]
            if len(merge_group) > 1:
                merged = merge_files(merge_group, B)
                new_runs.append(merged)
            else:
                new_runs.append(merge_group[0])
        runs = new_runs

    # Переименовываем финальный run в output_file
    os.rename(runs[0], output_file)

def merge_files(files, B):
    """
    Слияние нескольких файлов с использованием heap.
    """
    heap = []
    fds = [open(f, 'r') for f in files]
    buffers = [[] for _ in files]  # Буферы по B элементов

    # Инициализация heap первыми элементами
    for idx, fd in enumerate(fds):
        load_buffer(buffers, idx, fd, B)
        if buffers[idx]:
            heapq.heappush(heap, (buffers[idx].pop(0), idx))

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as out:
        while heap:
            val, idx = heapq.heappop(heap)
            out.write(f"{val}\n")
            if not buffers[idx]:
                load_buffer(buffers, idx, fds[idx], B)
            if buffers[idx]:
                heapq.heappush(heap, (buffers[idx].pop(0), idx))

    for fd in fds:
        fd.close()
    for f in files:
        os.remove(f)
    return out.name

def load_buffer(buffers, idx, fd, B):
    """Загрузка блока B элементов в буфер."""
    buffers[idx] = []
    for _ in range(B):
        line = fd.readline().strip()
        if not line:
            break
        buffers[idx].append(int(line))

def prepare_input_file(input_filename='input.txt'):
    if os.path.exists(input_filename):
        print(f"Файл {input_filename} уже существует. Используем его.")
        return
    print(f"Файл {input_filename} не найден. Скачиваем датасет Covertype с UCI...")

    zip_url = 'https://archive.ics.uci.edu/static/public/31/covertype.zip'
    zip_path = 'covertype.zip'
    data_path = 'covtype.data.gz'

    # Скачивание ZIP
    urllib.request.urlretrieve(zip_url, zip_path)
    print("ZIP скачан.")

    # Разархивирование
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall('.')
    print("ZIP разархивирован.")

    # Обработка данных: Берем первую колонку (elevation) и пишем в input.txt
    with gzip.open(data_path, 'rt') as csvfile, open(input_filename, 'w') as outfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if row:  # Пропускаем пустые строки
                elevation = row[0]  # Первая колонка — целое число
                outfile.write(f"{elevation}\n")
    print(f"Данные из первой колонки сохранены в {input_filename}.")

    # Очистка: Удаляем временные файлы
    os.remove(zip_path)
    os.remove(data_path)
    if os.path.exists('covtype.info'):
        os.remove('covtype.info')
    if os.path.exists('old_covtype.info'):
        os.remove('old_covtype.info')

# Основной запуск
prepare_input_file()
external_merge_sort('input.txt', 'output.txt', M=100000, B=1000)  # Подстройте M и B под вашу систему

# Проверка результата (опционально)
with open('output.txt', 'r') as f:
    sorted_data = [int(line.strip()) for line in f]
    print("Первые 10 отсортированных элементов:", sorted_data[:10])
    print("Последние 10 отсортированных элементов:", sorted_data[-10:])
    print("Общее количество элементов:", len(sorted_data))