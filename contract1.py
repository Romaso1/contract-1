import csv
import logging
import threading


class FileReader(threading.Thread):
    def __init__(self, filename, column_index):
        super().__init__()
        self.filename = filename
        self.column_index = column_index
        self.values = []  #додаєм список для збереження значень
        self.average = 0  #ініціалізація змінної 0

    def run(self):
        try:
            with open(self.filename, "r") as f:
                reader = csv.reader(f)
                next(reader)  #пропускаєм заголовок
                total = 0
                count = 0
                for row in reader:
                    if len(row) >= self.column_index + 1:
                        value = row[self.column_index]
                        try:
                            value = float(value)
                        except ValueError:
                            logging.error("Помилка при обробці файлу %s: невірне значення у колонці %d", self.filename, self.column_index)
                            continue
                        total += value
                        count += 1
                        self.values.append(value)
                if count == 0:
                    logging.error("Помилка при обробці файлу %s: відсутні дані у колонці %d", self.filename, self.column_index)
                else:
                    self.average = total / count
        except Exception as e:
            logging.error("Помилка при обробці файлу %s: %s", self.filename, e)


def main():
    logging.basicConfig(level=logging.INFO)
    filenames = ["data1.csv", "data2.csv", "data3.csv"]
    column_indexes = [2, 1, 0]  #номери колонок серендього значення

    #робиться список потоків
    threads = []
    for filename, column_index in zip(filenames, column_indexes):
        thread = FileReader(filename, column_index)
        thread.start()
        threads.append(thread)

    # очікування завершення потоків
    for thread in threads:
        thread.join()

    # вивід середнього значення для файлів
    for thread in threads:
        if thread.average is not None:
            logging.info("Середнє значення для файлу %s: %f", thread.filename, thread.average)
        else:
            logging.info("Середнє значення для файлу %s: відсутнє", thread.filename)

    #максимальне середнє значення
    max_average = None
    for thread in threads:
        if thread.average is not None and (max_average is None or thread.average > max_average):
            max_average = thread.average
            max_filename = thread.filename

    logging.info("Файл з максимальним середнім значенням: %s", max_filename)


if __name__ == "__main__":
    main()
