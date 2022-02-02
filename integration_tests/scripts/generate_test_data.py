import csv
from datetime import datetime, timedelta
from random import choice, randint, uniform

folder = "integration_tests/data/"


def iterate_number(start_number):
    return abs(start_number + uniform(-2, 2))


def generate_row(initial_date=datetime(2017, 1, 1),
                 last_date=datetime(2021, 1, 1)):

    curr_date = initial_date
    curr_open, curr_close = 100, 100
    while curr_date < last_date:

        start_time = curr_date + timedelta(hours=9)
        end_time = curr_date + timedelta(hours=17)

        curr_open = iterate_number(curr_close)
        curr_close = iterate_number(curr_open)

        yield {
            "date": curr_date.strftime("%Y-%m-%d"),
            "open": curr_open,
            "close": curr_close,
            "volume": randint(0, 10000),
            "height": start_time + timedelta(
                seconds=randint(0, int(
                    (end_time - start_time).total_seconds()
                    ))
                ),
            "isGood": choice([True, False])
        }

        curr_date += timedelta(days=1)


field_names = ["date", "open", "close", "volume", "height", "isGood"]

with open(folder + "0file.csv", "w") as file0:
    writer = csv.DictWriter(file0, fieldnames=field_names)
    writer.writeheader()

    for row in generate_row():
        writer.writerow(row)

with \
        open(folder + "1file.csv", "w") as file1, \
        open(folder + "1file-clean.csv", "w") as file1_clean, \
        open(folder + "1problems.csv", "w") as problems1:
    writer = csv.DictWriter(file1, fieldnames=field_names)
    writer_clean = csv.DictWriter(file1_clean, fieldnames=field_names)
    problems = csv.DictWriter(problems1, fieldnames=field_names)

    writer.writeheader()
    writer_clean.writeheader()
    problems.writeheader()

    for row in generate_row():

        if randint(1, 100) == 1:
            column = randint(1, 5)

            if column == 1:
                row["open"] = "abc"
            elif column == 2:
                row["close"] = "def"
            elif column == 3:
                row["volume"] = True
            elif column == 4:
                row["height"] = 6
            elif column == 5:
                row["isGood"] = ""

            problems.writerow(row)
        else:
            writer_clean.writerow(row)

        writer.writerow(row)
