import csv
from typing import List
from openpyxl import load_workbook


def get_individual_csv_file_name(xlsx_name: str, sheet_name: str) -> str:
    """
    From the original .xlsx file and the sheet name, produce the individual
    sheet's file name

    Parameters
    ----------
    xlsx_name : str
        Name of the .xlsx file to split
    sheet_name : str
        Name of the sheet to split out

    Returns
    -------
    str
        The file name of the individual sheet
    """
    return xlsx_name.replace(
        ".xlsx",
        f"_{sheet_name.replace(' ', '-').lower()}.csv")


def split_xlsx_to_csv(xlsx_path: str, xlsx_name: str,
                      sheet_names: List[str] = [], **kwargs) -> List[str]:
    """
    Function to split a .xlsx spreadsheet to individual .csv files

    Parameters
    ----------
    xlsx_path : str
        Folder paths that the .xlsx is held in. New .csv files will be created
        here as well
    xlsx_name : str
        Name of the .xlsx file to split
    sheet_names : List[str], optional
        A list of sheet names to create, by default []. If not passed, all
        sheets will be created

    Returns
    -------
    dict
        The names of the sheets that were created, and their file locations
    """

    wb = load_workbook(filename=xlsx_path + "/" + xlsx_name)

    produced_sheets = {}
    for sheet in wb.worksheets:

        # If we have specified sheets to extract, then limit to only those
        if sheet_names and sheet.title not in sheet_names:
            continue

        # Create new file for each sheet
        new_filename = get_individual_csv_file_name(xlsx_name, sheet.title)
        sheet_path = xlsx_path + "/" + new_filename
        with open(sheet_path, "w") as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=",", **kwargs)
            for row in sheet.rows:
                spamwriter.writerow([cell.value for cell in row])

        produced_sheets[sheet.title] = sheet_path

    return produced_sheets
