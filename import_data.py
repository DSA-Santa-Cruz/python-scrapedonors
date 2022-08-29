import glob
import pandas as pd
import re
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from dotenv import load_dotenv
import os

load_dotenv()
db_password = os.getenv("DB_PASSWORD")

if db_password:
    engine = create_engine(f"postgresql://postgres:{db_password}@/finances")
else:
    engine = create_engine("postgresql://postgres@/finances")

engine = create_engine("postgresql://postgres@/finances")
if not database_exists(engine.url):
    create_database(engine.url)

conn = engine.connect()

dry = False


def get_type(c):
    form_type = c["Form_Type"]
    if form_type == "A":
        return "monetary"
    elif form_type == "F497P1":
        return "monetary"
    elif form_type == "F496P3":
        return "monetary"
    elif form_type == "C":
        return "nonmonetary"
    elif form_type == "I":
        return "misc"
    return "unknown"


def get_donor_id(c):
    if not pd.isnull(c["Cmte_ID"]):
        return c["Cmte_ID"]
    elif not pd.isnull(c["donor_f_name"]):
        return re.sub(
            r"\W+",
            "",
            c["donor_l_name"] + c["donor_f_name"] + c["donor_zip"],
        )
    else:
        return re.sub(r"\W+", "", (c["donor_l_name"] + c["donor_zip"]))


def import_data(path):
    # open files with pandas
    datafiles = glob.glob(path)
    for path in datafiles:
        # 460 monetary contributions
        schedule_a = pd.read_excel(path, sheet_name="A-Contributions").set_index(
            "Tran_ID"
        )
        # 460 non-monetary contributions
        schedule_c = pd.read_excel(path, sheet_name="C-Contributions").set_index(
            "Tran_ID"
        )
        # 460 miscellaneous Increases to Cash
        schedule_i = pd.read_excel(path, sheet_name="I-Contributions").set_index(
            "Tran_ID"
        )

        # Contributions of $100 or More Received (not required)
        f496p3 = pd.read_excel(path, sheet_name="F496P3-Contributions").set_index(
            "Tran_ID"
        )
        # Expenditures Made
        f465p3 = pd.read_excel(path, sheet_name="F465P3-Expenditure").set_index(
            "Tran_ID"
        )
        # Expenditures Made
        f461p5 = pd.read_excel(path, sheet_name="F461P5-Expenditure").set_index(
            "Tran_ID"
        )

        # Summary of Expenditures Supporting/Opposing Other Candidates, Measures and Committees
        schedule_d = pd.read_excel(path, sheet_name="D-Expenditure").set_index(
            "Tran_ID"
        )
        # Payments Made
        schedule_e = pd.read_excel(path, sheet_name="E-Expenditure").set_index(
            "Tran_ID"
        )
        # Accrued Expenses (Unpaid Bills)
        schedule_f = pd.read_excel(path, sheet_name="F-Expenses").set_index("Tran_ID")
        # Payments Made by an Agent or Independent Contractor
        schedule_g = pd.read_excel(path, sheet_name="G-Expenditure").set_index(
            "Tran_ID"
        )

        # Loans Received / the loans themselves
        schedule_b1 = pd.read_excel(path, sheet_name="B1-Loans").set_index("Tran_ID")
        # Loan Guarantors / third party that co-signs loans
        schedule_b2 = pd.read_excel(path, sheet_name="B2-Loans").set_index("Tran_ID")
        # Loans Made to Others
        schedule_h = pd.read_excel(path, sheet_name="H-Loans").set_index("Tran_ID")

        # Summaries
        summary = pd.read_excel(path, sheet_name="Summary")

        # 24-hour large donations
        f497 = pd.read_excel(path, sheet_name="497").set_index("Tran_ID")
        # 24-hour large expenses
        f496 = pd.read_excel(path, sheet_name="496").set_index("Tran_ID")

        # combine contributions
        contributions = pd.concat(
            [
                f496p3,
                f497,
                schedule_a,
                schedule_c,
                schedule_i,
            ],
            axis=0,
        )
        contributions["amount"] = contributions.apply(
            lambda x: x["Amount"] if pd.isnull(x["Tran_Amt1"]) else x["Tran_Amt1"],
            axis=1,
        )
        contributions["date"] = contributions.apply(
            lambda x: x["Ctrib_Date"] if pd.isnull(x["Tran_Date"]) else x["Tran_Date"],
            axis=1,
        )
        contributions["reporter_id"] = contributions.apply(
            lambda x: str(x["Filer_ID"]),
            axis=1,
        )
        contributions["reporter_name"] = contributions["Filer_NamL"]
        contributions["report_date"] = contributions["Rpt_Date"]
        contributions["type"] = contributions.apply(
            lambda x: get_type(x),
            axis=1,
        )

        # Enty_NamL	Enty_NamF Enty_City	Enty_ST	Enty_Zip4 Ctrib_Emp	Ctrib_Occ
        # Tran_NamL	Tran_NamF Tran_City	Tran_State	Tran_Zip4	Tran_Emp	Tran_Occ

        contributions["code"] = contributions["Entity_Cd"]
        contributions["donor_l_name"] = contributions.apply(
            lambda x: x["Tran_NamL"] if pd.isnull(x["Enty_NamL"]) else x["Enty_NamL"],
            axis=1,
        )
        contributions["donor_f_name"] = contributions.apply(
            lambda x: x["Tran_NamF"] if pd.isnull(x["Enty_NamF"]) else x["Enty_NamF"],
            axis=1,
        )
        contributions["donor_city"] = contributions.apply(
            lambda x: x["Enty_City"] if pd.isnull(x["Tran_City"]) else x["Tran_City"],
            axis=1,
        )
        contributions["donor_state"] = contributions.apply(
            lambda x: x["Enty_ST"] if pd.isnull(x["Tran_State"]) else x["Tran_State"],
            axis=1,
        )
        contributions["donor_zip"] = contributions.apply(
            lambda x: str(
                str(
                    x["Enty_Zip4"] if pd.isnull(x["Tran_Zip4"]) else x["Tran_Zip4"]
                ).split(".")[0]
            ).zfill(5),
            axis=1,
        )
        contributions["donor_occupation"] = contributions.apply(
            lambda x: x["Ctrib_Occ"] if pd.isnull(x["Tran_Occ"]) else x["Tran_Occ"],
            axis=1,
        )
        contributions["donor_employer"] = contributions.apply(
            lambda x: x["Ctrib_Emp"] if pd.isnull(x["Tran_Emp"]) else x["Tran_Emp"],
            axis=1,
        )
        contributions["donor_id"] = contributions.apply(
            lambda x: get_donor_id(x),
            axis=1,
        )
        contributions = contributions[
            [
                "amount",
                "date",
                "report_date",
                "type",
                "code",
                "donor_l_name",
                "donor_f_name",
                "donor_city",
                "donor_state",
                "donor_zip",
                "donor_occupation",
                "donor_employer",
                "donor_id",
                "reporter_id",
                "reporter_name",
            ]
        ]

        contributions.index.names = ["id"]
        # dedupe contributions
        # get most recent contribution data as best data
        contributions.sort_values(by="date")
        contributions = contributions[~contributions.index.duplicated(keep="last")]

        donors = contributions.reset_index().set_index("donor_id")[
            [
                "donor_l_name",
                "donor_f_name",
                "donor_city",
                "donor_state",
                "donor_zip",
                "donor_occupation",
                "donor_employer",
            ]
        ]

        # dedup donors
        donors = donors[~donors.index.duplicated(keep="last")]

        committees = contributions.reset_index().set_index("reporter_id")[
            ["reporter_name"]
        ]
        committees = committees[~committees.index.duplicated(keep="last")]

        old_contributions = pd.read_sql_table("contributions", conn, index_col="id")
        old_donors = pd.read_sql_table("donors", conn, index_col="donor_id")
        old_committees = pd.read_sql_table("committees", conn, index_col="reporter_id")

        contributions = pd.concat([contributions, old_contributions]).drop_duplicates(
            keep="first"
        )
        donors = pd.concat([donors, old_donors]).drop_duplicates(keep="first")
        committees = pd.concat([committees, old_committees]).drop_duplicates(
            keep="first"
        )

        if not dry:
            contributions.to_sql("contributions", conn, if_exists="replace")
            donors.to_sql("donors", conn, if_exists="replace")
            committees.to_sql("committees", conn, if_exists="replace")
        else:
            print("dry")
