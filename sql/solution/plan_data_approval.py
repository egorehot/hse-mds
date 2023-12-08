from psycopg2 import connect
from datetime import datetime


def accept_plan(year: int, quarter: int, user: str, pwd: str):
    conn = connect(dbname='2023_plan_Dontsov', user=user, password=pwd, host='localhost')
    quarter_id = str(year) + '.' + str(quarter)

    # Clear the A version of plan data for specific quarter and countries accessible to the current user
    with conn.cursor() as cur:
        cur.execute(f"""DELETE FROM plan_data WHERE quarterid = {quarter_id}::varchar and versionid = 'A'
                        and country in (SELECT country FROM country_managers WHERE username = current_user);""")

    # Read data available to the current user from the version P and save its copy as the version A
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO plan_data (versionid, country, quarterid, pcid, salesamt)
            SELECT
                'A' versionid,
                country,
                quarterid,
                pcid,
                salesamt
            FROM plan_data
            WHERE versionid = 'P' and quarterid = {quarter_id}::varchar
                and country in (SELECT country FROM country_managers WHERE username = current_user);
        """)

    #  Change the status of the processed from 'R' to 'A'
    #  When updating the status, also save a timestamp in the modifiedtimestamp column
    modified_dttm = datetime.now()
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE plan_status SET status = 'A', modifieddatetime = '{modified_dttm}'::timestamp, author = current_user
            WHERE country in (SELECT country FROM country_managers WHERE username = current_user)
                and quarterid = {quarter_id}::varchar;
        """)

    conn.commit()


if __name__ == '__main__':
    accept_plan(2014, 1, 'kirill', 'kirill')
    accept_plan(2014, 1, 'sophie', 'sophie')
