from psycopg2 import connect
from datetime import datetime


def set_lock(year: int, quarter: int, user: str, pwd: str):
    conn = connect(dbname='2023_plan_Dontsov', user=user, password=pwd, host='localhost')
    quarter_id = str(year) + '.' + str(quarter)
    modified_dttm = datetime.now()

    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE plan_status
            SET status = 'L', modifieddatetime = '{modified_dttm}'::timestamp, author = current_user
            WHERE 1=1
                and country in (SELECT country FROM country_managers WHERE username = current_user)
                and quarterid = {quarter_id}::varchar;
        """)

    conn.commit()


def remove_lock(year: int, quarter: int, user: str, pwd: str):
    conn = connect(dbname='2023_plan_Dontsov', user=user, password=pwd, host='localhost')
    quarter_id = str(year) + '.' + str(quarter)
    modified_dttm = datetime.now()

    with conn.cursor() as cur:
        cur.execute(f"""
                UPDATE plan_status
                SET status = 'R', modifieddatetime = '{modified_dttm}'::timestamp, author = current_user
                WHERE 1=1
                    and country in (SELECT country FROM country_managers WHERE username = current_user)
                    and quarterid = {quarter_id}::varchar;
            """)

    conn.commit()


if __name__ == '__main__':
    # set_lock(2014, 1, 'kirill', 'kirill')
    # set_lock(2014, 1, 'sophie', 'sophie')

    remove_lock(2014, 1, 'kirill', 'kirill')
    remove_lock(2014, 1, 'sophie', 'sophie')
