import time


def tracking_timer(p_conn, etag, variable_name, tstart=time.time()):
    if str(variable_name).startswith('s'):
        diff = 0
        which_marker = str(variable_name).replace('s_', 'd_')
    else:
        enddiff = time.time()
        diff = enddiff - tstart
        which_marker = str(variable_name).replace('e_', 'd_')

    with p_conn.cursor() as cur:
        sql = f"""
        UPDATE tracking
        SET {variable_name}=NOW(), {which_marker}={diff}
        WHERE id = '{etag}'
        """
        cur.execute(sql)
    p_conn.commit()
    return time.time()


def tracking_data(p_conn, etag, variable_name, data):
    with p_conn.cursor() as cur:
        sql = f"""
        UPDATE tracking
        SET {variable_name}={data}
        WHERE id = '{etag}'
        """
        cur.execute(sql)
    p_conn.commit()


def tracking_data_str(p_conn, etag, variable_name, data):
    with p_conn.cursor() as cur:
        sql = f"""
        UPDATE tracking
        SET {variable_name}='{data}'
        WHERE id = '{etag}'
        """
        print(sql)
        cur.execute(sql)
    p_conn.commit()
