import time


def tracking_timer(p_conn, etag, variablename, tstart=time.time()):

    if str(variablename).startswith('s'):
        diff = 0
        whichmarker = str(variablename).replace('s_','d_')
    else:
        enddiff = time.time()
        diff =  enddiff - tstart
        whichmarker = str(variablename).replace('e_','d_')

    with p_conn.cursor() as cur:
        sql = f"UPDATE tracking SET {variablename}=NOW(), {whichmarker}={diff} WHERE id = '{etag}' "
        cur.execute(sql)
    p_conn.commit()
    return time.time()

def tracking_data(p_conn, etag, variablename, data):
    with p_conn.cursor() as cur:
            sql = f"UPDATE tracking SET {variablename}={data} WHERE id = '{etag}' "
            cur.execute(sql)
    p_conn.commit()