from datetime import timedelta, datetime
import string
import random
import sys
import psycopg2
import itertools
import json


def main():
    dbName = sys.argv[1] if len(sys.argv) > 1 else sys.exit("Input db name")
    tableName = sys.argv[2] if len(sys.argv) > 2 else sys.exit("Input table name")
    num = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    conn = psycopg2.connect(database=dbName, user="postgres", password="pass", host="127.0.0.1", port="5432")

    # only od_purchase
    if conn:
        cursor = conn.cursor()
        rows = od_purchase(num, getLastId(cursor, "od_purchase"))
        insertData(cursor, "od_purchase", rows)
        conn.commit()
        # test
        cursor.execute("select * from od_purchase")
        rows = cursor.fetchall()
        for row in rows:
            print("id = ", row[0])
            print("datecommit = ", row[1])
            print("datecreate = ", row[2], "\n")
        conn.close()
    else:
        print("No connect")


def print_json():
    for x in itertools.count(-1):
        data = generateQuery(x)
        date_handler = lambda obj: (
            obj.isoformat()
            if isinstance(obj, datetime)
            else None
        )
        encoded = json.dumps(data, default=date_handler)
        sys.stdout.write(encoded)


def od_purchase(num, old_id, start=datetime(2016, 5, 1), end=datetime(2016, 5, 31)):
    result = []
    id = old_id
    for _ in range(num):
        id += 1
        result.append(generateQuery(id, start, end))
    return result


def generateQuery(id, start=datetime(2016, 5, 1), end=datetime(2016, 5, 31)):
    datecommit = randomDate(start, end)
    datecreate = randomDate(start, end)
    fiscaldocnum = randomString(64)
    numberfield = randomNumber(16)
    id_session = randomNumber(16)
    id_shift = randomNumber(16)
    checkstatus = random.randint(0, 2)
    checksumend = randomNumber(16)
    checksumstart = randomNumber(16)
    discountvaluetotal = randomNumber(16)
    operationtype = bool(random.getrandbits(1))
    receivedate = randomDate(start, end)
    id_purchaseref = randomNumber(16) if not receivedate else None
    set5checknumber = randomString(16) if not receivedate else None
    client_guid = randomNumber(64)
    clienttype = randomNumber(4)
    denyprinttodocuments = bool(random.getrandbits(1))

    return (id, datecommit, datecreate, fiscaldocnum, numberfield, id_session, id_shift,
            checkstatus, checksumend, checksumstart, discountvaluetotal, operationtype, receivedate,
            id_purchaseref, set5checknumber, client_guid, clienttype, denyprinttodocuments)


def insertData(cursor, table, rows):
    for row in rows:
        sql = "insert into " + table + " values (" + ",".join(["%s"]*len(row)) + ")"
        cursor.execute(sql, row)


def getLastId(cursor, table):
    cursor.execute("select id from " + table)
    rows = cursor.fetchall()
    return rows[len(rows) - 1][0] if rows else -1


def randomDate(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


def randomString(limit):
    return ''.join(random.choice(string.digits + string.ascii_letters)
                   for _ in range(random.randint(1, limit - 1), limit))


def randomNumber(limit):
    return random.randint(1, (2 ** limit) / 2)


if __name__ == "__main__":
    main()
