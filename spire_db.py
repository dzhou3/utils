import sqlite3, argparse
class SqliteDatabase:
    def __init__(self, fpath):
        self._conn = sqlite3.connect(fpath)
        self._conn.row_factory = sqlite3.Row
        self.cursor = self._conn.cursor()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.commit()
        self.cursor.close()
        self._conn.close()
    def execute(self, query):
        return self.cursor.execute(query)
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', action='store')
    args = parser.parse_args()
    print('query: -----> "{}"'.format(args.query))
    with SqliteDatabase('/opt/spire-controller/data/server/datastore.sqlite3') as db:
        res = db.execute(args.query).fetchall()
    headers = list(map(lambda x: x[0], db.cursor.description))
    col_width = [len(h) for h in headers]
    rows = [headers]
    for row in res:
        vals = [None] * len(headers)
        for i, val in enumerate(row):
            val = '' if val is None else str(val)
            col_width[i] = max(col_width[i], len(val))
            vals[i] = val
        rows.append(vals)
    for row in rows:
        print(' | '.join(v.center(col_width[i]) for i, v in enumerate(row)))
