import argparse
import sqlite3

def main(args):
    db_filepath:str=args.db_filepath
    query:str=args.query

    with sqlite3.connect(db_filepath) as conn:
        cur=conn.cursor()
        
        cur.execute(query)
        for row in cur:
            print(row)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--db-filepath",type=str)
    parser.add_argument("-q","--query",type=str)
    args=parser.parse_args()

    main(args)
