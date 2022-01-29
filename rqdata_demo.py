from rqdatac import *
import rqdatac as rq


if __name__ == '__main__':
    rq.init('17512081226','lhjmj666')

    df = rq.get_price('IF2102',start_date=20180101,end_date=20210101);
    print(df.head())
    print(futures.get_contracts('IF','20210203'))